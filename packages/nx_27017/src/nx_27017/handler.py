"""NeoSQLite handler for MongoDB commands and SQLite operations."""

import functools
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from itertools import count
from typing import Any

from neosqlite import Connection
from nx_27017.changestream import (
    ChangeStreamManager,
    extract_change_stream_options,
    is_change_stream_pipeline,
)
from nx_27017.gridfs_adapter import (
    _get_gridfs_bucket_name,
    _is_gridfs_collection,
    create_gridfs_adapter,
)
from nx_27017.wire_protocol import (
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_SESSION_TIMEOUT_MINUTES,
    MAX_BSON_DOCUMENT_SIZE,
    MAX_MESSAGE_SIZE_BYTES,
    MAX_WIRE_VERSION,
    MAX_WRITE_BATCH_SIZE,
    MIN_WIRE_VERSION,
    _extract_session_id,
)

logger = logging.getLogger("nx_27017")


# Logical database names are mapped to SQLite files, so they must be
# filesystem-safe. MongoDB allows a wider charset; NX restricts it here
# and returns ok:0 for anything else (see _sanitize_db_name).
_DB_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")

# Data-cursor ids live far above ChangeStreamCursor ids (which start at
# 1000) so getMore/killCursors can tell the stores apart by lookup order.
_data_cursor_id_counter = count(10**12)

# Default first-batch size when the client sends no batchSize (matches the
# MongoDB server default of up to 101 documents in the first batch).
_DEFAULT_BATCH_SIZE = 101


def _search_index_fields(spec: dict[str, Any]) -> list[str]:
    """Extract FTS field names from an Atlas-style search index spec.

    Understands ``definition.mappings.fields`` maps, ``definition.fields``
    lists, ``definition.field`` / plain-string definitions, and falls back
    to the spec ``name``. Raises ValueError when no field is derivable.
    """
    definition = spec.get("definition", {})
    if isinstance(definition, str):
        return [definition]
    if isinstance(definition, dict):
        mappings = definition.get("mappings")
        if isinstance(mappings, dict):
            fields = mappings.get("fields")
            if isinstance(fields, dict) and fields:
                return [str(field) for field in fields]
        fields = definition.get("fields")
        if isinstance(fields, (list, tuple)) and fields:
            return [str(field) for field in fields]
        field = definition.get("field")
        if isinstance(field, str) and field:
            return [field]
    name = spec.get("name")
    if isinstance(name, str) and name:
        return [name]
    raise ValueError(f"No text field derivable from {spec!r}")


def _bulk_write_error(exc: Exception, index: int) -> dict[str, Any]:
    """Build a MongoDB-style writeError document for a failed bulk op."""
    from neosqlite._sqlite import sqlite3

    message = str(exc)
    if "UNIQUE" in message.upper() or isinstance(
        exc, sqlite3.IntegrityError
    ):
        code = 11000
    else:
        code = 8
    return {"index": index, "code": code, "errmsg": message}


def _sanitize_db_name(db_name: Any) -> str:
    """Validate a logical database name for file mapping.

    Raises:
        ValueError: If the name is not a safe file stem.
    """
    if not isinstance(db_name, str) or not _DB_NAME_RE.match(db_name):
        raise ValueError(f"Invalid database name: {db_name!r}")
    return db_name


def _msg_db_name(msg: Any) -> str:
    """Best-effort logical db name from a wire message (lock selection only).

    Mirrors the defaults used by the handlers themselves ($db -> "test"
    for OP_MSG commands, "db" -> "admin" for legacy OP_QUERY).
    """
    try:
        if isinstance(msg, dict):
            sections = msg.get("sections")
            if sections:
                for section_type, doc in sections:
                    if section_type == "body" and isinstance(doc, dict):
                        db_val = doc.get("$db", "test")
                        if isinstance(db_val, str) and db_val:
                            return db_val
                        return "test"
                return "test"
            if "collection" in msg or "query" in msg:
                db_val = msg.get("db", "admin")
                if isinstance(db_val, str) and db_val:
                    return db_val
                return "admin"
    except Exception:
        pass
    return "test"


def _serialize(func):
    """Serialize access to the SQLite connection(s).

    Single-file (legacy) mode uses one global RLock, as before. Multi-DB
    mode uses one RLock per logical database so different files can proceed
    in parallel. Lock ordering: per-db lock -> _dict_lock -> _sessions_lock.
    Never acquire a per-db lock while holding _dict_lock.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        msg = args[0] if args else {}
        db_name = _msg_db_name(msg) if isinstance(msg, dict) else "test"
        with self._lock_for(db_name):
            return func(self, *args, **kwargs)

    return wrapper


class NeoSQLiteHandler:
    """Handle MongoDB commands and translate them to SQLite operations."""

    KNOWN_COMMANDS = frozenset(
        {
            "ping",
            "ismaster",
            "isMaster",
            "hello",
            "Hello",
            "listDatabases",
            "listdatabases",
            "endSessions",
            "buildinfo",
            "buildInfo",
            "whatsmyuri",
            "serverStatus",
            "dbStats",
            "dbstats",
            "insert",
            "find",
            "update",
            "delete",
            "count",
            "distinct",
            "aggregate",
            "create",
            "drop",
            "collStats",
            "collstats",
            "listCollections",
            "listcollections",
            "renameCollection",
            "renamecollection",
        }
    )

    def __init__(
        self,
        db_path: str = ":memory:",
        tokenizers: list | None = None,
        journal_mode: str = "WAL",
        data_dir: str | None = None,
        single_db_compat: bool = False,
    ):
        """Create the handler.

        Args:
            db_path: Legacy single-file path or ":memory:". A directory path
                enables multi-file mode (one ``<db>.db`` per logical db).
            data_dir: Explicit multi-file base directory. Takes precedence
                over a file-style ``db_path``. ``"memory"`` selects isolated
                in-memory databases.
            single_db_compat: Force legacy behavior (all logical databases
                share one SQLite connection) even for file paths.
        """
        self.tokenizers = tokenizers
        self.journal_mode = journal_mode
        self.start_time = time.time()
        self._active_connections = 0
        self._connections_lock = threading.Lock()
        # Sessions are keyed by (session_id, logical db name): SQLite files
        # cannot share a transaction, so there is no cross-db session.
        self._sessions: dict[tuple[str, str], Any] = {}
        # RLock: _find_session() re-acquires it when callers already hold it.
        self._sessions_lock = threading.RLock()

        # A single SQLite connection is shared by all request handlers. SQLite
        # connections are not safe for concurrent use from multiple threads,
        # so every public handler entry point serializes access through a
        # reentrant lock. check_same_thread=False allows the connection to be
        # touched from whichever thread the async/server runtime schedules it
        # on, while the lock guarantees only one operation uses it at a time.
        self._db_lock = threading.RLock()
        self._dict_lock = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}
        # Live data cursors for find/aggregate pagination:
        # id -> {"ns": str, "db": str, "docs": list, "pos": int,
        #         "owner": Any}. Guarded by _cursors_lock.
        self._cursors: dict[int, dict[str, Any]] = {}
        self._cursors_lock = threading.Lock()

        if data_dir is not None:
            norm = data_dir.strip()
            if norm in (":memory:", "memory"):
                self._mode = "memory"
                self._data_dir: str | None = None
                self.db_path = ":memory:"
            else:
                self._mode = "files"
                self._data_dir = os.path.abspath(norm)
                os.makedirs(self._data_dir, exist_ok=True)
                self.db_path = self._data_dir
        elif db_path in (":memory:", "memory"):
            self._mode = "memory"
            self._data_dir = None
            self.db_path = ":memory:"
        elif os.path.isdir(db_path):
            self._mode = "files"
            self._data_dir = os.path.abspath(db_path)
            self.db_path = self._data_dir
        elif single_db_compat:
            self._mode = "single"
            self._data_dir = None
            self.db_path = db_path
        else:
            # Legacy default: a file path keeps sharing one connection so
            # existing deployments see no behavior change. Pass data_dir or
            # a directory db_path (or single_db_compat=False with an
            # explicit migration) for per-database files.
            self._mode = "single"
            self._data_dir = None
            self.db_path = db_path

        self._conns: dict[str, Connection] = {}
        if self._mode == "single":
            if db_path in (":memory:", "memory"):
                self.conn = Connection(
                    "file::memory:?cache=shared",
                    check_same_thread=False,
                    uri=True,
                    tokenizers=tokenizers,
                    journal_mode=journal_mode,
                )
            else:
                self.conn = Connection(
                    db_path,
                    check_same_thread=False,
                    tokenizers=tokenizers,
                    journal_mode=journal_mode,
                )
            self._conns["admin"] = self.conn
        else:
            # Default connection for backward compatibility (tests and
            # sessions use h.conn). Created lazily per logical db below,
            # starting with "admin".
            self.conn = self._open_db_conn("admin")
            self._conns["admin"] = self.conn
        self.databases: dict[str, Connection] = dict(self._conns)
        self._change_stream_manager = ChangeStreamManager()

    def _lock_for(self, db_name: str) -> threading.RLock:
        """Return the lock guarding a logical database."""
        if self._mode == "single":
            return self._db_lock
        with self._dict_lock:
            lock = self._locks.get(db_name)
            if lock is None:
                lock = threading.RLock()
                self._locks[db_name] = lock
            return lock

    def _db_file(self, db_name: str) -> str:
        """SQLite file backing a logical database (files mode only)."""
        assert self._data_dir is not None
        return os.path.join(self._data_dir, f"{db_name}.db")

    def _open_db_conn(self, db_name: str) -> Connection:
        """Open (but not yet cache) the connection for a logical database."""
        safe = _sanitize_db_name(db_name)
        if self._mode == "memory":
            return Connection(
                ":memory:",
                check_same_thread=False,
                tokenizers=self.tokenizers,
                journal_mode=self.journal_mode,
            )
        return Connection(
            self._db_file(safe),
            check_same_thread=False,
            tokenizers=self.tokenizers,
            journal_mode=self.journal_mode,
        )

    def get_database(self, db_name: str) -> Connection:
        if self._mode == "single":
            if db_name not in self.databases:
                self.databases[db_name] = self.conn
            return self.databases[db_name]
        _sanitize_db_name(db_name)
        with self._dict_lock:
            conn = self._conns.get(db_name)
            if conn is None:
                conn = self._open_db_conn(db_name)
                self._conns[db_name] = conn
                self.databases[db_name] = conn
            return conn

    def close_all(self) -> None:
        """Close every open database connection (multi-DB shutdown)."""
        with self._dict_lock:
            conns = list(self._conns.values())
            self._conns.clear()
            self.databases.clear()
        self._change_stream_manager.invalidate()
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass

    def _drop_file_db(self, db_name: str) -> None:
        """Close, unlink and forget a file/memory database."""
        self._change_stream_manager.invalidate(db_name)
        with self._dict_lock:
            conn = self._conns.pop(db_name, None)
            self.databases.pop(db_name, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if self._mode == "files":
            try:
                os.unlink(self._db_file(db_name))
            except OSError:
                pass
        with self._sessions_lock:
            for sess_key in [
                existing
                for existing in self._sessions
                if existing[0] and existing[1] == db_name
            ]:
                self._sessions.pop(sess_key, None)

    @staticmethod
    def _first_batch_size(cmd: dict[str, Any]) -> int:
        """Batch size for a find/aggregate first batch.

        Honors top-level ``batchSize`` and ``cursor: {batchSize}`` (the
        aggregate form). Missing/zero means the server default; negative
        mirrors MongoDB single-batch semantics via abs().
        """
        raw = cmd.get("batchSize", None)
        if raw is None:
            cursor_opts = cmd.get("cursor")
            if isinstance(cursor_opts, dict):
                raw = cursor_opts.get("batchSize", None)
        if raw is None:
            return _DEFAULT_BATCH_SIZE
        try:
            size = abs(int(raw))
        except (TypeError, ValueError):
            return _DEFAULT_BATCH_SIZE
        return size if size > 0 else _DEFAULT_BATCH_SIZE

    def _split_first_batch(
        self,
        docs: list[dict[str, Any]],
        batch_size: int,
        ns: str,
        db_name: str,
        owner: Any = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Split docs into (firstBatch, cursorId), storing the remainder."""
        first = docs[:batch_size]
        rest = docs[batch_size:]
        if not rest:
            return first, 0
        cursor_id = next(_data_cursor_id_counter)
        with self._cursors_lock:
            self._cursors[cursor_id] = {
                "ns": ns,
                "db": db_name,
                "docs": rest,
                "owner": owner,
            }
        return first, cursor_id

    def _getmore_data(
        self, cursor_id: int, batch_size: int
    ) -> dict[str, Any] | None:
        """Next batch for a data cursor, or None if unknown/exhausted."""
        with self._cursors_lock:
            entry = self._cursors.get(cursor_id)
            if entry is None:
                return None
            docs = entry["docs"]
            batch = docs[:batch_size]
            rest = docs[batch_size:]
            ns = entry["ns"]
            if rest:
                entry["docs"] = rest
                live_id = cursor_id
            else:
                del self._cursors[cursor_id]
                live_id = 0
        return {"id": live_id, "ns": ns, "nextBatch": batch}

    def _kill_data_cursors(self, cursor_ids: list[int]) -> list[int]:
        """Remove data cursors, returning the killed ids."""
        killed = []
        with self._cursors_lock:
            for cid in cursor_ids:
                if cid in self._cursors:
                    del self._cursors[cid]
                    killed.append(cid)
        return killed

    def close_cursors_for_owner(self, owner: Any) -> None:
        """Drop data cursors owned by a disconnected client connection."""
        if owner is None:
            return
        with self._cursors_lock:
            for cid in [
                cid
                for cid, entry in self._cursors.items()
                if entry.get("owner") == owner
            ]:
                del self._cursors[cid]

    def _convert_objectids(self, doc: dict) -> dict:
        """Convert PyMongo ObjectIds to NeoSQLite ObjectIds recursively."""
        from nx_27017.utils import convert_bson_to_neo_objectids

        return convert_bson_to_neo_objectids(doc)

    def _session_scope(self, db_name: str) -> str:
        """Session namespace: one shared scope in single-file mode (all
        logical databases share one connection there), per-database
        otherwise."""
        if self._mode == "single":
            return ""
        return db_name

    def _tx_sessions(self, session_id: str) -> list[Any]:
        """Sessions with an open transaction for an lsid (any scope)."""
        with self._sessions_lock:
            return [
                sess
                for (sid, _db), sess in self._sessions.items()
                if sid == session_id and sess.in_transaction
            ]

    def _find_session(
        self, session_id: str, db_name: str
    ) -> Any | None:
        """Find a session for (lsid, db), falling back to any db (compat)."""
        scope = self._session_scope(db_name)
        with self._sessions_lock:
            session = self._sessions.get((session_id, scope))
            if session is not None:
                return session
            if self._mode == "single":
                # Legacy compat: one connection backs every db, so any
                # session works anywhere. Isolated modes stay strict so an
                # lsid can never touch another database's file.
                for (sid, _db), sess in self._sessions.items():
                    if sid == session_id:
                        return sess
            return None

    def _get_or_create_session(
        self,
        command_doc: dict[str, Any],
        db: Connection | None = None,
        db_name: str = "test",
    ) -> Any | None:
        """Extract session from command document if lsid is provided.

        Sessions are scoped to one logical database because SQLite files
        cannot share a transaction.
        """
        lsid = command_doc.get("lsid")
        if not lsid:
            return None
        session_id = _extract_session_id(lsid)
        if not session_id:
            return None
        with self._sessions_lock:
            key = (session_id, self._session_scope(db_name))
            if key not in self._sessions:
                owner = db if db is not None else self.conn
                session = owner.start_session()
                session._in_transaction = False
                self._sessions[key] = session
            session_to_use = self._sessions[key]
            if (
                command_doc.get("startTransaction")
                and not session_to_use.in_transaction
            ):
                session_to_use.start_transaction()
            return session_to_use

    def increment_connections(self) -> None:
        """Increment the active connections counter."""
        with self._connections_lock:
            self._active_connections += 1

    def decrement_connections(self) -> None:
        """Decrement the active connections counter."""
        with self._connections_lock:
            self._active_connections -= 1

    def _is_gridfs_collection(self, coll_name: str) -> bool:
        """Check if collection name is a GridFS collection."""
        return _is_gridfs_collection(coll_name)

    def _get_gridfs_bucket_name(self, coll_name: str) -> str | None:
        """Extract bucket name from GridFS collection name, or None if not GridFS."""
        return _get_gridfs_bucket_name(coll_name)

    def _handle_gridfs_insert(
        self,
        request_id: int,
        coll_name: str,
        docs: list[dict],
        db: Connection,
    ) -> tuple[int, dict[str, Any]]:
        """Handle insert operations on GridFS collections using the GridFSAdapter."""
        logger.debug(
            f"_handle_gridfs_insert called: coll_name={coll_name}, doc_count={len(docs)}"
        )

        if not docs:
            logger.debug("_handle_gridfs_insert: no docs, returning success")
            return request_id, {"ok": 1, "n": 0}

        is_files = coll_name.endswith(".files")
        is_chunks = coll_name.endswith(".chunks")

        logger.debug(
            f"_handle_gridfs_insert: is_files={is_files}, is_chunks={is_chunks}"
        )

        logger.debug(
            f"Calling create_gridfs_adapter with db.db type={type(db.db)}, coll_name={coll_name}"
        )

        adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
        if adapter is None:
            logger.error(
                f"create_gridfs_adapter returned None for coll_name={coll_name}"
            )
            return request_id, {"ok": 0, "errmsg": "Invalid GridFS collection"}

        if is_chunks and docs:
            logger.debug(
                "_handle_gridfs_insert: Ensuring file metadata exists for chunks"
            )
            for doc in docs:
                files_id = doc.get("files_id")
                if files_id:
                    adapter.ensure_file_metadata_exists(files_id, db.db)

        logger.debug(f"Calling adapter.handle_insert: is_files={is_files}")
        result = adapter.handle_insert(docs, is_files=is_files)
        logger.debug(f"adapter.handle_insert result: {result}")
        return request_id, result

    @_serialize
    def handle_insert(self, msg: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        request_id = msg["request_id"]
        sections = msg["sections"]

        logger.debug(f"handle_insert sections: {sections}")

        command_doc = None
        payload_docs = []

        for section_type, doc in sections:
            if section_type == "body":
                command_doc = doc
            elif section_type == "payload_docs":
                payload_docs.extend(doc)
            elif section_type == "payload":
                for key, value in doc.items():
                    if isinstance(value, list):
                        payload_docs.extend(value)
                    elif isinstance(value, dict):
                        payload_docs.append(value)

        if not command_doc:
            return request_id, {"ok": 0, "errmsg": "No command document"}

        db_name = command_doc.pop("$db") if "$db" in command_doc else "test"

        db = self.get_database(db_name)

        coll_name = command_doc.get("insert")
        if not coll_name:
            for key in command_doc:
                if key not in (
                    "$db",
                    "ordered",
                    "writeConcern",
                    "lsid",
                ) and not key.startswith("$"):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        logger.debug(
            f"handle_insert: coll_name={coll_name}, is_gridfs={self._is_gridfs_collection(coll_name)}"
        )
        if coll_name == "fs.files":
            logger.debug(
                f"handle_insert: fs.files INSERT - command_doc keys={list(command_doc.keys())}, payload_docs count={len(payload_docs)}"
            )
        if not self._is_gridfs_collection(coll_name):
            logger.debug(
                f"handle_insert: NOT gridfs, coll_name={coll_name}, command_doc keys={list(command_doc.keys())}"
            )

        if self._is_gridfs_collection(coll_name):
            docs_to_insert = payload_docs.copy() if payload_docs else []
            for key, value in command_doc.items():
                if key == "documents" and isinstance(value, list):
                    docs_to_insert.extend(value)
                elif (
                    key
                    not in {"$db", "insert", "ordered", "writeConcern", "lsid"}
                    and not key.startswith("$")
                    and isinstance(value, dict)
                ):
                    docs_to_insert.append(value)
            return self._handle_gridfs_insert(
                request_id, coll_name, docs_to_insert, db
            )

        # Ensure the collection's underlying table exists. get_collection
        # only registers the collection in-memory; (re)create the table if it
        # is missing. This handles both brand-new collections and collections
        # dropped between operations: a dropped collection leaves a stale entry
        # in the in-memory cache, so db.create_collection would raise
        # "already exists" on the next insert. CREATE TABLE IF NOT EXISTS keeps
        # coll.create() safe for already-existing collections.
        coll = db[coll_name]
        if coll_name not in db.list_collection_names():
            coll.create()

        docs_to_insert = payload_docs.copy() if payload_docs else []
        for key, value in command_doc.items():
            if key == "documents" and isinstance(value, list):
                docs_to_insert.extend(value)
            elif (
                key not in {"$db", "insert", "ordered", "writeConcern", "lsid"}
                and not key.startswith("$")
                and isinstance(value, dict)
            ):
                docs_to_insert.append(value)

        docs_to_insert = [
            self._convert_objectids(doc) for doc in docs_to_insert
        ]

        session_to_use = self._get_or_create_session(
            command_doc, db, db_name
        )

        if docs_to_insert:
            # Per-document loop (not one insert_many) so a duplicate key
            # becomes a writeErrors entry instead of failing the batch:
            # ordered stops at the first error, unordered collects them.
            ordered = command_doc.get("ordered", True)
            inserted_ids: list[Any] = []
            write_errors: list[dict[str, Any]] = []
            for idx, doc in enumerate(docs_to_insert):
                try:
                    res = coll.insert_one(doc, session=session_to_use)
                    inserted_ids.append(res.inserted_id)
                except Exception as exc:
                    write_errors.append(_bulk_write_error(exc, idx))
                    if ordered:
                        break
            response: dict[str, Any] = {
                "ok": 1,
                "n": len(inserted_ids),
                "insertedIds": inserted_ids,
            }
            if write_errors:
                response["writeErrors"] = write_errors
            return request_id, response

        return request_id, {"ok": 1, "n": 0}

    @_serialize
    def handle_command(  # noqa: E501
        self, msg: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        """Handle command by passing directly to NeoSQLite."""
        request_id = msg["request_id"]
        sections = msg["sections"]

        command_doc = None
        payload_updates = []
        payload_deletes = []
        payload_ops: list[dict[str, Any]] = []
        payload_nsinfo: list[dict[str, Any]] = []
        for section_type, doc in sections:
            if section_type == "body":
                command_doc = doc
            elif section_type == "payload":
                if isinstance(doc, dict):
                    if "updates" in doc:
                        payload_updates = doc["updates"]
                    if "deletes" in doc:
                        payload_deletes = doc["deletes"]
                    # bulkWrite ships ops (and sometimes nsInfo) as a
                    # document sequence rather than inline in the body.
                    if "ops" in doc:
                        payload_ops = doc["ops"]
                    if "nsInfo" in doc:
                        payload_nsinfo = doc["nsInfo"]

        if not command_doc:
            return request_id, {"ok": 0, "errmsg": "No command document"}

        db_name = command_doc.get("$db", "test")
        db = self.get_database(db_name)

        for key in ["ismaster", "isMaster", "hello", "Hello"]:
            if key in command_doc:
                return request_id, {
                    "ok": 1,
                    "isWritablePrimary": True,
                    "maxBsonObjectSize": MAX_BSON_DOCUMENT_SIZE,
                    "maxMessageSizeBytes": MAX_MESSAGE_SIZE_BYTES,
                    "maxWriteBatchSize": MAX_WRITE_BATCH_SIZE,
                    "localTime": datetime.now(timezone.utc),
                    "logicalSessionTimeoutMinutes": DEFAULT_SESSION_TIMEOUT_MINUTES,
                    "connectionId": 1,
                    "minWireVersion": MIN_WIRE_VERSION,
                    "maxWireVersion": MAX_WIRE_VERSION,
                }

        if "startSession" in command_doc:
            session_id = f"session_{uuid.uuid4().hex}"
            session = db.start_session()
            with self._sessions_lock:
                self._sessions[
                    (session_id, self._session_scope(db_name))
                ] = session
            return request_id, {
                "ok": 1,
                "session": {"id": {"$oid": session_id}},
            }

        if "commitTransaction" in command_doc:
            lsid = command_doc.get("lsid")
            tx_session_id = _extract_session_id(lsid) if lsid else None
            if tx_session_id:
                # commitTransaction is routed to admin.$cmd by drivers while
                # the transaction lives on the data database, so resolve
                # across scopes and finish every open transaction for the
                # lsid (single-file transactions only; cross-db atomicity
                # is unsupported).
                acted = False
                for sess in self._tx_sessions(tx_session_id):
                    sess.commit_transaction()
                    acted = True
                if acted:
                    return request_id, {"ok": 1}
            return request_id, {"ok": 0, "errmsg": "No session specified"}

        if "abortTransaction" in command_doc:
            lsid = command_doc.get("lsid")
            tx_session_id = _extract_session_id(lsid) if lsid else None
            if tx_session_id:
                acted = False
                for sess in self._tx_sessions(tx_session_id):
                    sess.abort_transaction()
                    acted = True
                if acted:
                    return request_id, {"ok": 1}
            return request_id, {"ok": 0, "errmsg": "No session specified"}

        if "endSessions" in command_doc:
            session_ids = command_doc.get("endSessions", [])
            if isinstance(session_ids, str):
                session_ids = [session_ids]
            with self._sessions_lock:
                for sid in session_ids:
                    if isinstance(sid, dict):
                        sid = _extract_session_id(sid)
                    if isinstance(sid, bytes):
                        sid = sid.hex()
                    if sid:
                        for sess_key in [
                            existing
                            for existing in self._sessions
                            if existing[0] == sid
                        ]:
                            end_session = self._sessions.pop(sess_key, None)
                            if end_session:
                                try:
                                    end_session.end_session()
                                except Exception:
                                    pass
            return request_id, {"ok": 1}

        cmd_copy = dict(command_doc)

        db_name = cmd_copy.pop("$db", "test")
        db = self.get_database(db_name)

        if "create" in cmd_copy:
            coll_name = cmd_copy.pop("create")
            try:
                db.create_collection(coll_name)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
            return request_id, {"ok": 1}

        if "drop" in cmd_copy:
            coll_name = cmd_copy.pop("drop")
            if coll_name.startswith("sqlite_"):
                return request_id, {
                    "ok": 0,
                    "errmsg": f"Cannot drop internal table: {coll_name}",
                }
            if self._is_gridfs_collection(coll_name):
                # Dotted names (fs.files) cannot be tables in NeoSQLite;
                # drop the backing bucket instead.
                from neosqlite.gridfs import GridFSBucket

                bucket_name = self._get_gridfs_bucket_name(coll_name)
                if bucket_name is None:
                    return request_id, {
                        "ok": 0,
                        "errmsg": "Invalid GridFS collection",
                    }
                try:
                    _sanitize_db_name(bucket_name)
                except ValueError:
                    return request_id, {
                        "ok": 0,
                        "errmsg": f"Invalid bucket name: {bucket_name!r}",
                    }
                # Like real MongoDB, dropping one side leaves the other
                # (possibly orphaned) alone. Sanitized above: no quotes
                # possible, double-quoting keeps dashes valid.
                if coll_name.endswith(".chunks"):
                    target_table = f"{bucket_name}_chunks"
                else:
                    target_table = f"{bucket_name}_files"
                db.db.execute(f'DROP TABLE IF EXISTS "{target_table}"')
                try:
                    db.db.commit()
                except Exception:
                    pass
                return request_id, {"ok": 1}
            db[coll_name].drop()
            return request_id, {"ok": 1}

        if "dropDatabase" in cmd_copy or "dropdatabase" in cmd_copy:
            target_db = cmd_copy.pop("dropDatabase", None) or cmd_copy.pop(
                "dropdatabase", None
            )
            dropped_name = (
                db_name if target_db in (1, True, None, "") else str(target_db)
            )
            if self._mode == "single":
                # Legacy limitation: one file backs every logical database,
                # so dropping one drops all user tables. Prefer data_dir
                # (multi-file) mode for real isolation.
                self._change_stream_manager.invalidate(dropped_name)
                self.conn.drop_database()
                with self._dict_lock:
                    self.databases = {"admin": self.conn}
                    self._conns = {"admin": self.conn}
            else:
                try:
                    _sanitize_db_name(dropped_name)
                except ValueError:
                    return request_id, {
                        "ok": 0,
                        "errmsg": f"Invalid database name: {dropped_name!r}",
                    }
                self._drop_file_db(dropped_name)
            return request_id, {"dropped": dropped_name, "ok": 1}

        if "renameCollection" in cmd_copy:
            old_name = cmd_copy.pop("renameCollection")
            to_name = cmd_copy.pop("to", None)
            if to_name:
                old_coll = (
                    old_name.split(".")[-1] if "." in old_name else old_name
                )
                new_coll = to_name.split(".")[-1] if "." in to_name else to_name
                db[old_coll].rename(new_coll)
                return request_id, {"ok": 1}
            return request_id, {
                "ok": 0,
                "errmsg": "renameCollection requires 'to' parameter",
            }

        if "createIndexes" in cmd_copy:
            coll_name = cmd_copy.pop("createIndexes")
            indexes_spec = cmd_copy.pop("indexes", [])

            if _is_gridfs_collection(coll_name):
                adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
                if adapter is None:
                    return request_id, {
                        "ok": 0,
                        "errmsg": "Failed to create GridFS adapter",
                    }
                result = adapter.create_indexes()
                return request_id, result

            coll = db[coll_name]
            num_before = len(coll.list_indexes())
            created_names = []
            for index_spec in indexes_spec:
                key = index_spec.get("key", {})
                name = index_spec.get("name")
                unique = index_spec.get("unique", False)
                sparse = index_spec.get("sparse", False)
                fts = index_spec.get("fts", False)
                tokenizer = index_spec.get("tokenizer")
                expire_after = index_spec.get("expireAfterSeconds")

                if isinstance(key, dict):
                    # Convert {"field": 1, "field2": -1} to [("field", 1), ("field2", -1)]
                    keys_list = [(k, v) for k, v in key.items()]
                else:
                    keys_list = key

                idx_kwargs: dict[str, Any] = {
                    "unique": unique,
                    "sparse": sparse,
                    "fts": fts,
                    "tokenizer": tokenizer,
                }
                if name:
                    idx_kwargs["name"] = name
                if expire_after is not None:
                    idx_kwargs["expireAfterSeconds"] = expire_after

                idx_name = coll.create_index(keys_list, **idx_kwargs)
                created_names.append(name if name else idx_name)
            return request_id, {
                "ok": 1,
                "createdCollectionAutomatically": False,
                "numIndexesBefore": num_before,
                "numIndexesAfter": len(coll.list_indexes()),
                "indexesCreated": [{"name": n} for n in created_names],
            }

        if "dropIndexes" in cmd_copy:
            coll_name = cmd_copy.pop("dropIndexes")
            index = cmd_copy.pop("index", "*")
            coll = db[coll_name]
            num_before = len(coll.list_indexes())
            if index == "*":
                coll.drop_indexes()
            else:
                coll.drop_index(index)
            return request_id, {
                "ok": 1,
                "nIndexesWas": num_before,
            }

        if "createIndex" in cmd_copy:
            coll_name = cmd_copy.pop("createIndex")
            key = cmd_copy.get("key", {})
            name = cmd_copy.get("name")
            unique = cmd_copy.get("unique", False)
            sparse = cmd_copy.get("sparse", False)

            if self._is_gridfs_collection(coll_name):
                bucket_name = self._get_gridfs_bucket_name(coll_name)
                if bucket_name:
                    from neosqlite.gridfs import GridFSBucket

                    GridFSBucket(db.db, bucket_name=bucket_name)
                return request_id, {
                    "ok": 1,
                    "createdCollectionAutomatically": False,
                    "numIndexesBefore": 0,
                    "numIndexesAfter": 0,
                    "indexesCreated": [],
                }

            coll = db[coll_name]
            num_before = len(coll.list_indexes())

            if isinstance(key, dict):
                # Convert {"field": 1, "field2": -1} to [("field", 1), ("field2", -1)]
                keys_list = [(k, v) for k, v in key.items()]
            else:
                keys_list = key

            idx_name = coll.create_index(
                keys_list, unique=unique, sparse=sparse
            )
            return request_id, {
                "ok": 1,
                "createdCollectionAutomatically": False,
                "numIndexesBefore": num_before,
                "numIndexesAfter": len(coll.list_indexes()),
                "indexesCreated": [{"name": idx_name}],
            }

        if "dropIndex" in cmd_copy:
            coll_name = cmd_copy.pop("dropIndex")
            index = cmd_copy.pop("index")
            coll = db[coll_name]
            num_before = len(coll.list_indexes())
            coll.drop_index(index)
            return request_id, {
                "ok": 1,
                "nIndexesWas": num_before,
            }

        if "createSearchIndexes" in cmd_copy or (
            "createsearchindexes" in cmd_copy
        ):
            key = (
                "createSearchIndexes"
                if "createSearchIndexes" in cmd_copy
                else "createsearchindexes"
            )
            coll_name = cmd_copy.pop(key)
            specs = cmd_copy.pop("indexes", [])
            coll = db[coll_name]
            created = []
            for spec in specs:
                if not isinstance(spec, dict):
                    continue
                try:
                    fields = _search_index_fields(spec)
                except ValueError as exc:
                    return request_id, {"ok": 0, "errmsg": str(exc)}
                tokenizer = spec.get("tokenizer")
                if not isinstance(tokenizer, str):
                    tokenizer = None
                for field in fields:
                    coll.create_search_index(field, tokenizer=tokenizer)
                    created.append({"name": spec.get("name", field)})
            return request_id, {"ok": 1, "indexesCreated": created}

        if "updateSearchIndex" in cmd_copy or "updatesearchindex" in cmd_copy:
            key = (
                "updateSearchIndex"
                if "updateSearchIndex" in cmd_copy
                else "updatesearchindex"
            )
            coll_name = cmd_copy.pop(key)
            name = cmd_copy.get("name", "")
            definition = cmd_copy.get("definition", {})
            coll = db[coll_name]
            try:
                fields = _search_index_fields(
                    {"name": name, "definition": definition}
                )
            except ValueError:
                fields = [name] if name else []
            if not fields:
                return request_id, {
                    "ok": 0,
                    "errmsg": "updateSearchIndex requires a name or "
                    "a definition with text fields",
                }
            tokenizer = None
            if isinstance(definition, dict):
                tok = definition.get("tokenizer")
                if isinstance(tok, str):
                    tokenizer = tok
            coll.update_search_index(fields[0], tokenizer=tokenizer)
            return request_id, {"ok": 1}

        if "dropSearchIndex" in cmd_copy or "dropsearchindex" in cmd_copy:
            key = (
                "dropSearchIndex"
                if "dropSearchIndex" in cmd_copy
                else "dropsearchindex"
            )
            coll_name = cmd_copy.pop(key)
            name = cmd_copy.get("name", "")
            if not name:
                return request_id, {
                    "ok": 0,
                    "errmsg": "dropSearchIndex requires 'name'",
                }
            db[coll_name].drop_search_index(name)
            return request_id, {"ok": 1}

        if "delete" in cmd_copy:
            coll_name = cmd_copy.pop("delete")
            if self._is_gridfs_collection(coll_name):
                if "deletes" not in cmd_copy and payload_deletes:
                    cmd_copy["deletes"] = payload_deletes
                return self._handle_gridfs_delete(
                    request_id, cmd_copy, db, coll_name
                )
            cmd_copy["delete"] = coll_name
            if "deletes" not in cmd_copy and payload_deletes:
                cmd_copy["deletes"] = payload_deletes
            return self._handle_delete(request_id, cmd_copy, db, db_name)

        if "bulkWrite" in cmd_copy:
            if "ops" not in cmd_copy and payload_ops:
                cmd_copy["ops"] = payload_ops
            if "nsInfo" not in cmd_copy and payload_nsinfo:
                cmd_copy["nsInfo"] = payload_nsinfo
            return self._handle_bulk_write(request_id, cmd_copy, command_doc)

        if "upload" in cmd_copy:
            return self._handle_gridfs_upload(request_id, cmd_copy, db)

        if "openDownloadStream" in cmd_copy:
            file_id = cmd_copy.get("openDownloadStream")
            if isinstance(file_id, str):
                from neosqlite.objectid import ObjectId

                file_id = ObjectId(file_id)
            cmd_copy["fileId"] = file_id
            return self._handle_gridfs_download(request_id, cmd_copy, db)

        if "findAndModify" in cmd_copy:
            coll_name = cmd_copy.pop("findAndModify")
            query = cmd_copy.pop("query", {})
            query = self._convert_objectids(query)
            update_doc = cmd_copy.pop("update", None)
            remove = cmd_copy.pop("remove", False)
            new_doc = cmd_copy.pop("new", False)
            fields = cmd_copy.pop("fields", None)
            sort_val = cmd_copy.pop("sort", None)
            upsert = cmd_copy.pop("upsert", False)
            array_filters = cmd_copy.pop("arrayFilters", None)

            sort_tuples = None
            if isinstance(sort_val, dict):
                sort_tuples = list(sort_val.items())
            elif isinstance(sort_val, list):
                sort_tuples = sort_val

            coll = db[coll_name]
            session_to_use = self._get_or_create_session(
                command_doc, db, db_name
            )

            if remove:
                try:
                    doc = coll.find_one_and_delete(
                        query,
                        projection=fields,
                        sort=sort_tuples,
                        session=session_to_use,
                    )
                except Exception:
                    doc = None
                return request_id, {"ok": 1, "value": doc}
            elif update_doc:
                update_doc = self._convert_objectids(update_doc)
                is_replace = not any(
                    k.startswith("$") for k in update_doc.keys()
                )
                try:
                    if is_replace:
                        doc = coll.find_one_and_replace(
                            query,
                            update_doc,
                            projection=fields,
                            sort=sort_tuples,
                            upsert=upsert,
                            return_document=new_doc,
                            session=session_to_use,
                        )
                    else:
                        doc = coll.find_one_and_update(
                            query,
                            update_doc,
                            projection=fields,
                            sort=sort_tuples,
                            upsert=upsert,
                            return_document=new_doc,
                            array_filters=array_filters,
                            session=session_to_use,
                        )
                except Exception:
                    doc = None
                return request_id, {"ok": 1, "value": doc}
            else:
                return request_id, {
                    "ok": 0,
                    "errmsg": "findAndModify requires 'update' or 'remove'",
                }

        if "update" in cmd_copy:
            coll_name = cmd_copy.pop("update")
            updates = cmd_copy.pop("updates", []) or payload_updates

            if self._is_gridfs_collection(coll_name):
                adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
                if adapter is None:
                    return request_id, {
                        "ok": 0,
                        "errmsg": "Invalid GridFS collection",
                    }
                total_n = 0
                total_n_modified = 0
                for update in updates:
                    q = update.get("q", {})
                    u = update.get("u", {})
                    file_id = q.get("_id")
                    if file_id is not None:
                        res = adapter.handle_update(file_id, u)
                        total_n += res.get("n", 0)
                        total_n_modified += res.get("nModified", 0)
                return request_id, {
                    "ok": 1,
                    "n": total_n,
                    "nModified": total_n_modified,
                }

            coll = db[coll_name]
            session_to_use = self._get_or_create_session(
                command_doc, db, db_name
            )
            ordered = cmd_copy.get("ordered", True)
            matched = 0
            modified = 0
            upserted: list[dict[str, Any]] = []
            write_errors: list[dict[str, Any]] = []

            for idx, update in enumerate(updates):
                q = update.get("q", {})
                u = update.get("u", {})
                q = self._convert_objectids(q)
                u = self._convert_objectids(u)
                multi = update.get("multi", False)
                upsert = update.get("upsert", False)
                array_filters = update.get("arrayFilters")

                # Change events are captured trigger-side (see changestream
                # module), so no per-operation fan-out is needed here. This
                # also covers bulk writes, which never passed through here.
                try:
                    is_replace = not any(k.startswith("$") for k in u.keys())
                    if is_replace:
                        upd_result = coll.replace_one(
                            q, u, upsert=upsert, session=session_to_use
                        )
                    elif multi:
                        upd_result = coll.update_many(
                            q,
                            u,
                            upsert=upsert,
                            array_filters=array_filters,
                            session=session_to_use,
                        )
                    else:
                        upd_result = coll.update_one(
                            q,
                            u,
                            upsert=upsert,
                            array_filters=array_filters,
                            session=session_to_use,
                        )
                except Exception as exc:
                    write_errors.append(_bulk_write_error(exc, idx))
                    if ordered:
                        break
                    continue
                matched += upd_result.matched_count
                modified += upd_result.modified_count
                if upd_result.upserted_id is not None:
                    # Wire n counts an upserted doc as matched (real
                    # MongoDB reports n:1 for an upsert-insert).
                    matched += 1
                    upserted.append(
                        {"index": idx, "_id": upd_result.upserted_id}
                    )
            response = {
                "ok": 1,
                "n": matched,
                "nModified": modified,
            }
            if upserted:
                response["upserted"] = upserted
            if write_errors:
                response["writeErrors"] = write_errors
            return request_id, response

        if "find" in cmd_copy:
            coll_name = cmd_copy.pop("find")
            filter_query = cmd_copy.pop("filter", {})
            filter_query = self._convert_objectids(filter_query)
            projection = cmd_copy.pop("projection", None)

            if self._is_gridfs_collection(coll_name):
                cmd_copy["filter"] = filter_query
                return self._handle_gridfs_find(
                    request_id, cmd_copy, db, coll_name, db_name
                )

            try:
                collection_exists = coll_name in db.list_collection_names()
            except Exception:
                collection_exists = False

            if not collection_exists:
                return request_id, {
                    "ok": 1,
                    "cursor": {
                        "id": 0,
                        "ns": f"{db_name}.{coll_name}",
                        "firstBatch": [],
                    },
                }

            coll = db[coll_name]
            session_to_use = self._get_or_create_session(
                command_doc, db, db_name
            )
            cursor = (
                coll.find(filter_query, projection, session=session_to_use)
                if projection
                else coll.find(filter_query, session=session_to_use)
            )
            if "sort" in cmd_copy:
                cursor = cursor.sort(list(cmd_copy["sort"].items()))
            if "limit" in cmd_copy:
                cursor = cursor.limit(cmd_copy["limit"])
            if "skip" in cmd_copy:
                cursor = cursor.skip(cmd_copy["skip"])
            if "hint" in cmd_copy:
                hint_val = cmd_copy["hint"]
                if isinstance(hint_val, str):
                    cursor = cursor.hint(hint_val)
                elif isinstance(hint_val, list):
                    cursor = cursor.hint(hint_val)
            if "min" in cmd_copy:
                min_val = cmd_copy["min"]
                if isinstance(min_val, dict):
                    cursor = cursor.min(list(min_val.items()))
                elif isinstance(min_val, list):
                    cursor = cursor.min(min_val)
            if "max" in cmd_copy:
                max_val = cmd_copy["max"]
                if isinstance(max_val, dict):
                    cursor = cursor.max(list(max_val.items()))
                elif isinstance(max_val, list):
                    cursor = cursor.max(max_val)
            docs = list(cursor)
            first, cursor_id = self._split_first_batch(
                docs,
                self._first_batch_size(cmd_copy),
                f"{db_name}.{coll_name}",
                db_name,
                owner=msg.get("_conn_id"),
            )
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": cursor_id,
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": first,
                },
            }

        if "count" in cmd_copy:
            try:
                coll_name = cmd_copy.pop("count")
                query = cmd_copy.pop("query", {})
                limit = cmd_copy.pop("limit", None)
                skip = cmd_copy.pop("skip", None)
                if self._is_gridfs_collection(coll_name):
                    adapter, _bucket = create_gridfs_adapter(
                        db.db, coll_name
                    )
                    if adapter is None:
                        return request_id, {
                            "ok": 0,
                            "errmsg": "Invalid GridFS collection",
                        }
                    filt = self._convert_objectids(query or {})
                    if coll_name.endswith(".chunks"):
                        docs = adapter.handle_chunks_find(filt)
                    else:
                        docs = adapter.handle_find(filt)
                    if skip:
                        docs = docs[int(skip) :]
                    if limit:
                        docs = docs[: int(limit)]
                    return request_id, {"ok": 1, "n": len(docs)}
                coll = db[coll_name]
                if limit is None and skip is None:
                    count = coll.count_documents(query)
                else:
                    # count with limit/skip (legacy count command): apply
                    # pagination over the match set.
                    cursor = coll.find(query)
                    if skip:
                        cursor = cursor.skip(int(skip))
                    if limit:
                        cursor = cursor.limit(int(limit))
                    count = len(list(cursor))
                return request_id, {"ok": 1, "n": count}
            except Exception as e:
                logger.error(f"Error in count: {e}")
                return request_id, {"ok": 0, "errmsg": str(e)}

        if "distinct" in cmd_copy:
            try:
                coll_name = cmd_copy.pop("distinct")
                key = cmd_copy.pop("key", "")
                query = cmd_copy.pop("query", {})
                if self._is_gridfs_collection(coll_name):
                    return self._handle_gridfs_distinct(
                        request_id, coll_name, key, query, db
                    )
                coll = db[coll_name]
                values = coll.distinct(key, query)
                return request_id, {"ok": 1, "values": values}
            except Exception as e:
                logger.error(f"Error in distinct: {e}")
                return request_id, {"ok": 0, "errmsg": str(e)}

        if "aggregate" in cmd_copy:
            try:
                coll_name = cmd_copy.pop("aggregate")
                pipeline = cmd_copy.pop("pipeline", [])

                # Atlas-style search index listing arrives as an
                # aggregation stage; serve it from NeoSQLite FTS metadata.
                if any(
                    isinstance(stage, dict) and "$listSearchIndexes" in stage
                    for stage in pipeline
                ):
                    return self._handle_list_search_indexes(
                        request_id, db, coll_name, db_name
                    )

                # Check if this is a change stream request
                if is_change_stream_pipeline(pipeline):
                    return self._handle_change_stream(
                        request_id,
                        coll_name,
                        pipeline,
                        db,
                        db_name,
                        owner=msg.get("_conn_id"),
                    )

                coll = db[coll_name]
                cursor = coll.aggregate(pipeline)  # type: ignore[assignment]
                docs = cursor.to_list()
                first, cursor_id = self._split_first_batch(
                    docs,
                    self._first_batch_size(cmd_copy),
                    f"{db_name}.{coll_name}",
                    db_name,
                    owner=msg.get("_conn_id"),
                )
                return request_id, {
                    "ok": 1,
                    "cursor": {
                        "id": cursor_id,
                        "ns": f"{db_name}.{coll_name}",
                        "firstBatch": first,
                    },
                }
            except Exception as e:
                logger.error(f"Error in aggregate: {e}")
                return request_id, {"ok": 0, "errmsg": str(e)}

        if "listCollections" in cmd_copy:
            return self._handle_list_collections(request_id, db, db_name)

        if "serverStatus" in cmd_copy or "buildInfo" in cmd_copy:
            return self._handle_server_status(request_id, db)

        if "dbStats" in cmd_copy or "dbstats" in cmd_copy:
            db_stats_result = db.command({"dbStats": 1})
            return request_id, db_stats_result

        if "collStats" in cmd_copy or "collstats" in cmd_copy:
            coll_name = cmd_copy.get("collStats") or cmd_copy.get("collstats")
            coll_stats_result = db.command({"collstats": coll_name})
            return request_id, coll_stats_result

        if "listDatabases" in cmd_copy or "listdatabases" in cmd_copy:
            if self._mode == "single":
                databases_info = []
                if self.db_path == ":memory:":
                    size_on_disk = 0
                    is_empty = True
                else:
                    try:
                        size_on_disk = os.path.getsize(self.db_path)
                        is_empty = False
                    except OSError:
                        size_on_disk = 0
                        is_empty = True
                total_size = size_on_disk
                for name in self.databases:
                    databases_info.append(
                        {
                            "name": name,
                            "sizeOnDisk": size_on_disk,
                            "empty": is_empty,
                        }
                    )
                return request_id, {
                    "ok": 1,
                    "databases": databases_info,
                    "totalSize": total_size,
                }
            databases_info = []
            total_size = 0
            names: set[str] = set(self._conns.keys())
            if self._mode == "files" and self._data_dir is not None:
                try:
                    for entry in os.listdir(self._data_dir):
                        if entry.endswith(".db"):
                            stem = entry[:-3]
                            try:
                                names.add(_sanitize_db_name(stem))
                            except ValueError:
                                continue
                except OSError:
                    pass
            for name in sorted(names):
                try:
                    db_conn = self.get_database(name)
                except ValueError:
                    continue
                try:
                    empty = not db_conn.list_collection_names()
                except Exception:
                    empty = True
                if self._mode == "memory":
                    size_on_disk = 0
                else:
                    try:
                        size_on_disk = os.path.getsize(self._db_file(name))
                    except OSError:
                        size_on_disk = 0
                total_size += size_on_disk
                databases_info.append(
                    {
                        "name": name,
                        "sizeOnDisk": size_on_disk,
                        "empty": empty,
                    }
                )
            return request_id, {
                "ok": 1,
                "databases": databases_info,
                "totalSize": total_size,
            }

        if "listIndexes" in cmd_copy or "listindexes" in cmd_copy:
            coll_name = cmd_copy.get("listIndexes") or cmd_copy.get(
                "listindexes"
            )
            return self._handle_list_indexes(
                request_id, db, coll_name, db_name
            )

        if "listSearchIndexes" in cmd_copy or "listsearchindexes" in cmd_copy:
            coll_name = cmd_copy.get("listSearchIndexes") or cmd_copy.get(
                "listsearchindexes"
            )
            return self._handle_list_search_indexes(
                request_id, db, coll_name, db_name
            )

        if "explain" in cmd_copy:
            explain_value = cmd_copy.get("explain")
            if isinstance(explain_value, dict):
                inner_cmd = dict(explain_value)
                inner_cmd.pop("$db", None)

                if "find" in inner_cmd:
                    coll_name = inner_cmd.pop("find")
                    filter_query = inner_cmd.pop("filter", {})

                    if self._is_gridfs_collection(coll_name):
                        return request_id, {
                            "ok": 1,
                            "queryPlanner": {
                                "plannerVersion": 1,
                                "namespace": f"{db_name}.{coll_name}",
                                "indexFilterSet": False,
                                "parsedQuery": filter_query,
                                "winningPlan": {"stage": "COLLSCAN"},
                                "rejectedPlans": [],
                            },
                            "executionStats": {
                                "executionSuccess": True,
                                "nReturned": 0,
                                "executionTimeMillis": 0,
                                "totalKeysExamined": 0,
                                "totalDocsExamined": 0,
                            },
                            "serverInfo": {
                                "host": "localhost",
                                "port": 27017,
                                "version": "7.0.0",
                                "gitVersion": "unknown",
                            },
                        }

                    coll = db[coll_name]

                    cursor = coll.find(filter_query)
                    explain_result = cursor.explain()

                    return request_id, {
                        "ok": 1,
                        "queryPlanner": {
                            "plannerVersion": 1,
                            "namespace": f"{db_name}.{coll_name}",
                            "indexFilterSet": False,
                            "parsedQuery": filter_query,
                            "winningPlan": explain_result.get(
                                "queryPlanner", {}
                            ).get("winningPlan", []),
                            "rejectedPlans": [],
                        },
                        "executionStats": {
                            "executionSuccess": True,
                            "nReturned": 0,
                            "executionTimeMillis": 0,
                            "totalKeysExamined": 0,
                            "totalDocsExamined": 0,
                        },
                        "serverInfo": {
                            "host": "localhost",
                            "port": 27017,
                            "version": "7.0.0",
                            "gitVersion": "unknown",
                        },
                    }

                return request_id, {
                    "ok": 1,
                    "queryPlanner": {
                        "winningPlan": {"stage": "EOF"},
                    },
                }

        if "getMore" in cmd_copy:
            raw_id = cmd_copy.get("getMore")
            cursor_id = int(raw_id) if raw_id is not None else 0
            coll_name = cmd_copy.get("collection")
            stream = self._change_stream_manager.get_stream(cursor_id)
            if stream:
                next_batch, token = self._change_stream_manager.pull_stream(
                    stream, self._first_batch_size(cmd_copy)
                )
                return request_id, {
                    "ok": 1,
                    "cursor": {
                        "id": stream._id,
                        "ns": f"{db_name}.{coll_name}",
                        "nextBatch": next_batch,
                        "postBatchResumeToken": token,
                    },
                }
            data = self._getmore_data(
                cursor_id, self._first_batch_size(cmd_copy)
            )
            if data is not None:
                return request_id, {"ok": 1, "cursor": data}
            else:
                return request_id, {
                    "ok": 1,
                    "cursor": {
                        "id": 0,
                        "ns": f"{db_name}.{coll_name}",
                        "nextBatch": [],
                    },
                }

        if "killCursors" in cmd_copy:
            cursor_ids = cmd_copy.get("cursors", [])
            cursors_killed = []
            cursors_not_found = []
            data_ids = []
            for raw_cid in cursor_ids:
                try:
                    cid = int(raw_cid)
                except (TypeError, ValueError):
                    cursors_not_found.append(raw_cid)
                    continue
                if self._change_stream_manager.get_stream(cid) is not None:
                    self._change_stream_manager.close_stream(cid)
                    cursors_killed.append(cid)
                else:
                    data_ids.append(cid)
            if data_ids:
                with self._cursors_lock:
                    for cid in data_ids:
                        if cid in self._cursors:
                            del self._cursors[cid]
                            cursors_killed.append(cid)
                        else:
                            cursors_not_found.append(cid)
            return request_id, {
                "ok": 1,
                "cursorsKilled": cursors_killed,
                "cursorsNotFound": cursors_not_found,
                "cursorsAlive": [],
            }

        logger.info(f"Calling db.command with: {cmd_copy}")
        cmd_result = db.command(cmd_copy)
        logger.info(
            f"NeoSQLite returned: {list(cmd_result.keys()) if isinstance(cmd_result, dict) else type(cmd_result)}"
        )
        return request_id, cmd_result

    def _handle_gridfs_find(
        self,
        request_id: int,
        command_doc: dict,
        db: Connection,
        coll_name: str,
        db_name: str = "test",
    ) -> tuple[int, dict[str, Any]]:
        """Handle find command on GridFS collections (fs.files or fs.chunks)."""
        logger.debug(f"_handle_gridfs_find called with coll_name={coll_name}")

        if coll_name.endswith(".chunks"):
            return self._handle_gridfs_chunks_find(
                request_id, command_doc, db, coll_name, db_name
            )

        try:
            adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
            if adapter is None:
                return request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }

            filter_query = command_doc.get("filter", {})
            filter_query = self._convert_objectids(filter_query)

            skip = command_doc.get("skip", 0)
            sort = command_doc.get("sort", None)
            limit = command_doc.get("limit", 0)

            docs = adapter.handle_find(filter_query)

            if skip > 0:
                docs = docs[skip:]
            if sort:
                sort_list = (
                    list(sort.items()) if isinstance(sort, dict) else sort
                )
                # Stable multi-key sort honoring per-key direction
                # (1/-1). Missing keys sort Mongo-style: first in
                # ascending, last in descending.
                for sort_key, direction in reversed(list(sort_list)):
                    reverse = int(direction) < 0
                    present = [
                        doc for doc in docs if doc.get(sort_key) is not None
                    ]
                    missing = [
                        doc for doc in docs if doc.get(sort_key) is None
                    ]
                    try:
                        present.sort(
                            key=lambda doc: doc.get(sort_key),
                            reverse=reverse,
                        )
                    except TypeError:
                        present.sort(
                            key=lambda doc: str(doc.get(sort_key)),
                            reverse=reverse,
                        )
                    docs = (
                        missing + present if not reverse else present + missing
                    )

            if limit != 0:
                docs = docs[: abs(limit)]

            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": docs,
                },
            }
        except Exception as e:
            logger.error(f"GridFS find error: {e}")
            import traceback

            traceback.print_exc()
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_chunks_find(
        self,
        request_id: int,
        command_doc: dict,
        db: Connection,
        coll_name: str,
        db_name: str = "test",
    ) -> tuple[int, dict[str, Any]]:
        """Handle find command on fs.chunks collection."""
        try:
            adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
            if adapter is None:
                return request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }

            filter_query = command_doc.get("filter", {})
            filter_query = self._convert_objectids(filter_query)

            skip = command_doc.get("skip", 0)
            limit = command_doc.get("limit", 0)

            docs = adapter.handle_chunks_find(filter_query)

            if skip > 0:
                docs = docs[skip:]
            if limit != 0:
                docs = docs[: abs(limit)]

            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": docs,
                },
            }
        except Exception as e:
            logger.error(f"GridFS chunks find error: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_distinct(
        self,
        request_id: int,
        coll_name: str,
        key: str,
        query: dict[str, Any],
        db: Connection,
    ) -> tuple[int, dict[str, Any]]:
        """Distinct over a GridFS collection via the bucket adapter."""
        try:
            adapter, _bucket = create_gridfs_adapter(db.db, coll_name)
            if adapter is None:
                return request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }
            filt = self._convert_objectids(query or {})
            if coll_name.endswith(".chunks"):
                docs = adapter.handle_chunks_find(filt)
            else:
                docs = adapter.handle_find(filt)
            seen: list[Any] = []
            for doc in docs:
                value = doc.get(key) if isinstance(doc, dict) else None
                if value not in seen:
                    seen.append(value)
            return request_id, {"ok": 1, "values": seen}
        except Exception as exc:
            logger.error(f"GridFS distinct error: {exc}")
            return request_id, {"ok": 0, "errmsg": str(exc)}

    def _handle_gridfs_delete(
        self, request_id: int, cmd_copy: dict, db: Connection, coll_name: str
    ) -> tuple[int, dict[str, Any]]:
        """Handle delete command on GridFS collections."""
        logger.debug(f"_handle_gridfs_delete: coll_name={coll_name}")
        if coll_name.endswith(".chunks"):
            return request_id, {"ok": 1, "n": 0}
        if not coll_name.endswith(".files"):
            return request_id, {
                "ok": 0,
                "errmsg": "GridFS delete only supported on .files collections",
            }

        try:
            adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
            if adapter is None:
                return request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }

            deletes = cmd_copy.get("deletes", [])
            file_ids = []
            for delete in deletes:
                file_id = delete.get("q", {}).get("_id")
                if isinstance(file_id, dict) and "$eq" in file_id:
                    file_id = file_id["$eq"]
                if file_id:
                    file_ids.append(file_id)

            result = adapter.handle_delete(file_ids)
            return request_id, result
        except Exception as e:
            logger.error(f"GridFS delete error: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_upload(
        self, request_id: int, cmd_copy: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle GridFS upload commands."""
        try:
            filename = cmd_copy.get("filename")
            if not filename:
                return request_id, {
                    "ok": 0,
                    "errmsg": "filename required for GridFS upload",
                }

            bucket_name = cmd_copy.get("bucket", "fs")
            from neosqlite.gridfs import GridFSBucket

            bucket = GridFSBucket(db.db, bucket_name=bucket_name)

            metadata = cmd_copy.get("metadata", {})
            chunk_size = cmd_copy.get("chunkSize")

            grid_in = bucket.open_upload_stream(
                filename,
                chunk_size_bytes=chunk_size,
                metadata=metadata if metadata else None,
            )

            data = cmd_copy.get("data")
            if data:
                grid_in.write(data)
            grid_in.close()

            return request_id, {"ok": 1, "fileId": grid_in._file_id}
        except Exception as e:
            logger.error(f"GridFS find error: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_download(
        self, request_id: int, cmd_copy: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle GridFS download commands."""
        try:
            file_id = cmd_copy.get("fileId")
            if not file_id:
                return request_id, {
                    "ok": 0,
                    "errmsg": "fileId required for GridFS download",
                }

            bucket_name = cmd_copy.get("bucket", "fs")
            from neosqlite.gridfs import GridFSBucket

            bucket = GridFSBucket(db.db, bucket_name=bucket_name)

            grid_out = bucket.open_download_stream(file_id)
            data = grid_out.read()

            return request_id, {"ok": 1, "data": data}
        except Exception as e:
            logger.error(f"GridFS download error: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_bulk_write(
        self, request_id: int, cmd: dict, command_doc: dict
    ) -> tuple[int, dict[str, Any]]:
        """Handle the client-level bulkWrite command (MongoDB 8.0+).

        Fans each op out to the target namespace's collection. Ops may span
        databases; each runs under its own database session (cross-file
        atomicity is unsupported and documented).
        """
        ns_info = cmd.get("nsInfo", [])
        namespaces = [
            entry.get("ns", "") if isinstance(entry, dict) else ""
            for entry in ns_info
        ]
        ops = cmd.get("ops", [])
        ordered = cmd.get("ordered", True)
        errors_only = cmd.get("errorsOnly", False)

        results: list[dict[str, Any]] = []
        n_inserted = 0
        n_matched = 0
        n_modified = 0
        n_upserted = 0
        n_deleted = 0
        n_errors = 0

        for idx, op in enumerate(ops):
            if not isinstance(op, dict):
                continue
            try:
                if "insert" in op:
                    ns_idx = int(op["insert"])
                    doc = self._convert_objectids(op.get("document", {}))
                    ns_db, _, ns_coll = namespaces[ns_idx].partition(".")
                    target = self.get_database(ns_db or "test")
                    sess = self._get_or_create_session(
                        command_doc, target, ns_db or "test"
                    )
                    target[ns_coll].insert_one(doc, session=sess)
                    n_inserted += 1
                    if not errors_only:
                        results.append({"ok": 1, "idx": idx, "n": 1})
                elif "update" in op:
                    ns_idx = int(op["update"])
                    filt = self._convert_objectids(op.get("filter", {}))
                    mods = self._convert_objectids(op.get("updateMods", {}))
                    multi = bool(op.get("multi", False))
                    upsert = bool(op.get("upsert", False))
                    array_filters = op.get("arrayFilters")
                    ns_db, _, ns_coll = namespaces[ns_idx].partition(".")
                    target = self.get_database(ns_db or "test")
                    sess = self._get_or_create_session(
                        command_doc, target, ns_db or "test"
                    )
                    coll = target[ns_coll]
                    is_replace = not any(
                        k.startswith("$") for k in mods.keys()
                    )
                    if is_replace:
                        res = coll.replace_one(
                            filt, mods, upsert=upsert, session=sess
                        )
                    elif multi:
                        res = coll.update_many(
                            filt,
                            mods,
                            upsert=upsert,
                            array_filters=array_filters,
                            session=sess,
                        )
                    else:
                        res = coll.update_one(
                            filt,
                            mods,
                            upsert=upsert,
                            array_filters=array_filters,
                            session=sess,
                        )
                    n_matched += res.matched_count
                    n_modified += res.modified_count
                    if res.upserted_id is not None:
                        n_matched += 1
                        n_upserted += 1
                    if not errors_only:
                        entry: dict[str, Any] = {
                            "ok": 1,
                            "idx": idx,
                            "n": res.matched_count,
                            "nModified": res.modified_count,
                        }
                        if res.upserted_id is not None:
                            entry["upserted"] = {"_id": res.upserted_id}
                        results.append(entry)
                elif "delete" in op:
                    ns_idx = int(op["delete"])
                    filt = self._convert_objectids(op.get("filter", {}))
                    multi = bool(op.get("multi", False))
                    ns_db, _, ns_coll = namespaces[ns_idx].partition(".")
                    target = self.get_database(ns_db or "test")
                    sess = self._get_or_create_session(
                        command_doc, target, ns_db or "test"
                    )
                    coll = target[ns_coll]
                    if multi:
                        res = coll.delete_many(filt, session=sess)
                    else:
                        res = coll.delete_one(filt, session=sess)
                    n_deleted += res.deleted_count
                    if not errors_only:
                        results.append(
                            {"ok": 1, "idx": idx, "n": res.deleted_count}
                        )
                else:
                    raise ValueError(f"Unsupported bulkWrite op: {sorted(op)}")
            except Exception as exc:
                err = _bulk_write_error(exc, idx)
                n_errors += 1
                results.append(
                    {
                        "ok": 0,
                        "idx": idx,
                        "code": err["code"],
                        "errmsg": err["errmsg"],
                        "n": 0,
                    }
                )
                if ordered:
                    break

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": 0,
                "ns": "admin.$cmd.bulkWrite",
                "firstBatch": results,
            },
            "nErrors": n_errors,
            "nInserted": n_inserted,
            "nMatched": n_matched,
            "nModified": n_modified,
            "nUpserted": n_upserted,
            "nDeleted": n_deleted,
            "error": None,
            "writeErrors": [],
        }

    def _handle_delete(
        self,
        request_id: int,
        command_doc: dict,
        db: Connection,
        db_name: str = "test",
    ) -> tuple[int, dict[str, Any]]:
        coll_name = command_doc.get("delete")
        if not coll_name:
            for key in command_doc:
                if key not in (
                    "$db",
                    "deletes",
                    "ordered",
                    "writeConcern",
                    "lsid",
                ) and not key.startswith("$"):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        coll = db[coll_name]
        session_to_use = self._get_or_create_session(
            command_doc, db, db_name
        )
        deletes = command_doc.get("deletes", [])
        ordered = command_doc.get("ordered", True)

        removed = 0
        write_errors: list[dict[str, Any]] = []
        for idx, delete in enumerate(deletes):
            q = delete.get("q", {})
            q = self._convert_objectids(q)
            limit = delete.get("limit", 0)

            try:
                result = (
                    coll.delete_many(q, session=session_to_use)
                    if limit == 0
                    else coll.delete_one(q, session=session_to_use)
                )
            except Exception as exc:
                write_errors.append(_bulk_write_error(exc, idx))
                if ordered:
                    break
                continue
            removed += result.deleted_count

        response = {"ok": 1, "n": removed}
        if write_errors:
            response["writeErrors"] = write_errors
        return request_id, response

    def _handle_change_stream(
        self,
        request_id: int,
        coll_name: str,
        pipeline: list[dict],
        db: Connection,
        db_name: str = "test",
        owner: Any = None,
    ) -> tuple[int, dict[str, Any]]:
        """Handle change stream aggregate command.

        Tracking is trigger-backed (see changestream module): opening the
        stream installs SQLite triggers via a persistent NeoSQLite watcher,
        and getMore pulls new rows. Dotted names (e.g. GridFS ``fs.files``)
        cannot have triggers and yield empty streams.
        """
        try:
            options = extract_change_stream_options(pipeline)
            stream = self._change_stream_manager.create_stream(
                collection_name=coll_name,
                pipeline=pipeline,
                resume_after=options.get("resume_after"),
                start_at_operation_time=options.get("start_at_operation_time"),
                full_document=options.get("full_document"),
                db_name=db_name,
                owner=owner,
                start_after=options.get("start_after"),
                get_collection=lambda: db[coll_name],
            )

            # Return empty batch initially - change streams start empty
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": stream._id,  # Use stream ID as cursor ID
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": [],
                    "postBatchResumeToken": stream.get_resume_token(),
                },
            }
        except Exception as e:
            logger.error(f"Error in change stream: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def close_streams_for_connection(self, conn_id: Any) -> None:
        """Close all change streams opened by a given client connection.

        Called when a client disconnects so its change streams (and their
        registered listeners) do not leak for the life of the process.
        Data cursors owned by the connection are dropped as well.
        """
        if conn_id is None:
            return
        self._change_stream_manager.close_streams_for_owner(conn_id)
        self.close_cursors_for_owner(conn_id)

    def _handle_server_status(
        self, request_id: int, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle serverStatus command."""
        import os
        import platform
        from datetime import datetime, timezone

        resident_mem = 0
        virtual_mem = 0
        if platform.system() == "Linux":
            try:
                with open("/proc/self/statm") as statm:
                    parts = statm.read().split()
                page_size = os.sysconf("SC_PAGE_SIZE")
                virtual_mem = int(parts[0]) * page_size
                resident_mem = int(parts[1]) * page_size
            except (OSError, ValueError, IndexError):
                pass
        if resident_mem == 0 and virtual_mem == 0:
            try:
                import resource

                rusage = resource.getrusage(resource.RUSAGE_SELF)
                if (
                    platform.system() == "Darwin"
                    or platform.system().startswith("FreeBSD")
                ):
                    resident_mem = rusage.ru_maxrss
                    virtual_mem = rusage.ru_maxrss
                else:
                    resident_mem = rusage.ru_maxrss * 1024
                    virtual_mem = rusage.ru_maxrss * 1024
            except (ImportError, AttributeError):
                resident_mem = 0
                virtual_mem = 0

        uptime_seconds = time.time() - self.start_time
        uptime_millis = int(uptime_seconds * 1000)

        with self._connections_lock:
            current_connections = self._active_connections

        return request_id, {
            "ok": 1,
            "host": platform.node(),
            "version": "7.0.0",
            "process": "nx_27017",
            "pid": os.getpid(),
            "uptime": int(uptime_seconds),
            "uptimeMillis": uptime_millis,
            "uptimeEstimate": int(uptime_seconds),
            "localTime": datetime.now(timezone.utc),
            "asserts": {
                "regular": 0,
                "warning": 0,
                "msg": 0,
                "user": 0,
                "rollovers": 0,
            },
            "connections": {
                "current": current_connections,
                "available": DEFAULT_MAX_CONNECTIONS,
            },
            "mem": {
                "bits": 64,
                "resident": resident_mem,
                "virtual": virtual_mem,
            },
            "globalLock": {"totalTime": 0},
        }

    def _handle_list_indexes(
        self,
        request_id: int,
        db: Connection,
        coll_name: str | None,
        db_name: str = "test",
    ) -> tuple[int, dict[str, Any]]:
        """Handle listIndexes command."""
        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        try:
            coll = db[coll_name]
        except Exception:
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": [],
                },
            }

        index_list = [{"v": 2, "key": {"_id": 1}, "name": "_id_"}]
        try:
            info_dict = coll.index_information()
            prefix = f"idx_{coll.name}_"
            for idx_name, info in info_dict.items():
                key = info.get("key")
                if not key:
                    if idx_name in (f"{prefix}id", "_id_"):
                        key = {"_id": 1}
                    elif idx_name.startswith(prefix):
                        key_str = idx_name[len(prefix) :]
                        key = {key_str: 1}
                    else:
                        key = {idx_name: 1}
                entry: dict[str, Any] = {
                    "v": info.get("v", 2),
                    "key": key,
                    "name": idx_name,
                }
                if info.get("unique"):
                    entry["unique"] = True
                if "expireAfterSeconds" in info:
                    entry["expireAfterSeconds"] = info["expireAfterSeconds"]
                index_list.append(entry)
        except Exception as e:
            logger.debug(f"Failed to get index_information: {e}")

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": 0,
                "ns": f"{db_name}.{coll_name}",
                "firstBatch": index_list,
            },
        }

    def _handle_list_search_indexes(
        self,
        request_id: int,
        db: Connection,
        coll_name: str | None,
        db_name: str = "test",
    ) -> tuple[int, dict[str, Any]]:
        """Handle listSearchIndexes command."""
        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        try:
            coll = db[coll_name]
        except Exception:
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": [],
                },
            }

        search_indexes = coll.list_search_indexes()
        index_list = []
        for idx_name in search_indexes:
            index_list.append(
                {"v": 2, "key": {idx_name: "text"}, "name": f"{idx_name}_text"}
            )

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": 0,
                "ns": f"{db_name}.{coll_name}",
                "firstBatch": index_list,
            },
        }

    def _handle_list_collections(
        self, request_id: int, db: Connection, db_name: str = "test"
    ) -> tuple[int, dict[str, Any]]:
        """Handle listCollections command."""
        coll_names = db.list_collection_names()
        collections = []
        for name in coll_names:
            collections.append(
                {
                    "name": name,
                    "type": "collection",
                    "options": {},
                    "info": {
                        "readOnly": False,
                        "uuid": "00000000-0000-0000-0000-000000000000",
                    },
                }
            )

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": 0,
                "ns": f"{db_name}.$cmd.listCollections",
                "firstBatch": collections,
            },
        }

    @_serialize
    def handle_query(
        self, msg: dict[str, Any]
    ) -> tuple[int, list[dict[str, Any]]]:
        query = msg["query"]
        collection = msg["collection"]
        db_name = msg.get("db", "admin")

        db = self.get_database(db_name)

        if (
            not collection
            or collection == "$cmd"
            or collection.endswith(".$cmd")
        ):
            if "$query" in query:
                query = query["$query"]

            result = self.handle_command(
                {
                    "request_id": msg["request_id"],
                    "sections": [("body", query)],
                    "_conn_id": msg.get("_conn_id"),
                }
            )
            return result[0], [result[1]]

        skip = msg.get("number_to_skip", 0)
        limit = msg.get("number_to_return", 0)

        if self._is_gridfs_collection(collection):
            command_doc = dict(query)
            command_doc["find"] = collection
            if skip > 0:
                command_doc["skip"] = skip
            if limit != 0:
                command_doc["limit"] = limit
            _, response = self._handle_gridfs_find(
                msg["request_id"], command_doc, db, collection, db_name
            )
            docs = response.get("cursor", {}).get("firstBatch", [])
            return msg["request_id"], docs

        coll = db[collection]

        if "$query" in query:
            filter_query = query.get("$query", {})
            sort = query.get("$orderby", {})
            query_limit = query.get("$limit", limit)

            cursor = coll.find(filter_query)
            if sort:
                cursor = cursor.sort(list(sort.items()))
            if skip > 0:
                cursor = cursor.skip(skip)
            if query_limit != 0:
                cursor = cursor.limit(query_limit)
            docs = list(cursor)
        else:
            cursor = coll.find(query)
            if skip > 0:
                cursor = cursor.skip(skip)
            if limit != 0:
                cursor = cursor.limit(limit)
            docs = list(cursor)

        return msg["request_id"], docs
