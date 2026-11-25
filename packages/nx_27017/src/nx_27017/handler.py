"""NeoSQLite handler for MongoDB commands and SQLite operations."""

import contextlib
import functools
import logging
import os
import platform
import re
import secrets
import threading
import time
import uuid
import weakref
from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self

from bson import BSON
from bson.int64 import Int64

from neosqlite import Connection
from neosqlite.collection.type_utils import _hashable_group_key
from nx_27017.changestream import (
    ChangeStreamManager,
    extract_change_stream_options,
    is_change_stream_pipeline,
)
from nx_27017.gridfs_adapter import (
    _get_gridfs_bucket_name,
    _is_gridfs_collection,
    create_gridfs_adapter,
    is_regular_collection_table,
)
from nx_27017.utils import convert_neo_to_bson_objectids
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

# Default first-batch size when the client sends no batchSize (matches the
# MongoDB server default of up to 101 documents in the first batch).
_DEFAULT_BATCH_SIZE = 101

# Cap on simultaneously open per-file database connections (files mode).
# Least-recently-used databases are closed on demand; their files stay on
# disk and are reopened transparently. Memory mode never evicts (closing
# an in-memory database would destroy its data).
_MAX_OPEN_DBS = 100

# Upper bounds and idle timeouts for server-side state maps.
_DEFAULT_CURSOR_TIMEOUT_SECONDS = 600.0
_MAX_CURSORS = 10_000
_MAX_SESSIONS = 10_000
_MAX_SINGLE_MODE_DBS = 1_000
_DEFAULT_TX_TIMEOUT_SECONDS = 5.0


def _generate_cursor_id() -> int:
    """Return a non-zero unpredictable 63-bit cursor identifier."""
    return secrets.randbits(63) or 1


class CrossDBTransactionError(ValueError):
    """A transactional session touched a database other than its own.

    SQLite files cannot share a transaction, so a session (lsid) with an
    open transaction may only operate on the database where the
    transaction began; anything else gets an explicit error instead of
    silently executing outside the transaction.
    """


def _search_index_fields(spec: dict[str, Any]) -> list[str]:
    """Extract FTS field names from an Atlas-style search index spec.

    Understands ``definition.mappings.fields`` maps, ``definition.fields``
    lists, ``definition.field`` or plain-string definitions, and falls back
    to the spec ``name``.

    Args:
        spec: Atlas-style search index specification dictionary.

    Returns:
        list[str]: Non-empty list of indexed field names.

    Raises:
        ValueError: When no text field name can be derived from ``spec``.
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
    """Build a MongoDB-style writeError document for a failed bulk op.

    Maps SQLite ``IntegrityError`` or ``UNIQUE`` constraint messages to
    MongoDB duplicate-key error code ``11000``, and all other failures to
    error code ``8``.

    Args:
        exc: Exception raised by the underlying collection write.
        index: Zero-based index of the failing operation in the batch.

    Returns:
        dict[str, Any]: Dictionary containing ``index``, ``code``, and
            ``errmsg`` keys.
    """
    from neosqlite._sqlite import sqlite3

    message = str(exc)
    upper = message.upper()
    if "UNIQUE" in upper or isinstance(exc, sqlite3.IntegrityError):
        code = 11000
    elif "LOCKED" in upper or "BUSY" in upper:
        code = 112
    else:
        code = 8
    return {"index": index, "code": code, "errmsg": message}


def _sanitize_db_name(db_name: Any) -> str:
    """Validate a logical database name for file mapping.

    Args:
        db_name: Candidate database name to validate against ``_DB_NAME_RE``.

    Returns:
        str: The validated database name unchanged.

    Raises:
        ValueError: If ``db_name`` is not a safe alphanumeric/dash/underscore
            file stem matching ``_DB_NAME_RE``.
    """
    if not isinstance(db_name, str) or not _DB_NAME_RE.match(db_name):
        raise ValueError(f"Invalid database name: {db_name!r}")
    return db_name


def _msg_db_name(msg: Any) -> str:
    """Extract best-effort logical db name from a wire message for locking.

    Mirrors the defaults used by the handlers themselves (``$db`` defaulting
    to ``"test"`` for ``OP_MSG`` commands, ``"db"`` defaulting to ``"admin"``
    for legacy ``OP_QUERY``).

    Args:
        msg: Decoded wire-protocol message dictionary (or arbitrary value).

    Returns:
        str: Logical database name, defaulting to ``"test"`` on fallback.
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
    except Exception as exc:
        logger.debug(f"Could not infer database name from wire message: {exc}")
    return "test"


@dataclass
class _CommandContext:
    """Normalized inputs for a dispatched ``OP_MSG`` command handler."""

    request_id: int
    cmd_copy: dict[str, Any]
    command_doc: dict[str, Any]
    db: Connection
    db_name: str
    msg: dict[str, Any]
    payload_updates: list[dict[str, Any]]
    payload_deletes: list[dict[str, Any]]
    payload_ops: list[dict[str, Any]]
    payload_nsinfo: list[dict[str, Any]]


def _msg_body_doc(msg: Any) -> dict[str, Any] | None:
    """Return the first ``body`` command document from a wire message."""
    if isinstance(msg, dict):
        sections = msg.get("sections")
        if sections:
            for section_type, doc in sections:
                if section_type == "body" and isinstance(doc, dict):
                    return doc
    return None


def _msg_session_info(msg: Any) -> tuple[str | None, bool]:
    """Extract ``(session_id, starts_transaction)`` from an ``OP_MSG``."""
    doc = _msg_body_doc(msg)
    if doc is None:
        return None, False
    lsid = doc.get("lsid")
    if not isinstance(lsid, dict):
        return None, False
    return _extract_session_id(lsid), bool(doc.get("startTransaction"))


def _serialize(func):
    """Serialize access to the SQLite connection(s).

    Single-file (legacy) mode uses one global ``RLock`` for main connection
    access, while multi-DB mode uses one ``RLock`` per logical database so
    different files can proceed in parallel. Operations running on a
    session's dedicated transactional connection serialize on that session's
    lock instead of competing with non-transactional operations on the main
    connection, preventing lock-order deadlocks during ``commitTransaction``
    and ``abortTransaction``.

    Lock ordering: ``admin`` lock -> per-db lock -> per-session lock ->
    ``_dict_lock`` -> ``_sessions_lock``. Never acquire a per-db or session
    lock while holding ``_dict_lock`` or ``_sessions_lock``.

    Args:
        func: Handler method whose first argument after ``self`` is a wire
            message dictionary.

    Returns:
        Callable: Wrapped method protected by the appropriate lock and
            active-operation reference count.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        """Acquire the target database/session lock and invoke ``func``.

        Args:
            *args: Positional arguments forwarded to ``func``; ``args[0]``
                is inspected for the target database name and session.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            Any: Return value of ``func(self, *args, **kwargs)``.
        """
        msg = args[0] if args else {}
        db_name = _msg_db_name(msg) if isinstance(msg, dict) else "test"
        session_id, starts_tx = _msg_session_info(msg)
        use_session_lock = bool(
            session_id
            and (
                starts_tx or self._session_has_dedicated_tx(session_id, db_name)
            )
        )
        lock = (
            self._lock_for_session(session_id)
            if use_session_lock and session_id is not None
            else self._lock_for(db_name)
        )
        with lock, self._track_active_op(db_name):
            return func(self, *args, **kwargs)

    return wrapper


class NeoSQLiteHandler:
    """Handle MongoDB commands and translate them to SQLite operations.

    Supports single-file legacy mode, directory-backed per-database SQLite
    files (``files`` mode with LRU connection eviction), and isolated
    in-memory databases (``memory`` mode). Coordinates per-database reentrant
    locks, client sessions and transactions, cursor batching, GridFS bucket
    operations, and trigger-backed change streams across ``OP_MSG`` and
    legacy ``OP_QUERY`` wire commands.
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        tokenizers: list | None = None,
        journal_mode: str = "WAL",
        data_dir: str | None = None,
        single_db_compat: bool = False,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        session_timeout_seconds: float = DEFAULT_SESSION_TIMEOUT_MINUTES * 60,
        cursor_timeout_seconds: float = _DEFAULT_CURSOR_TIMEOUT_SECONDS,
        tx_timeout_seconds: float = _DEFAULT_TX_TIMEOUT_SECONDS,
    ):
        """Create the handler.

        Args:
            db_path: Legacy single-file path or ``":memory:"``. A directory
                path enables multi-file mode (one ``<db>.db`` per logical db).
            tokenizers: Optional list of FTS tokenizer configurations passed
                to each ``neosqlite.Connection``.
            journal_mode: SQLite journal mode string (such as ``"WAL"``)
                applied to opened connections.
            data_dir: Explicit multi-file base directory. Takes precedence
                over a file-style ``db_path``. ``"memory"`` selects isolated
                in-memory databases.
            single_db_compat: Force legacy behavior (all logical databases
                share one SQLite connection) even for file paths.
            max_connections: Maximum simultaneous client connections allowed.
            session_timeout_seconds: Idle timeout before server-side sessions
                are automatically expired and cleaned up.
            cursor_timeout_seconds: Idle timeout before abandoned data cursors
                are automatically expired.
            tx_timeout_seconds: SQLite lock/transaction wait timeout in seconds.
        """
        self.tokenizers = tokenizers
        self.journal_mode = journal_mode
        self.start_time = time.time()
        self._max_connections = max(1, int(max_connections))
        self._session_timeout_seconds = max(0.0, float(session_timeout_seconds))
        self._cursor_timeout_seconds = max(0.0, float(cursor_timeout_seconds))
        self._tx_timeout_seconds = max(0.05, float(tx_timeout_seconds))
        self._active_connections = 0
        self._connections_lock = threading.Lock()
        # Sessions are keyed by (session_id, logical db name): SQLite files
        # cannot share a transaction, so there is no cross-db session.
        self._sessions: dict[tuple[str, str], Any] = {}
        self._session_tx_conns: dict[tuple[str, str], Connection] = {}
        self._session_last_used: dict[tuple[str, str], float] = {}
        # RLock: _find_session() re-acquires it when callers already hold it.
        self._sessions_lock = threading.RLock()
        self._tx_cond = threading.Condition(self._sessions_lock)
        self._session_locks: weakref.WeakValueDictionary[
            str, threading.RLock
        ] = weakref.WeakValueDictionary()

        # SQLite connections are not safe for concurrent use from multiple
        # threads, so every public handler entry point serializes access
        # through a reentrant lock. check_same_thread=False allows each
        # connection to be touched from whichever worker thread runs it, while
        # the lock guarantees only one operation uses a connection at a time.
        self._db_lock = threading.RLock()
        self._dict_lock = threading.Lock()
        self._locks: weakref.WeakValueDictionary[str, threading.RLock] = (
            weakref.WeakValueDictionary()
        )
        self._active_ops: dict[str, int] = {}
        self._applied_wc: dict[str, str] = {}
        # Unique per-handler namespace for shared-cache memory URIs so separate
        # handlers in the same process never collide on logical database names.
        self._mem_namespace = uuid.uuid4().hex[:12]
        self._mem_generations: dict[str, int] = {}
        # Live data cursors for find/aggregate pagination:
        # id -> {"ns": str, "db": str, "docs": list, "owner": Any,
        #         "last_used": float}. Guarded by _cursors_lock.
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

        # Open per-file connections. In files mode this doubles as an
        # LRU ring (access order); see _evict_lru_locked for the cap.
        self._conns: OrderedDict[str, Connection] = OrderedDict()
        if self._mode == "single":
            if db_path in (":memory:", "memory"):
                self._single_mem_uri: str | None = (
                    f"file:nx_{self._mem_namespace}_single"
                    "?mode=memory&cache=shared"
                )
                self.conn = Connection(
                    self._single_mem_uri,
                    check_same_thread=False,
                    uri=True,
                    timeout=self._tx_timeout_seconds,
                    tokenizers=tokenizers,
                    journal_mode=journal_mode,
                )
            else:
                self._single_mem_uri = None
                self.conn = Connection(
                    db_path,
                    check_same_thread=False,
                    timeout=self._tx_timeout_seconds,
                    tokenizers=tokenizers,
                    journal_mode=journal_mode,
                )
            self._conns["admin"] = self.conn
        else:
            self._single_mem_uri = None
            # Default connection for backward compatibility (tests and
            # sessions use h.conn). Created lazily per logical db below,
            # starting with "admin".
            self.conn = self._open_db_conn("admin")
            self._conns["admin"] = self.conn
        self.databases: OrderedDict[str, Connection] = OrderedDict(self._conns)
        self._change_stream_manager = ChangeStreamManager()

    def _lock_for(self, db_name: str) -> threading.RLock:
        """Return the lock guarding a logical database.

        Validates ``db_name`` before allocating any entry in ``self._locks`` so
        untrusted/invalid database names cannot grow the lock table.

        Args:
            db_name: Logical database name.

        Returns:
            threading.RLock: Global ``_db_lock`` in single-file mode or the
                per-database ``RLock`` otherwise.

        Raises:
            ValueError: If ``db_name`` fails ``_sanitize_db_name`` in multi-DB
                mode.
        """
        if self._mode == "single":
            return self._db_lock
        safe = _sanitize_db_name(db_name)
        with self._dict_lock:
            lock = self._locks.get(safe)
            if lock is None:
                lock = threading.RLock()
                self._locks[safe] = lock
            return lock

    def _lock_for_session(self, session_id: str) -> threading.RLock:
        """Return the reentrant lock guarding a transactional session."""
        with self._sessions_lock:
            lock = self._session_locks.get(session_id)
            if lock is None:
                lock = threading.RLock()
                self._session_locks[session_id] = lock
            return lock

    def _session_has_dedicated_tx(self, session_id: str, db_name: str) -> bool:
        """Return whether ``session_id`` currently holds a dedicated tx handle."""
        with self._sessions_lock:
            if not self._session_tx_conns:
                return False
            scope = self._session_scope(db_name)
            if (session_id, scope) in self._session_tx_conns:
                return True
            if db_name == "admin":
                return any(
                    sid == session_id for sid, _sc in self._session_tx_conns
                )
            return False

    @contextlib.contextmanager
    def _track_active_op(self, db_name: str) -> Iterator[None]:
        """Reference-count an in-flight operation so LRU eviction skips it."""
        with self._dict_lock:
            self._active_ops[db_name] = self._active_ops.get(db_name, 0) + 1
        try:
            yield
        finally:
            with self._dict_lock:
                remaining = self._active_ops.get(db_name, 1) - 1
                if remaining <= 0:
                    self._active_ops.pop(db_name, None)
                else:
                    self._active_ops[db_name] = remaining

    def _db_file(self, db_name: str) -> str:
        """Return the SQLite file path backing a logical database.

        Intended for ``files`` mode only (requires ``self._data_dir``).

        Args:
            db_name: Sanitized logical database name.

        Returns:
            str: Filesystem path ``<data_dir>/<db_name>.db``.
        """
        assert self._data_dir is not None
        return os.path.join(self._data_dir, f"{db_name}.db")

    def _memory_uri(self, safe_db_name: str) -> str:
        """Return the handler-scoped shared-cache memory URI for a database."""
        gen = self._mem_generations.get(safe_db_name, 0)
        return (
            f"file:nx_{self._mem_namespace}_{safe_db_name}_{gen}"
            "?mode=memory&cache=shared"
        )

    def _open_db_conn(self, db_name: str) -> Connection:
        """Open (but not yet cache) the connection for a logical database.

        Args:
            db_name: Logical database name to validate and open.

        Returns:
            Connection: Newly opened ``neosqlite.Connection`` instance.

        Raises:
            ValueError: If ``db_name`` fails ``_sanitize_db_name``.
        """
        safe = _sanitize_db_name(db_name)
        if self._mode == "memory":
            return Connection(
                self._memory_uri(safe),
                check_same_thread=False,
                uri=True,
                timeout=self._tx_timeout_seconds,
                tokenizers=self.tokenizers,
                journal_mode=self.journal_mode,
            )
        return Connection(
            self._db_file(safe),
            check_same_thread=False,
            timeout=self._tx_timeout_seconds,
            tokenizers=self.tokenizers,
            journal_mode=self.journal_mode,
        )

    def _open_tx_conn(self, db_name: str) -> Connection:
        """Open a dedicated transactional ``Connection`` for ``db_name``.

        Each active transactional session uses its own SQLite connection handle
        so uncommitted writes are isolated from non-transactional operations on
        the main handle and ``abortTransaction`` cannot wipe other clients'
        acknowledged writes.
        """
        if self._mode == "single":
            if self._single_mem_uri is not None:
                return Connection(
                    self._single_mem_uri,
                    check_same_thread=False,
                    uri=True,
                    timeout=self._tx_timeout_seconds,
                    tokenizers=self.tokenizers,
                    journal_mode=self.journal_mode,
                )
            return Connection(
                self.db_path,
                check_same_thread=False,
                timeout=self._tx_timeout_seconds,
                tokenizers=self.tokenizers,
                journal_mode=self.journal_mode,
            )
        return self._open_db_conn(db_name)

    def get_database(self, db_name: str) -> Connection:
        """Return the ``Connection`` for ``db_name``, opening it if needed.

        In ``single`` mode all logical database names alias ``self.conn`` and
        ``self.databases`` is capped to avoid unbounded growth.
        In ``files`` or ``memory`` mode validates ``db_name``, looks up or
        opens a dedicated connection under ``_dict_lock``, updates LRU order,
        and triggers ``_evict_lru_locked`` when opening a new database.

        Args:
            db_name: Logical database name.

        Returns:
            Connection: Active connection backing ``db_name``.

        Raises:
            ValueError: If ``db_name`` is invalid.
        """
        safe = _sanitize_db_name(db_name)
        if self._mode == "single":
            with self._dict_lock:
                if safe in self.databases:
                    self.databases.move_to_end(safe)
                else:
                    self.databases[safe] = self.conn
                    while len(self.databases) > _MAX_SINGLE_MODE_DBS:
                        evict_name = next(
                            (k for k in self.databases if k != "admin"), None
                        )
                        if evict_name is None:
                            break
                        self.databases.pop(evict_name, None)
                return self.databases[safe]
        with self._dict_lock:
            conn = self._conns.get(safe)
            if conn is None:
                conn = self._open_db_conn(safe)
                self._conns[safe] = conn
                self.databases[safe] = conn
                self._evict_lru_locked()
            else:
                self._conns.move_to_end(safe)
            return conn

    def _conn_is_busy(self, db_name: str) -> bool:
        """Return whether a database has in-flight ops, a tx, or watch streams.

        Busy databases must not be evicted by ``_evict_lru_locked``. Called
        with ``_dict_lock`` held.

        Args:
            db_name: Logical database name to inspect.

        Returns:
            bool: ``True`` if an operation is in flight, a session on
                ``db_name`` has an open transaction, or active change stream
                activity exists.
        """
        if self._active_ops.get(db_name, 0) > 0:
            return True
        with self._sessions_lock:
            if any(
                key[1] == db_name and session.in_transaction
                for key, session in self._sessions.items()
            ):
                return True
            if any(key[1] == db_name for key in self._session_tx_conns):
                return True
        with self._cursors_lock:
            if any(
                entry.get("db") == db_name and entry.get("cursor") is not None
                for entry in self._cursors.values()
            ):
                return True
        return self._change_stream_manager.has_activity(db_name)

    def _evict_db_locked(self, db_name: str) -> None:
        """Close and forget one evicted connection (holds ``_dict_lock``).

        Ends any cached idle sessions bound to ``db_name``, clears its cached
        write concern state, and closes the connection, ignoring cleanup
        exceptions.

        Args:
            db_name: Logical database name to evict from ``_conns``.
        """
        conn = self._conns.pop(db_name, None)
        self.databases.pop(db_name, None)
        self._applied_wc.pop(db_name, None)
        with self._sessions_lock:
            for sess_key in [
                existing
                for existing in self._sessions
                if existing[1] == db_name
            ]:
                idle_session = self._sessions.pop(sess_key, None)
                self._session_last_used.pop(sess_key, None)
                tx_conn = self._session_tx_conns.pop(sess_key, None)
                self._safe_end_session(idle_session, f"evict {db_name}")
                self._safe_close_conn(tx_conn, f"evict tx {db_name}")
        self._safe_close_conn(conn, f"evict {db_name}")

    def _evict_lru_locked(self) -> None:
        """Cap open file connections, evicting least-recently-used dbs.

        Memory mode never evicts: closing a ``:memory:`` database would
        destroy its data, and listDatabases scans the cache. Called with
        ``_dict_lock`` held.
        """
        if self._mode != "files":
            return
        while len(self._conns) > _MAX_OPEN_DBS:
            victim = None
            for db_name in self._conns:
                if db_name == "admin" or self._conn_is_busy(db_name):
                    continue
                victim = db_name
                break
            if victim is None:
                # Every logged database is in use (active operation, open
                # transaction, or active change stream): stay over the cap
                # until it drains rather than dropping live state.
                return
            self._evict_db_locked(victim)

    def close_all(self) -> None:
        """Close every open database connection for shutdown.

        Clears cached connection, session, cursor, and write-concern maps,
        invalidates all change streams, and closes every connection including
        dedicated transactional connections and ``self.conn`` while ignoring
        close errors.
        """
        with self._sessions_lock:
            sessions = list(self._sessions.values())
            tx_conns = list(self._session_tx_conns.values())
            self._sessions.clear()
            self._session_tx_conns.clear()
            self._session_last_used.clear()
            self._tx_cond.notify_all()
        for sess in sessions:
            self._safe_end_session(sess, "close_all session")
        for tx_conn in tx_conns:
            self._safe_close_conn(tx_conn, "close_all tx")
        with self._cursors_lock:
            cursor_entries = [
                (entry.get("db"), entry.get("cursor"))
                for entry in self._cursors.values()
            ]
            self._cursors.clear()
        for _c_db, cursor_obj in cursor_entries:
            self._close_cursor_obj(cursor_obj)
        with self._dict_lock:
            conns = list(self._conns.values())
            self._conns.clear()
            self.databases.clear()
            self._applied_wc.clear()
        self._change_stream_manager.invalidate()
        for conn in conns:
            self._safe_close_conn(conn, "close_all db")
        if hasattr(self, "conn"):
            self._safe_close_conn(self.conn, "close_all primary")

    def __enter__(self) -> Self:
        """Enter the handler context manager.

        Returns:
            Self: This ``NeoSQLiteHandler`` instance.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager and close all database connections.

        Args:
            exc_type: Exception class if raised within the context, else
                ``None``.
            exc_val: Exception instance if raised within the context, else
                ``None``.
            exc_tb: Traceback object if raised within the context, else
                ``None``.
        """
        self.close_all()

    def __del__(self) -> None:
        """Close open database connections on garbage collection."""
        try:
            self.close_all()
        except Exception as exc:
            logger.debug(f"Ignoring cleanup error during __del__: {exc}")

    @staticmethod
    def _safe_end_session(session: Any, context: str = "session") -> None:
        """End a NeoSQLite session quietly while debug-logging any error."""
        if session is not None:
            try:
                session.end_session()
            except Exception as exc:
                logger.debug(f"Ignoring {context} cleanup error: {exc}")

    @staticmethod
    def _safe_close_conn(conn: Any, context: str = "connection") -> None:
        """Close a NeoSQLite connection quietly while debug-logging errors."""
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.debug(f"Ignoring {context} close error: {exc}")

    def _drop_file_db(self, db_name: str) -> None:
        """Close, unlink, and forget a file or memory database.

        Invalidates change streams, aborts and closes any transactional
        connections bound to ``db_name``, clears cached write concern state,
        closes and removes the cached connection, bumps the in-memory URI
        generation (in ``memory`` mode) or deletes the ``.db`` file (in
        ``files`` mode), and removes matching scoped sessions.

        Args:
            db_name: Logical database name to drop.
        """
        self._change_stream_manager.invalidate(db_name)
        self._close_cursors_for_namespace(db_name)
        dropped_sessions: list[tuple[str, Any, Connection | None]] = []
        with self._sessions_lock:
            for sess_key in [
                existing
                for existing in self._sessions
                if existing[0] and existing[1] == db_name
            ]:
                sess = self._sessions.pop(sess_key, None)
                self._session_last_used.pop(sess_key, None)
                tx_conn = self._session_tx_conns.pop(sess_key, None)
                if sess is not None or tx_conn is not None:
                    dropped_sessions.append((sess_key[0], sess, tx_conn))
            self._tx_cond.notify_all()
        for sid, sess, tx_conn in dropped_sessions:
            with self._lock_for_session(sid):
                self._safe_end_session(sess, f"drop_file_db {db_name}")
                self._safe_close_conn(tx_conn, f"drop_file_db tx {db_name}")
        with self._dict_lock:
            conn = self._conns.pop(db_name, None)
            self.databases.pop(db_name, None)
            self._applied_wc.pop(db_name, None)
            if self._mode == "memory":
                self._mem_generations[db_name] = (
                    self._mem_generations.get(db_name, 0) + 1
                )
        self._safe_close_conn(conn, f"drop_file_db {db_name}")
        if self._mode == "files":
            db_file = self._db_file(db_name)
            for path in (db_file, f"{db_file}-wal", f"{db_file}-shm"):
                try:
                    os.unlink(path)
                except OSError:
                    pass

    @staticmethod
    def _first_batch_size(cmd: dict[str, Any]) -> int:
        """Return the first-batch size for a find or aggregate command.

        Honors top-level ``batchSize`` and ``cursor: {batchSize}`` (the
        aggregate form). Missing or zero uses ``_DEFAULT_BATCH_SIZE``;
        negative values mirror MongoDB single-batch semantics via ``abs()``.

        Args:
            cmd: Parsed command body dictionary.

        Returns:
            int: Positive batch size for the initial cursor response.
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

    @staticmethod
    def _getmore_batch_size(cmd: dict[str, Any]) -> int | None:
        """Return the document batch size limit for a ``getMore`` command.

        In MongoDB, ``_DEFAULT_BATCH_SIZE`` (101) applies only to initial
        ``find`` / ``aggregate`` commands; ``getMore`` without an explicit
        positive ``batchSize`` fills up to the 16 MB BSON budget.

        Args:
            cmd: Parsed ``getMore`` command body dictionary.

        Returns:
            int | None: Explicit positive batch size, or ``None`` when
                omitted/zero (bounded only by BSON size budget).
        """
        raw = cmd.get("batchSize", None)
        if raw is None:
            return None
        try:
            size = abs(int(raw))
        except (TypeError, ValueError):
            return None
        return size if size > 0 else None

    @staticmethod
    def _estimate_bson_size(doc: dict[str, Any]) -> int:
        """Compute exact BSON size after converting NeoSQLite types."""
        try:
            return len(BSON.encode(convert_neo_to_bson_objectids(doc)))
        except Exception as exc:
            logger.debug(
                f"Failed BSON-encoding document for size check, falling back to repr: {exc}"
            )
            return len(repr(doc))

    @classmethod
    def _fit_bson_budget(
        cls,
        docs: list[dict[str, Any]],
        limit: int = MAX_BSON_DOCUMENT_SIZE,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Split documents so the head fits within a BSON size budget.

        Always keeps at least one document in ``head`` (an oversized single
        document is returned as-is rather than raising, documenting NX's
        leniency compared to MongoDB). Converts NeoSQLite types to BSON before
        encoding so ``BSON.encode`` computes exact byte sizes without raising
        ``InvalidDocument`` on ``neosqlite.objectid.ObjectId`` values.

        Args:
            docs: Ordered list of candidate response documents.
            limit: Maximum cumulative BSON byte budget for ``head``.

        Returns:
            tuple[list[dict[str, Any]], list[dict[str, Any]]]: Tuple of
                ``(head, remainder)`` slices.
        """
        head: list[dict[str, Any]] = []
        used = 0
        for doc in docs:
            size = cls._estimate_bson_size(doc)
            if head and used + size > limit:
                break
            head.append(doc)
            used += size
        return head, docs[len(head) :]

    @staticmethod
    def _close_cursor_obj(cursor_obj: Any) -> None:
        """Safely close a live NeoSQLite cursor or iterator."""
        if cursor_obj is not None and hasattr(cursor_obj, "close"):
            try:
                cursor_obj.close()
            except Exception as exc:
                logger.debug(f"Failed closing wire cursor handle: {exc}")

    def _close_cursor_entries(
        self,
        entries: list[tuple[str | None, Any]],
        current_db: str | None = None,
    ) -> None:
        """Close popped cursor handles outside ``_cursors_lock`` safely."""
        for c_db, cursor_obj in entries:
            if cursor_obj is None or not hasattr(cursor_obj, "close"):
                continue
            if (
                current_db is not None
                and (c_db == current_db or self._mode == "single")
            ) or not c_db:
                self._close_cursor_obj(cursor_obj)
            else:
                try:
                    with self._lock_for(c_db):
                        self._close_cursor_obj(cursor_obj)
                except Exception as exc:
                    logger.debug(
                        f"Failed acquiring lock for {c_db} while closing cursor: {exc}"
                    )
                    self._close_cursor_obj(cursor_obj)

    def _close_cursors_for_namespace(
        self, db_name: str, coll_name: str | None = None
    ) -> None:
        """Close and drop open data cursors belonging to a dropped db/collection."""
        target_ns = f"{db_name}.{coll_name}" if coll_name is not None else None
        popped: list[tuple[str | None, Any]] = []
        with self._cursors_lock:
            to_delete = [
                cid
                for cid, entry in self._cursors.items()
                if entry.get("db") == db_name
                and (target_ns is None or entry.get("ns") == target_ns)
            ]
            for cid in to_delete:
                entry = self._cursors.pop(cid, None)
                if entry is not None:
                    popped.append((entry.get("db"), entry.get("cursor")))
        self._close_cursor_entries(popped, current_db=db_name)

    def _expire_cursors_locked(
        self,
        now: float | None = None,
        closed_out: list[tuple[str | None, Any]] | None = None,
    ) -> None:
        """Drop idle data cursors past timeout and enforce ``_MAX_CURSORS``.

        Must be called while holding ``self._cursors_lock``. Removed cursor
        handles are appended to ``closed_out`` so callers can close them after
        releasing ``_cursors_lock``.
        """
        if not self._cursors:
            return
        current = time.monotonic() if now is None else now
        cutoff = current - self._cursor_timeout_seconds
        expired = [
            cid
            for cid, entry in self._cursors.items()
            if entry.get("last_used", current) < cutoff
        ]
        for cid in expired:
            entry = self._cursors.pop(cid, None)
            if entry is not None and closed_out is not None:
                closed_out.append((entry.get("db"), entry.get("cursor")))
        if len(self._cursors) > _MAX_CURSORS:
            by_age = sorted(
                self._cursors.items(),
                key=self._cursor_last_used_key,
            )
            excess = len(self._cursors) - _MAX_CURSORS
            for cid, _entry in by_age[:excess]:
                entry = self._cursors.pop(cid, None)
                if entry is not None and closed_out is not None:
                    closed_out.append((entry.get("db"), entry.get("cursor")))

    @staticmethod
    def _cursor_last_used_key(item: tuple[int, dict[str, Any]]) -> float:
        """Return sort key for cursor eviction by ``last_used`` timestamp."""
        return float(item[1].get("last_used", 0.0))

    @classmethod
    def _take_batch_from_iter(
        cls,
        buffered: list[dict[str, Any]],
        source_iter: Iterator[dict[str, Any]] | None,
        batch_size: int | None,
        limit: int = MAX_BSON_DOCUMENT_SIZE,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
        """Take one batch from ``buffered`` + lazy ``source_iter`` under limits.

        Pulls at most ``batch_size`` documents (when given) while staying within
        ``limit`` BSON bytes, peeking at most one trailing document past an exact
        count boundary so exhausted streams report ``has_more = False``.
        """
        if source_iter is None:
            candidates = (
                buffered if batch_size is None else buffered[:batch_size]
            )
            list_head, budget_rest = cls._fit_bson_budget(
                candidates, limit=limit
            )
            list_rest = budget_rest + (
                [] if batch_size is None else buffered[batch_size:]
            )
            return list_head, list_rest, bool(list_rest)

        head: list[dict[str, Any]] = []
        used = 0
        budget_hit = False
        rest_start = len(buffered)
        for idx, doc in enumerate(buffered):
            if batch_size is not None and len(head) >= batch_size:
                rest_start = idx
                break
            size = cls._estimate_bson_size(doc)
            if head and used + size > limit:
                rest_start = idx
                budget_hit = True
                break
            head.append(doc)
            used += size

        rest = buffered[rest_start:]
        exhausted = False
        if not budget_hit:
            while batch_size is None or len(head) < batch_size:
                try:
                    doc = next(source_iter)
                except StopIteration:
                    exhausted = True
                    break
                size = cls._estimate_bson_size(doc)
                if head and used + size > limit:
                    rest.append(doc)
                    budget_hit = True
                    break
                head.append(doc)
                used += size
            else:
                if not rest:
                    try:
                        rest.append(next(source_iter))
                    except StopIteration:
                        exhausted = True

        has_more = bool(rest) or not exhausted
        return head, rest, has_more

    def _split_first_batch(
        self,
        docs: Any,
        batch_size: int,
        ns: str,
        db_name: str,
        owner: Any = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Split ``docs`` (list or lazy cursor) into ``(firstBatch, cursorId)``.

        Args:
            docs: Full list of result documents or lazy iterable/cursor.
            batch_size: Maximum document count for the first batch.
            ns: Target namespace string (``"<db>.<collection>"``).
            db_name: Logical database name owning the cursor.
            owner: Optional connection identifier owning the cursor.

        Returns:
            tuple[list[dict[str, Any]], int]: First batch of documents and
                allocated cursor ID (``0`` if all documents fit).
        """
        if isinstance(docs, list):
            cursor_obj = None
            source_iter: Iterator[dict[str, Any]] | None = None
            first, rest, has_more = self._take_batch_from_iter(
                docs, None, batch_size
            )
        else:
            cursor_obj = docs
            source_iter = iter(docs)
            try:
                first, rest, has_more = self._take_batch_from_iter(
                    [], source_iter, batch_size
                )
            except Exception as exc:
                logger.debug(f"Wire cursor first-batch iteration failed: {exc}")
                self._close_cursor_obj(cursor_obj)
                raise

        if not has_more:
            self._close_cursor_obj(cursor_obj)
            return first, 0

        now = time.monotonic()
        expired_to_close: list[tuple[str | None, Any]] = []
        with self._cursors_lock:
            self._expire_cursors_locked(now, closed_out=expired_to_close)
            cursor_id = _generate_cursor_id()
            while (
                cursor_id in self._cursors
                or self._change_stream_manager.get_stream(cursor_id) is not None
            ):
                cursor_id = _generate_cursor_id()
            self._cursors[cursor_id] = {
                "ns": ns,
                "db": db_name,
                "docs": rest,
                "iter": source_iter,
                "cursor": cursor_obj,
                "owner": owner,
                "last_used": now,
            }
            if len(self._cursors) > _MAX_CURSORS:
                self._expire_cursors_locked(now, closed_out=expired_to_close)
        if expired_to_close:
            self._close_cursor_entries(expired_to_close, current_db=db_name)
        return first, cursor_id

    def _getmore_data(
        self,
        cursor_id: int,
        batch_size: int | None,
        expected_ns: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the next batch for a data cursor, or ``None`` if absent.

        Args:
            cursor_id: Numeric cursor identifier from a prior response.
            batch_size: Maximum number of documents to return in this batch,
                or ``None`` to fill up to the 16 MB BSON budget.
            expected_ns: Optional ``"<db>.<collection>"`` namespace that the
                cursor must match; returns ``{"_ns_mismatch": True}`` when
                provided and mismatched without consuming cursor state.

        Returns:
            dict[str, Any] | None: Cursor payload with ``id`` (``0`` when
                exhausted), ``ns``, and ``nextBatch``, ``{"_ns_mismatch": True}``
                on namespace mismatch, or ``None`` if unknown.
        """
        now = time.monotonic()
        expired_to_close: list[tuple[str | None, Any]] = []
        finished_to_close: list[tuple[str | None, Any]] = []
        with self._cursors_lock:
            self._expire_cursors_locked(now, closed_out=expired_to_close)
            entry = self._cursors.get(cursor_id)
            if entry is None:
                result_payload: dict[str, Any] | None = None
                entry_db: str | None = None
            elif expected_ns is not None and entry["ns"] != expected_ns:
                result_payload = {"_ns_mismatch": True}
                entry_db = entry.get("db")
            else:
                ns = entry["ns"]
                entry_db = entry.get("db")
                docs = entry["docs"]
                source_iter = entry.get("iter")
                batch, rest, has_more = self._take_batch_from_iter(
                    docs, source_iter, batch_size
                )
                if has_more:
                    entry["docs"] = rest
                    if not rest and source_iter is None:
                        entry["iter"] = None
                    entry["last_used"] = now
                    live_id = cursor_id
                else:
                    del self._cursors[cursor_id]
                    if entry.get("cursor") is not None:
                        finished_to_close.append(
                            (entry_db, entry.get("cursor"))
                        )
                    live_id = 0
                result_payload = {
                    "id": Int64(live_id),
                    "ns": ns,
                    "nextBatch": batch,
                }
        if expired_to_close or finished_to_close:
            self._close_cursor_entries(
                expired_to_close + finished_to_close, current_db=entry_db
            )
        return result_payload

    def _kill_data_cursors(
        self, cursor_ids: list[int], expected_ns: str | None = None
    ) -> list[int]:
        """Remove data cursors by ID and return the removed identifiers.

        Args:
            cursor_ids: List of numeric data cursor IDs to close.
            expected_ns: Optional namespace filter; when provided, cursors
                belonging to another namespace are not removed.

        Returns:
            list[int]: Subset of ``cursor_ids`` that existed and were removed.
        """
        killed = []
        to_close: list[tuple[str | None, Any]] = []
        with self._cursors_lock:
            self._expire_cursors_locked(closed_out=to_close)
            for cid in cursor_ids:
                entry = self._cursors.get(cid)
                if entry is not None and (
                    expected_ns is None or entry.get("ns") == expected_ns
                ):
                    del self._cursors[cid]
                    if entry.get("cursor") is not None:
                        to_close.append((entry.get("db"), entry.get("cursor")))
                    killed.append(cid)
        if to_close:
            self._close_cursor_entries(to_close)
        return killed

    def close_cursors_for_owner(self, owner: Any) -> None:
        """Drop data cursors owned by a disconnected client connection.

        Args:
            owner: Client connection identifier, or ``None`` (no-op).
        """
        if owner is None:
            return
        to_close: list[tuple[str | None, Any]] = []
        with self._cursors_lock:
            for cid in [
                cid
                for cid, entry in self._cursors.items()
                if entry.get("owner") == owner
            ]:
                entry = self._cursors.pop(cid, None)
                if entry is not None and entry.get("cursor") is not None:
                    to_close.append((entry.get("db"), entry.get("cursor")))
        if to_close:
            self._close_cursor_entries(to_close)

    @staticmethod
    def _extract_insert_documents(
        command_doc: dict[str, Any], payload_docs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Extract documents to insert from ``payload_docs`` and ``documents``."""
        docs_to_insert = payload_docs.copy() if payload_docs else []
        documents = command_doc.get("documents")
        if isinstance(documents, list):
            docs_to_insert.extend(documents)
        elif isinstance(documents, dict):
            docs_to_insert.append(documents)
        return docs_to_insert

    def _convert_command_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Convert incoming BSON types on a command body, keeping ``lsid`` raw."""
        converted: dict[str, Any] = {}
        for key, value in doc.items():
            if key == "lsid":
                converted[key] = value
            else:
                converted[key] = self._convert_objectids(value)
        return converted

    def _convert_objectids(self, doc: Any) -> Any:
        """Convert PyMongo ObjectIds to NeoSQLite ObjectIds recursively.

        Args:
            doc: Document, list, or value to transform.

        Returns:
            Any: Converted value with NeoSQLite ``ObjectId`` instances.
        """
        from nx_27017.utils import convert_bson_to_neo_objectids

        return convert_bson_to_neo_objectids(doc)

    def _session_scope(self, db_name: str) -> str:
        """Return the session namespace key for ``db_name``.

        Single-file mode uses one shared empty-string scope because all
        logical databases share one SQLite connection; multi-DB modes scope
        sessions per database.

        Args:
            db_name: Logical database name.

        Returns:
            str: ``""`` in single-file mode, otherwise ``db_name``.
        """
        if self._mode == "single":
            return ""
        return db_name

    def _tx_sessions(self, session_id: str) -> list[Any]:
        """Return all sessions with an open transaction for ``session_id``.

        Args:
            session_id: Extracted logical session ID string.

        Returns:
            list[Any]: Active transactional session objects across all scopes.
        """
        with self._sessions_lock:
            return [
                sess
                for (sid, _db), sess in self._sessions.items()
                if sid == session_id and sess.in_transaction
            ]

    def _tx_session_entries(
        self, session_id: str
    ) -> list[tuple[tuple[str, str], Any, Connection | None]]:
        """Return ``(key, session, tx_conn)`` for active transactions."""
        with self._sessions_lock:
            return [
                (key, sess, self._session_tx_conns.get(key))
                for key, sess in self._sessions.items()
                if key[0] == session_id and sess.in_transaction
            ]

    @staticmethod
    def _session_last_used_key(item: tuple[tuple[str, str], float]) -> float:
        """Return sort key for session eviction by ``last_used`` timestamp."""
        return item[1]

    def _expire_sessions(self) -> None:
        """Expire idle sessions past ``_session_timeout_seconds`` and cap count."""
        now = time.monotonic()
        cutoff = now - self._session_timeout_seconds
        expired: list[tuple[tuple[str, str], Any, Connection | None]] = []
        with self._sessions_lock:
            expired_keys = [
                key
                for key in list(self._sessions)
                if self._session_last_used.get(key, now) < cutoff
            ]
            for key in expired_keys:
                sess = self._sessions.pop(key, None)
                self._session_last_used.pop(key, None)
                tx_conn = self._session_tx_conns.pop(key, None)
                if sess is not None or tx_conn is not None:
                    expired.append((key, sess, tx_conn))
            if len(self._sessions) > _MAX_SESSIONS:
                idle_ages = sorted(
                    (
                        (key, self._session_last_used.get(key, 0.0))
                        for key, sess in self._sessions.items()
                        if not sess.in_transaction
                    ),
                    key=self._session_last_used_key,
                )
                excess = len(self._sessions) - _MAX_SESSIONS
                for key, _ts in idle_ages[:excess]:
                    sess = self._sessions.pop(key, None)
                    self._session_last_used.pop(key, None)
                    tx_conn = self._session_tx_conns.pop(key, None)
                    if sess is not None or tx_conn is not None:
                        expired.append((key, sess, tx_conn))
            if expired:
                self._tx_cond.notify_all()
        for key, sess, tx_conn in expired:
            sid, scope = key
            lock = (
                self._lock_for_session(sid)
                if tx_conn is not None
                else self._lock_for(scope or "admin")
            )
            with lock:
                self._safe_end_session(sess, f"expire session {sid}")
                self._safe_close_conn(tx_conn, f"expire session tx {sid}")

    def _uses_shared_memory_tx(self) -> bool:
        """Return whether transactions share an in-memory SQLite cache."""
        return self._mode == "memory" or self._single_mem_uri is not None

    def _wait_for_active_memory_tx(
        self, scope: str, exclude_session_id: str | None = None
    ) -> None:
        """Wait briefly for an active shared-memory tx from another client.

        In SQLite shared-cache memory mode, ``SQLITE_LOCKED`` does not honor
        ``busy_timeout``. Waiting briefly on ``_tx_cond`` lets multi-threaded
        clients serialize cleanly once the active transaction finishes while
        still bounding wait time so single-threaded callers never deadlock.
        """
        if not self._uses_shared_memory_tx():
            return
        deadline = time.monotonic() + min(self._tx_timeout_seconds, 0.25)
        with self._sessions_lock:
            while True:
                has_other_tx = any(
                    key[1] == scope
                    and key[0] != exclude_session_id
                    and (sess.in_transaction or key in self._session_tx_conns)
                    for key, sess in self._sessions.items()
                )
                if not has_other_tx:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                self._tx_cond.wait(timeout=remaining)

    def _apply_write_concern(
        self, db: Connection, db_name: str, write_concern: Any
    ) -> None:
        """Map a ``writeConcern`` document onto SQLite PRAGMAs for one db.

        ``w:0`` maps to ``synchronous=OFF``, ``w:1`` maps to ``NORMAL``, and
        ``j:true`` maps to ``FULL`` (see ``Connection._apply_write_concern``).
        ``w:majority``/``w:2+`` and ``wtimeoutMS`` have no single-node meaning
        and are accepted without effect. Re-applies only on change per db.

        Args:
            db: Target ``neosqlite.Connection`` instance.
            db_name: Logical database name used as cache key.
            write_concern: Raw ``writeConcern`` value from the command doc.
        """
        if not isinstance(write_concern, dict) or not write_concern:
            return
        key = repr(sorted(write_concern.items()))
        is_main = False
        with self._dict_lock:
            main_conn = (
                self.conn
                if self._mode == "single"
                else self._conns.get(db_name)
            )
            if db is main_conn:
                is_main = True
                if self._applied_wc.get(db_name) == key:
                    return
                self._applied_wc[db_name] = key
        try:
            db._apply_write_concern(write_concern)
        except Exception as exc:
            if is_main:
                with self._dict_lock:
                    self._applied_wc.pop(db_name, None)
            logger.debug("writeConcern apply skipped: %s", exc)

    def _find_session(self, session_id: str, db_name: str) -> Any | None:
        """Find a cached session for ``(session_id, db_name)``.

        In ``single`` mode, falls back to any session matching ``session_id``
        regardless of database scope.

        Args:
            session_id: Extracted logical session ID string.
            db_name: Target logical database name.

        Returns:
            Any | None: Matching session object, or ``None`` if absent.
        """
        self._expire_sessions()
        scope = self._session_scope(db_name)
        with self._sessions_lock:
            session = self._sessions.get((session_id, scope))
            if session is not None:
                self._session_last_used[(session_id, scope)] = time.monotonic()
                return session
            if self._mode == "single":
                # Legacy compat: one connection backs every db, so any
                # session works anywhere. Isolated modes stay strict so an
                # lsid can never touch another database's file.
                for (sid, sc), sess in self._sessions.items():
                    if sid == session_id:
                        self._session_last_used[(sid, sc)] = time.monotonic()
                        return sess
            return None

    def _reject_cross_db_tx(self, session_id: str, scope: str) -> None:
        """Fail if an ``lsid`` with an open transaction touches another db.

        SQLite files cannot share a transaction, so a transactional session
        may only operate on the database where its transaction began. Must
        be called while holding ``_sessions_lock``.

        Args:
            session_id: Extracted logical session ID string.
            scope: Target session scope from ``_session_scope``.

        Raises:
            CrossDBTransactionError: If ``session_id`` already has an open
                transaction on a different database scope.
        """
        if self._mode == "single" or not scope:
            return
        for (other_sid, other_scope), session in self._sessions.items():
            if (
                other_sid == session_id
                and other_scope
                and other_scope != scope
                and session.in_transaction
            ):
                raise CrossDBTransactionError(
                    "Cross-database transactions are not supported: "
                    f"transaction open on {other_scope!r}, "
                    f"operation targets {scope!r}"
                )

    def _resolve_db_and_session(
        self,
        command_doc: dict[str, Any],
        db: Connection | None = None,
        db_name: str = "test",
    ) -> tuple[Connection, Any | None]:
        """Resolve the active ``(Connection, ClientSession | None)`` pair.

        When ``startTransaction`` is set on a command, opens a dedicated
        ``neosqlite.Connection`` for that session so its transaction is
        isolated from non-transactional reads and writes on the main
        connection. Subsequent commands carrying the same ``lsid`` while the
        transaction is open are routed to that dedicated connection.

        Args:
            command_doc: Parsed command body dictionary.
            db: Main ``Connection`` for ``db_name`` (falls back to
                ``self.conn``).
            db_name: Logical database name for session scoping.

        Returns:
            tuple[Connection, Any | None]: Target ``Connection`` to execute on
                and the bound ``ClientSession`` (or ``None`` if no ``lsid``).
        """
        self._expire_sessions()
        base_db = db if db is not None else self.conn
        scope = self._session_scope(db_name)
        lsid = command_doc.get("lsid")
        session_id = _extract_session_id(lsid) if lsid else None
        if not session_id:
            self._wait_for_active_memory_tx(scope)
            return base_db, None

        now = time.monotonic()
        key = (session_id, scope)
        need_new_tx = False
        old_idle_session: Any | None = None

        with self._sessions_lock:
            self._reject_cross_db_tx(session_id, scope)
            existing = self._sessions.get(key)
            if command_doc.get("startTransaction"):
                if existing is not None and existing.in_transaction:
                    self._session_last_used[key] = now
                    tx_conn = self._session_tx_conns.get(key)
                    return (
                        tx_conn if tx_conn is not None else base_db,
                        existing,
                    )
                need_new_tx = True
            else:
                if existing is not None and existing.in_transaction:
                    self._session_last_used[key] = now
                    tx_conn = self._session_tx_conns.get(key)
                    return (
                        tx_conn if tx_conn is not None else base_db,
                        existing,
                    )
                if existing is None or getattr(existing, "has_ended", False):
                    existing = base_db.start_session()
                    self._sessions[key] = existing
                self._session_last_used[key] = now

        if not need_new_tx:
            self._wait_for_active_memory_tx(
                scope, exclude_session_id=session_id
            )
            return base_db, existing

        # Open a dedicated Connection for this transaction outside _sessions_lock
        # so opening SQLite files or waiting on BEGIN IMMEDIATE never blocks
        # unrelated session lookups.
        self._wait_for_active_memory_tx(scope, exclude_session_id=session_id)
        tx_conn = self._open_tx_conn(db_name)
        try:
            tx_session = tx_conn.start_session()
            tx_session.start_transaction()
        except Exception as exc:
            logger.debug(f"Failed starting transaction session: {exc}")
            self._safe_close_conn(tx_conn, "aborted tx open")
            raise

        with self._sessions_lock:
            old_idle_session = self._sessions.get(key)
            old_tx_conn = self._session_tx_conns.pop(key, None)
            self._sessions[key] = tx_session
            self._session_tx_conns[key] = tx_conn
            self._session_last_used[key] = time.monotonic()

        if old_idle_session is not tx_session:
            self._safe_end_session(old_idle_session, "replaced idle session")
        if old_tx_conn is not tx_conn:
            self._safe_close_conn(old_tx_conn, "replaced tx conn")

        return tx_conn, tx_session

    def _get_or_create_session(
        self,
        command_doc: dict[str, Any],
        db: Connection | None = None,
        db_name: str = "test",
    ) -> Any | None:
        """Get or create a session from ``command_doc`` if ``lsid`` is set.

        Args:
            command_doc: Parsed command body dictionary.
            db: Optional ``Connection`` used to create a new session; falls
                back to ``self.conn``.
            db_name: Logical database name for session scoping.

        Returns:
            Any | None: Session object for ``lsid``, or ``None`` if ``lsid``
                is missing or yields no session ID.
        """
        _resolved_db, session = self._resolve_db_and_session(
            command_doc, db, db_name
        )
        return session

    def try_acquire_connection(self) -> bool:
        """Reserve a client connection slot if under ``_max_connections``.

        Returns:
            bool: ``True`` if a slot was acquired, ``False`` if the server has
                reached its connection limit.
        """
        with self._connections_lock:
            if self._active_connections >= self._max_connections:
                return False
            self._active_connections += 1
            return True

    def increment_connections(self) -> None:
        """Increment the active connections counter."""
        with self._connections_lock:
            self._active_connections += 1

    def decrement_connections(self) -> None:
        """Decrement the active connections counter."""
        with self._connections_lock:
            if self._active_connections > 0:
                self._active_connections -= 1

    def _is_gridfs_collection(self, coll_name: str) -> bool:
        """Check whether ``coll_name`` refers to a GridFS collection.

        Args:
            coll_name: Collection name string.

        Returns:
            bool: ``True`` if ``coll_name`` is a ``.files`` or ``.chunks``
                GridFS collection.
        """
        return _is_gridfs_collection(coll_name)

    def _get_gridfs_bucket_name(self, coll_name: str) -> str | None:
        """Extract the GridFS bucket name from ``coll_name``, if any.

        Args:
            coll_name: Collection name string.

        Returns:
            str | None: Bucket name prefix, or ``None`` if not GridFS.
        """
        return _get_gridfs_bucket_name(coll_name)

    def _handle_gridfs_insert(
        self,
        request_id: int,
        coll_name: str,
        docs: list[dict],
        db: Connection,
    ) -> tuple[int, dict[str, Any]]:
        """Handle insert operations on GridFS collections via adapter.

        Ensures parent file metadata exists before inserting ``.chunks``
        documents and delegates writes to ``adapter.handle_insert``.

        Args:
            request_id: Wire-protocol request identifier.
            coll_name: Target GridFS collection name (``*.files`` or
                ``*.chunks``).
            docs: List of documents to insert.
            db: Active ``neosqlite.Connection`` for the target database.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                document.
        """
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
        """Handle an ``OP_MSG`` ``insert`` command.

        Extracts the command body and payload documents, resolves the target
        database and session, dispatches GridFS collections to
        ``_handle_gridfs_insert``, ensures the collection table exists, and
        inserts documents sequentially while honoring ``ordered`` and
        collecting ``insertedIds`` and ``writeErrors``.

        Args:
            msg: Decoded ``OP_MSG`` request dictionary containing
                ``request_id`` and ``sections``.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                with ``ok``, ``n``, ``insertedIds``, and optional
                ``writeErrors``.
        """
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

        # Session/tx resolution happens first so a cross-DB violation
        # errors before any table gets created as a side effect, and
        # transactional writes execute on the session's isolated Connection.
        db, session_to_use = self._resolve_db_and_session(
            command_doc, db, db_name
        )

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

        docs_to_insert = [
            self._convert_objectids(doc)
            for doc in self._extract_insert_documents(command_doc, payload_docs)
        ]

        if self._is_gridfs_collection(coll_name):
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

        self._apply_write_concern(db, db_name, command_doc.get("writeConcern"))

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
                    logger.debug(f"Insert op {idx} failed: {exc}")
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

    _PRE_COMMAND_DISPATCH: dict[str, str] = {
        "ismaster": "_cmd_handshake",
        "isMaster": "_cmd_handshake",
        "hello": "_cmd_handshake",
        "Hello": "_cmd_handshake",
        "startSession": "_cmd_start_session",
        "commitTransaction": "_cmd_commit_transaction",
        "abortTransaction": "_cmd_abort_transaction",
        "endSessions": "_cmd_end_sessions",
    }

    _COMMAND_DISPATCH: dict[str, str] = {
        "create": "_cmd_create",
        "drop": "_cmd_drop",
        "dropDatabase": "_cmd_drop_database",
        "dropdatabase": "_cmd_drop_database",
        "renameCollection": "_cmd_rename_collection",
        "dropIndexes": "_cmd_drop_indexes",
        "createIndexes": "_cmd_create_indexes",
        "createIndex": "_cmd_create_indexes",
        "dropIndex": "_cmd_drop_index",
        "createSearchIndexes": "_cmd_create_search_indexes",
        "createsearchindexes": "_cmd_create_search_indexes",
        "updateSearchIndex": "_cmd_update_search_index",
        "updatesearchindex": "_cmd_update_search_index",
        "dropSearchIndex": "_cmd_drop_search_index",
        "dropsearchindex": "_cmd_drop_search_index",
        "delete": "_cmd_delete",
        "bulkWrite": "_cmd_bulk_write",
        "upload": "_cmd_upload",
        "openDownloadStream": "_cmd_open_download_stream",
        "findAndModify": "_cmd_find_and_modify",
        "update": "_cmd_update",
        "find": "_cmd_find",
        "count": "_cmd_count",
        "distinct": "_cmd_distinct",
        "aggregate": "_cmd_aggregate",
        "listCollections": "_cmd_list_collections",
        "serverStatus": "_cmd_server_status",
        "buildInfo": "_cmd_server_status",
        "dbStats": "_cmd_db_stats",
        "dbstats": "_cmd_db_stats",
        "collStats": "_cmd_coll_stats",
        "collstats": "_cmd_coll_stats",
        "listDatabases": "_cmd_list_databases",
        "listdatabases": "_cmd_list_databases",
        "listIndexes": "_cmd_list_indexes",
        "listindexes": "_cmd_list_indexes",
        "listSearchIndexes": "_cmd_list_search_indexes",
        "listsearchindexes": "_cmd_list_search_indexes",
        "explain": "_cmd_explain",
        "getMore": "_cmd_get_more",
        "killCursors": "_cmd_kill_cursors",
        "vacuum": "_cmd_vacuum",
        "compact": "_cmd_compact",
        "compactcollection": "_cmd_compact",
        "validate": "_cmd_validate",
        "reindex": "_cmd_reindex",
        "reIndex": "_cmd_reindex",
    }

    @staticmethod
    def _extract_command_and_payloads(
        sections: list[tuple[str, Any]],
    ) -> tuple[
        dict[str, Any] | None,
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        """Extract command body and wire payload sequences from OP_MSG sections."""
        command_doc = None
        payload_updates: list[dict[str, Any]] = []
        payload_deletes: list[dict[str, Any]] = []
        payload_ops: list[dict[str, Any]] = []
        payload_nsinfo: list[dict[str, Any]] = []
        for section_type, doc in sections:
            if section_type == "body":
                command_doc = doc
            elif section_type == "payload" and isinstance(doc, dict):
                if isinstance(doc.get("updates"), list):
                    payload_updates.extend(doc["updates"])
                if isinstance(doc.get("deletes"), list):
                    payload_deletes.extend(doc["deletes"])
                # bulkWrite ships ops (and sometimes nsInfo) as a
                # document sequence rather than inline in the body.
                if isinstance(doc.get("ops"), list):
                    payload_ops.extend(doc["ops"])
                if isinstance(doc.get("nsInfo"), list):
                    payload_nsinfo.extend(doc["nsInfo"])
        return (
            command_doc,
            payload_updates,
            payload_deletes,
            payload_ops,
            payload_nsinfo,
        )

    @staticmethod
    def _pop_alias(mapping: dict[str, Any], *keys: str) -> Any:
        """Pop and return the value of the first matching key in ``mapping``."""
        for key in keys:
            if key in mapping:
                return mapping.pop(key)
        return None

    @_serialize
    def handle_command(self, msg: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        """Dispatch a MongoDB ``$cmd`` message to NeoSQLite or GridFS handlers.

        Parses ``body`` and ``payload`` sections from ``msg`` and dispatches
        handshake, session/transaction lifecycle, DDL, CRUD, aggregation,
        GridFS, cursor, diagnostic, and maintenance commands via dispatch
        tables, falling back to ``db.command`` for unrecognized commands.

        Args:
            msg: Decoded ``OP_MSG`` (or adapted ``OP_QUERY``) message dict
                with ``request_id`` and ``sections``.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                dictionary.
        """
        request_id = msg["request_id"]
        (
            command_doc,
            payload_updates,
            payload_deletes,
            payload_ops,
            payload_nsinfo,
        ) = self._extract_command_and_payloads(msg["sections"])

        if not command_doc:
            return request_id, {"ok": 0, "errmsg": "No command document"}

        db_name = command_doc.get("$db", "test")
        db = self.get_database(db_name)

        for key, method_name in self._PRE_COMMAND_DISPATCH.items():
            if key in command_doc:
                handler_fn = getattr(self, method_name)
                return handler_fn(request_id, command_doc, db, db_name)

        # Explicit cross-DB transaction rejection: a session (lsid) with
        # an open transaction may only operate on its own database.
        lsid_doc = command_doc.get("lsid")
        if lsid_doc and self._mode != "single":
            guard_session_id = _extract_session_id(lsid_doc)
            if guard_session_id:
                with self._sessions_lock:
                    self._reject_cross_db_tx(
                        guard_session_id, self._session_scope(db_name)
                    )

        cmd_copy = self._convert_command_doc(command_doc)
        payload_updates = self._convert_objectids(payload_updates)
        payload_deletes = self._convert_objectids(payload_deletes)
        payload_ops = self._convert_objectids(payload_ops)

        db_name = cmd_copy.pop("$db", "test")
        db = self.get_database(db_name)

        ctx = _CommandContext(
            request_id=request_id,
            cmd_copy=cmd_copy,
            command_doc=command_doc,
            db=db,
            db_name=db_name,
            msg=msg,
            payload_updates=payload_updates,
            payload_deletes=payload_deletes,
            payload_ops=payload_ops,
            payload_nsinfo=payload_nsinfo,
        )
        for key, method_name in self._COMMAND_DISPATCH.items():
            if key in cmd_copy:
                handler_fn = getattr(self, method_name)
                return handler_fn(ctx)

        logger.debug(f"Calling db.command with: {cmd_copy}")
        cmd_result = db.command(cmd_copy)
        logger.debug(
            f"NeoSQLite returned: {list(cmd_result.keys()) if isinstance(cmd_result, dict) else type(cmd_result)}"
        )
        return request_id, cmd_result

    def _cmd_handshake(
        self,
        request_id: int,
        command_doc: dict[str, Any],
        db: Connection,
        db_name: str,
    ) -> tuple[int, dict[str, Any]]:
        """Respond to MongoDB wire-protocol handshake commands."""
        return request_id, {
            "ok": 1,
            "isWritablePrimary": True,
            "maxBsonObjectSize": MAX_BSON_DOCUMENT_SIZE,
            "maxMessageSizeBytes": MAX_MESSAGE_SIZE_BYTES,
            "maxWriteBatchSize": MAX_WRITE_BATCH_SIZE,
            "localTime": datetime.now(UTC),
            "logicalSessionTimeoutMinutes": DEFAULT_SESSION_TIMEOUT_MINUTES,
            "connectionId": 1,
            "minWireVersion": MIN_WIRE_VERSION,
            "maxWireVersion": MAX_WIRE_VERSION,
        }

    def _cmd_start_session(
        self,
        request_id: int,
        command_doc: dict[str, Any],
        db: Connection,
        db_name: str,
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``startSession`` and register the server-side session."""
        self._expire_sessions()
        session_id = f"session_{uuid.uuid4().hex}"
        session = db.start_session()
        session_key = (session_id, self._session_scope(db_name))
        with self._sessions_lock:
            self._sessions[session_key] = session
            self._session_last_used[session_key] = time.monotonic()
        return request_id, {
            "ok": 1,
            "session": {"id": {"$oid": session_id}},
        }

    def _cmd_commit_transaction(
        self,
        request_id: int,
        command_doc: dict[str, Any],
        db: Connection,
        db_name: str,
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``commitTransaction`` for a session."""
        lsid = command_doc.get("lsid")
        tx_session_id = _extract_session_id(lsid) if lsid else None
        if tx_session_id and self._finish_transaction(
            tx_session_id, commit=True
        ):
            return request_id, {"ok": 1}
        return request_id, {"ok": 0, "errmsg": "No session specified"}

    def _cmd_abort_transaction(
        self,
        request_id: int,
        command_doc: dict[str, Any],
        db: Connection,
        db_name: str,
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``abortTransaction`` for a session."""
        lsid = command_doc.get("lsid")
        tx_session_id = _extract_session_id(lsid) if lsid else None
        if tx_session_id and self._finish_transaction(
            tx_session_id, commit=False
        ):
            return request_id, {"ok": 1}
        return request_id, {"ok": 0, "errmsg": "No session specified"}

    def _cmd_end_sessions(
        self,
        request_id: int,
        command_doc: dict[str, Any],
        db: Connection,
        db_name: str,
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``endSessions`` by closing matching idle and tx sessions."""
        session_ids = command_doc.get("endSessions", [])
        if isinstance(session_ids, str):
            session_ids = [session_ids]
        to_close: list[tuple[tuple[str, str], Any, Connection | None]] = []
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
                        self._session_last_used.pop(sess_key, None)
                        tx_conn = self._session_tx_conns.pop(sess_key, None)
                        if end_session is not None or tx_conn is not None:
                            to_close.append((sess_key, end_session, tx_conn))
            if to_close:
                self._tx_cond.notify_all()
        for sess_key, end_session, tx_conn in to_close:
            sid, scope = sess_key
            lock = (
                self._lock_for_session(sid)
                if tx_conn is not None
                else self._lock_for(scope or "admin")
            )
            with lock:
                self._safe_end_session(end_session, f"endSessions {sid}")
                self._safe_close_conn(tx_conn, f"endSessions tx {sid}")
        return request_id, {"ok": 1}

    def _cmd_create(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle the ``create`` collection command."""
        coll_name = ctx.cmd_copy.pop("create")
        try:
            ctx.db.create_collection(coll_name)
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise
            logger.debug(
                f"Ignoring existing collection during create {coll_name!r}: {e}"
            )
        return ctx.request_id, {"ok": 1}

    def _cmd_drop(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle the ``drop`` collection command, including GridFS tables."""
        coll_name = ctx.cmd_copy.pop("drop")
        if coll_name.startswith("sqlite_"):
            return ctx.request_id, {
                "ok": 0,
                "errmsg": f"Cannot drop internal table: {coll_name}",
            }
        if self._is_gridfs_collection(coll_name):
            # Dotted names (fs.files) cannot be tables in NeoSQLite;
            # drop the backing bucket instead, unless a regular
            # collection owns <bucket>_files or <bucket>_chunks.
            bucket_name = self._get_gridfs_bucket_name(coll_name)
            if bucket_name is None:
                return ctx.request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }
            try:
                _sanitize_db_name(bucket_name)
            except ValueError:
                return ctx.request_id, {
                    "ok": 0,
                    "errmsg": f"Invalid bucket name: {bucket_name!r}",
                }
            is_chunks = coll_name.endswith(".chunks")
            target_table = (
                f"{bucket_name}_chunks" if is_chunks else f"{bucket_name}_files"
            )
            if is_regular_collection_table(
                ctx.db.db, target_table, is_chunks=is_chunks
            ):
                return ctx.request_id, {
                    "ok": 0,
                    "errmsg": (
                        f"Cannot drop {coll_name!r}: {target_table!r} is "
                        "a regular collection"
                    ),
                }
            # Like real MongoDB, dropping one side leaves the other
            # (possibly orphaned) alone. Sanitized above: no quotes
            # possible, double-quoting keeps dashes valid.
            ctx.db.db.execute(f'DROP TABLE IF EXISTS "{target_table}"')
            try:
                ctx.db.db.commit()
            except Exception as exc:
                logger.debug(
                    f"Ignoring commit error after GridFS drop of {target_table!r}: {exc}"
                )
            return ctx.request_id, {"ok": 1}
        self._close_cursors_for_namespace(ctx.db_name, coll_name)
        ctx.db[coll_name].drop()
        return ctx.request_id, {"ok": 1}

    def _cmd_drop_database(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``dropDatabase``."""
        target_db = self._pop_alias(
            ctx.cmd_copy, "dropDatabase", "dropdatabase"
        )
        dropped_name = (
            ctx.db_name if target_db in (1, True, None, "") else str(target_db)
        )
        if self._mode == "single":
            # Legacy limitation: one file backs every logical database,
            # so dropping one drops all user tables. Prefer data_dir
            # (multi-file) mode for real isolation.
            self._change_stream_manager.invalidate(dropped_name)
            self._close_cursors_for_namespace(dropped_name)
            self.conn.drop_database()
            with self._dict_lock:
                self.databases = OrderedDict([("admin", self.conn)])
                self._conns = OrderedDict([("admin", self.conn)])
                self._applied_wc.clear()
        else:
            try:
                _sanitize_db_name(dropped_name)
            except ValueError:
                return ctx.request_id, {
                    "ok": 0,
                    "errmsg": f"Invalid database name: {dropped_name!r}",
                }
            if dropped_name != ctx.db_name:
                with (
                    self._lock_for(dropped_name),
                    self._track_active_op(dropped_name),
                ):
                    self._drop_file_db(dropped_name)
            else:
                self._drop_file_db(dropped_name)
        return ctx.request_id, {"dropped": dropped_name, "ok": 1}

    def _cmd_rename_collection(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``renameCollection`` within the logical database."""
        old_name = ctx.cmd_copy.pop("renameCollection")
        to_name = ctx.cmd_copy.pop("to", None)
        drop_target = bool(ctx.cmd_copy.pop("dropTarget", False))
        if (
            not isinstance(old_name, str)
            or not to_name
            or not isinstance(to_name, str)
        ):
            return ctx.request_id, {
                "ok": 0,
                "errmsg": "renameCollection requires 'to' parameter",
            }
        if "." in old_name:
            src_db, _, old_coll = old_name.partition(".")
        else:
            src_db, old_coll = ctx.db_name, old_name
        if "." in to_name:
            dst_db, _, new_coll = to_name.partition(".")
        else:
            dst_db, new_coll = ctx.db_name, to_name
        if not src_db or not dst_db or not old_coll or not new_coll:
            return ctx.request_id, {
                "ok": 0,
                "errmsg": "Invalid namespace for renameCollection",
            }
        if self._mode != "single" and src_db != dst_db:
            return ctx.request_id, {
                "ok": 0,
                "errmsg": (
                    "Cannot rename collection across databases: "
                    f"{src_db!r} != {dst_db!r}"
                ),
            }
        try:
            _sanitize_db_name(src_db)
        except ValueError as exc:
            return ctx.request_id, {"ok": 0, "errmsg": str(exc)}
        cross_db = src_db != ctx.db_name and self._mode != "single"
        lock_ctx = (
            self._lock_for(src_db) if cross_db else contextlib.nullcontext()
        )
        op_ctx = (
            self._track_active_op(src_db)
            if cross_db
            else contextlib.nullcontext()
        )
        try:
            with lock_ctx, op_ctx:
                target_db = self.get_database(src_db)
                existing_colls = set(target_db.list_collection_names())
                if (
                    drop_target
                    and new_coll != old_coll
                    and old_coll in existing_colls
                    and new_coll in existing_colls
                ):
                    target_db[new_coll].drop()
                target_db[old_coll].rename(new_coll)
        except Exception as exc:
            logger.debug(
                f"renameCollection {old_name!r} -> {to_name!r} failed: {exc}"
            )
            return ctx.request_id, {"ok": 0, "errmsg": str(exc)}
        return ctx.request_id, {"ok": 1}

    def _cmd_drop_indexes(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``dropIndexes``."""
        coll_name = ctx.cmd_copy.pop("dropIndexes")
        index = ctx.cmd_copy.pop("index", "*")
        coll = ctx.db[coll_name]
        num_before = len(coll.list_indexes())
        if index == "*":
            coll.drop_indexes()
        else:
            coll.drop_index(index)
        return ctx.request_id, {
            "ok": 1,
            "nIndexesWas": num_before,
        }

    def _cmd_create_indexes(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``createIndexes`` and legacy ``createIndex``."""
        if "createIndexes" in ctx.cmd_copy:
            coll_name = ctx.cmd_copy.pop("createIndexes")
            indexes_spec = ctx.cmd_copy.pop("indexes", [])
        else:
            coll_name = ctx.cmd_copy.pop("createIndex")
            indexes_spec = [ctx.cmd_copy]

        if _is_gridfs_collection(coll_name):
            adapter, _bucket_name = create_gridfs_adapter(ctx.db.db, coll_name)
            if adapter is None:
                return ctx.request_id, {
                    "ok": 0,
                    "errmsg": "Failed to create GridFS adapter",
                }
            return ctx.request_id, adapter.create_indexes()

        coll = ctx.db[coll_name]
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

            keys_list: Any
            if isinstance(key, dict):
                # Convert {"field": 1, "field2": -1} to [("field", 1), ("field2", -1)]
                # (or [("field", "text")] when fts=True so IndexManager honors fts & tokenizer).
                keys_list = [(k, "text" if fts else v) for k, v in key.items()]
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
            created_names.append(name or idx_name)
        return ctx.request_id, {
            "ok": 1,
            "createdCollectionAutomatically": False,
            "numIndexesBefore": num_before,
            "numIndexesAfter": len(coll.list_indexes()),
            "indexesCreated": [{"name": n} for n in created_names],
        }

    def _cmd_drop_index(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``dropIndex``."""
        coll_name = ctx.cmd_copy.pop("dropIndex")
        index = ctx.cmd_copy.pop("index")
        coll = ctx.db[coll_name]
        num_before = len(coll.list_indexes())
        coll.drop_index(index)
        return ctx.request_id, {
            "ok": 1,
            "nIndexesWas": num_before,
        }

    def _cmd_create_search_indexes(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``createSearchIndexes``."""
        coll_name = self._pop_alias(
            ctx.cmd_copy, "createSearchIndexes", "createsearchindexes"
        )
        specs = ctx.cmd_copy.pop("indexes", [])
        coll = ctx.db[coll_name]
        created = []
        for spec in specs:
            if not isinstance(spec, dict):
                continue
            try:
                fields = _search_index_fields(spec)
            except ValueError as exc:
                return ctx.request_id, {"ok": 0, "errmsg": str(exc)}
            tokenizer = spec.get("tokenizer")
            if not isinstance(tokenizer, str):
                tokenizer = None
            for field in fields:
                coll.create_search_index(field, tokenizer=tokenizer)
                created.append({"name": spec.get("name", field)})
        return ctx.request_id, {"ok": 1, "indexesCreated": created}

    def _cmd_update_search_index(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``updateSearchIndex``."""
        coll_name = self._pop_alias(
            ctx.cmd_copy, "updateSearchIndex", "updatesearchindex"
        )
        name = ctx.cmd_copy.get("name", "")
        definition = ctx.cmd_copy.get("definition", {})
        coll = ctx.db[coll_name]
        try:
            fields = _search_index_fields(
                {"name": name, "definition": definition}
            )
        except ValueError:
            fields = [name] if name else []
        if not fields:
            return ctx.request_id, {
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
        return ctx.request_id, {"ok": 1}

    def _cmd_drop_search_index(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``dropSearchIndex``."""
        coll_name = self._pop_alias(
            ctx.cmd_copy, "dropSearchIndex", "dropsearchindex"
        )
        name = ctx.cmd_copy.get("name", "")
        if not name:
            return ctx.request_id, {
                "ok": 0,
                "errmsg": "dropSearchIndex requires 'name'",
            }
        ctx.db[coll_name].drop_search_index(name)
        return ctx.request_id, {"ok": 1}

    def _cmd_delete(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``delete`` for regular and GridFS collections."""
        coll_name = ctx.cmd_copy.pop("delete")
        if self._is_gridfs_collection(coll_name):
            if "deletes" not in ctx.cmd_copy and ctx.payload_deletes:
                ctx.cmd_copy["deletes"] = ctx.payload_deletes
            return self._handle_gridfs_delete(
                ctx.request_id, ctx.cmd_copy, ctx.db, coll_name
            )
        ctx.cmd_copy["delete"] = coll_name
        if "deletes" not in ctx.cmd_copy and ctx.payload_deletes:
            ctx.cmd_copy["deletes"] = ctx.payload_deletes
        return self._handle_delete(
            ctx.request_id, ctx.cmd_copy, ctx.db, ctx.db_name
        )

    def _cmd_bulk_write(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle the MongoDB 8+ ``bulkWrite`` command."""
        if "ops" not in ctx.cmd_copy and ctx.payload_ops:
            ctx.cmd_copy["ops"] = ctx.payload_ops
        if "nsInfo" not in ctx.cmd_copy and ctx.payload_nsinfo:
            ctx.cmd_copy["nsInfo"] = ctx.payload_nsinfo
        return self._handle_bulk_write(
            ctx.request_id, ctx.cmd_copy, ctx.command_doc
        )

    def _cmd_upload(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle custom GridFS ``upload`` commands."""
        return self._handle_gridfs_upload(ctx.request_id, ctx.cmd_copy, ctx.db)

    def _cmd_open_download_stream(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle custom GridFS ``openDownloadStream`` commands."""
        file_id = ctx.cmd_copy.get("openDownloadStream")
        if isinstance(file_id, str):
            from neosqlite.objectid import ObjectId

            file_id = ObjectId(file_id)
        ctx.cmd_copy["fileId"] = file_id
        return self._handle_gridfs_download(
            ctx.request_id, ctx.cmd_copy, ctx.db
        )

    def _cmd_find_and_modify(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``findAndModify`` delete, replace, and update requests."""
        coll_name = ctx.cmd_copy.pop("findAndModify")
        query = self._convert_objectids(ctx.cmd_copy.pop("query", {}))
        update_doc = ctx.cmd_copy.pop("update", None)
        remove = ctx.cmd_copy.pop("remove", False)
        new_doc = ctx.cmd_copy.pop("new", False)
        fields = ctx.cmd_copy.pop("fields", None)
        sort_val = ctx.cmd_copy.pop("sort", None)
        upsert = ctx.cmd_copy.pop("upsert", False)
        array_filters = self._convert_objectids(
            ctx.cmd_copy.pop("arrayFilters", None)
        )

        sort_tuples = None
        if isinstance(sort_val, dict):
            sort_tuples = list(sort_val.items())
        elif isinstance(sort_val, list):
            sort_tuples = sort_val

        db, session_to_use = self._resolve_db_and_session(
            ctx.command_doc, ctx.db, ctx.db_name
        )
        coll = db[coll_name]
        self._apply_write_concern(
            db, ctx.db_name, ctx.cmd_copy.get("writeConcern")
        )

        if remove:
            try:
                doc = coll.find_one_and_delete(
                    query,
                    projection=fields,
                    sort=sort_tuples,
                    session=session_to_use,
                )
            except Exception as exc:
                logger.debug(f"findAndModify delete failed: {exc}")
                err = _bulk_write_error(exc, 0)
                return ctx.request_id, {
                    "ok": 0,
                    "code": err["code"],
                    "errmsg": err["errmsg"],
                }
            return ctx.request_id, {"ok": 1, "value": doc}

        if update_doc is None:
            return ctx.request_id, {
                "ok": 0,
                "errmsg": "findAndModify requires 'update' or 'remove'",
            }

        update_doc = self._convert_objectids(update_doc)
        is_replace = not any(k.startswith("$") for k in update_doc.keys())
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
        except Exception as exc:
            logger.debug(f"findAndModify update failed: {exc}")
            err = _bulk_write_error(exc, 0)
            return ctx.request_id, {
                "ok": 0,
                "code": err["code"],
                "errmsg": err["errmsg"],
            }
        return ctx.request_id, {"ok": 1, "value": doc}

    def _cmd_update(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``update`` for regular and GridFS collections."""
        coll_name = ctx.cmd_copy.pop("update")
        updates = ctx.cmd_copy.pop("updates", []) or ctx.payload_updates

        if self._is_gridfs_collection(coll_name):
            adapter, _bucket_name = create_gridfs_adapter(ctx.db.db, coll_name)
            if adapter is None:
                return ctx.request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }
            total_n = 0
            total_n_modified = 0
            for update in updates:
                q = self._convert_objectids(update.get("q", {}))
                u = self._convert_objectids(update.get("u", {}))
                file_id = q.get("_id")
                if file_id is not None:
                    res = adapter.handle_update(file_id, u)
                    total_n += res.get("n", 0)
                    total_n_modified += res.get("nModified", 0)
            return ctx.request_id, {
                "ok": 1,
                "n": total_n,
                "nModified": total_n_modified,
            }

        db, session_to_use = self._resolve_db_and_session(
            ctx.command_doc, ctx.db, ctx.db_name
        )
        coll = db[coll_name]
        self._apply_write_concern(
            db, ctx.db_name, ctx.cmd_copy.get("writeConcern")
        )
        ordered = ctx.cmd_copy.get("ordered", True)
        matched = 0
        modified = 0
        upserted: list[dict[str, Any]] = []
        write_errors: list[dict[str, Any]] = []

        for idx, update in enumerate(updates):
            q = self._convert_objectids(update.get("q", {}))
            u = self._convert_objectids(update.get("u", {}))
            multi = update.get("multi", False)
            upsert = update.get("upsert", False)
            array_filters = self._convert_objectids(update.get("arrayFilters"))

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
                logger.debug(f"Update op {idx} failed: {exc}")
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
                upserted.append({"index": idx, "_id": upd_result.upserted_id})

        response: dict[str, Any] = {
            "ok": 1,
            "n": matched,
            "nModified": modified,
        }
        if upserted:
            response["upserted"] = upserted
        if write_errors:
            response["writeErrors"] = write_errors
        return ctx.request_id, response

    def _cmd_find(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``find`` queries with sorting, paging, and cursor state."""
        coll_name = ctx.cmd_copy.pop("find")
        filter_query = self._convert_objectids(ctx.cmd_copy.pop("filter", {}))
        projection = ctx.cmd_copy.pop("projection", None)

        if self._is_gridfs_collection(coll_name):
            ctx.cmd_copy["filter"] = filter_query
            return self._handle_gridfs_find(
                ctx.request_id,
                ctx.cmd_copy,
                ctx.db,
                coll_name,
                ctx.db_name,
                owner=ctx.msg.get("_conn_id"),
            )

        db, session_to_use = self._resolve_db_and_session(
            ctx.command_doc, ctx.db, ctx.db_name
        )

        try:
            collection_exists = coll_name in db.list_collection_names()
        except Exception as exc:
            logger.debug(
                f"Failed checking existence of {coll_name!r} in find: {exc}"
            )
            collection_exists = False

        if not collection_exists:
            return ctx.request_id, {
                "ok": 1,
                "cursor": {
                    "id": Int64(0),
                    "ns": f"{ctx.db_name}.{coll_name}",
                    "firstBatch": [],
                },
            }

        coll = db[coll_name]
        cursor = (
            coll.find(filter_query, projection, session=session_to_use)
            if projection
            else coll.find(filter_query, session=session_to_use)
        )
        if "sort" in ctx.cmd_copy:
            cursor = cursor.sort(list(ctx.cmd_copy["sort"].items()))
        if "limit" in ctx.cmd_copy:
            cursor = cursor.limit(ctx.cmd_copy["limit"])
        if "skip" in ctx.cmd_copy:
            cursor = cursor.skip(ctx.cmd_copy["skip"])
        if "hint" in ctx.cmd_copy:
            hint_val = ctx.cmd_copy["hint"]
            if isinstance(hint_val, (str, list)):
                cursor = cursor.hint(hint_val)
        if "min" in ctx.cmd_copy:
            min_val = self._convert_objectids(ctx.cmd_copy["min"])
            if isinstance(min_val, dict):
                cursor = cursor.min(list(min_val.items()))
            elif isinstance(min_val, list):
                cursor = cursor.min(min_val)
        if "max" in ctx.cmd_copy:
            max_val = self._convert_objectids(ctx.cmd_copy["max"])
            if isinstance(max_val, dict):
                cursor = cursor.max(list(max_val.items()))
            elif isinstance(max_val, list):
                cursor = cursor.max(max_val)
        first, cursor_id = self._split_first_batch(
            cursor,
            self._first_batch_size(ctx.cmd_copy),
            f"{ctx.db_name}.{coll_name}",
            ctx.db_name,
            owner=ctx.msg.get("_conn_id"),
        )
        return ctx.request_id, {
            "ok": 1,
            "cursor": {
                "id": Int64(cursor_id),
                "ns": f"{ctx.db_name}.{coll_name}",
                "firstBatch": first,
            },
        }

    def _cmd_count(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle legacy ``count`` commands."""
        try:
            coll_name = ctx.cmd_copy.pop("count")
            query = self._convert_objectids(ctx.cmd_copy.pop("query", {}) or {})
            limit = ctx.cmd_copy.pop("limit", None)
            skip = ctx.cmd_copy.pop("skip", None)
            if self._is_gridfs_collection(coll_name):
                adapter, _bucket = create_gridfs_adapter(ctx.db.db, coll_name)
                if adapter is None:
                    return ctx.request_id, {
                        "ok": 0,
                        "errmsg": "Invalid GridFS collection",
                    }
                if coll_name.endswith(".chunks"):
                    docs = adapter.handle_chunks_find(query)
                else:
                    docs = adapter.handle_find(query)
                if skip:
                    docs = docs[int(skip) :]
                if limit:
                    docs = docs[: int(limit)]
                return ctx.request_id, {"ok": 1, "n": len(docs)}
            db, session_to_use = self._resolve_db_and_session(
                ctx.command_doc, ctx.db, ctx.db_name
            )
            coll = db[coll_name]
            if limit is None and skip is None:
                count = coll.count_documents(query, session=session_to_use)
            else:
                # count with limit/skip (legacy count command): apply
                # pagination over the match set.
                cursor = coll.find(query, session=session_to_use)
                if skip:
                    cursor = cursor.skip(int(skip))
                if limit:
                    cursor = cursor.limit(int(limit))
                count = len(list(cursor))
            return ctx.request_id, {"ok": 1, "n": count}
        except Exception as e:
            logger.error(f"Error in count: {e}")
            return ctx.request_id, {"ok": 0, "errmsg": str(e)}

    def _cmd_distinct(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``distinct`` commands."""
        try:
            coll_name = ctx.cmd_copy.pop("distinct")
            key = ctx.cmd_copy.pop("key", "")
            query = self._convert_objectids(ctx.cmd_copy.pop("query", {}) or {})
            if self._is_gridfs_collection(coll_name):
                return self._handle_gridfs_distinct(
                    ctx.request_id, coll_name, key, query, ctx.db
                )
            db, session_to_use = self._resolve_db_and_session(
                ctx.command_doc, ctx.db, ctx.db_name
            )
            coll = db[coll_name]
            values = coll.distinct(key, query, session=session_to_use)
            return ctx.request_id, {"ok": 1, "values": values}
        except Exception as e:
            logger.error(f"Error in distinct: {e}")
            return ctx.request_id, {"ok": 0, "errmsg": str(e)}

    def _cmd_aggregate(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``aggregate``, search-index listing, and change streams."""
        try:
            coll_name = ctx.cmd_copy.pop("aggregate")
            if self._is_gridfs_collection(coll_name):
                return self._handle_gridfs_aggregate(
                    ctx.request_id,
                    ctx.cmd_copy,
                    ctx.db,
                    coll_name,
                    ctx.db_name,
                    owner=ctx.msg.get("_conn_id"),
                )
            pipeline = self._convert_objectids(
                ctx.cmd_copy.pop("pipeline", []) or []
            )

            # Atlas-style search index listing arrives as an
            # aggregation stage; serve it from NeoSQLite FTS metadata.
            if any(
                isinstance(stage, dict) and "$listSearchIndexes" in stage
                for stage in pipeline
            ):
                return self._handle_list_search_indexes(
                    ctx.request_id, ctx.db, coll_name, ctx.db_name
                )

            if is_change_stream_pipeline(pipeline):
                return self._handle_change_stream(
                    ctx.request_id,
                    coll_name,
                    pipeline,
                    ctx.db,
                    ctx.db_name,
                    owner=ctx.msg.get("_conn_id"),
                )

            db, session_to_use = self._resolve_db_and_session(
                ctx.command_doc, ctx.db, ctx.db_name
            )
            coll = db[coll_name]
            cursor = coll.aggregate(pipeline, session=session_to_use)  # type: ignore[assignment]
            first, cursor_id = self._split_first_batch(
                cursor,
                self._first_batch_size(ctx.cmd_copy),
                f"{ctx.db_name}.{coll_name}",
                ctx.db_name,
                owner=ctx.msg.get("_conn_id"),
            )
            return ctx.request_id, {
                "ok": 1,
                "cursor": {
                    "id": Int64(cursor_id),
                    "ns": f"{ctx.db_name}.{coll_name}",
                    "firstBatch": first,
                },
            }
        except Exception as e:
            logger.error(f"Error in aggregate: {e}")
            return ctx.request_id, {"ok": 0, "errmsg": str(e)}

    def _cmd_list_collections(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``listCollections``."""
        return self._handle_list_collections(
            ctx.request_id, ctx.db, ctx.db_name
        )

    def _cmd_server_status(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``serverStatus`` and ``buildInfo``."""
        return self._handle_server_status(ctx.request_id, ctx.db)

    def _cmd_db_stats(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``dbStats``."""
        return ctx.request_id, ctx.db.command({"dbStats": 1})

    def _cmd_coll_stats(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``collStats``."""
        coll_name = ctx.cmd_copy.get("collStats") or ctx.cmd_copy.get(
            "collstats"
        )
        return ctx.request_id, ctx.db.command({"collstats": coll_name})

    def _cmd_list_databases(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``listDatabases`` in single-file, memory, and files modes."""
        if self._mode == "single":
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
            with self._dict_lock:
                single_db_names = list(self.databases)
            databases_info = [
                {
                    "name": name,
                    "sizeOnDisk": size_on_disk,
                    "empty": is_empty,
                }
                for name in single_db_names
            ]
            return ctx.request_id, {
                "ok": 1,
                "databases": databases_info,
                "totalSize": size_on_disk,
            }

        databases_info = []
        total_size = 0
        with self._dict_lock:
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
                _sanitize_db_name(name)
            except ValueError:
                continue
            with self._lock_for(name), self._track_active_op(name):
                try:
                    db_conn = self.get_database(name)
                except ValueError:
                    continue
                try:
                    empty = not db_conn.list_collection_names()
                except Exception as exc:
                    logger.debug(
                        f"Failed checking collection list for db {name!r}: {exc}"
                    )
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
        return ctx.request_id, {
            "ok": 1,
            "databases": databases_info,
            "totalSize": total_size,
        }

    def _cmd_list_indexes(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``listIndexes``."""
        coll_name = ctx.cmd_copy.get("listIndexes") or ctx.cmd_copy.get(
            "listindexes"
        )
        return self._handle_list_indexes(
            ctx.request_id, ctx.db, coll_name, ctx.db_name
        )

    def _cmd_list_search_indexes(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``listSearchIndexes``."""
        coll_name = ctx.cmd_copy.get("listSearchIndexes") or ctx.cmd_copy.get(
            "listsearchindexes"
        )
        return self._handle_list_search_indexes(
            ctx.request_id, ctx.db, coll_name, ctx.db_name
        )

    def _cmd_explain(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``explain`` commands."""
        explain_value = ctx.cmd_copy.get("explain")
        if isinstance(explain_value, dict):
            inner_cmd = self._convert_objectids(dict(explain_value))
            inner_cmd.pop("$db", None)

            if "find" in inner_cmd:
                coll_name = inner_cmd.pop("find")
                filter_query = self._convert_objectids(
                    inner_cmd.pop("filter", {})
                )

                if self._is_gridfs_collection(coll_name):
                    return ctx.request_id, {
                        "ok": 1,
                        "queryPlanner": {
                            "plannerVersion": 1,
                            "namespace": f"{ctx.db_name}.{coll_name}",
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

                cursor = ctx.db[coll_name].find(filter_query)
                explain_result = cursor.explain()

                return ctx.request_id, {
                    "ok": 1,
                    "queryPlanner": {
                        "plannerVersion": 1,
                        "namespace": f"{ctx.db_name}.{coll_name}",
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

        return ctx.request_id, {
            "ok": 1,
            "queryPlanner": {
                "winningPlan": {"stage": "EOF"},
            },
        }

    def _cmd_get_more(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``getMore`` for change streams and paginated data cursors."""
        raw_id = ctx.cmd_copy.get("getMore")
        cursor_id = int(raw_id) if raw_id is not None else 0
        coll_name = ctx.cmd_copy.get("collection")
        expected_ns = f"{ctx.db_name}.{coll_name}"
        stream = self._change_stream_manager.get_stream(cursor_id)
        if stream:
            if (
                stream.db_name != ctx.db_name
                or stream.collection_name != coll_name
            ):
                return ctx.request_id, {
                    "ok": 0,
                    "code": 43,
                    "codeName": "CursorNotFound",
                    "errmsg": (
                        f"cursor id {cursor_id} not found for "
                        f"namespace {expected_ns}"
                    ),
                }
            next_batch, token = self._change_stream_manager.pull_stream(
                stream, self._getmore_batch_size(ctx.cmd_copy)
            )
            return ctx.request_id, {
                "ok": 1,
                "cursor": {
                    "id": Int64(stream._id),
                    "ns": expected_ns,
                    "nextBatch": next_batch,
                    "postBatchResumeToken": token,
                },
            }
        data = self._getmore_data(
            cursor_id,
            self._getmore_batch_size(ctx.cmd_copy),
            expected_ns=expected_ns,
        )
        if data is not None:
            if data.get("_ns_mismatch"):
                return ctx.request_id, {
                    "ok": 0,
                    "code": 43,
                    "codeName": "CursorNotFound",
                    "errmsg": (
                        f"cursor id {cursor_id} not found for "
                        f"namespace {expected_ns}"
                    ),
                }
            return ctx.request_id, {"ok": 1, "cursor": data}
        return ctx.request_id, {
            "ok": 1,
            "cursor": {
                "id": Int64(0),
                "ns": expected_ns,
                "nextBatch": [],
            },
        }

    def _cmd_kill_cursors(
        self, ctx: _CommandContext
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``killCursors`` for change streams and data cursors."""
        kill_coll = ctx.cmd_copy.get("killCursors")
        kill_expected_ns = (
            f"{ctx.db_name}.{kill_coll}"
            if isinstance(kill_coll, str) and kill_coll
            else None
        )
        cursor_ids = ctx.cmd_copy.get("cursors", [])
        cursors_killed = []
        cursors_not_found = []
        data_ids = []
        for raw_cid in cursor_ids:
            try:
                cid = int(raw_cid)
            except (TypeError, ValueError):
                cursors_not_found.append(raw_cid)
                continue
            stream = self._change_stream_manager.get_stream(cid)
            if stream is not None:
                if stream.db_name == ctx.db_name and (
                    not isinstance(kill_coll, str)
                    or not kill_coll
                    or stream.collection_name == kill_coll
                ):
                    self._change_stream_manager.close_stream(cid)
                    cursors_killed.append(cid)
                else:
                    cursors_not_found.append(cid)
            else:
                data_ids.append(cid)
        if data_ids:
            killed_ids = self._kill_data_cursors(
                data_ids, expected_ns=kill_expected_ns
            )
            killed_set = set(killed_ids)
            for cid in data_ids:
                if cid in killed_set:
                    cursors_killed.append(cid)
                else:
                    cursors_not_found.append(cid)
        return ctx.request_id, {
            "ok": 1,
            "cursorsKilled": cursors_killed,
            "cursorsNotFound": cursors_not_found,
            "cursorsAlive": [],
        }

    def _cmd_vacuum(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``vacuum`` maintenance commands."""
        return ctx.request_id, ctx.db.command("vacuum")

    def _cmd_compact(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``compact`` / ``compactcollection`` commands."""
        coll_name = self._pop_alias(
            ctx.cmd_copy, "compact", "compactcollection"
        )
        kwargs: dict[str, Any] = {}
        if "dryRun" in ctx.cmd_copy:
            kwargs["dryRun"] = bool(ctx.cmd_copy.pop("dryRun"))
        if "freeSpaceTargetMB" in ctx.cmd_copy:
            kwargs["freeSpaceTargetMB"] = ctx.cmd_copy.pop("freeSpaceTargetMB")
        try:
            result = ctx.db.command("compact", coll_name, **kwargs)
        except Exception as exc:
            logger.debug(f"compact failed for {coll_name!r}: {exc}")
            return ctx.request_id, {"ok": 0, "errmsg": str(exc)}
        return ctx.request_id, result

    def _cmd_validate(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``validate`` collection commands."""
        coll_name = ctx.cmd_copy.pop("validate")
        if not isinstance(coll_name, str) or not coll_name:
            return ctx.request_id, {
                "ok": 0,
                "errmsg": "validate requires a collection name",
            }
        return ctx.request_id, ctx.db.command("validate", coll_name)

    def _cmd_reindex(self, ctx: _CommandContext) -> tuple[int, dict[str, Any]]:
        """Handle ``reindex`` / ``reIndex`` commands."""
        coll_name = self._pop_alias(ctx.cmd_copy, "reindex", "reIndex")
        if isinstance(coll_name, int) and coll_name == 1:
            # {"reIndex": 1} shape: rebuild every index in the file.
            coll_name = None
        try:
            return ctx.request_id, ctx.db.command("reindex", coll_name)
        except Exception as exc:
            logger.debug(f"reindex failed for {coll_name!r}: {exc}")
            return ctx.request_id, {"ok": 0, "errmsg": str(exc)}

    def _build_gridfs_cursor_response(
        self,
        request_id: int,
        docs: list[dict[str, Any]],
        command_doc: dict[str, Any],
        db_name: str,
        coll_name: str,
        owner: Any = None,
        paginate: bool = True,
    ) -> tuple[int, dict[str, Any]]:
        """Build a wire cursor response for GridFS query results."""
        resolved_owner = (
            owner if owner is not None else command_doc.get("_conn_id")
        )
        if paginate:
            first, cursor_id = self._split_first_batch(
                docs,
                self._first_batch_size(command_doc),
                f"{db_name}.{coll_name}",
                db_name,
                owner=resolved_owner,
            )
        else:
            first, cursor_id = docs, 0
        return request_id, {
            "ok": 1,
            "cursor": {
                "id": Int64(cursor_id),
                "ns": f"{db_name}.{coll_name}",
                "firstBatch": first,
            },
        }

    def _handle_gridfs_find(
        self,
        request_id: int,
        command_doc: dict,
        db: Connection,
        coll_name: str,
        db_name: str = "test",
        owner: Any = None,
        paginate: bool = True,
    ) -> tuple[int, dict[str, Any]]:
        """Handle find command on GridFS collections (fs.files or fs.chunks).

        Delegates ``.chunks`` queries to ``_handle_gridfs_chunks_find`` and
        queries ``.files`` via ``GridFSAdapter``, applying multi-key sorting,
        ``skip``, and ``limit`` before returning a cursor response.

        Args:
            request_id: Wire-protocol request identifier.
            command_doc: Parsed ``find`` command dictionary.
            db: Active ``neosqlite.Connection`` for the target database.
            coll_name: GridFS collection name (``*.files`` or ``*.chunks``).
            db_name: Logical database name for cursor namespace formatting.
            owner: Optional connection identifier owning the cursor.
            paginate: Whether to split results via ``_split_first_batch``.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                with ``cursor`` or error information.
        """
        logger.debug(f"_handle_gridfs_find called with coll_name={coll_name}")

        if coll_name.endswith(".chunks"):
            return self._handle_gridfs_chunks_find(
                request_id,
                command_doc,
                db,
                coll_name,
                db_name,
                owner=owner,
                paginate=paginate,
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

            if sort:
                sort_list = (
                    list(sort.items()) if isinstance(sort, dict) else sort
                )

                # Stable multi-key sort honoring per-key direction
                # (1/-1). Missing keys sort Mongo-style: first in
                # ascending, last in descending.
                def _nested_get(doc: dict[str, Any], key: str) -> Any:
                    """Resolve a dotted key path inside ``doc``."""
                    cur: Any = doc
                    for part in key.split("."):
                        if isinstance(cur, dict) and part in cur:
                            cur = cur[part]
                        else:
                            return None
                    return cur

                for sort_key, direction in reversed(list(sort_list)):
                    reverse = int(direction) < 0

                    present = [
                        doc
                        for doc in docs
                        if _nested_get(doc, sort_key) is not None
                    ]
                    missing = [
                        doc
                        for doc in docs
                        if _nested_get(doc, sort_key) is None
                    ]

                    def sort_value(
                        doc: dict[str, Any], key: str = sort_key
                    ) -> Any:
                        """Extract the sort key value at dotted ``key``."""
                        return _nested_get(doc, key)

                    def sort_value_str(
                        doc: dict[str, Any], key: str = sort_key
                    ) -> str:
                        """Return string sort key for mixed-type fallback."""
                        v = _nested_get(doc, key)
                        return str(v) if v is not None else ""

                    try:
                        present.sort(key=sort_value, reverse=reverse)
                    except TypeError:
                        present.sort(key=sort_value_str, reverse=reverse)
                    docs = (
                        missing + present if not reverse else present + missing
                    )

            if skip > 0:
                docs = docs[int(skip) :]
            if limit != 0:
                docs = docs[: abs(limit)]

            return self._build_gridfs_cursor_response(
                request_id,
                docs,
                command_doc,
                db_name,
                coll_name,
                owner=owner,
                paginate=paginate,
            )
        except Exception as e:
            logger.debug(f"GridFS find error: {e}", exc_info=True)
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_aggregate(
        self,
        request_id: int,
        command_doc: dict,
        db: Any,
        coll_name: str,
        db_name: str = "test",
        owner: Any = None,
    ) -> tuple[int, dict[str, Any]]:
        """Handle aggregate command on GridFS collections.

        GridFS collections (``fs.files`` / ``fs.chunks``) are backed by
        NeoSQLite tables without a ``data`` column, so the generic
        temporary-table aggregation path fails. This handler supports the
        subset of pipelines used by PyMongo (``count_documents``, simple
        find-like aggregations) by delegating to the GridFS adapter.

        Args:
            request_id: Wire-protocol request identifier.
            command_doc: Parsed ``aggregate`` command dictionary.
            db: Active ``neosqlite.Connection`` for the target database.
            coll_name: GridFS collection name (``*.files`` or ``*.chunks``).
            db_name: Logical database name for cursor namespace formatting.
            owner: Optional connection identifier owning the cursor.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                containing a cursor document or error response.
        """
        try:
            adapter, bucket_name = create_gridfs_adapter(db.db, coll_name)
            if adapter is None:
                return request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }

            pipeline = command_doc.get("pipeline", [])
            is_chunks = coll_name.endswith(".chunks")

            # Extract filter from a leading $match (if present).
            match_filter: dict[str, Any] = {}
            for stage in pipeline:
                if isinstance(stage, dict) and "$match" in stage:
                    match_filter = stage.get("$match") or {}
                    break

            match_filter = self._convert_objectids(match_filter or {}) or {}

            if is_chunks:
                docs = adapter.handle_chunks_find(match_filter)
            else:
                docs = adapter.handle_find(match_filter)

            # Apply $sort / $skip / $limit stages.
            for stage in pipeline:
                if not isinstance(stage, dict):
                    continue
                if "$sort" in stage:
                    sort_spec = stage["$sort"]
                    sort_list = (
                        list(sort_spec.items())
                        if isinstance(sort_spec, dict)
                        else sort_spec
                    )
                    for sort_key, direction in reversed(list(sort_list)):
                        reverse = int(direction) < 0
                        present = [
                            d for d in docs if d.get(sort_key) is not None
                        ]
                        missing = [d for d in docs if d.get(sort_key) is None]

                        def _sv(doc, key=sort_key):
                            """Return the sort field value for ``doc``.

                            Args:
                                doc: Document dictionary to read.
                                key: Field name to look up.

                            Returns:
                                Any: Value at ``key`` in ``doc``.
                            """
                            return doc.get(key)

                        def _sv_str(doc: Any, key: Any = sort_key) -> str:
                            """Return stringified sort field value.

                            Args:
                                doc: Document dictionary to read.
                                key: Field name to look up.

                            Returns:
                                str: ``str(doc.get(key))``.
                            """
                            return str(doc.get(key))

                        try:
                            present.sort(key=_sv, reverse=reverse)
                        except TypeError:
                            present.sort(key=_sv_str, reverse=reverse)
                        docs = (
                            missing + present
                            if not reverse
                            else present + missing
                        )
                if "$skip" in stage:
                    docs = docs[int(stage["$skip"]) :]
                if "$limit" in stage:
                    docs = docs[: int(stage["$limit"])]

            # If the pipeline is a count ($group with $sum), return count.
            is_count = any(
                isinstance(stage, dict)
                and "$group" in stage
                and isinstance(stage["$group"], dict)
                and any(
                    isinstance(v, dict) and "$sum" in v
                    for v in stage["$group"].values()
                )
                for stage in pipeline
            )
            if is_count:
                group_stage: dict[str, Any] = next(
                    (
                        stage["$group"]
                        for stage in pipeline
                        if isinstance(stage, dict) and "$group" in stage
                    ),
                    {},
                )
                # Determine group _id field name (usually None).
                group_id = group_stage.get("_id", None)
                result_doc = {"_id": group_id, "n": len(docs)}
                return self._build_gridfs_cursor_response(
                    request_id,
                    [result_doc],
                    command_doc,
                    db_name,
                    coll_name,
                    owner=owner,
                )

            return self._build_gridfs_cursor_response(
                request_id,
                docs,
                command_doc,
                db_name,
                coll_name,
                owner=owner,
            )
        except Exception as e:
            logger.debug(f"GridFS aggregate error: {e}", exc_info=True)
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_chunks_find(
        self,
        request_id: int,
        command_doc: dict,
        db: Connection,
        coll_name: str,
        db_name: str = "test",
        owner: Any = None,
        paginate: bool = True,
    ) -> tuple[int, dict[str, Any]]:
        """Handle find command on a GridFS ``.chunks`` collection.

        Args:
            request_id: Wire-protocol request identifier.
            command_doc: Parsed ``find`` command dictionary.
            db: Active ``neosqlite.Connection`` for the target database.
            coll_name: GridFS ``*.chunks`` collection name.
            db_name: Logical database name for cursor namespace formatting.
            owner: Optional connection identifier owning the cursor.
            paginate: Whether to split results via ``_split_first_batch``.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                with the matching chunk cursor batch or error details.
        """
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
                docs = docs[int(skip) :]
            if limit != 0:
                docs = docs[: abs(limit)]

            return self._build_gridfs_cursor_response(
                request_id,
                docs,
                command_doc,
                db_name,
                coll_name,
                owner=owner,
                paginate=paginate,
            )
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
        """Compute distinct field values over a GridFS collection.

        Args:
            request_id: Wire-protocol request identifier.
            coll_name: GridFS collection name (``*.files`` or ``*.chunks``).
            key: Top-level field name to collect distinct values from.
            query: Filter query dictionary applied before collecting values.
            db: Active ``neosqlite.Connection`` for the target database.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                containing ``values`` or an error message.
        """
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
            seen_keys: set[tuple] = set()
            for doc in docs:
                value = doc.get(key) if isinstance(doc, dict) else None
                val_key = _hashable_group_key(value)
                if val_key not in seen_keys:
                    seen_keys.add(val_key)
                    seen.append(value)
            return request_id, {"ok": 1, "values": seen}
        except Exception as exc:
            logger.error(f"GridFS distinct error: {exc}")
            return request_id, {"ok": 0, "errmsg": str(exc)}

    def _handle_gridfs_delete(
        self, request_id: int, cmd_copy: dict, db: Connection, coll_name: str
    ) -> tuple[int, dict[str, Any]]:
        """Handle delete command on GridFS collections.

        ``fs.files`` deletes match ``_id`` or ``filename`` (filename
        deletes remove every version, like legacy ``delete_by_name``);
        ``fs.chunks`` deletes match ``files_id``.

        Args:
            request_id: Wire-protocol request identifier.
            cmd_copy: Parsed ``delete`` command dictionary.
            db: Active ``neosqlite.Connection`` for the target database.
            coll_name: GridFS collection name (``*.files`` or ``*.chunks``).

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                from the GridFS adapter or an error dictionary.
        """
        logger.debug(f"_handle_gridfs_delete: coll_name={coll_name}")
        if not coll_name.endswith((".files", ".chunks")):
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
            if coll_name.endswith(".chunks"):
                chunk_file_ids = []
                for delete in deletes:
                    files_id = (delete.get("q") or {}).get("files_id")
                    if isinstance(files_id, dict) and "$eq" in files_id:
                        files_id = files_id["$eq"]
                    if files_id is not None:
                        chunk_file_ids.append(self._convert_objectids(files_id))
                return request_id, adapter.handle_chunks_delete(chunk_file_ids)

            file_ids = []
            filenames = []
            for delete in deletes:
                query = delete.get("q", {}) or {}
                file_id = query.get("_id")
                if isinstance(file_id, dict) and "$eq" in file_id:
                    file_id = file_id["$eq"]
                if file_id is not None:
                    file_ids.append(self._convert_objectids(file_id))
                elif query.get("filename"):
                    filenames.append(query["filename"])

            result = adapter.handle_delete(file_ids, filenames)
            return request_id, result
        except Exception as e:
            logger.error(f"GridFS delete error: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_gridfs_upload(
        self, request_id: int, cmd_copy: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle custom GridFS upload commands via ``GridFSBucket``.

        Args:
            request_id: Wire-protocol request identifier.
            cmd_copy: Parsed upload command dictionary containing
                ``filename`` and optional ``bucket``, ``metadata``,
                ``chunkSize``, and ``data``.
            db: Active ``neosqlite.Connection`` for the target database.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                containing ``fileId`` on success or an error message.
        """
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
                metadata=metadata or None,
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
        """Handle custom GridFS download commands via ``GridFSBucket``.

        Args:
            request_id: Wire-protocol request identifier.
            cmd_copy: Parsed download command dictionary containing
                ``fileId`` and optional ``bucket``.
            db: Active ``neosqlite.Connection`` for the target database.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                containing ``data`` bytes on success or an error message.
        """
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

    def _finish_transaction(self, session_id: str, *, commit: bool) -> bool:
        """Commit or abort all active transactions for ``session_id``.

        Acquires each transaction's session lock (for dedicated transactional
        connections) or target database lock (for direct fallback sessions),
        closes dedicated transactional connections, cleans up session entries,
        and wakes any memory-mode waiters.

        Args:
            session_id: Extracted logical session ID string.
            commit: ``True`` to commit, ``False`` to abort.

        Returns:
            bool: ``True`` if at least one active transaction was finished.
        """
        entries = self._tx_session_entries(session_id)
        if not entries:
            return False
        acted = False
        for key, sess, tx_conn in entries:
            sid, scope = key
            lock = (
                self._lock_for_session(sid)
                if tx_conn is not None
                else self._lock_for(scope or "admin")
            )
            with lock:
                try:
                    if commit:
                        sess.commit_transaction()
                    else:
                        sess.abort_transaction()
                    acted = True
                finally:
                    if tx_conn is not None:
                        self._safe_end_session(sess, f"finish tx {session_id}")
                        self._safe_close_conn(
                            tx_conn, f"finish tx {session_id}"
                        )
                        with self._sessions_lock:
                            if self._session_tx_conns.get(key) is tx_conn:
                                self._session_tx_conns.pop(key, None)
                            if self._sessions.get(key) is sess:
                                self._sessions.pop(key, None)
                                self._session_last_used.pop(key, None)
                            self._tx_cond.notify_all()
                    else:
                        with self._sessions_lock:
                            self._tx_cond.notify_all()
        return acted

    @contextlib.contextmanager
    def _target_op_lock(
        self, target_db_name: str, command_doc: dict[str, Any]
    ) -> Iterator[None]:
        """Acquire the per-db or per-session lock for a cross-DB op."""
        lsid = command_doc.get("lsid")
        sid = _extract_session_id(lsid) if isinstance(lsid, dict) else None
        starts_tx = bool(command_doc.get("startTransaction"))
        if sid and (
            starts_tx or self._session_has_dedicated_tx(sid, target_db_name)
        ):
            lock = self._lock_for_session(sid)
        else:
            lock = self._lock_for(target_db_name)
        with lock, self._track_active_op(target_db_name):
            yield

    def _handle_bulk_write(
        self, request_id: int, cmd: dict, command_doc: dict
    ) -> tuple[int, dict[str, Any]]:
        """Handle the client-level ``bulkWrite`` command (MongoDB 8.0+).

        Fans each op out to the target namespace's collection under that
        target database's lock. Ops may span databases; each runs under its
        own database session (cross-file atomicity is unsupported and
        documented).

        Args:
            request_id: Wire-protocol request identifier.
            cmd: Mutable command dictionary containing ``ops``, ``nsInfo``,
                ``ordered``, ``errorsOnly``, and optional ``writeConcern``.
            command_doc: Original command body used to resolve client
                sessions (``lsid``).

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                summarizing operation counts and cursor batch results.
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
                    target_db_name = ns_db or "test"
                    with self._target_op_lock(target_db_name, command_doc):
                        target = self.get_database(target_db_name)
                        target, sess = self._resolve_db_and_session(
                            command_doc, target, target_db_name
                        )
                        self._apply_write_concern(
                            target, target_db_name, cmd.get("writeConcern")
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
                    array_filters = self._convert_objectids(
                        op.get("arrayFilters")
                    )
                    ns_db, _, ns_coll = namespaces[ns_idx].partition(".")
                    target_db_name = ns_db or "test"
                    with self._target_op_lock(target_db_name, command_doc):
                        target = self.get_database(target_db_name)
                        target, sess = self._resolve_db_and_session(
                            command_doc, target, target_db_name
                        )
                        self._apply_write_concern(
                            target, target_db_name, cmd.get("writeConcern")
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
                        n_upserted += 1
                    if not errors_only:
                        upserted_inc = 1 if res.upserted_id is not None else 0
                        entry: dict[str, Any] = {
                            "ok": 1,
                            "idx": idx,
                            "n": res.matched_count + upserted_inc,
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
                    target_db_name = ns_db or "test"
                    with self._target_op_lock(target_db_name, command_doc):
                        target = self.get_database(target_db_name)
                        target, sess = self._resolve_db_and_session(
                            command_doc, target, target_db_name
                        )
                        self._apply_write_concern(
                            target, target_db_name, cmd.get("writeConcern")
                        )
                        coll = target[ns_coll]
                        if multi:
                            del_res = coll.delete_many(filt, session=sess)
                        else:
                            del_res = coll.delete_one(filt, session=sess)
                    n_deleted += del_res.deleted_count
                    if not errors_only:
                        results.append(
                            {"ok": 1, "idx": idx, "n": del_res.deleted_count}
                        )
                else:
                    raise ValueError(f"Unsupported bulkWrite op: {sorted(op)}")
            except Exception as exc:
                logger.debug(f"bulkWrite op {idx} failed: {exc}")
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
                "id": Int64(0),
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
        """Handle a ``delete`` command against a regular collection.

        Resolves the collection name, binds any session and write concern,
        and executes each entry in ``deletes`` via ``delete_many`` (when
        ``limit == 0``) or ``delete_one``, honoring ``ordered`` and
        collecting any ``writeErrors``.

        Args:
            request_id: Wire-protocol request identifier.
            command_doc: Parsed ``delete`` command dictionary.
            db: Active ``neosqlite.Connection`` for the target database.
            db_name: Logical database name for session/writeConcern scoping.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                with ``ok``, ``n`` deleted count, and optional
                ``writeErrors``.
        """
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

        db, session_to_use = self._resolve_db_and_session(
            command_doc, db, db_name
        )
        coll = db[coll_name]
        self._apply_write_concern(db, db_name, command_doc.get("writeConcern"))
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
                logger.debug(f"Delete op {idx} failed: {exc}")
                write_errors.append(_bulk_write_error(exc, idx))
                if ordered:
                    break
                continue
            removed += result.deleted_count

        response: dict[str, Any] = {"ok": 1, "n": removed}
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

        Tracking is trigger-backed (see ``changestream`` module): opening the
        stream installs SQLite triggers via a persistent NeoSQLite watcher,
        and ``getMore`` pulls new rows. Dotted names (e.g. GridFS
        ``fs.files``) cannot have triggers and yield empty streams.

        Args:
            request_id: Wire-protocol request identifier.
            coll_name: Target collection name being watched.
            pipeline: Aggregation pipeline beginning with ``$changeStream``.
            db: Active ``neosqlite.Connection`` for the target database.
            db_name: Logical database name for cursor namespace formatting.
            owner: Optional connection identifier owning the change stream.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                containing an initial empty cursor batch and resume token.
        """
        try:
            pipeline = self._convert_objectids(pipeline)
            options = extract_change_stream_options(pipeline)

            def _get_watched_collection() -> Any:
                return db[coll_name]

            stream = self._change_stream_manager.create_stream(
                collection_name=coll_name,
                pipeline=pipeline,
                resume_after=options.get("resume_after"),
                start_at_operation_time=options.get("start_at_operation_time"),
                full_document=options.get("full_document"),
                db_name=db_name,
                owner=owner,
                start_after=options.get("start_after"),
                get_collection=_get_watched_collection,
            )

            # Return empty batch initially - change streams start empty
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": Int64(stream._id),  # Use stream ID as cursor ID
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

        Args:
            conn_id: Client connection identifier, or ``None`` (no-op).
        """
        if conn_id is None:
            return
        self._change_stream_manager.close_streams_for_owner(
            conn_id, lock_for=self._lock_for
        )
        self.close_cursors_for_owner(conn_id)

    def _handle_server_status(
        self, request_id: int, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle ``serverStatus`` and ``buildInfo`` diagnostics commands.

        Collects process resident/virtual memory statistics, server uptime,
        and active connection counts into a MongoDB-compatible status doc.

        Args:
            request_id: Wire-protocol request identifier.
            db: Active ``neosqlite.Connection`` instance (unused).

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                status dictionary.
        """
        resident_mem = 0
        virtual_mem = 0
        if platform.system() == "Linux":
            try:
                with open("/proc/self/statm") as statm:
                    parts = statm.read().split()
                sysconf = getattr(os, "sysconf", None)
                if sysconf is not None:
                    page_size = sysconf("SC_PAGE_SIZE")
                    virtual_mem = int(parts[0]) * page_size
                    resident_mem = int(parts[1]) * page_size
            except (OSError, ValueError, IndexError):
                pass
        if resident_mem == 0 and virtual_mem == 0:
            try:
                import resource

                getrusage = getattr(resource, "getrusage", None)
                rusage_self = getattr(resource, "RUSAGE_SELF", None)
                if getrusage is not None and rusage_self is not None:
                    rusage = getrusage(rusage_self)
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
            "localTime": datetime.now(UTC),
            "asserts": {
                "regular": 0,
                "warning": 0,
                "msg": 0,
                "user": 0,
                "rollovers": 0,
            },
            "connections": {
                "current": current_connections,
                "available": max(
                    0, self._max_connections - current_connections
                ),
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
        """Handle the ``listIndexes`` command for a collection.

        Args:
            request_id: Wire-protocol request identifier.
            db: Active ``neosqlite.Connection`` for the target database.
            coll_name: Target collection name, or ``None`` (returns error).
            db_name: Logical database name for cursor namespace formatting.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                with a cursor containing index descriptor documents.
        """
        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        try:
            coll = db[coll_name]
        except Exception as exc:
            logger.debug(
                f"Collection {coll_name!r} unavailable for listIndexes: {exc}"
            )
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": Int64(0),
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
                "id": Int64(0),
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
        """Handle ``listSearchIndexes`` and ``$listSearchIndexes`` requests.

        Args:
            request_id: Wire-protocol request identifier.
            db: Active ``neosqlite.Connection`` for the target database.
            coll_name: Target collection name, or ``None`` (returns error).
            db_name: Logical database name for cursor namespace formatting.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                with a cursor containing FTS index descriptors.
        """
        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        try:
            coll = db[coll_name]
        except Exception as exc:
            logger.debug(
                f"Collection {coll_name!r} unavailable for listIndexes: {exc}"
            )
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": Int64(0),
                    "ns": f"{db_name}.{coll_name}",
                    "firstBatch": [],
                },
            }

        search_indexes = coll.list_search_indexes()
        index_list = [
            {"v": 2, "key": {idx_name: "text"}, "name": f"{idx_name}_text"}
            for idx_name in search_indexes
        ]

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": Int64(0),
                "ns": f"{db_name}.{coll_name}",
                "firstBatch": index_list,
            },
        }

    def _handle_list_collections(
        self, request_id: int, db: Connection, db_name: str = "test"
    ) -> tuple[int, dict[str, Any]]:
        """Handle the ``listCollections`` command for a database.

        Args:
            request_id: Wire-protocol request identifier.
            db: Active ``neosqlite.Connection`` for the target database.
            db_name: Logical database name for cursor namespace formatting.

        Returns:
            tuple[int, dict[str, Any]]: Pair of ``(request_id, response)``
                containing collection metadata entries in ``firstBatch``.
        """
        coll_names = db.list_collection_names()
        collections = [
            {
                "name": name,
                "type": "collection",
                "options": {},
                "info": {
                    "readOnly": False,
                    "uuid": "00000000-0000-0000-0000-000000000000",
                },
            }
            for name in coll_names
        ]

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": Int64(0),
                "ns": f"{db_name}.$cmd.listCollections",
                "firstBatch": collections,
            },
        }

    @_serialize
    def handle_query(
        self, msg: dict[str, Any]
    ) -> tuple[int, list[dict[str, Any]]]:
        """Handle legacy ``OP_QUERY`` (frozen).

        ``OP_QUERY`` was removed server-side in MongoDB 5.1; every modern
        driver uses ``OP_MSG``. This path stays as a find-only (plus
        ``$cmd`` passthrough) fallback and is intentionally not extended.

        Args:
            msg: Decoded ``OP_QUERY`` message dictionary containing
                ``request_id``, ``query``, ``collection``, and optional
                ``db``, ``number_to_skip``, and ``number_to_return``.

        Returns:
            tuple[int, list[dict[str, Any]]]: Pair of ``(request_id, docs)``
                matching the query or wrapping the command response.
        """
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
        query = self._convert_objectids(query)

        if self._is_gridfs_collection(collection):
            command_doc = dict(query)
            command_doc["find"] = collection
            if skip > 0:
                command_doc["skip"] = skip
            if limit != 0:
                command_doc["limit"] = limit
            _, response = self._handle_gridfs_find(
                msg["request_id"],
                command_doc,
                db,
                collection,
                db_name,
                owner=msg.get("_conn_id"),
                paginate=False,
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
