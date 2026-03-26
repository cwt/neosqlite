"""NeoSQLite handler for MongoDB commands and SQLite operations."""

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId as BsonObjectId

from neosqlite import Connection
from neosqlite.objectid import ObjectId as NeoObjectId
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
    ):
        self.db_path = db_path
        self.tokenizers = tokenizers
        self.journal_mode = journal_mode
        self.start_time = time.time()
        self._active_connections = 0
        self._connections_lock = threading.Lock()
        self._sessions: dict[str, Any] = {}
        self._sessions_lock = threading.Lock()

        if db_path == ":memory:":
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
        self.databases: dict[str, Connection] = {"admin": self.conn}
        self._change_stream_manager = ChangeStreamManager()

    def get_database(self, db_name: str) -> Connection:
        if db_name not in self.databases:
            self.databases[db_name] = self.conn
        return self.databases[db_name]

    def _convert_objectids(self, doc: dict) -> dict:
        """Convert PyMongo ObjectIds to NeoSQLite ObjectIds recursively."""

        def convert_value(value):
            if isinstance(value, dict):
                return self._convert_objectids(value)
            elif isinstance(value, list):
                return [convert_value(item) for item in value]
            elif isinstance(value, BsonObjectId):
                return NeoObjectId(value.binary)
            return value

        if not isinstance(doc, dict):
            return doc

        result = {}
        for key, value in doc.items():
            result[key] = convert_value(value)
        return result

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

    def _get_gridfs_bucket(self, db: Connection, coll_name: str) -> Any | None:
        """Get GridFSBucket for a GridFS collection."""
        from neosqlite.gridfs import GridFSBucket

        bucket_name = _get_gridfs_bucket_name(coll_name)
        if bucket_name is None:
            return None
        return GridFSBucket(db.db, bucket_name=bucket_name)

    def _convert_gridfs_result(self, grid_out) -> dict:
        """Convert GridOut to MongoDB-compatible dict."""
        return {
            "_id": grid_out._id,
            "filename": grid_out.filename,
            "length": grid_out.length,
            "chunkSize": grid_out.chunk_size,
            "uploadDate": grid_out.upload_date,
            "md5": grid_out.md5,
            "metadata": grid_out.metadata,
        }

    def _handle_gridfs_insert(
        self,
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
            return 0, {"ok": 1, "n": 0}

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
            return 0, {"ok": 0, "errmsg": "Invalid GridFS collection"}

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
        return 0, result

    def _gridfs_insert_file(self, bucket, doc: dict) -> BsonObjectId:
        """Insert a file document into GridFS files collection."""
        from datetime import datetime

        from bson import ObjectId

        file_id = doc.get("_id") or ObjectId()
        filename = doc.get("filename", "")
        length = doc.get("length", 0)
        chunk_size = doc.get("chunkSize", bucket._chunk_size_bytes)
        upload_date = doc.get("uploadDate")
        if isinstance(upload_date, datetime):
            upload_date = upload_date.isoformat()
        elif upload_date is None:
            upload_date = datetime.now().isoformat()
        md5 = doc.get("md5")
        metadata = doc.get("metadata", {})

        bucket._db.execute(
            f"""
            INSERT INTO {bucket._files_collection}
            (id, _id, filename, length, chunkSize, uploadDate, md5, metadata, content_type, aliases)
            VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                str(file_id),
                filename,
                length,
                chunk_size,
                upload_date,
                md5,
                str(metadata) if isinstance(metadata, dict) else metadata,
                None,
                None,
            ),
        )

        return file_id

    def _gridfs_insert_chunk(self, bucket, doc: dict) -> BsonObjectId:
        """Insert a chunk document into GridFS chunks collection."""
        from bson import ObjectId

        chunk_id = doc.get("_id") or ObjectId()
        files_id = doc.get("files_id")
        n = doc.get("n", 0)
        data = doc.get("data", b"")

        if isinstance(files_id, ObjectId):
            files_id = str(files_id)

        bucket._db.execute(
            f"""
            INSERT INTO {bucket._chunks_collection}
            (_id, files_id, n, data)
            VALUES (?, ?, ?, ?)
        """,
            (str(chunk_id) if chunk_id else None, files_id, n, data),
        )

        return chunk_id

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
            return self._handle_gridfs_insert(coll_name, docs_to_insert, db)

        if coll_name not in db._collections:
            coll = db.create_collection(coll_name)
        else:
            coll = db[coll_name]
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

        session_to_use = None
        if command_doc.get("startTransaction") and "lsid" in command_doc:
            lsid = command_doc.get("lsid")
            session_id = _extract_session_id(lsid) if lsid else None
            if session_id:
                with self._sessions_lock:
                    if session_id not in self._sessions:
                        session = self.conn.start_session()
                        session._in_transaction = False
                        self._sessions[session_id] = session
                    session_to_use = self._sessions[session_id]
                    if not session_to_use._in_transaction:
                        session_to_use.start_transaction()

        if docs_to_insert:
            result = coll.insert_many(docs_to_insert, session=session_to_use)
            return request_id, {
                "ok": 1,
                "n": len(result.inserted_ids),
                "insertedIds": result.inserted_ids,
            }

        return request_id, {"ok": 1, "n": 0}

    def handle_command(  # noqa: E501
        self, msg: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        """Handle command by passing directly to NeoSQLite."""
        request_id = msg["request_id"]
        sections = msg["sections"]

        command_doc = None
        payload_updates = []
        payload_deletes = []
        for section_type, doc in sections:
            if section_type == "body":
                command_doc = doc
            elif section_type == "payload":
                if isinstance(doc, dict):
                    if "updates" in doc:
                        payload_updates = doc["updates"]
                    if "deletes" in doc:
                        payload_deletes = doc["deletes"]

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
            session = self.conn.start_session()
            with self._sessions_lock:
                self._sessions[session_id] = session
            return request_id, {
                "ok": 1,
                "session": {"id": {"$oid": session_id}},
            }

        if "commitTransaction" in command_doc:
            lsid = command_doc.get("lsid")
            tx_session_id = _extract_session_id(lsid) if lsid else None
            if tx_session_id:
                with self._sessions_lock:
                    commit_session: Any = self._sessions.get(tx_session_id)
                if commit_session and commit_session.in_transaction:
                    commit_session.commit_transaction()
                    return request_id, {"ok": 1}
            return request_id, {"ok": 0, "errmsg": "No session specified"}

        if "abortTransaction" in command_doc:
            lsid = command_doc.get("lsid")
            tx_session_id = _extract_session_id(lsid) if lsid else None
            if tx_session_id:
                with self._sessions_lock:
                    abort_session: Any = self._sessions.get(tx_session_id)
                if abort_session and abort_session.in_transaction:
                    abort_session.abort_transaction()
                    return request_id, {"ok": 1}
            return request_id, {"ok": 0, "errmsg": "No session specified"}

        if "endSessions" in command_doc:
            session_ids = command_doc.get("endSessions", [])
            if isinstance(session_ids, str):
                session_ids = [session_ids]
            with self._sessions_lock:
                for sid in session_ids:
                    end_session: Any = None
                    if isinstance(sid, dict):
                        sid = _extract_session_id(sid)
                    elif isinstance(sid, bytes):
                        from bson import Binary

                        if isinstance(sid, Binary):
                            sid = sid.hex()
                    if sid:
                        end_session = self._sessions.pop(sid, None)
                    if end_session:
                        end_session.end_session()
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
            db[coll_name].drop()
            return request_id, {"ok": 1}

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
            created_names = []
            for index_spec in indexes_spec:
                key = index_spec.get("key", {})
                name = index_spec.get("name")
                unique = index_spec.get("unique", False)
                sparse = index_spec.get("sparse", False)
                fts = index_spec.get("fts", False)
                tokenizer = index_spec.get("tokenizer")

                if isinstance(key, dict):
                    keys_list = list(key.keys())
                else:
                    keys_list = key

                idx_name = coll.create_index(
                    keys_list,
                    unique=unique,
                    sparse=sparse,
                    fts=fts,
                    tokenizer=tokenizer,
                )
                if name and name != idx_name:
                    logger.warning(
                        f"Index name mismatch: requested '{name}', got '{idx_name}'"
                    )
                created_names.append(idx_name)
            return request_id, {
                "ok": 1,
                "createdCollectionAutomatically": False,
                "numIndexesBefore": len(coll.list_indexes()),
                "numIndexesAfter": len(coll.list_indexes()),
                "indexesCreated": [{"name": n} for n in created_names],
            }

        if "dropIndexes" in cmd_copy:
            coll_name = cmd_copy.pop("dropIndexes")
            index = cmd_copy.pop("index", "*")
            coll = db[coll_name]
            if index == "*":
                coll.drop_indexes()
                return request_id, {
                    "ok": 1,
                    "nIndexesWas": len(coll.list_indexes()) + 1,
                }
            else:
                coll.drop_index(index)
                return request_id, {
                    "ok": 1,
                    "nIndexesWas": len(coll.list_indexes()) + 1,
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

            if isinstance(key, dict):
                keys_list = list(key.keys())
            else:
                keys_list = key

            idx_name = coll.create_index(
                keys_list, unique=unique, sparse=sparse
            )
            return request_id, {
                "ok": 1,
                "createdCollectionAutomatically": False,
                "numIndexesBefore": len(coll.list_indexes()),
                "numIndexesAfter": len(coll.list_indexes()),
                "indexesCreated": [{"name": idx_name}],
            }

        if "dropIndex" in cmd_copy:
            coll_name = cmd_copy.pop("dropIndex")
            index = cmd_copy.pop("index")
            coll = db[coll_name]
            coll.drop_index(index)
            return request_id, {
                "ok": 1,
                "nIndexesWas": len(coll.list_indexes()) + 1,
            }

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
            return self._handle_delete(request_id, cmd_copy, db)

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
            cmd_copy.pop("fields", None)
            upsert = cmd_copy.pop("upsert", False)

            coll = db[coll_name]

            if remove:
                doc = coll.find_one_and_delete(query)
                return request_id, {"ok": 1, "value": doc}
            elif update_doc:
                update_doc = self._convert_objectids(update_doc)
                if upsert:
                    doc = coll.find_one_and_replace(
                        query, update_doc, upsert=True, return_document=new_doc
                    )
                else:
                    doc = coll.find_one_and_replace(
                        query, update_doc, return_document=new_doc
                    )
                return request_id, {"ok": 1, "value": doc}
            else:
                return request_id, {
                    "ok": 0,
                    "errmsg": "findAndModify requires 'update' or 'remove'",
                }

        if "update" in cmd_copy:
            coll_name = cmd_copy.pop("update")
            updates = cmd_copy.pop("updates", []) or payload_updates
            coll = db[coll_name]
            modified = 0
            for update in updates:
                q = update.get("q", {})
                u = update.get("u", {})
                q = self._convert_objectids(q)
                u = self._convert_objectids(u)
                multi = update.get("multi", False)
                upsert = update.get("upsert", False)
                is_replace = not any(k.startswith("$") for k in u.keys())
                if is_replace:
                    upd_result = coll.replace_one(q, u, upsert=upsert)
                    modified += upd_result.modified_count
                elif multi:
                    upd_result = coll.update_many(q, u, upsert=upsert)
                    modified += upd_result.modified_count
                else:
                    upd_result = coll.update_one(q, u, upsert=upsert)
                    modified += upd_result.modified_count
            return request_id, {"ok": 1, "n": modified, "nModified": modified}

        if "find" in cmd_copy:
            coll_name = cmd_copy.pop("find")
            filter_query = cmd_copy.pop("filter", {})
            filter_query = self._convert_objectids(filter_query)
            projection = cmd_copy.pop("projection", None)

            if self._is_gridfs_collection(coll_name):
                cmd_copy["filter"] = filter_query
                return self._handle_gridfs_find(
                    request_id, cmd_copy, db, coll_name
                )

            coll = db[coll_name]
            cursor = (
                coll.find(filter_query, projection)
                if projection
                else coll.find(filter_query)
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
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db.name}.{coll_name}",
                    "firstBatch": docs,
                },
            }

        if "count" in cmd_copy:
            try:
                coll_name = cmd_copy.pop("count")
                query = cmd_copy.pop("query", {})
                coll = db[coll_name]
                count = coll.count_documents(query)
                return request_id, {"ok": 1, "n": count}
            except Exception as e:
                logger.error(f"Error in count: {e}")
                return request_id, {"ok": 0, "errmsg": str(e)}

        if "distinct" in cmd_copy:
            try:
                coll_name = cmd_copy.pop("distinct")
                key = cmd_copy.pop("key", "")
                query = cmd_copy.pop("query", {})
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

                # Check if this is a change stream request
                if is_change_stream_pipeline(pipeline):
                    return self._handle_change_stream(
                        request_id, coll_name, pipeline, db
                    )

                coll = db[coll_name]
                cursor = coll.aggregate(pipeline)  # type: ignore[assignment]
                docs = cursor.to_list()
                return request_id, {
                    "ok": 1,
                    "cursor": {
                        "id": 0,
                        "ns": f"{db.name}.{coll_name}",
                        "firstBatch": docs,
                    },
                }
            except Exception as e:
                logger.error(f"Error in aggregate: {e}")
                return request_id, {"ok": 0, "errmsg": str(e)}

        if "listCollections" in cmd_copy:
            return self._handle_list_collections(request_id, db)

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
            databases_info = []
            total_size = 0
            for db_name, db_conn in self.databases.items():
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
                databases_info.append(
                    {
                        "name": db_name,
                        "sizeOnDisk": size_on_disk,
                        "empty": is_empty,
                    }
                )
                total_size += size_on_disk
            return request_id, {
                "ok": 1,
                "databases": databases_info,
                "totalSize": total_size,
            }

        if "listIndexes" in cmd_copy or "listindexes" in cmd_copy:
            coll_name = cmd_copy.get("listIndexes") or cmd_copy.get(
                "listindexes"
            )
            return self._handle_list_indexes(request_id, db, coll_name)

        if "listSearchIndexes" in cmd_copy or "listsearchindexes" in cmd_copy:
            coll_name = cmd_copy.get("listSearchIndexes") or cmd_copy.get(
                "listsearchindexes"
            )
            return self._handle_list_search_indexes(request_id, db, coll_name)

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
                                "namespace": f"{db.name}.{coll_name}",
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
                            "namespace": f"{db.name}.{coll_name}",
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

        logger.info(f"Calling db.command with: {cmd_copy}")
        cmd_result = db.command(cmd_copy)
        logger.info(
            f"NeoSQLite returned: {list(cmd_result.keys()) if isinstance(cmd_result, dict) else type(cmd_result)}"
        )
        return request_id, cmd_result

    def _handle_find(
        self, request_id: int, command_doc: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        coll_name = command_doc.get("find")
        if not coll_name:
            for key in command_doc:
                if key not in (
                    "$db",
                    "filter",
                    "projection",
                    "sort",
                    "limit",
                    "skip",
                    "lsid",
                ) and not key.startswith("$"):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        if self._is_gridfs_collection(coll_name):
            return self._handle_gridfs_find(
                request_id, command_doc, db, coll_name
            )

        coll = db[coll_name]
        filter_query = command_doc.get("filter", {})
        filter_query = self._convert_objectids(filter_query)

        cursor = coll.find(filter_query)

        if "sort" in command_doc:
            cursor = cursor.sort(list(command_doc["sort"].items()))
        if "limit" in command_doc:
            limit = command_doc["limit"]
            if limit > 0:
                cursor = cursor.limit(limit)
        if "skip" in command_doc:
            cursor = cursor.skip(command_doc["skip"])

        docs = list(cursor)
        return request_id, {
            "ok": 1,
            "cursor": {
                "id": 0,
                "ns": f"{db.name}.{coll_name}",
                "firstBatch": docs,
            },
        }

    def _handle_gridfs_find(
        self,
        request_id: int,
        command_doc: dict,
        db: Connection,
        coll_name: str,
    ) -> tuple[int, dict[str, Any]]:
        """Handle find command on GridFS collections (fs.files or fs.chunks)."""
        logger.debug(f"_handle_gridfs_find called with coll_name={coll_name}")

        if coll_name.endswith(".chunks"):
            return self._handle_gridfs_chunks_find(
                request_id, command_doc, db, coll_name
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

                def _sort_key(doc: dict) -> tuple:
                    """Extract sort key values from document."""
                    return tuple(doc.get(k, v) for k, v in sort_list)

                docs = sorted(docs, key=_sort_key)

            if limit > 0:
                docs = docs[:limit]

            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db.name}.{coll_name}",
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
            if limit > 0:
                docs = docs[:limit]

            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db.name}.{coll_name}",
                    "firstBatch": docs,
                },
            }
        except Exception as e:
            logger.error(f"GridFS chunks find error: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

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

    def _handle_update(
        self, request_id: int, command_doc: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        coll_name = command_doc.get("update")
        if not coll_name:
            for key in command_doc:
                if key not in (
                    "$db",
                    "updates",
                    "ordered",
                    "writeConcern",
                    "lsid",
                ) and not key.startswith("$"):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        coll = db[coll_name]
        updates = command_doc.get("updates", [])

        modified = 0
        upserted = 0
        for update in updates:
            q = update.get("q", {})
            u = update.get("u", {})
            q = self._convert_objectids(q)
            u = self._convert_objectids(u)
            upsert = update.get("upsert", False)
            multi = update.get("multi", False)

            if multi:
                result = coll.update_many(q, u, upsert=upsert)
            else:
                result = coll.update_one(q, u, upsert=upsert)
            modified += result.modified_count
            if upsert:
                upserted += 1

        return request_id, {"ok": 1, "n": modified, "nModified": modified}

    def _handle_delete(
        self, request_id: int, command_doc: dict, db: Connection
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
        deletes = command_doc.get("deletes", [])

        removed = 0
        for delete in deletes:
            q = delete.get("q", {})
            q = self._convert_objectids(q)
            limit = delete.get("limit", 0)

            result = coll.delete_many(q) if limit == 0 else coll.delete_one(q)
            removed += result.deleted_count

        return request_id, {"ok": 1, "n": removed}

    def _handle_aggregate(
        self, request_id: int, command_doc: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        coll_name = command_doc.get("aggregate")
        if not coll_name:
            for key in command_doc:
                if key not in (
                    "$db",
                    "pipeline",
                    "cursor",
                    "lsid",
                ) and not key.startswith("$"):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        coll = db[coll_name]
        pipeline = command_doc.get("pipeline", [])

        try:
            cursor = coll.aggregate(pipeline)
            docs = list(cursor)
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db.name}.{coll_name}",
                    "firstBatch": docs,
                },
            }
        except Exception as e:
            logger.error(f"Error in aggregate: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_change_stream(
        self,
        request_id: int,
        coll_name: str,
        pipeline: list[dict],
        db: Connection,
    ) -> tuple[int, dict[str, Any]]:
        """Handle change stream aggregate command."""
        try:
            options = extract_change_stream_options(pipeline)
            stream = self._change_stream_manager.create_stream(
                collection_name=coll_name,
                pipeline=pipeline,
                resume_after=options.get("resume_after"),
                start_at_operation_time=options.get("start_at_operation_time"),
                full_document=options.get("full_document"),
            )

            # Return empty batch initially - change streams start empty
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": stream._id,  # Use stream ID as cursor ID
                    "ns": f"{db.name}.{coll_name}",
                    "firstBatch": [],
                    "postBatchResumeToken": stream.get_resume_token(),
                },
            }
        except Exception as e:
            logger.error(f"Error in change stream: {e}")
            return request_id, {"ok": 0, "errmsg": str(e)}

    def _handle_count(
        self, request_id: int, command_doc: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        coll_name = command_doc.get("count")
        if not coll_name:
            for key in command_doc:
                if key not in ("$db", "query", "lsid") and not key.startswith(
                    "$"
                ):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        coll = db[coll_name]
        filter_query = command_doc.get("query", {})

        count = coll.count_documents(filter_query)
        return request_id, {"ok": 1, "n": count}

    def _handle_distinct(
        self, request_id: int, command_doc: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        coll_name = command_doc.get("distinct")
        if not coll_name:
            for key in command_doc:
                if key not in (
                    "$db",
                    "key",
                    "query",
                    "lsid",
                ) and not key.startswith("$"):
                    coll_name = key
                    break

        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        coll = db[coll_name]
        key = command_doc.get("key", "")
        filter_query = command_doc.get("query", {})

        values = coll.distinct(key, filter_query)
        return request_id, {"ok": 1, "values": values}

    def _handle_server_status(
        self, request_id: int, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle serverStatus command."""
        import os
        import platform
        from datetime import datetime, timezone

        try:
            import resource

            rusage = resource.getrusage(resource.RUSAGE_SELF)
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
        self, request_id: int, db: Connection, coll_name: str | None
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
                    "ns": f"{db.name}.{coll_name}",
                    "firstBatch": [],
                },
            }

        index_names = coll.list_indexes()
        index_list = []
        prefix = f"idx_{coll.name}_"
        for idx_name in index_names:
            if idx_name == f"{prefix}id":
                key = {"_id": 1}
            else:
                key_str = idx_name.removeprefix(prefix).replace("_", ".")
                key = {key_str: 1}
            index_list.append({"v": 2, "key": key, "name": idx_name})

        return request_id, {
            "ok": 1,
            "cursor": {
                "id": 0,
                "ns": f"{db.name}.{coll_name}",
                "firstBatch": index_list,
            },
        }

    def _handle_list_search_indexes(
        self, request_id: int, db: Connection, coll_name: str | None
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
                    "ns": f"{db.name}.{coll_name}",
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
                "ns": f"{db.name}.{coll_name}",
                "firstBatch": index_list,
            },
        }

    def _handle_list_collections(
        self, request_id: int, db: Connection
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
                "ns": f"{db.name}.$cmd.listCollections",
                "firstBatch": collections,
            },
        }

    def _handle_rename_collection(
        self, request_id: int, command_doc: dict, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle renameCollection command."""
        rename = command_doc.get("renameCollection")
        if not rename:
            return request_id, {"ok": 0, "errmsg": "No collection to rename"}

        to = command_doc.get("to", "")
        if not to:
            return request_id, {"ok": 0, "errmsg": "No target name specified"}

        old_coll = rename.split(".")[-1] if "." in rename else rename
        new_coll = to.split(".")[-1] if "." in to else to

        try:
            old_coll_obj = db[old_coll]
            docs = list(old_coll_obj.find({}))
            if docs:
                db[new_coll].insert_many(docs)
            db.drop_collection(old_coll)
            return request_id, {"ok": 1}
        except Exception as e:
            return request_id, {"ok": 0, "errmsg": str(e)}

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

            for key in query:
                if not key.startswith("$"):
                    result = self.handle_command(
                        {
                            "request_id": msg["request_id"],
                            "sections": [("body", query)],
                        }
                    )
                    return result[0], [result[1]]

            result = self.handle_command(
                {
                    "request_id": msg["request_id"],
                    "sections": [("body", query)],
                }
            )
            return result[0], [result[1]]

        if self._is_gridfs_collection(collection):
            command_doc = dict(query)
            command_doc["find"] = collection
            _, response = self._handle_gridfs_find(
                msg["request_id"], command_doc, db, collection
            )
            docs = response.get("cursor", {}).get("firstBatch", [])
            return msg["request_id"], docs

        coll = db[collection]

        if "$query" in query:
            filter_query = query.get("$query", {})
            sort = query.get("$orderby", {})
            limit = query.get("$limit", 0)

            cursor = coll.find(filter_query)
            if sort:
                cursor = cursor.sort(list(sort.items()))
            docs = list(cursor)
            if limit > 0:
                docs = docs[:limit]
        else:
            docs = list(coll.find(query))

        return msg["request_id"], docs
