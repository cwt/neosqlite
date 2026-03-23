#!/usr/bin/env python3
"""
NX-27017 - NeoSQLite Experimental Project 27017

A MongoDB wire protocol compatibility layer that uses SQLite as the backend
storage engine. Allows MongoDB clients to connect and perform operations while
data is actually stored in SQLite.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import socket
import struct
import sys

# Try to use uvloop for better async performance
try:
    import uvloop

    ASYNC_LIBRARY = "uvloop"
except ImportError:
    uvloop = None  # type: ignore
    ASYNC_LIBRARY = "asyncio"
import threading
import time
import uuid
from datetime import datetime, timezone
from itertools import count
from typing import Any

from bson import BSON, Int64, encode
from bson import ObjectId as BsonObjectId

from neosqlite import Connection
from neosqlite.objectid import ObjectId as NeoObjectId
from neosqlite.options import JournalMode
from nx_27017.gridfs_adapter import (
    GridFSAdapter,
    _convert_gridfs_collection_name,
    _get_gridfs_bucket_name,
    _is_gridfs_collection,
    create_gridfs_adapter,
)

logger = logging.getLogger("nx_27017")

PID_FILE = "/tmp/nx_27017.pid"
LOG_FILE = "/tmp/nx_27017.log"

# Counter for generating unique request IDs
_request_id_counter = count(1)


def _get_next_request_id() -> int:
    """Generate a unique request ID for responses."""
    return next(_request_id_counter)


def _extract_session_id(lsid: dict) -> str | None:
    """Extract session ID from lsid document, handling Binary and dict formats."""
    if not lsid:
        return None
    # Handle {"$oid": session_id} format (direct session ID in dict)
    if "$oid" in lsid:
        return lsid["$oid"]
    # Handle {"id": Binary(...)} format (PyMongo style)
    session_id_obj = lsid.get("id")
    if session_id_obj is None:
        return None
    if isinstance(session_id_obj, dict):
        return session_id_obj.get("$oid")
    from bson import Binary

    if isinstance(session_id_obj, Binary):
        return session_id_obj.hex()
    return str(session_id_obj)


def _convert_objectids(doc):
    """Convert NeoSQLite ObjectIds to BSON ObjectIds, and Decimal to float."""
    from decimal import Decimal as PyDecimal

    def _convert_value(value):
        if isinstance(value, NeoObjectId):
            return BsonObjectId(value.binary)
        elif isinstance(value, PyDecimal):
            return float(value)
        elif isinstance(value, list):
            return [_convert_value(item) for item in value]
        elif isinstance(value, dict):
            return _convert_objectids(value)
        return value

    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            if key == "id" and value == 0:
                result[key] = Int64(0)
            else:
                result[key] = _convert_value(value)
        return result
    elif isinstance(doc, list):
        return [_convert_value(item) for item in doc]
    elif isinstance(doc, PyDecimal):
        return float(doc)
    elif isinstance(doc, NeoObjectId):
        return BsonObjectId(doc.binary)
    return doc


class WireProtocol:
    """MongoDB wire protocol opcodes."""

    OP_MSG = 2013
    OP_QUERY = 2004


class ResponseBuilder:
    """Build MongoDB wire protocol responses."""

    @staticmethod
    def build_op_msg_reply(
        request_id: int,
        response_to: int,
        document: dict[str, Any],
        flags: int = 0,
    ) -> bytes:
        doc_data = encode(_convert_objectids(document))  # type: ignore[arg-type]
        sections = struct.pack("<B", 0) + doc_data
        body = struct.pack("<I", flags) + sections
        header = struct.pack(
            "<iiii",
            16 + len(body),
            request_id,
            response_to,
            WireProtocol.OP_MSG,
        )
        return header + body

    @staticmethod
    def build_reply(
        request_id: int,
        response_to: int,
        documents: list[dict[str, Any]],
        flags: int = 0,
    ) -> bytes:
        sections = []
        for doc in documents:
            doc_data = encode(_convert_objectids(doc))  # type: ignore[arg-type]
            sections.append(struct.pack("<B", 0))
            sections.append(doc_data)
        body = struct.pack("<I", flags) + b"".join(sections)
        header = struct.pack(
            "<iiii",
            16 + len(body),
            request_id,
            response_to,
            WireProtocol.OP_MSG,
        )
        return header + body


class OP_MSG:  # noqa: N801
    """Parser for MongoDB OP_MSG wire protocol messages."""

    @staticmethod
    def parse(data: bytes) -> dict[str, Any]:
        message_length = struct.unpack("<i", data[0:4])[0]
        request_id = struct.unpack("<i", data[4:8])[0]
        response_to = struct.unpack("<i", data[8:12])[0]
        opcode = struct.unpack("<i", data[12:16])[0]
        offset = 16
        flags = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        # Check if checksum is present (flag bit 1)
        has_checksum = bool(flags & 0x2)
        effective_length = (
            message_length - 4 if has_checksum else message_length
        )

        sections: list[tuple[str, Any]] = []
        while offset < effective_length:
            kind = data[offset]
            offset += 1

            if kind == 0:
                doc_len = struct.unpack("<i", data[offset : offset + 4])[0]

                # Sanity check
                if doc_len <= 0 or doc_len > 16777216:
                    logger.error(
                        f"Invalid document length: {doc_len} at offset {offset}"
                    )
                    raise ValueError(f"Invalid BSON document length: {doc_len}")

                if offset + doc_len > len(data):
                    logger.error(
                        f"Document extends beyond data: offset={offset}, "
                        f"doc_len={doc_len}, data_len={len(data)}, "
                        f"effective_length={effective_length}"
                    )
                    raise ValueError("Document extends beyond message")

                doc_data = data[offset : offset + doc_len]
                doc = BSON(doc_data).decode()
                sections.append(("body", doc))
                offset += doc_len
            elif kind == 1:
                # Kind 1: Document Sequence
                # Format: kind(1) + size(int32) + CString(identifier) + BSON documents*
                # Note: size includes the 4 bytes for itself
                size = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4

                # Read CString (null-terminated field name)
                cstring_end = data.index(b"\x00", offset)
                field_name = data[offset:cstring_end].decode("utf-8")
                offset = cstring_end + 1

                # Documents start here and go until size bytes are consumed
                # The size includes: 4 (size field) + identifier + null + documents
                docs_start = offset
                docs_end = offset - 4 + size
                doc_data = data[docs_start:docs_end]

                # Parse individual BSON documents from the concatenated data
                docs = []
                doc_offset = 0
                while doc_offset < len(doc_data):
                    doc_len = struct.unpack(
                        "<i", doc_data[doc_offset : doc_offset + 4]
                    )[0]
                    if doc_len <= 0 or doc_len > len(doc_data) - doc_offset:
                        break
                    doc = BSON(
                        doc_data[doc_offset : doc_offset + doc_len]
                    ).decode()
                    docs.append(doc)
                    doc_offset += doc_len

                sections.append(("payload", {field_name: docs}))
                offset = docs_end
            elif kind == 2:
                count = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                docs = []
                for _ in range(count):
                    doc_len = struct.unpack("<i", data[offset : offset + 4])[0]
                    doc_data = data[offset : offset + doc_len]
                    docs.append(BSON(doc_data).decode())
                    offset += doc_len
                sections.append(("payload_docs", docs))
            else:
                logger.error(f"Unknown section kind: {kind}")
                raise ValueError(f"Unknown section kind: {kind}")

        return {
            "request_id": request_id,
            "response_to": response_to,
            "opcode": opcode,
            "flags": flags,
            "sections": sections,
        }


class OP_QUERY:  # noqa: N801
    """Parser for MongoDB OP_QUERY wire protocol messages."""

    @staticmethod
    def parse(data: bytes) -> dict[str, Any]:
        struct.unpack("<i", data[0:4])[0]  # message_length (unused)
        request_id = struct.unpack("<i", data[4:8])[0]
        response_to = struct.unpack("<i", data[8:12])[0]
        struct.unpack("<i", data[12:16])[0]  # opcode (unused)
        offset = 16

        flags = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        null_pos = data.index(b"\x00", offset)
        full_collection_name = data[offset:null_pos].decode("utf-8")
        offset = null_pos + 1

        number_to_skip = struct.unpack("<i", data[offset : offset + 4])[0]
        offset += 4
        number_to_return = struct.unpack("<i", data[offset : offset + 4])[0]
        offset += 4

        doc_len = struct.unpack("<i", data[offset : offset + 4])[0]
        doc_data = data[offset : offset + doc_len]
        query = BSON(doc_data).decode()

        parts = full_collection_name.split(".", 1)
        db = parts[0] or "admin"
        collection = parts[1] if len(parts) > 1 else None

        return {
            "request_id": request_id,
            "response_to": response_to,
            "flags": flags,
            "db": db,
            "collection": collection,
            "number_to_skip": number_to_skip,
            "number_to_return": number_to_return,
            "query": query,
        }


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
        with self._connections_lock:
            self._active_connections += 1

    def decrement_connections(self) -> None:
        with self._connections_lock:
            self._active_connections -= 1

    def _is_gridfs_collection(self, coll_name: str) -> bool:
        """Check if collection name is a GridFS collection."""
        return _is_gridfs_collection(coll_name)

    def _get_gridfs_bucket_name(self, coll_name: str) -> str | None:
        """Extract bucket name from GridFS collection name, or None if not GridFS."""
        return _get_gridfs_bucket_name(coll_name)

    def _get_gridfs_bucket(self, db: Connection, coll_name: str):
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
        """Handle insert operations on GridFS collections using the GridFSAdapter.

        GridFS uses direct column storage (not JSON-in-column like regular collections).
        This method converts PyMongo's document format to GridFS's internal format.

        For chunks inserts, this method auto-creates file metadata if the file doesn't exist,
        since PyMongo doesn't explicitly send file metadata inserts.
        """
        logger.debug(
            f"_handle_gridfs_insert called: coll_name={coll_name}, doc_count={len(docs)}"
        )

        if not docs:
            logger.debug(f"_handle_gridfs_insert: no docs, returning success")
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

        # If inserting chunks, ensure the corresponding file metadata exists
        if is_chunks and docs:
            logger.debug(
                f"_handle_gridfs_insert: Ensuring file metadata exists for chunks"
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

        # Get database from command doc
        db_name = command_doc.get("$db", "test")
        db = self.get_database(db_name)

        # Handle handshake commands specially (need to report wire version)
        for key in ["ismaster", "isMaster", "hello", "Hello"]:
            if key in command_doc:
                return request_id, {
                    "ok": 1,
                    "isWritablePrimary": True,
                    "maxBsonObjectSize": 16777216,
                    "maxMessageSizeBytes": 48000000,
                    "maxWriteBatchSize": 100000,
                    "localTime": datetime.now(timezone.utc),
                    "logicalSessionTimeoutMinutes": 30,
                    "connectionId": 1,
                    "minWireVersion": 17,
                    "maxWireVersion": 21,
                }

        # Handle startSession
        if "startSession" in command_doc:
            session_id = f"session_{uuid.uuid4().hex}"
            session = self.conn.start_session()
            with self._sessions_lock:
                self._sessions[session_id] = session
            return request_id, {
                "ok": 1,
                "session": {"id": {"$oid": session_id}},
            }

        # Handle commitTransaction
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

        # Handle abortTransaction
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

        # Handle endSessions
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

        # Translate MongoDB commands to NeoSQLite API
        cmd_copy = dict(command_doc)

        # Remove $db from command doc (we use it to select database)
        db_name = cmd_copy.pop("$db", "test")
        db = self.get_database(db_name)

        # Handle collection creation via NeoSQLite method
        if "create" in cmd_copy:
            coll_name = cmd_copy.pop("create")
            try:
                db.create_collection(coll_name)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
            return request_id, {"ok": 1}

        # Handle collection deletion via NeoSQLite method
        if "drop" in cmd_copy:
            coll_name = cmd_copy.pop("drop")
            if coll_name.startswith("sqlite_"):
                return request_id, {
                    "ok": 0,
                    "errmsg": f"Cannot drop internal table: {coll_name}",
                }
            db[coll_name].drop()
            return request_id, {"ok": 1}

        # Handle renameCollection via NeoSQLite method
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

        # Handle createIndexes via NeoSQLite collection method
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

        # Handle dropIndexes via NeoSQLite collection method
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

        # Handle createIndex (singular - legacy)
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

        # Handle dropIndex (singular - legacy)
        if "dropIndex" in cmd_copy:
            coll_name = cmd_copy.pop("dropIndex")
            index = cmd_copy.pop("index")
            coll = db[coll_name]
            coll.drop_index(index)
            return request_id, {
                "ok": 1,
                "nIndexesWas": len(coll.list_indexes()) + 1,
            }

        # Handle delete (both GridFS and regular collections)
        if "delete" in cmd_copy:
            coll_name = cmd_copy.pop("delete")
            if self._is_gridfs_collection(coll_name):
                return self._handle_gridfs_delete(
                    request_id, cmd_copy, db, coll_name
                )
            # Non-GridFS delete - build command doc for _handle_delete
            cmd_copy["delete"] = coll_name
            # Merge deletes from payload if not already in cmd_copy
            if "deletes" not in cmd_copy and payload_deletes:
                cmd_copy["deletes"] = payload_deletes
            return self._handle_delete(request_id, cmd_copy, db)

        # Handle GridFS upload
        if "upload" in cmd_copy:
            return self._handle_gridfs_upload(request_id, cmd_copy, db)

        # Handle GridFS download (openDownloadStream)
        if "openDownloadStream" in cmd_copy:
            file_id = cmd_copy.get("openDownloadStream")
            if isinstance(file_id, str):
                from neosqlite.objectid import ObjectId

                file_id = ObjectId(file_id)
            cmd_copy["fileId"] = file_id
            return self._handle_gridfs_download(request_id, cmd_copy, db)

        # Handle findAndModify via NeoSQLite collection method
        # IMPORTANT: This must be checked BEFORE "update" because findAndModify
        # commands contain an 'update' or 'remove' field
        if "findAndModify" in cmd_copy:
            coll_name = cmd_copy.pop("findAndModify")
            query = cmd_copy.pop("query", {})
            query = self._convert_objectids(query)
            update_doc = cmd_copy.pop("update", None)
            remove = cmd_copy.pop("remove", False)
            new_doc = cmd_copy.pop("new", False)
            fields = cmd_copy.pop("fields", None)
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

        # Handle update via NeoSQLite collection method
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
                # Check if this is a replace operation (u has no operators)
                is_replace = not any(k.startswith("$") for k in u.keys())
                if is_replace:
                    # Replace operation - use replace_one
                    upd_result = coll.replace_one(q, u, upsert=upsert)
                    modified += upd_result.modified_count
                elif multi:
                    upd_result = coll.update_many(q, u, upsert=upsert)
                    modified += upd_result.modified_count
                else:
                    upd_result = coll.update_one(q, u, upsert=upsert)
                    modified += upd_result.modified_count
            return request_id, {"ok": 1, "n": modified, "nModified": modified}

        # Handle find via NeoSQLite collection method
        if "find" in cmd_copy:
            coll_name = cmd_copy.pop("find")
            filter_query = cmd_copy.pop("filter", {})
            filter_query = self._convert_objectids(filter_query)
            projection = cmd_copy.pop("projection", None)

            # Check if this is a GridFS collection
            if self._is_gridfs_collection(coll_name):
                # Put filter back for _handle_gridfs_find which expects it in command_doc
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
            # Handle hint (index hint)
            if "hint" in cmd_copy:
                hint_val = cmd_copy["hint"]
                if isinstance(hint_val, str):
                    cursor = cursor.hint(hint_val)
                elif isinstance(hint_val, list):
                    cursor = cursor.hint(hint_val)
            # Handle min (minimum index bounds)
            # PyMongo sends min/max as dicts, but cursor.min() expects list of tuples
            if "min" in cmd_copy:
                min_val = cmd_copy["min"]
                if isinstance(min_val, dict):
                    # Convert dict to list of tuples
                    cursor = cursor.min(list(min_val.items()))
                elif isinstance(min_val, list):
                    cursor = cursor.min(min_val)
            # Handle max (maximum index bounds)
            if "max" in cmd_copy:
                max_val = cmd_copy["max"]
                if isinstance(max_val, dict):
                    # Convert dict to list of tuples
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

        # Handle count via NeoSQLite collection method
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

        # Handle distinct via NeoSQLite collection method
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

        # Handle aggregate via NeoSQLite
        if "aggregate" in cmd_copy:
            try:
                coll_name = cmd_copy.pop("aggregate")
                pipeline = cmd_copy.pop("pipeline", [])
                coll = db[coll_name]
                cursor = coll.aggregate(pipeline)  # type: ignore[assignment]
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

        # Handle listCollections via NeoSQLite
        if "listCollections" in cmd_copy:
            return self._handle_list_collections(request_id, db)

        # Handle serverStatus - needs special handling for proper MongoDB format
        if "serverStatus" in cmd_copy or "buildInfo" in cmd_copy:
            return self._handle_server_status(request_id, db)

        # Handle dbStats - delegate to NeoSQLite (returns MongoDB format)
        if "dbStats" in cmd_copy or "dbstats" in cmd_copy:
            db_stats_result = db.command({"dbStats": 1})
            return request_id, db_stats_result

        # Handle collStats - delegate to NeoSQLite (returns MongoDB format)
        if "collStats" in cmd_copy or "collstats" in cmd_copy:
            coll_name = cmd_copy.get("collStats") or cmd_copy.get("collstats")
            coll_stats_result = db.command({"collstats": coll_name})
            return request_id, coll_stats_result

        # Handle listDatabases
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

        # Handle listIndexes
        if "listIndexes" in cmd_copy or "listindexes" in cmd_copy:
            coll_name = cmd_copy.get("listIndexes") or cmd_copy.get(
                "listindexes"
            )
            return self._handle_list_indexes(request_id, db, coll_name)

        # Handle listSearchIndexes
        if "listSearchIndexes" in cmd_copy or "listsearchindexes" in cmd_copy:
            coll_name = cmd_copy.get("listSearchIndexes") or cmd_copy.get(
                "listsearchindexes"
            )
            return self._handle_list_search_indexes(request_id, db, coll_name)

        # Handle explain command - extract inner command and execute with explain
        if "explain" in cmd_copy:
            explain_value = cmd_copy.get("explain")
            if isinstance(explain_value, dict):
                # Execute the inner command with explain
                inner_cmd = dict(explain_value)
                # Remove $db if present (already selected)
                inner_cmd.pop("$db", None)

                # Handle find command with explain
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

                    # Execute find and get explain info
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

                # Fall back to basic explain
                return request_id, {
                    "ok": 1,
                    "queryPlanner": {
                        "winningPlan": {"stage": "EOF"},
                    },
                }

        # Pass all other commands to NeoSQLite's command() method
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

        # Handle fs.chunks - return empty results (PyMongo queries chunks internally during uploads)
        if coll_name.endswith(".chunks"):
            logger.debug(f"Returning empty cursor for fs.chunks query")
            return request_id, {
                "ok": 1,
                "cursor": {
                    "id": 0,
                    "ns": f"{db.name}.{coll_name}",
                    "firstBatch": [],
                },
            }

        if not coll_name.endswith(".files"):
            return request_id, {
                "ok": 0,
                "errmsg": "GridFS find only supported on .files or .chunks collections",
            }

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
                docs = sorted(
                    docs,
                    key=lambda f: tuple(f.get(k, v) for k, v in sort_list),
                )

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

    def _handle_gridfs_delete(
        self, request_id: int, cmd_copy: dict, db: Connection, coll_name: str
    ) -> tuple[int, dict[str, Any]]:
        """Handle delete command on GridFS collections."""
        if not coll_name.endswith(".files"):
            return request_id, {
                "ok": 0,
                "errmsg": "GridFS delete only supported on .files collections",
            }

        try:
            bucket = self._get_gridfs_bucket(db, coll_name)
            if bucket is None:
                return request_id, {
                    "ok": 0,
                    "errmsg": "Invalid GridFS collection",
                }

            deletes = cmd_copy.get("deletes", [])
            removed = 0
            for delete in deletes:
                file_id = delete.get("q", {}).get("_id")
                if file_id:
                    bucket.delete(file_id)
                    removed += 1

            return request_id, {"ok": 1, "n": removed}
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
            "connections": {"current": current_connections, "available": 1000},
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

        # Extract collection name from full namespace
        old_coll = rename.split(".")[-1] if "." in rename else rename
        new_coll = to.split(".")[-1] if "." in to else to

        try:
            # NeoSQLite doesn't have native rename, so we copy and delete
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
            )  # noqa: E501
            return result[0], [result[1]]

        # Check if this is a GridFS collection BEFORE accessing db[collection]
        # because accessing db[collection] creates a table with that name
        if self._is_gridfs_collection(collection):
            # Route to GridFS handler - build command doc structure
            command_doc = dict(query)
            command_doc["find"] = collection
            _, response = self._handle_gridfs_find(
                msg["request_id"], command_doc, db, collection
            )
            # Extract firstBatch from cursor response
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


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handler: NeoSQLiteHandler,
):
    """Handle a single client connection."""
    handler.increment_connections()
    try:
        while True:
            # Read header (16 bytes)
            header_bytes = bytearray(16)
            pos = 0
            while pos < 16:
                chunk = await reader.read(16 - pos)
                if not chunk:
                    return  # Connection closed
                header_bytes[pos : pos + len(chunk)] = chunk
                pos += len(chunk)
            header = bytes(header_bytes)

            message_length = struct.unpack("<i", header[0:4])[0]
            request_id = struct.unpack("<i", header[4:8])[0]
            struct.unpack("<i", header[8:12])[0]  # response_to (unused)
            opcode = struct.unpack("<i", header[12:16])[0]

            # Sanity check message length
            if message_length < 16 or message_length > 48000000:
                logger.warning(f"Invalid message length: {message_length}")
                return

            # Read body
            if message_length > 16:
                body = bytearray(message_length - 16)
                pos = 0
                remaining = message_length - 16
                while pos < remaining:
                    chunk = await reader.read(remaining - pos)
                    if not chunk:
                        logger.warning(
                            f"Incomplete message: expected {remaining}, "
                            f"got {pos}"
                        )
                        return
                    body[pos : pos + len(chunk)] = chunk
                    pos += len(chunk)
                full_message = header + bytes(body)
            else:
                full_message = header

            match opcode:
                case WireProtocol.OP_MSG:
                    msg = OP_MSG.parse(full_message)

                    is_insert = False
                    for s_type, s_data in msg["sections"]:
                        if (
                            s_type == "body"
                            and isinstance(s_data, dict)
                            and "insert" in s_data
                        ):
                            is_insert = True
                            break

                    has_payload_docs = any(
                        s[0] == "payload_docs" for s in msg["sections"]
                    )

                    try:
                        logger.debug(
                            f"OP_MSG about to handle: is_insert={is_insert}, has_payload={has_payload_docs}"
                        )
                        logger.debug(
                            f"OP_MSG msg['request_id']={msg.get('request_id')}, msg['response_to']={msg.get('response_to')}"
                        )
                        # Log the body of non-insert messages
                        if not is_insert and not has_payload_docs:
                            for s_type, s_data in msg["sections"]:
                                if s_type == "body":
                                    logger.debug(
                                        f"Non-insert command body: {s_data}"
                                    )
                        client_request_id = msg.get("request_id", 0)
                        if is_insert or has_payload_docs:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_insert, msg
                            )
                        else:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_command, msg
                            )
                        logger.debug(
                            f"OP_MSG handled: request_id={request_id}, response_doc={response_doc.get('ok') if isinstance(response_doc, dict) else 'N/A'}"
                        )
                    except Exception as e:
                        logger.error(f"Error handling OP_MSG: {e}")
                        import traceback

                        traceback.print_exc()
                        response_doc = {"ok": 0, "errmsg": str(e)}
                        request_id = msg.get("request_id", 0)

                    try:
                        reply = ResponseBuilder.build_op_msg_reply(
                            request_id=0,
                            response_to=msg.get("request_id", 0),
                            document=response_doc,
                        )  # noqa: E501
                        writer.write(reply)
                        await writer.drain()
                    except Exception as e:
                        logger.error(f"Error sending response: {e}")

                case WireProtocol.OP_QUERY:
                    msg = OP_QUERY.parse(full_message)
                    orig_request_id = msg["request_id"]
                    try:
                        request_id, docs = await asyncio.to_thread(
                            handler.handle_query, msg
                        )
                        reply = ResponseBuilder.build_reply(
                            request_id, orig_request_id, docs
                        )
                        writer.write(reply)
                        await writer.drain()
                    except Exception as e:
                        logger.error(f"Error handling OP_QUERY: {e}")

                case _:
                    error_reply = ResponseBuilder.build_op_msg_reply(
                        request_id=0,
                        response_to=request_id,
                        document={
                            "ok": 0,
                            "errmsg": f"Unsupported opcode: {opcode}",
                        },
                    )
                    writer.write(error_reply)
                    await writer.drain()

    except (
        ConnectionResetError,
        BrokenPipeError,
        asyncio.IncompleteReadError,
    ):
        pass
    finally:
        handler.decrement_connections()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def handle_client_threaded(
    client_socket: socket.socket,
    handler: NeoSQLiteHandler,
):
    """Handle a single client connection (threaded version)."""
    handler.increment_connections()
    try:
        with client_socket:
            while True:
                # Read header (16 bytes)
                header_bytes = bytearray(16)
                pos = 0
                while pos < 16:
                    chunk = client_socket.recv(16 - pos)
                    if not chunk:
                        return  # Connection closed
                    header_bytes[pos : pos + len(chunk)] = chunk
                    pos += len(chunk)
                header = bytes(header_bytes)

                message_length = struct.unpack("<i", header[0:4])[0]
                request_id = struct.unpack("<i", header[4:8])[0]
                response_to = struct.unpack("<i", header[8:12])[
                    0
                ]  # Save original response_to
                opcode = struct.unpack("<i", header[12:16])[0]

                # Sanity check message length
                if message_length < 16 or message_length > 48000000:
                    logger.warning(f"Invalid message length: {message_length}")
                    return

                # Read body
                if message_length > 16:
                    body = bytearray(message_length - 16)
                    pos = 0
                    remaining = message_length - 16
                    while pos < remaining:
                        chunk = client_socket.recv(remaining - pos)
                        if not chunk:
                            logger.warning(
                                f"Incomplete message: expected {remaining}, "
                                f"got {pos}"
                            )
                            return
                        body[pos : pos + len(chunk)] = chunk
                        pos += len(chunk)
                    full_message = header + bytes(body)
                else:
                    full_message = header

                logger.debug(
                    f"Received: len={len(full_message)}, opcode={opcode}"
                )

                if opcode == WireProtocol.OP_MSG:
                    msg = OP_MSG.parse(full_message)

                    is_insert = False
                    for s_type, s_data in msg["sections"]:
                        if (
                            s_type == "body"
                            and isinstance(s_data, dict)
                            and "insert" in s_data
                        ):
                            is_insert = True
                            break

                    if is_insert or any(
                        s[0] == "payload_docs" for s in msg["sections"]
                    ):
                        _, response_doc = (
                            handler.handle_insert(msg)
                            if is_insert
                            else (request_id, {"ok": 0})
                        )
                    else:
                        _, response_doc = handler.handle_command(msg)

                    logger.info(
                        f"Command response: response_keys={list(response_doc.keys()) if isinstance(response_doc, dict) else type(response_doc)}"
                    )
                    # Generate new request_id for response, use client's request_id as response_to
                    response_request_id = _get_next_request_id()
                    reply = ResponseBuilder.build_op_msg_reply(
                        request_id=response_request_id,
                        response_to=request_id,  # Client's request_id
                        document=response_doc,
                    )
                    logger.info(
                        f"Sending reply: len={len(reply)}, "
                        f"first_40_bytes={list(reply[:40])}"
                    )
                    client_socket.sendall(reply)

                elif opcode == WireProtocol.OP_QUERY:
                    msg = OP_QUERY.parse(full_message)
                    orig_request_id = msg["request_id"]
                    _, docs = handler.handle_query(msg)
                    # Generate new request_id for response
                    response_request_id = _get_next_request_id()
                    reply = ResponseBuilder.build_reply(
                        response_request_id, orig_request_id, docs
                    )
                    client_socket.sendall(reply)

                else:
                    error_reply = ResponseBuilder.build_op_msg_reply(
                        request_id=0,
                        response_to=request_id,
                        document={
                            "ok": 0,
                            "errmsg": f"Unsupported opcode: {opcode}",
                        },
                    )
                    client_socket.sendall(error_reply)

    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        logger.debug(f"Client connection error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in client thread: {e}")
    finally:
        handler.decrement_connections()


def run_server_threaded(
    host: str,
    port: int,
    handler: NeoSQLiteHandler,
    use_threading: bool = True,
):
    """Run the MongoDB wire protocol server (threaded or async)."""
    if not use_threading:
        # Use async version with uvloop for better performance if available
        if uvloop is not None:
            uvloop.install()
        asyncio.run(run_server(host, port, handler))
        return

    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(128)

    logger.info(f"Listening on {host}:{port} (threaded mode)")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            logger.info(f"Accepted connection from {addr}")

            # Handle each client in a separate thread
            client_thread = threading.Thread(
                target=handle_client_threaded,
                args=(client_socket, handler),
                daemon=True,
            )
            client_thread.start()

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        server_socket.close()


async def run_server(host: str, port: int, handler: NeoSQLiteHandler):
    """Run the MongoDB wire protocol server."""

    async def handle_client_closure(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await handle_client(reader, writer, handler)

    server = await asyncio.start_server(
        handle_client_closure,
        host,
        port,
    )

    addr = server.sockets[0].getsockname()
    logger.info(f"Listening on {addr[0]}:{addr[1]}")

    async with server:
        await server.serve_forever()


def write_pid_file(pid_file: str) -> bool:
    """Write PID to file. Returns False if already running."""
    if is_running(pid_file):
        pid = get_pid(pid_file)
        logger.error(f"NX-27017 is already running (PID: {pid})")
        return False

    try:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))
        return True
    except OSError as e:
        logger.error(f"Cannot write PID file: {e}")
        return False


def remove_pid_file(pid_file: str):
    """Remove PID file on shutdown."""
    try:
        if os.path.exists(pid_file):
            os.remove(pid_file)
    except OSError:
        pass


def is_running(pid_file: str) -> bool:
    """Check if daemon is already running."""
    if not os.path.exists(pid_file):
        return False

    pid = get_pid(pid_file)
    if pid is None:
        return False

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_pid(pid_file: str) -> int | None:
    """Get PID from file."""
    try:
        with open(pid_file) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def stop_daemon(pid_file: str) -> int:
    """Stop the running daemon."""
    if not os.path.exists(pid_file):
        print("NX-27017 is not running (no PID file found)")
        return 1

    pid = get_pid(pid_file)
    if pid is None:
        print("Invalid PID file")
        return 1

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to NX-27017 (PID: {pid})")

        for _ in range(10):
            try:
                os.kill(pid, 0)
            except OSError:
                print("NX-27017 stopped")
                return 0
            time.sleep(0.5)

        os.kill(pid, signal.SIGKILL)
        print(f"Forcefully killed NX-27017 (PID: {pid})")
        return 0
    except ProcessLookupError:
        print("NX-27017 is not running")
        remove_pid_file(pid_file)
        return 1
    except PermissionError:
        print(f"Permission denied to send signal to PID {pid}")
        return 1


def check_status(pid_file: str) -> int:
    """Check daemon status."""
    if is_running(pid_file):
        pid = get_pid(pid_file)
        print(f"NX-27017 is running (PID: {pid})")
        return 0
    else:
        print("NX-27017 is not running")
        return 1


def daemonize():
    """Perform Unix daemonization (double-fork)."""
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(f"First fork failed: {e}")
        sys.exit(1)

    os.chdir("/")
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(f"Second fork failed: {e}")
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()


def _signal_exit_handler(signum: int, frame: Any) -> None:
    """Handle termination signals by exiting cleanly."""
    sys.exit(0)


def run_as_daemon(args: argparse.Namespace):
    """Run the server as a background daemon."""
    if is_running(args.pid_file):
        pid = get_pid(args.pid_file)
        logger.error(f"NX-27017 is already running (PID: {pid})")
        sys.exit(1)

    if args.db_path != "memory" and not os.path.isabs(args.db_path):
        args.db_path = os.path.abspath(args.db_path)

    if args.fts5_tokenizers:
        args.fts5_tokenizers = [
            (name, os.path.abspath(path)) for name, path in args.fts5_tokenizers
        ]

    daemonize()

    with open(args.log_file, "a") as log_fh:
        os.dup2(log_fh.fileno(), sys.stdout.fileno())
        os.dup2(log_fh.fileno(), sys.stderr.fileno())

    if not write_pid_file(args.pid_file):
        sys.exit(1)

    signal.signal(signal.SIGTERM, _signal_exit_handler)
    signal.signal(signal.SIGINT, _signal_exit_handler)

    try:
        run_server_sync(args)
    finally:
        remove_pid_file(args.pid_file)


def run_foreground(args: argparse.Namespace):
    """Run the server in the foreground."""
    if not write_pid_file(args.pid_file):
        sys.exit(1)

    signal.signal(signal.SIGTERM, _signal_exit_handler)
    signal.signal(signal.SIGINT, _signal_exit_handler)

    try:
        run_server_sync(args)
    finally:
        remove_pid_file(args.pid_file)


def run_server_sync(args: argparse.Namespace):
    """Run the server synchronously."""
    db_path = args.db_path
    if db_path == "memory":
        db_path = ":memory:"

    tokenizers = args.fts5_tokenizers

    async_lib = "uvloop" if uvloop is not None else "asyncio"
    logger.info(
        "Starting NX-27017 with db_path=%s, host=%s, port=%s, journal_mode=%s, tokenizers=%s (async=%s, threaded=%s)",
        db_path,
        args.host,
        args.port,
        args.journal_mode,
        tokenizers,
        async_lib,
        args.threaded,
    )

    handler = NeoSQLiteHandler(
        db_path, tokenizers=tokenizers, journal_mode=args.journal_mode
    )

    try:
        run_server_threaded(
            args.host, args.port, handler, use_threading=args.threaded
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        prog="nx_27017",
        description=(
            "NeoSQLite Experimental Project 27017 (NX-27017) - "
            "MongoDB Wire Protocol Server"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run in foreground with default db
  %(prog)s -d                       # Run as daemon in background
  %(prog)s -d --db /data/mongo.db   # Daemon with specific database
  %(prog)s --host 0.0.0.0 -p 27018  # Listen on all interfaces, port 27018
  %(prog)s --stop                   # Stop running daemon
  %(prog)s --status                 # Check daemon status
  %(prog)s --verbose                # Enable debug logging
  %(prog)s --threaded               # Use threaded server (debugging)
        """,
    )

    parser.add_argument(
        "-d",
        "--daemon",
        action="store_true",
        help="Run as a background daemon",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="nx-27017.db",
        help=(
            "SQLite database path (default: nx-27017.db, "
            "use 'memory' for in-memory)"
        ),
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=27017,
        help="Port to listen on (default: 27017)",
    )
    parser.add_argument(
        "--log-file",
        default=LOG_FILE,
        help=f"Log file path (default: {LOG_FILE})",
    )
    parser.add_argument(
        "--pid-file",
        default=PID_FILE,
        help=f"PID file path (default: {PID_FILE})",
    )
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Stop the running daemon",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check if daemon is running",
    )
    parser.add_argument(
        "--threaded",
        action="store_true",
        help="Use threaded server instead of asyncio (for debugging)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    parser.add_argument(
        "--fts5-tokenizer",
        dest="fts5_tokenizers",
        action="append",
        default=None,
        help=(
            "FTS5 tokenizer as 'name=path' (can be specified multiple times). "
            "Example: --fts5-tokenizer icu=/path/to/libfts5_icu.so"
        ),
    )
    parser.add_argument(
        "-j",
        "--journal-mode",
        dest="journal_mode",
        default="WAL",
        choices=["WAL", "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"],
        help=(
            "SQLite journal mode (default: WAL). "
            "WAL provides best concurrency; DELETE is traditional rollback."
        ),
    )

    args = parser.parse_args()

    args.journal_mode = JournalMode.validate(args.journal_mode)

    if args.fts5_tokenizers:
        args.fts5_tokenizers = [
            tuple(t.split("=", 1)) for t in args.fts5_tokenizers
        ]
    else:
        args.fts5_tokenizers = None

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    match (args.stop, args.status, args.daemon):
        case (True, _, _):
            return stop_daemon(args.pid_file)
        case (_, True, _):
            return check_status(args.pid_file)
        case (_, _, True):
            return run_as_daemon(args)
        case _:
            run_foreground(args)


if __name__ == "__main__":
    main()
