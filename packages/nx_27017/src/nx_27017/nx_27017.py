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
import threading
import time
from datetime import datetime, timezone
from itertools import count
from typing import Any

from bson import BSON, Int64, encode
from bson import ObjectId as BsonObjectId
from neosqlite import Connection
from neosqlite.objectid import ObjectId as NeoObjectId

logger = logging.getLogger("nx_27017")

PID_FILE = "/tmp/nx_27017.pid"
LOG_FILE = "/tmp/nx_27017.log"

# Counter for generating unique request IDs
_request_id_counter = count(1)


def _get_next_request_id() -> int:
    """Generate a unique request ID for responses."""
    return next(_request_id_counter)


def _convert_objectids(doc):
    """Convert NeoSQLite ObjectIds to BSON ObjectIds, and Decimal to float."""
    from decimal import Decimal as PyDecimal

    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            if key == "id" and value == 0:
                result[key] = Int64(0)
            elif isinstance(value, NeoObjectId):
                result[key] = BsonObjectId(value.binary)
            elif isinstance(value, PyDecimal):
                result[key] = float(value)
            elif isinstance(value, list):
                result[key] = [_convert_objectids(item) for item in value]
            elif isinstance(value, dict):
                result[key] = _convert_objectids(value)
            else:
                result[key] = value
        return result
    elif isinstance(doc, list):
        return [_convert_objectids(item) for item in doc]
    elif isinstance(doc, PyDecimal):
        return float(doc)
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
        sections = b""
        for doc in documents:
            doc_data = encode(_convert_objectids(doc))  # type: ignore[arg-type]
            sections += struct.pack("<B", 0) + doc_data
        body = struct.pack("<I", flags) + sections
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
        section_num = 0
        while offset < effective_length:
            kind = data[offset]
            offset += 1
            section_num += 1

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
        db = parts[0] if len(parts) > 0 else "admin"
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

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = Connection(db_path, check_same_thread=False)
        self.databases: dict[str, Connection] = {"admin": self.conn}

    def get_database(self, db_name: str) -> Connection:
        if db_name not in self.databases:
            # Use :memory: or create file-based db
            if self.db_path == ":memory:":
                db_path = f"{db_name}.db"
            else:
                # Use base path with db name prefix
                base_path = self.db_path.replace(".db", "")
                db_path = f"{base_path}_{db_name}.db"
            self.databases[db_name] = Connection(
                db_path, check_same_thread=False
            )
        return self.databases[db_name]

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

        coll = db[coll_name]

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

        if docs_to_insert:
            result = coll.insert_many(docs_to_insert)
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
        for section_type, doc in sections:
            if section_type == "body":
                command_doc = doc
                break

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

        # Handle ping specially (MongoDB expects simple {"ok": 1})
        if "ping" in command_doc:
            return request_id, {"ok": 1}

        # Translate MongoDB commands to NeoSQLite API
        cmd_copy = dict(command_doc)

        # Remove $db from command doc (we use it to select database)
        db_name = cmd_copy.pop("$db", "test")
        db = self.get_database(db_name)

        # Handle collection creation via NeoSQLite method
        if "create" in cmd_copy:
            coll_name = cmd_copy.pop("create")
            db.create_collection(coll_name)
            return request_id, {"ok": 1}

        # Handle collection deletion via NeoSQLite method
        if "drop" in cmd_copy:
            coll_name = cmd_copy.pop("drop")
            db[coll_name].drop()
            return request_id, {"ok": 1}

        # Handle renameCollection via NeoSQLite method
        if "renameCollection" in cmd_copy:
            old_name = cmd_copy.pop("renameCollection")
            to_name = cmd_copy.pop("to", None)
            if to_name:
                db[old_name].rename(to_name)
                return request_id, {"ok": 1}
            return request_id, {
                "ok": 0,
                "errmsg": "renameCollection requires 'to' parameter",
            }

        # Handle insert via NeoSQLite collection method
        if "insert" in cmd_copy:
            coll_name = cmd_copy.pop("insert")
            documents = cmd_copy.pop("documents", [])
            coll = db[coll_name]
            if documents:
                result = coll.insert_many(documents)
                return request_id, {
                    "ok": 1,
                    "n": len(result.inserted_ids),
                    "insertedIds": result.inserted_ids,
                }
            return request_id, {"ok": 1, "n": 0}

        # Handle delete via NeoSQLite collection method
        if "delete" in cmd_copy:
            coll_name = cmd_copy.pop("delete")
            deletes = cmd_copy.pop("deletes", [])
            coll = db[coll_name]
            removed = 0
            for delete in deletes:
                q = delete.get("q", {})
                limit = delete.get("limit", 0)
                if limit == 0:
                    del_result = coll.delete_many(q)
                    removed += del_result.deleted_count
                else:
                    del_result = coll.delete_one(q)
                    removed += del_result.deleted_count
            return request_id, {"ok": 1, "n": removed}

        # Handle update via NeoSQLite collection method
        if "update" in cmd_copy:
            coll_name = cmd_copy.pop("update")
            updates = cmd_copy.pop("updates", [])
            coll = db[coll_name]
            modified = 0
            for update in updates:
                q = update.get("q", {})
                u = update.get("u", {})
                multi = update.get("multi", False)
                if multi:
                    upd_result = coll.update_many(q, u)
                    modified += upd_result.modified_count
                else:
                    upd_result = coll.update_one(q, u)
                    modified += upd_result.modified_count
            return request_id, {"ok": 1, "n": modified, "nModified": modified}

        # Handle find via NeoSQLite collection method
        if "find" in cmd_copy:
            coll_name = cmd_copy.pop("find")
            filter_query = cmd_copy.pop("filter", {})
            coll = db[coll_name]
            cursor = coll.find(filter_query)
            if "sort" in cmd_copy:
                cursor = cursor.sort(list(cmd_copy["sort"].items()))
            if "limit" in cmd_copy:
                cursor = cursor.limit(cmd_copy["limit"])
            if "skip" in cmd_copy:
                cursor = cursor.skip(cmd_copy["skip"])
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

        # Handle serverStatus
        if "serverStatus" in cmd_copy or "buildInfo" in cmd_copy:
            return self._handle_server_status(request_id, db)

        # Handle dbStats
        if "dbStats" in cmd_copy or "dbstats" in cmd_copy:
            return self._handle_db_stats(request_id, db)

        # Handle collStats
        if "collStats" in cmd_copy or "collstats" in cmd_copy:
            coll_name = cmd_copy.get("collStats") or cmd_copy.get("collstats")
            return self._handle_coll_stats(request_id, db, coll_name)

        # Handle listDatabases
        if "listDatabases" in cmd_copy or "listdatabases" in cmd_copy:
            return request_id, {
                "ok": 1,
                "databases": [
                    {"name": db.name, "sizeOnDisk": 0, "empty": False}
                ],
                "totalSize": 0,
            }

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

        coll = db[coll_name]
        filter_query = command_doc.get("filter", {})

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

        if pipeline and isinstance(pipeline, list):
            for stage in pipeline:
                if isinstance(stage, dict) and "$count" in stage:
                    count_field = stage["$count"]
                    cursor = coll.find({})
                    count = len(list(cursor))
                    return request_id, {
                        "ok": 1,
                        "cursor": {
                            "id": 0,
                            "ns": f"{db.name}.{coll_name}",
                            "firstBatch": [{count_field: count}],
                        },
                    }

            if len(pipeline) == 2:
                match_stage = pipeline[0]
                group_stage = pipeline[1]
                if (
                    isinstance(match_stage, dict)
                    and "$match" in match_stage
                    and isinstance(group_stage, dict)
                    and "$group" in group_stage
                ):
                    group_spec = group_stage["$group"]
                    if (
                        group_spec.get("_id") == 1
                        and len(group_spec) == 2
                        and "n" in group_spec
                        and group_spec["n"].get("$sum") == 1
                    ):
                        filter_query = match_stage.get("$match", {})
                        count = coll.count_documents(filter_query)
                        return request_id, {
                            "ok": 1,
                            "cursor": {
                                "id": 0,
                                "ns": f"{db.name}.{coll_name}",
                                "firstBatch": [{"_id": 1, "n": count}],
                            },
                        }

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

        return request_id, {
            "ok": 1,
            "host": platform.node(),
            "version": "7.0.0",
            "process": "nx_27017",
            "pid": os.getpid(),
            "uptime": 0,
            "uptimeMillis": 0,
            "uptimeEstimate": 0,
            "localTime": datetime.now(timezone.utc),
            "asserts": {
                "regular": 0,
                "warning": 0,
                "msg": 0,
                "user": 0,
                "rollovers": 0,
            },
            "connections": {"current": 1, "available": 1000},
            "mem": {"bits": 64, "resident": 0, "virtual": 0},
            "globalLock": {"totalTime": 0},
        }

    def _handle_db_stats(
        self, request_id: int, db: Connection
    ) -> tuple[int, dict[str, Any]]:
        """Handle dbStats command."""
        # Get collection count
        coll_names = db.list_collection_names()
        num_collections = len(coll_names)

        # Get total document count
        num_objects = 0
        for coll_name in coll_names:
            try:
                num_objects += db[coll_name].count_documents({})
            except Exception:
                pass

        return request_id, {
            "ok": 1,
            "db": db.name,
            "collections": num_collections,
            "views": 0,
            "objects": num_objects,
            "avgObjSize": 0,
            "dataSize": 0,
            "storageSize": 0,
            "indexes": num_collections,
            "indexSize": 0,
            "totalSize": 0,
            "scaleFactor": 1,
            "fsTotalSize": 0,
            "fsUsedSize": 0,
        }

    def _handle_coll_stats(
        self, request_id: int, db: Connection, coll_name: str | None
    ) -> tuple[int, dict[str, Any]]:
        """Handle collStats command."""
        if not coll_name:
            return request_id, {"ok": 0, "errmsg": "No collection specified"}

        coll = db[coll_name]
        count = coll.count_documents({})

        return request_id, {
            "ok": 1,
            "ns": f"{db.name}.{coll_name}",
            "count": count,
            "size": 0,
            "storageSize": 0,
            "totalIndexSize": 0,
            "totalSize": 0,
            "scaleFactor": 1,
            "avgObjSize": 0,
            "nindexes": 1,
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
    try:
        while True:
            # Read header (16 bytes)
            header = b""
            while len(header) < 16:
                chunk = await reader.read(16 - len(header))
                if not chunk:
                    return  # Connection closed
                header += chunk

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
                body = b""
                remaining = message_length - 16
                while len(body) < remaining:
                    chunk = await reader.read(remaining - len(body))
                    if not chunk:
                        logger.warning(
                            f"Incomplete message: expected {remaining}, "
                            f"got {len(body)}"
                        )
                        return
                    body += chunk
                full_message = header + body
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
                        if is_insert or has_payload_docs:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_insert, msg
                            )
                        else:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_command, msg
                            )
                    except Exception as e:
                        logger.error(f"Error handling OP_MSG: {e}")
                        response_doc = {"ok": 0, "errmsg": str(e)}
                        request_id = msg.get("request_id", 0)

                    try:
                        reply = ResponseBuilder.build_op_msg_reply(
                            request_id=0,
                            response_to=request_id,
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
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def handle_client_threaded(
    client_socket: socket.socket,
    handler: NeoSQLiteHandler,
):
    """Handle a single client connection (threaded version)."""
    try:
        with client_socket:
            while True:
                # Read header (16 bytes)
                header = b""
                while len(header) < 16:
                    chunk = client_socket.recv(16 - len(header))
                    if not chunk:
                        return  # Connection closed
                    header += chunk

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
                    body = b""
                    remaining = message_length - 16
                    while len(body) < remaining:
                        chunk = client_socket.recv(remaining - len(body))
                        if not chunk:
                            logger.warning(
                                f"Incomplete message: expected {remaining}, "
                                f"got {len(body)}"
                            )
                            return
                        body += chunk
                    full_message = header + body
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


def run_server_threaded(
    host: str,
    port: int,
    handler: NeoSQLiteHandler,
    use_threading: bool = True,
):
    """Run the MongoDB wire protocol server (threaded or async)."""
    if not use_threading:
        # Use async version
        return asyncio.run(run_server(host, port, handler))

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
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, handler),
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


def run_as_daemon(args: argparse.Namespace):
    """Run the server as a background daemon."""
    if is_running(args.pid_file):
        pid = get_pid(args.pid_file)
        logger.error(f"NX-27017 is already running (PID: {pid})")
        sys.exit(1)

    daemonize()

    with open(args.log_file, "a") as log_fh:
        os.dup2(log_fh.fileno(), sys.stdout.fileno())
        os.dup2(log_fh.fileno(), sys.stderr.fileno())

    if not write_pid_file(args.pid_file):
        sys.exit(1)

    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    try:
        run_server_sync(args)
    finally:
        remove_pid_file(args.pid_file)


def run_foreground(args: argparse.Namespace):
    """Run the server in the foreground."""
    if not write_pid_file(args.pid_file):
        sys.exit(1)

    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    try:
        run_server_sync(args)
    finally:
        remove_pid_file(args.pid_file)


def run_server_sync(args: argparse.Namespace):
    """Run the server synchronously."""
    db_path = args.db_path
    if db_path == "memory":
        db_path = ":memory:"

    logger.info(
        "Starting NX-27017 with db_path=%s, host=%s, port=%s (threaded=%s)",
        db_path,
        args.host,
        args.port,
        args.threaded,
    )

    handler = NeoSQLiteHandler(db_path)

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

    args = parser.parse_args()

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
