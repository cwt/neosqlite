"""Wire protocol parsers and response builders for MongoDB wire protocol."""

from __future__ import annotations

import logging
import struct
from itertools import count
from typing import Any

from bson import BSON, Int64, encode
from bson import ObjectId as BsonObjectId

from neosqlite.objectid import ObjectId as NeoObjectId

logger = logging.getLogger("nx_27017")

PID_FILE = "/tmp/nx_27017.pid"
LOG_FILE = "/tmp/nx_27017.log"

MESSAGE_HEADER_SIZE = 16
MAX_BSON_DOCUMENT_SIZE = 16 * 1024 * 1024  # 16MB
MAX_MESSAGE_SIZE_BYTES = 48_000_000
MAX_WRITE_BATCH_SIZE = 100_000

MIN_WIRE_VERSION = 17
MAX_WIRE_VERSION = 21

DEFAULT_SESSION_TIMEOUT_MINUTES = 30
DEFAULT_MAX_CONNECTIONS = 1000
SOCKET_BACKLOG = 128

SHUTDOWN_RETRY_COUNT = 10
SHUTDOWN_POLL_INTERVAL = 0.5

_request_id_counter = count(1)


def _get_next_request_id() -> int:
    """Generate a unique request ID for responses."""
    return next(_request_id_counter)


def _extract_session_id(lsid: dict) -> str | None:
    """Extract session ID from lsid document, handling Binary and dict formats."""
    if not lsid:
        return None
    if "$oid" in lsid:
        return lsid["$oid"]
    session_id_obj = lsid.get("id")
    if session_id_obj is None:
        return None
    if isinstance(session_id_obj, dict):
        return session_id_obj.get("$oid")
    from bson import Binary

    if isinstance(session_id_obj, Binary):
        return session_id_obj.hex()
    return str(session_id_obj)


def _convert_objectids(doc: dict) -> dict:
    """Convert NeoSQLite ObjectIds to BSON ObjectIds, and Decimal to float."""
    from decimal import Decimal as PyDecimal

    def _convert_value(value: Any) -> Any:
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
        doc_data = encode(_convert_objectids(document))
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
            doc_data = encode(_convert_objectids(doc))
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


class OP_MSG:
    """Parser for MongoDB OP_MSG wire protocol messages."""

    @staticmethod
    def parse(message_bytes: bytes) -> dict[str, Any]:
        message_length = struct.unpack("<i", message_bytes[0:4])[0]
        request_id = struct.unpack("<i", message_bytes[4:8])[0]
        response_to = struct.unpack("<i", message_bytes[8:12])[0]
        opcode = struct.unpack("<i", message_bytes[12:16])[0]
        offset = 16
        flags = struct.unpack("<I", message_bytes[offset : offset + 4])[0]
        offset += 4

        has_checksum = bool(flags & 0x2)
        effective_length = (
            message_length - 4 if has_checksum else message_length
        )

        sections: list[tuple[str, Any]] = []
        while offset < effective_length:
            section_type = message_bytes[offset]
            offset += 1

            if section_type == 0:
                doc_len = struct.unpack(
                    "<i", message_bytes[offset : offset + 4]
                )[0]

                if doc_len <= 0 or doc_len > MAX_BSON_DOCUMENT_SIZE:
                    logger.error(
                        f"Invalid document length: {doc_len} at offset {offset}"
                    )
                    raise ValueError(f"Invalid BSON document length: {doc_len}")

                if offset + doc_len > len(message_bytes):
                    logger.error(
                        f"Document extends beyond data: offset={offset}, "
                        f"doc_len={doc_len}, data_len={len(message_bytes)}, "
                        f"effective_length={effective_length}"
                    )
                    raise ValueError("Document extends beyond message")

                doc_data = message_bytes[offset : offset + doc_len]
                doc = BSON(doc_data).decode()
                sections.append(("body", doc))
                offset += doc_len
            elif section_type == 1:
                size = struct.unpack("<I", message_bytes[offset : offset + 4])[
                    0
                ]
                offset += 4

                cstring_end = message_bytes.index(b"\x00", offset)
                field_name = message_bytes[offset:cstring_end].decode("utf-8")
                offset = cstring_end + 1

                docs_start = offset
                docs_end = offset - 4 + size
                doc_data = message_bytes[docs_start:docs_end]

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
            elif section_type == 2:
                count = struct.unpack("<I", message_bytes[offset : offset + 4])[
                    0
                ]
                offset += 4
                docs = []
                for _ in range(count):
                    doc_len = struct.unpack(
                        "<i", message_bytes[offset : offset + 4]
                    )[0]
                    doc_data = message_bytes[offset : offset + doc_len]
                    docs.append(BSON(doc_data).decode())
                    offset += doc_len
                sections.append(("payload_docs", docs))
            else:
                logger.error(f"Unknown section kind: {section_type}")
                raise ValueError(f"Unknown section kind: {section_type}")

        return {
            "request_id": request_id,
            "response_to": response_to,
            "opcode": opcode,
            "flags": flags,
            "sections": sections,
        }


class OP_QUERY:
    """Parser for MongoDB OP_QUERY wire protocol messages."""

    @staticmethod
    def parse(data: bytes) -> dict[str, Any]:
        struct.unpack("<i", data[0:4])[0]
        request_id = struct.unpack("<i", data[4:8])[0]
        response_to = struct.unpack("<i", data[8:12])[0]
        struct.unpack("<i", data[12:16])[0]
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
