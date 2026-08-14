"""Unit tests for OP_MSG and OP_QUERY wire protocol parsing and validation."""

import struct

import pytest
from bson import encode
from nx_27017.wire_protocol import OP_MSG, OP_QUERY, WireProtocol


class TestWireProtocolParsing:
    def test_op_msg_section_type_1_offset_calculation(self):
        """Test that section type 1 (document sequence) accurately calculates section bounds."""
        doc1 = {"_id": 1, "name": "doc1"}
        doc2 = {"_id": 2, "name": "doc2"}
        encoded_doc1 = encode(doc1)
        encoded_doc2 = encode(doc2)

        identifier = b"documents\x00"
        seq_payload = identifier + encoded_doc1 + encoded_doc2
        section_size = 4 + len(seq_payload)

        # Build Section 1: type byte (1) + size (4 bytes) + identifier + docs
        section_1 = struct.pack("<BI", 1, section_size) + seq_payload

        # Build Body Section 0: type byte (0) + BSON body
        body_doc = {"insert": "test_coll", "$db": "test"}
        encoded_body = encode(body_doc)
        section_0 = struct.pack("<B", 0) + encoded_body

        sections = section_0 + section_1
        flags = 0
        body = struct.pack("<I", flags) + sections
        total_len = 16 + len(body)
        header = struct.pack("<iiii", total_len, 101, 0, WireProtocol.OP_MSG)
        full_msg = header + body

        parsed = OP_MSG.parse(full_msg)
        assert parsed["request_id"] == 101
        assert len(parsed["sections"]) == 2
        assert parsed["sections"][0] == ("body", body_doc)

        section_type, payload = parsed["sections"][1]
        assert section_type == "payload"
        assert "documents" in payload
        assert len(payload["documents"]) == 2
        assert payload["documents"][0] == doc1
        assert payload["documents"][1] == doc2

    def test_op_msg_malformed_errors(self):
        """Test error handling for malformed OP_MSG packets."""
        # Short message length in section 0
        header = struct.pack("<iiii", 25, 1, 0, WireProtocol.OP_MSG)
        body = struct.pack("<IB", 0, 0) + struct.pack("<i", 50) + b"short"
        with pytest.raises(ValueError):
            OP_MSG.parse(header + body)

    def test_op_query_malformed_errors(self):
        """Test error handling for malformed OP_QUERY packets."""
        with pytest.raises(ValueError, match="too short"):
            OP_QUERY.parse(b"short_bytes")

        # Missing null terminator in collection name
        header = struct.pack("<iiii", 30, 1, 0, 2004)
        flags = struct.pack("<I", 0)
        col_without_null = b"no_null_collection"
        with pytest.raises(ValueError, match="null terminator"):
            OP_QUERY.parse(header + flags + col_without_null)
