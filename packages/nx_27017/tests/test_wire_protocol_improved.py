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


class TestIssue204WireProtocolFixes:
    """Regression tests for #204 wire protocol & BSON conversion fixes."""

    def test_op_msg_checksum_and_more_to_come_flag_bits(self):
        body_doc = {"ping": 1, "$db": "test"}
        encoded_body = encode(body_doc)
        section_0 = struct.pack("<B", 0) + encoded_body

        # Bit 0x1 = checksumPresent: trailing 4-byte CRC-32C must be stripped
        checksum_bytes = b"\xde\xad\xbe\xef"
        msg_body_0x1 = struct.pack("<I", 0x1) + section_0 + checksum_bytes
        header_0x1 = struct.pack(
            "<iiii", 16 + len(msg_body_0x1), 11, 0, WireProtocol.OP_MSG
        )
        parsed_0x1 = OP_MSG.parse(header_0x1 + msg_body_0x1)
        assert parsed_0x1["more_to_come"] is False
        assert parsed_0x1["sections"] == [("body", body_doc)]

        # Bit 0x2 = moreToCome: no checksum stripped, flag reported on parsed dict
        msg_body_0x2 = struct.pack("<I", 0x2) + section_0
        header_0x2 = struct.pack(
            "<iiii", 16 + len(msg_body_0x2), 12, 0, WireProtocol.OP_MSG
        )
        parsed_0x2 = OP_MSG.parse(header_0x2 + msg_body_0x2)
        assert parsed_0x2["more_to_come"] is True
        assert parsed_0x2["sections"] == [("body", body_doc)]

    def test_op_msg_bounds_and_section1_strict_validation(self):
        # 16-20 byte frames must raise ValueError instead of struct.error
        for short_len in (16, 18, 20):
            with pytest.raises(ValueError, match="too short"):
                OP_MSG.parse(b"\x00" * short_len)

        # Truncated kind-1 inner doc must raise ValueError instead of break
        body_doc = {"insert": "c", "$db": "test"}
        sec_0 = struct.pack("<B", 0) + encode(body_doc)
        bad_inner = struct.pack("<i", 100) + b"\x00\x00"
        seq_payload = b"documents\x00" + bad_inner
        sec_1 = struct.pack("<BI", 1, 4 + len(seq_payload)) + seq_payload
        msg_body = struct.pack("<I", 0) + sec_0 + sec_1
        header = struct.pack(
            "<iiii", 16 + len(msg_body), 13, 0, WireProtocol.OP_MSG
        )
        with pytest.raises(ValueError, match="Invalid BSON document length"):
            OP_MSG.parse(header + msg_body)

        # Null terminator outside section 1 bounds must fail
        bad_seq = b"notsafe"
        bad_sec_1 = struct.pack("<BI", 1, 4 + len(bad_seq)) + bad_seq
        trail = encode({"x": 1})
        msg_body_2 = struct.pack("<I", 0) + bad_sec_1 + b"\x00" + trail
        hdr_2 = struct.pack(
            "<iiii", 16 + len(msg_body_2), 14, 0, WireProtocol.OP_MSG
        )
        with pytest.raises(ValueError):
            OP_MSG.parse(hdr_2 + msg_body_2)

    def test_server_more_to_come_no_reply_and_op_query_error_reply(self):
        import socket
        import threading

        from nx_27017.handler import NeoSQLiteHandler
        from nx_27017.server import _is_insert_command, handle_client_threaded

        # Non-insert command carrying payload_docs must NOT route to handle_insert
        assert not _is_insert_command(
            {
                "sections": [
                    ("body", {"find": "c", "comment": {"insert": 1}}),
                    ("payload_docs", [{"x": 1}]),
                ]
            }
        )
        assert _is_insert_command(
            {"sections": [("body", {"$db": "test", "insert": "c"})]}
        )

        h = NeoSQLiteHandler(":memory:")
        s_srv, s_cli = socket.socketpair()
        s_cli.settimeout(3.0)
        t = threading.Thread(
            target=handle_client_threaded, args=(s_srv, h), daemon=True
        )
        t.start()
        try:
            # 1) Unacknowledged moreToCome (0x2) write produces NO reply frame,
            #    so next ping response has response_to == ping's request_id!
            ins_doc = {
                "insert": "items",
                "documents": [{"x": 42}],
                "$db": "test",
            }
            ins_body = (
                struct.pack("<I", 0x2) + struct.pack("<B", 0) + encode(ins_doc)
            )
            ins_frame = (
                struct.pack("<iiii", 16 + len(ins_body), 101, 0, 2013)
                + ins_body
            )
            ping_body = (
                struct.pack("<I", 0)
                + struct.pack("<B", 0)
                + encode({"ping": 1, "$db": "test"})
            )
            ping_frame = (
                struct.pack("<iiii", 16 + len(ping_body), 102, 0, 2013)
                + ping_body
            )
            s_cli.sendall(ins_frame + ping_frame)

            hdr = s_cli.recv(16)
            msg_len, _req_id, resp_to, _op = struct.unpack("<iiii", hdr)
            assert resp_to == 102
            _payload = s_cli.recv(msg_len - 16)

            # Verify w=0 insert actually persisted
            _, find_res = h.handle_command(
                {
                    "request_id": 103,
                    "sections": [
                        (
                            "body",
                            {"find": "items", "filter": {}, "$db": "test"},
                        )
                    ],
                }
            )
            assert find_res["cursor"]["firstBatch"][0]["x"] == 42

            # 2) Malformed OP_QUERY must return an OP_REPLY error frame instead of hanging
            bad_op_query = (
                struct.pack("<iiiiI", 24, 104, 0, WireProtocol.OP_QUERY, 0)
                + b"bad!"
            )
            s_cli.sendall(bad_op_query)
            err_hdr = s_cli.recv(16)
            err_len, _eid, err_resp_to, err_op = struct.unpack("<iiii", err_hdr)
            assert err_resp_to == 104
            assert err_op == WireProtocol.OP_REPLY
            err_body = s_cli.recv(err_len - 16)
            reply_flags = struct.unpack("<I", err_body[0:4])[0]
            assert reply_flags == 2
        finally:
            s_cli.close()
            t.join(timeout=2.0)
            h.close_all()

    def test_handler_repeated_payload_decimal128_regex_and_int64_cursor(self):
        from bson import Decimal128, Int64, Regex
        from nx_27017.handler import NeoSQLiteHandler

        h = NeoSQLiteHandler(":memory:")
        try:
            # Decimal128 insert + Regex find
            _, ins_res = h.handle_insert(
                {
                    "request_id": 1,
                    "sections": [
                        (
                            "body",
                            {
                                "insert": "products",
                                "documents": [
                                    {
                                        "name": "Alpha",
                                        "price": Decimal128("9.99"),
                                        "id": False,
                                    },
                                    {"name": "Beta", "price": 5, "id": 0.0},
                                ],
                                "$db": "test",
                            },
                        )
                    ],
                }
            )
            assert ins_res["n"] == 2

            _, find_res = h.handle_command(
                {
                    "request_id": 2,
                    "sections": [
                        (
                            "body",
                            {
                                "find": "products",
                                "filter": {"name": Regex("^alpha$", "i")},
                                "$db": "test",
                            },
                        )
                    ],
                }
            )
            assert isinstance(find_res["cursor"]["id"], Int64)
            assert len(find_res["cursor"]["firstBatch"]) == 1
            assert find_res["cursor"]["firstBatch"][0]["price"] == 9.99

            # Repeated Kind-1 payload sections in update extend rather than overwrite
            _, upd_res = h.handle_command(
                {
                    "request_id": 3,
                    "sections": [
                        ("body", {"update": "products", "$db": "test"}),
                        (
                            "payload",
                            {
                                "updates": [
                                    {
                                        "q": {"name": "Alpha"},
                                        "u": {"$set": {"tag": "a"}},
                                    }
                                ]
                            },
                        ),
                        (
                            "payload",
                            {
                                "updates": [
                                    {
                                        "q": {"name": "Beta"},
                                        "u": {"$set": {"tag": "b"}},
                                    }
                                ]
                            },
                        ),
                    ],
                }
            )
            assert upd_res["nModified"] == 2
        finally:
            h.close_all()
