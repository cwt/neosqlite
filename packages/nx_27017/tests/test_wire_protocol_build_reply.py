"""Tests for ResponseBuilder.build_reply (legacy OP_REPLY)."""

import struct

from nx_27017.wire_protocol import ResponseBuilder, WireProtocol


class TestBuildReply:
    def test_build_reply_uses_op_reply_opcode(self):
        """build_reply must emit a legacy OP_REPLY (opcode 1), not
        OP_MSG, so legacy OP_QUERY clients can parse the response."""
        reply = ResponseBuilder.build_reply(1, 2, [{"a": 1}])
        opcode = struct.unpack("<i", reply[12:16])[0]
        assert opcode == WireProtocol.OP_REPLY
        assert opcode != WireProtocol.OP_MSG

    def test_build_reply_body_format(self):
        """OP_REPLY body is responseFlags + cursorId + startingFrom +
        numberReturned + documents."""
        docs = [{"a": 1}, {"b": 2}]
        reply = ResponseBuilder.build_reply(1, 2, docs)
        body = reply[16:]
        response_flags, cursor_id, starting_from, number_returned = (
            struct.unpack("<Iqii", body[:20])
        )
        assert response_flags == 0
        assert cursor_id == 0
        assert starting_from == 0
        assert number_returned == 2
        assert b"a" in body
        assert b"b" in body
