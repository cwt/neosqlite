"""Tests for session lifecycle (endSessions cleanup)."""

import os

import pytest
from bson import Binary
from nx_27017.nx_27017 import NeoSQLiteHandler


class _FakeSession:
    def __init__(self):
        self.ended = False

    def end_session(self):
        self.ended = True


@pytest.fixture
def handler(tmp_path):
    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestEndSessions:
    def test_end_sessions_cleans_raw_bytes_id(self, handler):
        """A session id provided as raw bytes (not bson.Binary) in
        endSessions must still be cleaned up - regression where only
        Binary-typed ids were converted to a key."""
        sid_bytes = os.urandom(16)
        key = sid_bytes.hex()
        fake = _FakeSession()
        handler._sessions[key] = fake

        handler.handle_command(
            {
                "request_id": 2,
                "sections": [
                    ("body", {"endSessions": [sid_bytes], "$db": "test"})
                ],
            }
        )
        assert key not in handler._sessions
        assert fake.ended is True

    def test_end_sessions_cleans_binary_id(self, handler):
        """The standard bson.Binary id form is also cleaned up."""
        sid_bytes = os.urandom(16)
        key = sid_bytes.hex()
        fake = _FakeSession()
        handler._sessions[key] = fake

        handler.handle_command(
            {
                "request_id": 2,
                "sections": [
                    (
                        "body",
                        {"endSessions": [Binary(sid_bytes)], "$db": "test"},
                    )
                ],
            }
        )
        assert key not in handler._sessions
