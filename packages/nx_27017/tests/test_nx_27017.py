"""Tests for NX-27017 MongoDB Wire Protocol Server."""

import os

import pytest
from bson import ObjectId


@pytest.fixture(autouse=True)
def cleanup_test_dbs():
    """Clean up test database files before and after each test."""
    # Clean up before test
    for f in os.listdir("."):
        if f.startswith("test_") and f.endswith(".db"):
            os.remove(f)
    yield
    # Clean up after test
    for f in os.listdir("."):
        if f.startswith("test_") and f.endswith(".db"):
            os.remove(f)


class TestConvertObjectIds:
    """Test ObjectId conversion utilities."""

    def test_convert_dict_with_objectid(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "test"}
        result = _convert_objectids(doc)
        assert result["name"] == "test"

    def test_convert_dict_with_zero_id(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {"id": 0, "name": "test"}
        result = _convert_objectids(doc)
        from bson import Int64

        assert result["id"] == Int64(0)

    def test_convert_nested_dict(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {
            "name": "test",
            "nested": {"_id": ObjectId("507f1f77bcf86cd799439011")},
        }
        result = _convert_objectids(doc)
        assert result["name"] == "test"
        assert "_id" in result["nested"]

    def test_convert_list(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = [
            {"_id": ObjectId("507f1f77bcf86cd799439011")},
            {"name": "test"},
        ]
        result = _convert_objectids(doc)
        assert len(result) == 2

    def test_convert_nested_list(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {
            "items": [
                {"_id": ObjectId("507f1f77bcf86cd799439011")},
                {"name": "test"},
            ]
        }
        result = _convert_objectids(doc)
        assert len(result["items"]) == 2


class TestWireProtocol:
    """Test wire protocol constants."""

    def test_opcodes(self):
        from nx_27017.nx_27017 import WireProtocol

        assert WireProtocol.OP_MSG == 2013
        assert WireProtocol.OP_QUERY == 2004


class TestNeoSQLiteHandler:
    """Test NeoSQLiteHandler command handling."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        # Use a unique temp database file for each test
        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_ping(self, handler):
        msg = {
            "request_id": 1,
            "sections": [("body", {"ping": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert request_id == 1
        assert response["ok"] == 1

    def test_ismaster(self, handler):
        msg = {
            "request_id": 2,
            "sections": [("body", {"ismaster": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1
        assert response["isWritablePrimary"] is True
        assert "maxWireVersion" in response

    def test_hello(self, handler):
        msg = {
            "request_id": 3,
            "sections": [("body", {"hello": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1
        assert response["isWritablePrimary"] is True

    def test_insert(self, handler):
        msg = {
            "request_id": 4,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        request_id, response = handler.handle_insert(msg)
        assert response["ok"] == 1
        assert response["n"] == 2

    def test_find(self, handler):
        # First insert some data
        insert_msg = {
            "request_id": 5,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        handler.handle_insert(insert_msg)

        # Then find
        find_msg = {
            "request_id": 6,
            "sections": [
                ("body", {"find": "users", "filter": {}, "$db": "test"})
            ],
        }
        request_id, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert "cursor" in response
        assert len(response["cursor"]["firstBatch"]) == 1

    def test_count(self, handler):
        # Insert data
        insert_msg = {
            "request_id": 7,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        # Count
        count_msg = {
            "request_id": 8,
            "sections": [("body", {"count": "users", "$db": "test"})],
        }
        request_id, response = handler.handle_command(count_msg)
        assert response["ok"] == 1
        assert response["n"] == 2

    def test_delete(self, handler):
        # Insert data
        insert_msg = {
            "request_id": 9,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        # Delete
        delete_msg = {
            "request_id": 10,
            "sections": [
                (
                    "body",
                    {
                        "delete": "users",
                        "deletes": [{"q": {"name": "Alice"}, "limit": 1}],
                        "$db": "test",
                    },
                )
            ],
        }
        request_id, response = handler.handle_command(delete_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

    def test_create_collection(self, handler):
        msg = {
            "request_id": 11,
            "sections": [("body", {"create": "new_collection", "$db": "test"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1

    def test_drop_collection(self, handler):
        # Create first
        create_msg = {
            "request_id": 12,
            "sections": [
                ("body", {"create": "temp_collection", "$db": "test"})
            ],
        }
        handler.handle_command(create_msg)

        # Then drop
        drop_msg = {
            "request_id": 13,
            "sections": [("body", {"drop": "temp_collection", "$db": "test"})],
        }
        request_id, response = handler.handle_command(drop_msg)
        assert response["ok"] == 1

    def test_end_sessions(self, handler):
        msg = {
            "request_id": 14,
            "sections": [("body", {"endSessions": [], "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1

    def test_unknown_command(self, handler):
        msg = {
            "request_id": 15,
            "sections": [("body", {"unknownCommand": 1, "$db": "admin"})],
        }
        # Should not raise, will delegate to SQLite
        request_id, response = handler.handle_command(msg)
        assert "ok" in response


class TestDaemonFunctions:
    """Test daemon management functions."""

    def test_pid_file_operations(self, tmp_path):
        from nx_27017.nx_27017 import (
            get_pid,
            is_running,
            remove_pid_file,
            write_pid_file,
        )

        pid_file = str(tmp_path / "test.pid")

        # Write PID
        assert write_pid_file(pid_file) is True

        # Check running
        assert is_running(pid_file) is True

        # Get PID
        pid = get_pid(pid_file)
        assert pid is not None
        assert pid > 0

        # Remove PID file
        remove_pid_file(pid_file)
        assert not is_running(pid_file)

    def test_status_check(self, tmp_path):
        from nx_27017.nx_27017 import check_status

        pid_file = str(tmp_path / "test.pid")
        # Should return 1 (not running) when no PID file
        assert check_status(pid_file) == 1
