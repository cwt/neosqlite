"""Tests for listDatabases size accounting."""

import os

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestListDatabasesSize:
    def test_total_size_not_double_counted(self, handler):
        """totalSize must reflect the backing file once, not be
        multiplied by the number of logical databases."""
        for db_name in ("test", "test2"):
            msg = {
                "request_id": 1,
                "sections": [
                    ("body", {"insert": "c", "$db": db_name}),
                    ("payload_docs", [{"x": 1}]),
                ],
            }
            handler.handle_insert(msg)

        list_msg = {
            "request_id": 99,
            "sections": [("body", {"listDatabases": 1})],
        }
        _, response = handler.handle_command(list_msg)
        assert response["ok"] == 1
        assert len(response["databases"]) >= 2

        file_size = os.path.getsize(handler.db_path)
        assert response["totalSize"] == file_size
        # Not multiplied by the number of logical databases.
        assert response["totalSize"] < file_size * len(response["databases"])
        for entry in response["databases"]:
            assert entry["sizeOnDisk"] == file_size
