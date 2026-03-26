"""Tests for commands delegated to NeoSQLite."""

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestDelegatedCommands:
    """Test commands delegated to NeoSQLite (db.command())."""

    def test_db_stats(self, handler):
        """Test dbStats - delegated to NeoSQLite."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        db_stats_msg = {
            "request_id": 2,
            "sections": [("body", {"dbStats": 1, "$db": "test"})],
        }
        _, response = handler.handle_command(db_stats_msg)
        assert response["ok"] == 1
        assert "collections" in response
        assert "objects" in response
        assert response["objects"] >= 2

    def test_coll_stats(self, handler):
        """Test collStats - delegated to NeoSQLite."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        coll_stats_msg = {
            "request_id": 2,
            "sections": [("body", {"collstats": "users", "$db": "test"})],
        }
        _, response = handler.handle_command(coll_stats_msg)
        assert response["ok"] == 1
        assert "count" in response
        assert response["count"] == 3

    def test_list_collections_delegated(self, handler):
        """Test listCollections - delegated to NeoSQLite."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        list_msg = {
            "request_id": 2,
            "sections": [("body", {"listCollections": 1, "$db": "test"})],
        }
        _, response = handler.handle_command(list_msg)
        assert response["ok"] == 1
        assert "cursor" in response
        coll_names = [c["name"] for c in response["cursor"]["firstBatch"]]
        assert "users" in coll_names
