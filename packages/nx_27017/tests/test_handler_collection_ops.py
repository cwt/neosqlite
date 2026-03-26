"""Tests for collection operations."""

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestCollectionOperations:
    """Test collection operations like create, drop, rename."""

    def test_create_index(self, handler):
        """Test createIndexes command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        create_index_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "createIndexes": "users",
                        "indexes": [{"name": "name_1", "key": {"name": 1}}],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(create_index_msg)
        assert response["ok"] == 1

    def test_create_index_legacy(self, handler):
        """Test createIndex command (singular - legacy)."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        create_index_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "createIndex": "users",
                        "key": {"name": 1},
                        "name": "name_1",
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(create_index_msg)
        assert response["ok"] == 1
        assert response["numIndexesAfter"] >= response["numIndexesBefore"]

    def test_drop_indexes(self, handler):
        """Test dropIndexes command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        create_index_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "createIndexes": "users",
                        "indexes": [{"name": "name_1", "key": {"name": 1}}],
                        "$db": "test",
                    },
                )
            ],
        }
        handler.handle_command(create_index_msg)

        drop_index_msg = {
            "request_id": 3,
            "sections": [
                (
                    "body",
                    {
                        "dropIndexes": "users",
                        "index": "name_1",
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(drop_index_msg)
        assert response["ok"] == 1

    def test_drop_all_indexes(self, handler):
        """Test dropIndexes with index='*' to drop all indexes."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        create_index_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "createIndexes": "users",
                        "indexes": [
                            {"name": "name_1", "key": {"name": 1}},
                            {"name": "age_1", "key": {"age": 1}},
                        ],
                        "$db": "test",
                    },
                )
            ],
        }
        handler.handle_command(create_index_msg)

        drop_all_msg = {
            "request_id": 3,
            "sections": [
                (
                    "body",
                    {
                        "dropIndexes": "users",
                        "index": "*",
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(drop_all_msg)
        assert response["ok"] == 1

    def test_list_indexes(self, handler):
        """Test listIndexes command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        create_index_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "createIndexes": "users",
                        "indexes": [{"name": "name_1", "key": {"name": 1}}],
                        "$db": "test",
                    },
                )
            ],
        }
        handler.handle_command(create_index_msg)

        list_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"listIndexes": "users", "$db": "test"}),
            ],
        }
        _, response = handler.handle_command(list_msg)
        assert response["ok"] == 1
        assert "cursor" in response
        assert len(response["cursor"]["firstBatch"]) >= 1

    def test_list_collections(self, handler):
        """Test listCollections command."""
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
            "sections": [
                ("body", {"listCollections": 1, "$db": "test"}),
            ],
        }
        _, response = handler.handle_command(list_msg)
        assert response["ok"] == 1

    def test_rename_collection(self, handler):
        """Test renameCollection command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        rename_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "renameCollection": "test.users",
                        "to": "test.renamed_users",
                    },
                )
            ],
        }
        _, response = handler.handle_command(rename_msg)
        assert response["ok"] == 1
