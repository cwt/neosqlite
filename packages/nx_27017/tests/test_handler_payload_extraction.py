"""Tests for update/delete payload extraction from OP_MSG sections."""

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestUpdatePayloadExtraction:
    """Test that update/deletes are correctly extracted from payload section.

    This tests the fix for a bug where PyMongo sends updates/deletes in the
    payload section of OP_MSG messages, but NX-27017 only looked in the body.
    """

    def test_update_with_updates_in_payload(self, handler):
        """Test update where updates array is in payload section (PyMongo style)."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"_id": 1, "name": "Alice", "age": 30}]),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"update": "users", "$db": "test"}),
                (
                    "payload",
                    {
                        "updates": [
                            {
                                "q": {"_id": 1},
                                "u": {"$set": {"age": 31}},
                                "multi": False,
                            }
                        ]
                    },
                ),
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1
        assert response["nModified"] == 1

        find_msg = {
            "request_id": 3,
            "sections": [
                (
                    "body",
                    {"find": "users", "filter": {"_id": 1}, "$db": "test"},
                )
            ],
        }
        _, find_response = handler.handle_command(find_msg)
        assert find_response["cursor"]["firstBatch"][0]["age"] == 31

    def test_update_with_pull_all_in_payload(self, handler):
        """Test $pullAll update where updates array is in payload section."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {
                            "_id": 1,
                            "name": "test1",
                            "scores": [80, 90, 80, 100, 90, 80],
                        }
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"update": "users", "$db": "test"}),
                (
                    "payload",
                    {
                        "updates": [
                            {
                                "q": {"_id": 1},
                                "u": {"$pullAll": {"scores": [80, 90]}},
                                "multi": False,
                            }
                        ]
                    },
                ),
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1
        assert response["nModified"] == 1

        find_msg = {
            "request_id": 3,
            "sections": [
                (
                    "body",
                    {"find": "users", "filter": {"_id": 1}, "$db": "test"},
                )
            ],
        }
        _, find_response = handler.handle_command(find_msg)
        assert find_response["cursor"]["firstBatch"][0]["scores"] == [100]

    def test_delete_with_deletes_in_payload(self, handler):
        """Test delete where deletes array is in payload section (PyMongo style)."""
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

        delete_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"delete": "users", "$db": "test"}),
                (
                    "payload",
                    {"deletes": [{"q": {"name": "Bob"}, "limit": 1}]},
                ),
            ],
        }
        _, response = handler.handle_command(delete_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

        find_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"find": "users", "filter": {}, "$db": "test"})
            ],
        }
        _, find_response = handler.handle_command(find_msg)
        names = [doc["name"] for doc in find_response["cursor"]["firstBatch"]]
        assert "Alice" in names
        assert "Bob" not in names
        assert "Charlie" in names

    def test_update_with_push_in_payload(self, handler):
        """Test $push update where updates array is in payload section."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [{"_id": 1, "name": "Alice", "tags": ["python"]}],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"update": "users", "$db": "test"}),
                (
                    "payload",
                    {
                        "updates": [
                            {
                                "q": {"_id": 1},
                                "u": {"$push": {"tags": "sql"}},
                                "multi": False,
                            }
                        ]
                    },
                ),
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1
        assert response["nModified"] == 1

        find_msg = {
            "request_id": 3,
            "sections": [
                (
                    "body",
                    {"find": "users", "filter": {"_id": 1}, "$db": "test"},
                )
            ],
        }
        _, find_response = handler.handle_command(find_msg)
        tags = find_response["cursor"]["firstBatch"][0]["tags"]
        assert "python" in tags
        assert "sql" in tags

    def test_update_with_upsert_in_payload(self, handler):
        """Test upsert update where updates array is in payload section."""
        update_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"update": "users", "$db": "test"}),
                (
                    "payload",
                    {
                        "updates": [
                            {
                                "q": {"email": "new@example.com"},
                                "u": {
                                    "$set": {
                                        "name": "NewUser",
                                        "email": "new@example.com",
                                    }
                                },
                                "upsert": True,
                                "multi": False,
                            }
                        ]
                    },
                ),
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1
        assert response["nModified"] == 0

        find_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {"email": "new@example.com"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, find_response = handler.handle_command(find_msg)
        assert len(find_response["cursor"]["firstBatch"]) == 1
        assert find_response["cursor"]["firstBatch"][0]["name"] == "NewUser"

    def test_update_multi_in_payload(self, handler):
        """Test multi update where updates array is in payload section."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {"name": "Alice", "score": 10},
                        {"name": "Bob", "score": 20},
                        {"name": "Charlie", "score": 30},
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"update": "users", "$db": "test"}),
                (
                    "payload",
                    {
                        "updates": [
                            {
                                "q": {},
                                "u": {"$inc": {"score": 5}},
                                "multi": True,
                            }
                        ]
                    },
                ),
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1
        assert response["nModified"] == 3

        find_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"find": "users", "filter": {}, "$db": "test"})
            ],
        }
        _, find_response = handler.handle_command(find_msg)
        scores = {
            doc["name"]: doc["score"]
            for doc in find_response["cursor"]["firstBatch"]
        }
        assert scores["Alice"] == 15
        assert scores["Bob"] == 25
        assert scores["Charlie"] == 35
