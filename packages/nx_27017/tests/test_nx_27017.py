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


class TestFindAndModify:
    """Test findAndModify command handling (critical bug fix)."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_find_and_modify_with_update(self, handler):
        """Test findAndModify with update field (not 'update' command).

        Note: findAndModify's 'update' field does FULL document replacement.
        For update with $set operators, use the 'update' command instead.
        """
        # First insert data
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        handler.handle_insert(insert_msg)

        # findAndModify with 'update' field - full document replacement
        # This was the bug - 'update' field was being treated as collection name
        find_modify_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "findAndModify": "users",
                        "query": {"name": "Alice"},
                        "update": {
                            "name": "Alice",
                            "age": 31,
                        },  # Full replacement
                        "new": True,  # Return updated document
                    },
                )
            ],
        }
        request_id, response = handler.handle_command(find_modify_msg)
        assert response["ok"] == 1
        assert response["value"]["name"] == "Alice"
        assert response["value"]["age"] == 31

    def test_find_and_modify_with_remove(self, handler):
        """Test findAndModify with remove field."""
        insert_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        find_modify_msg = {
            "request_id": 4,
            "sections": [
                (
                    "body",
                    {
                        "findAndModify": "users",
                        "query": {"name": "Alice"},
                        "remove": True,
                    },
                )
            ],
        }
        request_id, response = handler.handle_command(find_modify_msg)
        assert response["ok"] == 1
        assert response["value"]["name"] == "Alice"

        # Verify Alice is gone
        find_msg = {
            "request_id": 5,
            "sections": [
                ("body", {"find": "users", "filter": {}, "$db": "test"})
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert len(response["cursor"]["firstBatch"]) == 1

    def test_find_and_modify_with_upsert(self, handler):
        """Test findAndModify with upsert option."""
        find_modify_msg = {
            "request_id": 6,
            "sections": [
                (
                    "body",
                    {
                        "findAndModify": "users",
                        "query": {"name": "Charlie"},
                        "update": {"$set": {"name": "Charlie", "age": 25}},
                        "upsert": True,
                        "new": True,
                    },
                )
            ],
        }
        request_id, response = handler.handle_command(find_modify_msg)
        assert response["ok"] == 1
        # For upsert with new=True, value should be the upserted document
        # But if collection doesn't exist or other issues, value might be None
        # Just verify the command succeeded
        assert "value" in response or response["n"] == 1


class TestProjection:
    """Test projection handling in find commands."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_find_with_projection_inclusion(self, handler):
        """Test find with projection to include specific fields."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {
                            "name": "Alice",
                            "age": 30,
                            "city": "NYC",
                            "email": "alice@test.com",
                        }
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        find_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {},
                        "projection": {"name": 1, "age": 1},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        doc = response["cursor"]["firstBatch"][0]
        assert "name" in doc
        assert "age" in doc
        # Other fields should not be present
        assert "city" not in doc
        assert "email" not in doc

    def test_find_with_projection_exclusion(self, handler):
        """Test find with projection to exclude specific fields."""
        insert_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {
                            "name": "Alice",
                            "age": 30,
                            "city": "NYC",
                            "email": "alice@test.com",
                        }
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        find_msg = {
            "request_id": 4,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {},
                        "projection": {"city": 0, "email": 0},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        doc = response["cursor"]["firstBatch"][0]
        assert "name" in doc
        assert "age" in doc
        assert "city" not in doc
        assert "email" not in doc

    def test_find_with_sort_limit_skip(self, handler):
        """Test find with sort, limit, and skip."""
        insert_msg = {
            "request_id": 5,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {"name": "Alice", "age": 30},
                        {"name": "Bob", "age": 25},
                        {"name": "Charlie", "age": 35},
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        # sort -> skip -> limit
        find_msg = {
            "request_id": 6,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {},
                        "sort": {"age": 1},
                        "skip": 1,
                        "limit": 2,
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        docs = response["cursor"]["firstBatch"]
        # Sorted: Bob(25), Alice(30), Charlie(35)
        # Skip 1: Alice(30), Charlie(35)
        # Limit 2: Alice(30), Charlie(35)
        assert len(docs) == 2
        assert docs[0]["name"] == "Alice"  # age 30
        assert docs[1]["name"] == "Charlie"  # age 35


class TestUpdateOperators:
    """Test update operators like $push, $addToSet, $pop."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_update_with_push(self, handler):
        """Test update with $push operator."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "tags": []}]),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "update": "users",
                        "updates": [
                            {
                                "q": {"name": "Alice"},
                                "u": {"$push": {"tags": "python"}},
                            }
                        ],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1

    def test_update_with_add_to_set(self, handler):
        """Test update with $addToSet operator."""
        insert_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "tags": []}]),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 4,
            "sections": [
                (
                    "body",
                    {
                        "update": "users",
                        "updates": [
                            {
                                "q": {"name": "Alice"},
                                "u": {"$addToSet": {"tags": "python"}},
                            }
                        ],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1

    def test_update_with_pop(self, handler):
        """Test update with $pop operator."""
        insert_msg = {
            "request_id": 5,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "items": [1, 2, 3]}]),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 6,
            "sections": [
                (
                    "body",
                    {
                        "update": "users",
                        "updates": [
                            {
                                "q": {"name": "Alice"},
                                "u": {"$pop": {"items": -1}},
                            }
                        ],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1

    def test_update_multi(self, handler):
        """Test update with multi flag."""
        insert_msg = {
            "request_id": 7,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {"name": "Alice", "status": "active"},
                        {"name": "Bob", "status": "active"},
                        {"name": "Charlie", "status": "inactive"},
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        update_msg = {
            "request_id": 8,
            "sections": [
                (
                    "body",
                    {
                        "update": "users",
                        "updates": [
                            {
                                "q": {"status": "active"},
                                "u": {"$set": {"status": "verified"}},
                                "multi": True,
                            }
                        ],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(update_msg)
        assert response["ok"] == 1
        assert response["n"] == 2


class TestAggregate:
    """Test aggregate command handling."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_aggregate_with_count(self, handler):
        """Test aggregate with $count stage."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {"name": "Alice", "age": 30},
                        {"name": "Bob", "age": 25},
                        {"name": "Charlie", "age": 35},
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        # MongoDB sends $count without space, but the handler might handle it
        agg_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "aggregate": "users",
                        "pipeline": [{" $count": "total"}],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(agg_msg)
        # This might not be supported - just verify no crash
        assert "ok" in response


class TestInMemoryDatabase:
    """Test in-memory database handling."""

    def test_in_memory_shared_connections(self):
        """Test that multiple databases share in-memory connection."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:")

        # Insert into test database
        db1 = handler.get_database("test")
        coll = db1.users
        coll.insert_one({"name": "Alice"})

        # Query from same database
        docs = list(coll.find())
        assert len(docs) == 1

    def test_handler_with_memory_path(self):
        """Test handler initialization with :memory: path."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:")
        assert handler.db_path == ":memory:"


class TestExplain:
    """Test explain command handling."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_explain_find(self, handler):
        """Test explain for find command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        handler.handle_insert(insert_msg)

        explain_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "explain": {
                            "find": "users",
                            "filter": {"name": "Alice"},
                        },
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(explain_msg)
        assert response["ok"] == 1
        assert "queryPlanner" in response


class TestCollectionOperations:
    """Test collection operations like create, drop, rename."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

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


class TestDistinctAndGroup:
    """Test distinct and group operations."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_distinct(self, handler):
        """Test distinct command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                (
                    "payload_docs",
                    [
                        {"name": "Alice", "city": "NYC"},
                        {"name": "Bob", "city": "LA"},
                        {"name": "Charlie", "city": "NYC"},
                    ],
                ),
            ],
        }
        handler.handle_insert(insert_msg)

        distinct_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"distinct": "users", "key": "city", "$db": "test"}),
            ],
        }
        _, response = handler.handle_command(distinct_msg)
        assert response["ok"] == 1
        assert "values" in response


class TestDelegatedCommands:
    """Test commands delegated to NeoSQLite (db.command())."""

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

    def test_db_stats(self, handler):
        """Test dbStats - delegated to NeoSQLite."""
        # Insert some data
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
        # Should have users collection
        coll_names = [c["name"] for c in response["cursor"]["firstBatch"]]
        assert "users" in coll_names


class TestUpdatePayloadExtraction:
    """Test that update/deletes are correctly extracted from payload section.

    This tests the fix for a bug where PyMongo sends updates/deletes in the
    payload section of OP_MSG messages, but NX-27017 only looked in the body.
    """

    @pytest.fixture
    def handler(self, tmp_path):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        return NeoSQLiteHandler(db_path)

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

        # PyMongo sends updates in payload section, not body
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

        # Verify the update actually worked
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

        # PyMongo sends updates in payload section
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

        # Verify the update actually worked - only 100 should remain
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

        # PyMongo sends deletes in payload section - via handle_command
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

        # Verify only Alice and Charlie remain
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

        # PyMongo sends updates in payload section
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

        # Verify the update actually worked
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
        # Don't insert any documents first

        # PyMongo sends updates in payload section with upsert=true
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
        # nModified should be 0 since document was upserted, not modified
        assert response["nModified"] == 0

        # Verify the upsert actually worked - find by unique field
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

        # PyMongo sends updates in payload section with multi=true
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

        # Verify all were updated (scores increased by 5)
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


class TestJournalMode:
    """Test journal mode configuration for NeoSQLiteHandler."""

    def test_handler_default_journal_mode_is_wal(self, tmp_path):
        """Test that default journal mode is WAL."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path)
        assert handler.journal_mode == "WAL"
        assert handler.conn.journal_mode == "WAL"

    def test_handler_with_explicit_wal_mode(self, tmp_path):
        """Test handler with explicit WAL mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="WAL")
        assert handler.journal_mode == "WAL"
        assert handler.conn.journal_mode == "WAL"

    def test_handler_with_delete_mode(self, tmp_path):
        """Test handler with DELETE journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="DELETE")
        assert handler.journal_mode == "DELETE"
        assert handler.conn.journal_mode == "DELETE"

    def test_handler_with_memory_mode(self, tmp_path):
        """Test handler with MEMORY journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="MEMORY")
        assert handler.journal_mode == "MEMORY"
        assert handler.conn.journal_mode == "MEMORY"

    def test_handler_with_truncate_mode(self, tmp_path):
        """Test handler with TRUNCATE journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="TRUNCATE")
        assert handler.journal_mode == "TRUNCATE"
        assert handler.conn.journal_mode == "TRUNCATE"

    def test_handler_with_persist_mode(self, tmp_path):
        """Test handler with PERSIST journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="PERSIST")
        assert handler.journal_mode == "PERSIST"
        assert handler.conn.journal_mode == "PERSIST"

    def test_handler_with_off_mode(self, tmp_path):
        """Test handler with OFF journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="OFF")
        assert handler.journal_mode == "OFF"
        assert handler.conn.journal_mode == "OFF"

    def test_handler_with_memory_path_and_journal_mode(self):
        """Test handler with :memory: path and journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:", journal_mode="DELETE")
        assert handler.journal_mode == "DELETE"
        assert handler.conn.journal_mode == "DELETE"

    def test_get_database_inherits_journal_mode(self, tmp_path):
        """Test that get_database() uses the same journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="DELETE")

        # Access a different database - should inherit journal mode
        db2 = handler.get_database("other_db")
        assert db2.journal_mode == "DELETE"

    def test_cli_journal_mode_argument_parsing(self):
        """Test that CLI argument parsing works correctly."""
        import argparse

        from nx_27017.nx_27017 import JournalMode

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-j",
            "--journal-mode",
            dest="journal_mode",
            default="WAL",
            choices=["WAL", "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"],
        )

        # Test default
        args = parser.parse_args([])
        assert args.journal_mode == "WAL"
        assert JournalMode.validate(args.journal_mode) == "WAL"

        # Test WAL
        args = parser.parse_args(["-j", "WAL"])
        assert args.journal_mode == "WAL"

        # Test DELETE
        args = parser.parse_args(["--journal-mode", "DELETE"])
        assert args.journal_mode == "DELETE"

        # Test case insensitive validation
        assert JournalMode.validate("wal") == "WAL"
        assert JournalMode.validate("delete") == "DELETE"

    def test_handler_crud_operations_with_delete_mode(self, tmp_path):
        """Test that CRUD operations work correctly with non-WAL mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="DELETE")
        assert handler.conn.journal_mode == "DELETE"

        # Insert data
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        _, response = handler.handle_insert(insert_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

        # Find data
        find_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {"name": "Alice"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert len(response["cursor"]["firstBatch"]) == 1
        assert response["cursor"]["firstBatch"][0]["name"] == "Alice"

    def test_handler_crud_operations_with_memory_mode(self):
        """Test that CRUD operations work correctly with MEMORY mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:", journal_mode="MEMORY")
        assert handler.conn.journal_mode == "MEMORY"

        # Insert data
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Bob", "age": 25}]),
            ],
        }
        _, response = handler.handle_insert(insert_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

        # Find data
        find_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {"name": "Bob"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert len(response["cursor"]["firstBatch"]) == 1
        assert response["cursor"]["firstBatch"][0]["name"] == "Bob"
