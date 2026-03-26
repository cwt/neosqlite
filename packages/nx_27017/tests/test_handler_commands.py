"""Tests for NeoSQLiteHandler basic commands."""

import pytest


@pytest.fixture(autouse=True)
def cleanup_test_dbs():
    """Clean up test database files before and after each test."""
    import os

    for f in os.listdir("."):
        if f.startswith("test_") and f.endswith(".db"):
            os.remove(f)
    yield
    for f in os.listdir("."):
        if f.startswith("test_") and f.endswith(".db"):
            os.remove(f)


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestPing:
    def test_ping(self, handler):
        msg = {
            "request_id": 1,
            "sections": [("body", {"ping": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert request_id == 1
        assert response["ok"] == 1


class TestIsMaster:
    def test_ismaster(self, handler):
        msg = {
            "request_id": 2,
            "sections": [("body", {"ismaster": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1
        assert response["isWritablePrimary"] is True
        assert "maxWireVersion" in response


class TestHello:
    def test_hello(self, handler):
        msg = {
            "request_id": 3,
            "sections": [("body", {"hello": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1
        assert response["isWritablePrimary"] is True


class TestInsert:
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


class TestFind:
    def test_find(self, handler):
        insert_msg = {
            "request_id": 5,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        handler.handle_insert(insert_msg)

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


class TestCount:
    def test_count(self, handler):
        insert_msg = {
            "request_id": 7,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        handler.handle_insert(insert_msg)

        count_msg = {
            "request_id": 8,
            "sections": [("body", {"count": "users", "$db": "test"})],
        }
        request_id, response = handler.handle_command(count_msg)
        assert response["ok"] == 1
        assert response["n"] == 2


class TestDelete:
    def test_delete(self, handler):
        insert_msg = {
            "request_id": 9,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}, {"name": "Bob"}]),
            ],
        }
        handler.handle_insert(insert_msg)

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


class TestCollectionCreateDrop:
    def test_create_collection(self, handler):
        msg = {
            "request_id": 11,
            "sections": [("body", {"create": "new_collection", "$db": "test"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1

    def test_drop_collection(self, handler):
        create_msg = {
            "request_id": 12,
            "sections": [
                ("body", {"create": "temp_collection", "$db": "test"})
            ],
        }
        handler.handle_command(create_msg)

        drop_msg = {
            "request_id": 13,
            "sections": [("body", {"drop": "temp_collection", "$db": "test"})],
        }
        request_id, response = handler.handle_command(drop_msg)
        assert response["ok"] == 1


class TestSessions:
    def test_end_sessions(self, handler):
        msg = {
            "request_id": 14,
            "sections": [("body", {"endSessions": [], "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1

    def test_start_session(self, handler):
        msg = {
            "request_id": 16,
            "sections": [("body", {"startSession": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert response["ok"] == 1
        assert "session" in response
        assert "id" in response["session"]
        assert "$oid" in response["session"]["id"]
        session_id = response["session"]["id"]["$oid"]
        assert session_id in handler._sessions

    def test_commit_transaction(self, handler):
        start_msg = {
            "request_id": 17,
            "sections": [("body", {"startSession": 1, "$db": "admin"})],
        }
        _, start_response = handler.handle_command(start_msg)
        session_id = start_response["session"]["id"]["$oid"]

        with handler._sessions_lock:
            session = handler._sessions[session_id]
            session.start_transaction()

        commit_msg = {
            "request_id": 18,
            "sections": [
                (
                    "body",
                    {
                        "commitTransaction": 1,
                        "$db": "admin",
                        "lsid": {"id": {"$oid": session_id}},
                    },
                )
            ],
        }
        _, commit_response = handler.handle_command(commit_msg)
        assert commit_response["ok"] == 1

    def test_abort_transaction(self, handler):
        start_msg = {
            "request_id": 19,
            "sections": [("body", {"startSession": 1, "$db": "admin"})],
        }
        _, start_response = handler.handle_command(start_msg)
        session_id = start_response["session"]["id"]["$oid"]

        with handler._sessions_lock:
            session = handler._sessions[session_id]
            session.start_transaction()

        abort_msg = {
            "request_id": 20,
            "sections": [
                (
                    "body",
                    {
                        "abortTransaction": 1,
                        "$db": "admin",
                        "lsid": {"id": {"$oid": session_id}},
                    },
                )
            ],
        }
        _, abort_response = handler.handle_command(abort_msg)
        assert abort_response["ok"] == 1

    def test_end_sessions_with_session(self, handler):
        start_msg = {
            "request_id": 21,
            "sections": [("body", {"startSession": 1, "$db": "admin"})],
        }
        _, start_response = handler.handle_command(start_msg)
        session_id = start_response["session"]["id"]["$oid"]
        assert session_id in handler._sessions

        end_msg = {
            "request_id": 22,
            "sections": [
                (
                    "body",
                    {
                        "endSessions": [{"$oid": session_id}],
                        "$db": "admin",
                    },
                )
            ],
        }
        _, end_response = handler.handle_command(end_msg)
        assert end_response["ok"] == 1
        assert session_id not in handler._sessions


class TestUnknownCommand:
    def test_unknown_command(self, handler):
        msg = {
            "request_id": 15,
            "sections": [("body", {"unknownCommand": 1, "$db": "admin"})],
        }
        request_id, response = handler.handle_command(msg)
        assert "ok" in response


class TestWireProtocol:
    def test_opcodes(self):
        from nx_27017.nx_27017 import WireProtocol

        assert WireProtocol.OP_MSG == 2013
        assert WireProtocol.OP_QUERY == 2004


class TestInMemoryDatabase:
    def test_in_memory_shared_connections(self):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:")
        db1 = handler.get_database("test")
        coll = db1.users
        coll.insert_one({"name": "Alice"})
        docs = list(coll.find())
        assert len(docs) == 1

    def test_handler_with_memory_path(self):
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:")
        assert handler.db_path == ":memory:"


class TestExplain:
    def test_explain_find(self, handler):
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


class TestDistinct:
    def test_distinct(self, handler):
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


class TestFindAndModify:
    def test_find_and_modify_with_update(self, handler):
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        handler.handle_insert(insert_msg)

        find_modify_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "findAndModify": "users",
                        "query": {"name": "Alice"},
                        "update": {"name": "Alice", "age": 31},
                        "new": True,
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_modify_msg)
        assert response["ok"] == 1
        assert response["value"]["name"] == "Alice"
        assert response["value"]["age"] == 31

    def test_find_and_modify_with_remove(self, handler):
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
        _, response = handler.handle_command(find_modify_msg)
        assert response["ok"] == 1
        assert response["value"]["name"] == "Alice"

        find_msg = {
            "request_id": 5,
            "sections": [
                ("body", {"find": "users", "filter": {}, "$db": "test"})
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert len(response["cursor"]["firstBatch"]) == 1

    def test_find_and_modify_with_upsert(self, handler):
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
        _, response = handler.handle_command(find_modify_msg)
        assert response["ok"] == 1
        assert "value" in response or response["n"] == 1


class TestProjection:
    def test_find_with_projection_inclusion(self, handler):
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
        assert "city" not in doc
        assert "email" not in doc

    def test_find_with_projection_exclusion(self, handler):
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
        assert len(docs) == 2
        assert docs[0]["name"] == "Alice"
        assert docs[1]["name"] == "Charlie"


class TestUpdateOperators:
    def test_update_with_push(self, handler):
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
    def test_aggregate_with_count(self, handler):
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
        assert "ok" in response

    def test_aggregate_coll_stats(self, handler):
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

        agg_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "aggregate": "users",
                        "pipeline": [{" $collStats": {}}],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(agg_msg)
        assert response["ok"] == 1
        assert "cursor" in response
        assert len(response["cursor"]["firstBatch"]) == 1
        stats = response["cursor"]["firstBatch"][0]
        assert stats["count"] == 3
        assert "size" in stats
        assert "avgObjSize" in stats
        assert "storageSize" in stats
        assert "totalIndexSize" in stats
        assert "indexSizes" in stats
