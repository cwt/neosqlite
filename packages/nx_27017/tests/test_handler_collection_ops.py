"""Tests for collection operations."""

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.close_all()


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

    def test_insert_after_drop_recreates_collection(self, handler):
        """Inserting into a collection that was dropped (leaving a stale
        in-memory cache entry) must transparently re-create the table
        instead of raising 'already exists' or 'no such table'.
        """
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "items", "$db": "test"}),
                ("payload_docs", [{"x": 1}]),
            ],
        }
        handler.handle_insert(insert_msg)

        # Simulate the comparison harness cleanup dropping the physical
        # table while the in-memory collection cache keeps a stale entry.
        handler.conn["items"].drop()

        reinsert_msg = {
            "request_id": 2,
            "sections": [
                ("body", {"insert": "items", "$db": "test"}),
                ("payload_docs", [{"x": 2}, {"x": 3}]),
            ],
        }
        handler.handle_insert(reinsert_msg)

        find_msg = {
            "request_id": 3,
            "sections": [("body", {"find": "items", "$db": "test"})],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert len(response["cursor"]["firstBatch"]) == 2

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
        assert response["nIndexesWas"] == 1

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
        assert response["nIndexesWas"] == 2

    def test_list_indexes(self, handler):
        """Test listIndexes command."""
        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "user_id": 100}]),
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
                            {"name": "user_id_1", "key": {"user_id": 1}}
                        ],
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
        first_batch = response["cursor"]["firstBatch"]
        assert len(first_batch) >= 1
        keys = [idx["key"] for idx in first_batch]
        assert {"user_id": 1} in keys

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

    def test_insert_into_existing_collection(self, handler):
        """Inserting into an already-existing collection must not call
        create() on the existing table (regression for coll.create()
        being invoked on the else branch)."""
        first = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice"}]),
            ],
        }
        _, r1 = handler.handle_insert(first)
        assert r1["ok"] == 1

        second = {
            "request_id": 2,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Bob"}]),
            ],
        }
        _, r2 = handler.handle_insert(second)
        assert r2["ok"] == 1
        assert r2["n"] == 1

        count_msg = {
            "request_id": 3,
            "sections": [
                ("body", {"count": "users", "$db": "test", "query": {}}),
            ],
        }
        _, count_resp = handler.handle_command(count_msg)
        assert count_resp["n"] == 2

    def test_create_index_preserves_name_fts_tokenizer_ttl(self, handler):
        """Legacy createIndex must forward name, fts, tokenizer, and TTL."""
        handler.handle_insert(
            {
                "request_id": 1,
                "sections": [
                    ("body", {"insert": "articles", "$db": "test"}),
                    ("payload_docs", [{"body": "hello world"}]),
                ],
            }
        )
        _, fts_res = handler.handle_command(
            {
                "request_id": 2,
                "sections": [
                    (
                        "body",
                        {
                            "createIndex": "articles",
                            "key": {"body": 1},
                            "name": "custom_fts_idx",
                            "fts": True,
                            "tokenizer": "porter",
                            "$db": "test",
                        },
                    )
                ],
            }
        )
        assert fts_res["ok"] == 1
        assert fts_res["indexesCreated"] == [{"name": "custom_fts_idx"}]

        _, ttl_res = handler.handle_command(
            {
                "request_id": 3,
                "sections": [
                    (
                        "body",
                        {
                            "createIndex": "articles",
                            "key": {"created_at": 1},
                            "name": "ttl_idx",
                            "expireAfterSeconds": 120,
                            "$db": "test",
                        },
                    )
                ],
            }
        )
        assert ttl_res["ok"] == 1
        assert ttl_res["indexesCreated"] == [{"name": "ttl_idx"}]

        # Confirm both FTS virtual table and TTL metadata were actually created.
        db = handler.get_database("test")
        _, search_res = handler.handle_command(
            {
                "request_id": 4,
                "sections": [
                    ("body", {"listSearchIndexes": "articles", "$db": "test"})
                ],
            }
        )
        assert any(
            "body" in entry.get("name", "")
            for entry in search_res["cursor"]["firstBatch"]
        )
        ttl_rows = db.db.execute(
            "SELECT index_name, expire_after_seconds FROM _neosqlite_ttl_indexes "
            "WHERE collection_name = ?",
            ("articles",),
        ).fetchall()
        assert ttl_rows and ttl_rows[0][1] == 120

    def test_rename_collection_routes_source_db_and_drop_target(self, tmp_path):
        """renameCollection resolves src_db (not $db='admin') and handles dropTarget."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            h.handle_insert(
                {
                    "request_id": 1,
                    "sections": [
                        ("body", {"insert": "u", "$db": "mydb"}),
                        ("payload_docs", [{"_id": 1, "name": "Picard"}]),
                    ],
                }
            )
            _, res = h.handle_command(
                {
                    "request_id": 2,
                    "sections": [
                        (
                            "body",
                            {
                                "renameCollection": "mydb.u",
                                "to": "mydb.users",
                                "$db": "admin",
                            },
                        )
                    ],
                }
            )
            assert res["ok"] == 1
            assert "users" in h.get_database("mydb").list_collection_names()
            assert "u" not in h.get_database("mydb").list_collection_names()
            assert (
                "users" not in h.get_database("admin").list_collection_names()
            )

            # Cross-DB rename is rejected in multi-DB mode.
            _, cross_res = h.handle_command(
                {
                    "request_id": 3,
                    "sections": [
                        (
                            "body",
                            {
                                "renameCollection": "mydb.users",
                                "to": "otherdb.users",
                                "$db": "admin",
                            },
                        )
                    ],
                }
            )
            assert cross_res["ok"] == 0

            # dropTarget=True overwrites existing destination collection.
            h.handle_insert(
                {
                    "request_id": 4,
                    "sections": [
                        ("body", {"insert": "old_target", "$db": "mydb"}),
                        ("payload_docs", [{"_id": 99}]),
                    ],
                }
            )
            _, drop_res = h.handle_command(
                {
                    "request_id": 5,
                    "sections": [
                        (
                            "body",
                            {
                                "renameCollection": "mydb.users",
                                "to": "mydb.old_target",
                                "dropTarget": True,
                                "$db": "admin",
                            },
                        )
                    ],
                }
            )
            assert drop_res["ok"] == 1
            docs = list(h.get_database("mydb")["old_target"].find())
            assert [d["_id"] for d in docs] == [1]
        finally:
            h.close_all()
