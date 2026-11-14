"""Comprehensive coverage tests for $lookup temporary-table SQL optimization."""

import sqlite3
from unittest.mock import patch

import pytest

import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    HASH_JOIN_MEMORY_THRESHOLD,
    TemporaryTableAggregationProcessor,
)
from neosqlite.collection.temporary_table_aggregation.operators_lookup import (
    _serialize_lookup_key,
)


class TestLookupHelpersAndFallback:
    """Direct tests for _serialize_lookup_key, memory/size estimation, and fallback paths."""

    def test_serialize_lookup_key_variants(self):
        assert _serialize_lookup_key(None) is None
        assert _serialize_lookup_key({"b": 1, "a": 2}) == '{"b":1,"a":2}'
        assert _serialize_lookup_key([1, 2]) == "[1,2]"
        assert _serialize_lookup_key(42) == "42"
        assert _serialize_lookup_key("abc") == "abc"

    def test_extract_field_value_missing_and_non_dict(self):
        with neosqlite.Connection(":memory:") as conn:
            proc = TemporaryTableAggregationProcessor(conn.orders)
            assert proc._extract_field_value({"a": 1}, "a.b.c") is None
            assert proc._extract_field_value({"a": {"b": 2}}, "a.b") == 2
            assert proc._extract_field_value({"a": "str"}, "a.b") is None

    def test_create_lookup_hash_table_variants_and_pipeline(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.orders.insert_one({"item": "x"})
            i1 = conn.items.insert_one({"sku": "x", "qty": 5}).inserted_id
            proc = TemporaryTableAggregationProcessor(conn.orders)

            # foreign_field=None defaults to "_id"
            tbl_name, join_key = proc._create_lookup_hash_table("items", None)
            rows = conn.db.execute(
                f"SELECT {join_key}, data FROM {tbl_name}"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0][0] == str(i1)
            conn.db.execute(f"DROP TABLE IF EXISTS {tbl_name}")

            # Pipeline producing empty result creates empty hash table
            tbl_empty, _ = proc._create_lookup_hash_table(
                "items", "sku", pipeline=[{"$match": {"sku": "nonexistent"}}]
            )
            count = conn.db.execute(
                f"SELECT COUNT(*) FROM {tbl_empty}"
            ).fetchone()[0]
            assert count == 0
            conn.db.execute(f"DROP TABLE IF EXISTS {tbl_empty}")

            # Pipeline with non-empty docs and foreign_field == "_id"
            tbl_pipe_id, join_key_id = proc._create_lookup_hash_table(
                "items", "_id", pipeline=[{"$match": {"qty": {"$gte": 1}}}]
            )
            pipe_rows = conn.db.execute(
                f"SELECT {join_key_id} FROM {tbl_pipe_id}"
            ).fetchall()
            assert [r[0] for r in pipe_rows] == [str(i1)]
            conn.db.execute(f"DROP TABLE IF EXISTS {tbl_pipe_id}")

            # Pipeline with non-empty docs and regular foreign_field
            tbl_pipe_sku, join_key_sku = proc._create_lookup_hash_table(
                "items", "sku", pipeline=[{"$match": {"qty": {"$gte": 1}}}]
            )
            sku_rows = conn.db.execute(
                f"SELECT {join_key_sku} FROM {tbl_pipe_sku}"
            ).fetchall()
            assert [r[0] for r in sku_rows] == ["x"]
            conn.db.execute(f"DROP TABLE IF EXISTS {tbl_pipe_sku}")

    def test_create_lookup_hash_table_malformed_json_fallback(self):
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.orders
            items = conn.items
            orders.insert_many(
                [
                    {"sku": "almonds", "qty": 2},
                    {"sku": {"nested": 1}, "qty": 3},
                ]
            )
            items.insert_many(
                [
                    {"sku": "almonds", "stock": 10},
                    {"sku": ["t1", "t2"], "stock": 20},
                    {"sku": {"nested": 1}, "stock": 30},
                    {"sku": None, "stock": 0},
                ]
            )
            # Corrupt one JSON row directly so json_extract throws malformed JSON
            conn.db.execute(
                "INSERT INTO items (_id, data) VALUES (?, ?)",
                ("bad_row_id", "{not valid json"),
            )

            # Triggers _create_lookup_hash_table_fallback via Tier 2 processor
            proc = TemporaryTableAggregationProcessor(orders)
            res = proc.process_pipeline(
                [
                    {
                        "$lookup": {
                            "from": "items",
                            "localField": "sku",
                            "foreignField": "sku",
                            "as": "matched",
                        }
                    }
                ]
            )
            by_qty = {r["qty"]: r["matched"] for r in res}
            assert len(by_qty[2]) == 1 and by_qty[2][0]["stock"] == 10
            assert isinstance(by_qty[3], list)

            # Also test _create_lookup_hash_table_fallback with foreign_field="_id"
            proc._create_lookup_hash_table_fallback(
                "_test_fallback_id", "items", "_id", "_join_key"
            )
            count = conn.db.execute(
                "SELECT COUNT(*) FROM _test_fallback_id"
            ).fetchone()[0]
            assert count == 4  # 4 valid rows; bad JSON row skipped
            conn.db.execute("DROP TABLE IF EXISTS _test_fallback_id")

    def test_memory_and_should_use_hash_join_fallbacks(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.orders.insert_one({"a": 1})
            conn.items.insert_one({"a": 1})
            proc = TemporaryTableAggregationProcessor(conn.orders)

            # Fallback when sqlite PRAGMA fails -> uses resource.getrlimit
            class _BrokenConn:
                def execute(self, *args, **kwargs):
                    raise sqlite3.OperationalError("pragma failed")

            with patch.object(proc, "db", _BrokenConn()):
                with patch(
                    "resource.getrlimit",
                    return_value=(1024 * 1024, 1024 * 1024),
                ):
                    assert proc._get_available_memory() == int(
                        1024 * 1024 * 0.3
                    )
                with patch(
                    "resource.getrlimit",
                    side_effect=ValueError("bad limit"),
                ):
                    assert (
                        proc._get_available_memory()
                        == HASH_JOIN_MEMORY_THRESHOLD
                    )

            # _should_use_hash_join returns False on huge foreign estimate
            with (
                patch.object(
                    proc, "_estimate_collection_size", return_value=10**9
                ),
                patch.object(proc, "_get_available_memory", return_value=100),
            ):
                assert proc._should_use_hash_join("items") is False

            # _should_use_hash_join falls back to True if estimation raises
            with patch.object(
                proc,
                "_estimate_collection_size",
                side_effect=RuntimeError("boom"),
            ):
                assert proc._should_use_hash_join("items") is True


class TestLookupCorrelatedSubqueryAndErrors:
    """Cover _process_lookup_correlated_subquery and hash join pipeline paths."""

    def test_hash_join_with_pipeline_and_local_id(self):
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.orders
            items = conn.items
            o1 = orders.insert_one({"tag": "A"}).inserted_id
            orders.insert_one({"tag": "B"})
            items.insert_one({"order_id": o1, "qty": 15})
            items.insert_one({"order_id": o1, "qty": 2})

            proc = TemporaryTableAggregationProcessor(orders)
            res = proc.process_pipeline(
                [
                    {
                        "$lookup": {
                            "from": "items",
                            "localField": "_id",
                            "foreignField": "order_id",
                            "pipeline": [{"$match": {"qty": {"$gte": 10}}}],
                            "as": "big_items",
                        }
                    }
                ]
            )
            by_tag = {r["tag"]: r["big_items"] for r in res}
            assert [d["qty"] for d in by_tag["A"]] == [15]
            assert by_tag["B"] == []

    def test_correlated_subquery_standard_fields_and_id_keys(self):
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.orders
            items = conn.items

            i1 = items.insert_one({"sku": "almonds", "price": 10}).inserted_id
            i2 = items.insert_one({"sku": "pecans", "price": 20}).inserted_id
            o1 = orders.insert_one(
                {"item": "almonds", "item_id": i1, "qty": 2}
            ).inserted_id
            orders.insert_one({"item": "cashews", "item_id": None, "qty": 1})
            items.update_one({"_id": i2}, {"$set": {"order_id": o1}})

            proc = TemporaryTableAggregationProcessor(orders)
            with patch.object(
                proc, "_should_use_hash_join", return_value=False
            ):
                # Standard fields
                res1 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "item",
                                "foreignField": "sku",
                                "as": "inv",
                            }
                        }
                    ]
                )
                by_qty = {r["qty"]: r["inv"] for r in res1}
                assert [d["price"] for d in by_qty[2]] == [10]
                assert by_qty[1] == []

                # foreignField == "_id"
                res2 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "item_id",
                                "foreignField": "_id",
                                "as": "by_id",
                            }
                        }
                    ]
                )
                by_qty2 = {r["qty"]: r["by_id"] for r in res2}
                assert [d["sku"] for d in by_qty2[2]] == ["almonds"]
                assert by_qty2[1] == []

                # localField == "_id"
                res3 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "_id",
                                "foreignField": "order_id",
                                "as": "from_oid",
                            }
                        }
                    ]
                )
                by_qty3 = {r["qty"]: r["from_oid"] for r in res3}
                assert [d["sku"] for d in by_qty3[2]] == ["pecans"]
                assert by_qty3[1] == []

    def test_correlated_subquery_with_pipeline_and_id_variants(self):
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.orders
            items = conn.items

            i1 = items.insert_one({"sku": "almonds", "qty": 50}).inserted_id
            o1 = orders.insert_one(
                {"sku": "almonds", "item_id": i1, "tag": "A"}
            ).inserted_id
            items.update_one({"_id": i1}, {"$set": {"order_id": o1}})
            orders.insert_one({"sku": "other", "item_id": None, "tag": "B"})

            proc = TemporaryTableAggregationProcessor(orders)
            with patch.object(
                proc, "_should_use_hash_join", return_value=False
            ):
                # Pipeline with normal local/foreign fields + matches
                res1 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "sku",
                                "foreignField": "sku",
                                "pipeline": [{"$match": {"qty": {"$gte": 10}}}],
                                "as": "matches",
                            }
                        }
                    ]
                )
                by_tag1 = {r["tag"]: r["matches"] for r in res1}
                assert len(by_tag1["A"]) == 1
                assert by_tag1["B"] == []

                # Pipeline returning empty result set
                res2 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "sku",
                                "foreignField": "sku",
                                "pipeline": [{"$match": {"qty": {"$gt": 999}}}],
                                "as": "matches",
                            }
                        }
                    ]
                )
                assert all(r["matches"] == [] for r in res2)

                # Pipeline with foreignField == "_id"
                res3 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "item_id",
                                "foreignField": "_id",
                                "pipeline": [{"$match": {"qty": {"$gte": 10}}}],
                                "as": "matches",
                            }
                        }
                    ]
                )
                by_tag3 = {r["tag"]: r["matches"] for r in res3}
                assert len(by_tag3["A"]) == 1
                assert by_tag3["B"] == []

                # Pipeline with localField == "_id"
                res4 = proc.process_pipeline(
                    [
                        {
                            "$lookup": {
                                "from": "items",
                                "localField": "_id",
                                "foreignField": "order_id",
                                "pipeline": [{"$match": {"qty": {"$gte": 10}}}],
                                "as": "matches",
                            }
                        }
                    ]
                )
                by_tag4 = {r["tag"]: r["matches"] for r in res4}
                assert len(by_tag4["A"]) == 1
                assert by_tag4["B"] == []

    def test_lookup_validation_errors_in_hash_and_correlated_paths(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.orders.insert_one({"a": 1})
            conn.items.insert_one({"a": 1})
            proc = TemporaryTableAggregationProcessor(conn.orders)

            with pytest.raises(NotImplementedError, match="localField"):
                proc._process_lookup_hash_join(
                    lambda s, q: "tmp",
                    "orders",
                    {
                        "from": "items",
                        "pipeline": [{"$match": {"a": 1}}],
                        "as": "out",
                    },
                )

            with pytest.raises(ValueError, match="requires"):
                proc._process_lookup_hash_join(
                    lambda s, q: "tmp",
                    "orders",
                    {"from": "items", "as": "out"},
                )

            with pytest.raises(NotImplementedError, match="localField"):
                proc._process_lookup_correlated_subquery(
                    lambda s, q: "tmp",
                    "orders",
                    {
                        "from": "items",
                        "pipeline": [{"$match": {"a": 1}}],
                        "as": "out",
                    },
                )

            with pytest.raises(ValueError, match="requires"):
                proc._process_lookup_correlated_subquery(
                    lambda s, q: "tmp",
                    "orders",
                    {"from": "items", "as": "out"},
                )
