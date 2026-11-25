"""
Tests for Tier 2 (Temporary Table) operators.

This module tests the Tier 2 implementation of:
- $replaceRoot / $replaceWith stages
- $first / $last accumulators
- $addToSet accumulator
- $replaceOne string operator
"""

import pytest

import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    can_process_with_temporary_tables,
)


class TestReplaceRootTier2:
    """Test $replaceRoot and $replaceWith stages with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_replace_root
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "profile": {"age": 30, "city": "NYC"},
                },
                {"_id": 2, "name": "Bob", "profile": {"age": 25, "city": "LA"}},
                {
                    "_id": 3,
                    "name": "Charlie",
                    "profile": {"age": 35, "city": "SF"},
                },
            ]
        )
        yield coll
        conn.close()

    def test_replace_root_with_field_reference(self, collection):
        """Test $replaceRoot with field reference."""
        pipeline = [{"$replaceRoot": {"newRoot": "$profile"}}]

        # Check if pipeline can be processed with temporary tables
        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 3
        # Each result should be the profile object
        assert results[0]["age"] == 30
        assert results[0]["city"] == "NYC"
        assert results[1]["age"] == 25
        assert results[1]["city"] == "LA"

    def test_replace_with_field_reference(self, collection):
        """Test $replaceWith with field reference."""
        pipeline = [{"$replaceWith": "$profile"}]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 3
        assert results[0]["age"] == 30
        assert results[0]["city"] == "NYC"


class TestFirstLastAccumulatorsTier2:
    """Test $first and $last accumulators with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_first_last
        coll.insert_many(
            [
                {"_id": 1, "category": "A", "value": 10, "order": 1},
                {"_id": 2, "category": "A", "value": 20, "order": 2},
                {"_id": 3, "category": "A", "value": 30, "order": 3},
                {"_id": 4, "category": "B", "value": 100, "order": 1},
                {"_id": 5, "category": "B", "value": 200, "order": 2},
            ]
        )
        yield coll
        conn.close()

    def test_first_accumulator(self, collection):
        """Test $first accumulator in $group."""
        pipeline = [
            {"$sort": {"order": 1}},  # Ensure ordering
            {
                "$group": {
                    "_id": "$category",
                    "first_value": {"$first": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Category A first value should be 10
        category_a = next(r for r in results if r["_id"] == "A")
        assert category_a["first_value"] == 10
        # Category B first value should be 100
        category_b = next(r for r in results if r["_id"] == "B")
        assert category_b["first_value"] == 100

    def test_last_accumulator(self, collection):
        """Test $last accumulator in $group."""
        pipeline = [
            {"$sort": {"order": 1}},  # Ensure ordering
            {"$group": {"_id": "$category", "last_value": {"$last": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Category A last value should be 30
        category_a = next(r for r in results if r["_id"] == "A")
        assert category_a["last_value"] == 30
        # Category B last value should be 200
        category_b = next(r for r in results if r["_id"] == "B")
        assert category_b["last_value"] == 200


class TestAddToSetAccumulatorTier2:
    """Test $addToSet accumulator with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_add_to_set
        coll.insert_many(
            [
                {"_id": 1, "category": "A", "tag": "x"},
                {"_id": 2, "category": "A", "tag": "y"},
                {"_id": 3, "category": "A", "tag": "x"},  # Duplicate
                {"_id": 4, "category": "B", "tag": "z"},
                {"_id": 5, "category": "B", "tag": "y"},
            ]
        )
        yield coll
        conn.close()

    def test_add_to_set_accumulator(self, collection):
        """Test $addToSet accumulator in $group."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Category A should have unique tags ['x', 'y']
        category_a = next(r for r in results if r["_id"] == "A")
        assert "unique_tags" in category_a
        assert len(category_a["unique_tags"]) == 2  # x and y (no duplicates)
        assert "x" in category_a["unique_tags"]
        assert "y" in category_a["unique_tags"]

        # Category B should have unique tags ['z', 'y']
        category_b = next(r for r in results if r["_id"] == "B")
        assert "unique_tags" in category_b
        assert len(category_b["unique_tags"]) == 2


class TestReplaceOneTier2:
    """Test $replaceOne string operator with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_replace_one
        coll.insert_many(
            [
                {"_id": 1, "text": "hello world", "name": "Alice"},
                {"_id": 2, "text": "foo bar", "name": "Bob"},
                {"_id": 3, "text": "hello again", "name": "Charlie"},
            ]
        )
        yield coll
        conn.close()

    def test_replace_one_operator(self, collection):
        """Test $replaceOne in $addFields."""
        pipeline = [
            {
                "$addFields": {
                    "modified_text": {
                        "$replaceOne": {
                            "input": "$text",
                            "find": "hello",
                            "replacement": "hi",
                        }
                    }
                }
            }
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 3
        # First document: "hello world" -> "hi world"
        assert results[0]["modified_text"] == "hi world"
        # Second document: "foo bar" unchanged (no "hello")
        assert results[1]["modified_text"] == "foo bar"
        # Third document: "hello again" -> "hi again"
        assert results[2]["modified_text"] == "hi again"

    def test_replace_one_with_special_chars(self, collection):
        """Test $replaceOne with special characters."""
        # Insert document with special chars
        collection.insert_one({"_id": 10, "text": "a-b-c", "name": "Test"})

        pipeline = [
            {"$match": {"name": "Test"}},
            {
                "$addFields": {
                    "modified": {
                        "$replaceOne": {
                            "input": "$text",
                            "find": "-",
                            "replacement": "_",
                        }
                    }
                }
            },
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 1
        # $replaceOne only replaces the FIRST occurrence
        assert results[0]["modified"] == "a_b-c"  # Only first "-" replaced


class TestCombinedPipelineTier2:
    """Test combined pipelines with multiple Tier 2 operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_combined
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "category": "A",
                    "name": "Product 1",
                    "description": "hello world",
                    "price": 100,
                    "order": 1,
                },
                {
                    "_id": 2,
                    "category": "A",
                    "name": "Product 2",
                    "description": "hello again",
                    "price": 50,
                    "order": 2,
                },
                {
                    "_id": 3,
                    "category": "B",
                    "name": "Product 3",
                    "description": "foo bar",
                    "price": 75,
                    "order": 1,
                },
            ]
        )
        yield coll
        conn.close()

    def test_combined_pipeline(self, collection):
        """Test pipeline with multiple Tier 2 operators."""
        pipeline = [
            # Replace "hello" with "hi" in description
            {
                "$addFields": {
                    "modified_desc": {
                        "$replaceOne": {
                            "input": "$description",
                            "find": "hello",
                            "replacement": "hi",
                        }
                    }
                }
            },
            # Group by category
            {
                "$group": {
                    "_id": "$category",
                    "total_price": {"$sum": "$price"},
                    "avg_price": {"$avg": "$price"},
                    "unique_descriptions": {"$addToSet": "$modified_desc"},
                }
            },
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Check that we have the expected fields
        for result in results:
            assert "total_price" in result
            assert "avg_price" in result
            assert "unique_descriptions" in result

        # Check totals (order may vary)
        total_prices = sorted([r["total_price"] for r in results])
        assert total_prices == [75, 150]  # Category B: 75, Category A: 150

        # Check averages
        avg_prices = sorted([r["avg_price"] for r in results])
        assert avg_prices == [75.0, 75.0]


class TestIssue199DocumentReshaping:
    """Multi-tier regression tests for #199 document reshaping fixes."""

    @pytest.fixture(autouse=True)
    def _reset_fallback(self):
        yield
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)

    def _run_both_tiers(self, coll, pipeline):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)
        res_sql = list(coll.aggregate(pipeline))
        set_force_fallback(True)
        res_py = list(coll.aggregate(pipeline))
        set_force_fallback(False)
        return res_sql, res_py

    def test_replace_root_preserves_child_id_and_root_var_all_tiers(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.c.insert_one(
                {"_id": 1, "sub": {"_id": 999, "x": 2}, "tags": [1]}
            )
            # Tier 1 & Tier 3 child _id preservation + downstream $match
            sql1, py1 = self._run_both_tiers(
                conn.c,
                [
                    {"$replaceRoot": {"newRoot": "$sub"}},
                    {"$match": {"_id": 999}},
                ],
            )
            assert sql1 == py1 == [{"_id": 999, "x": 2}]

            # Tier 2 ($unwind forces Tier 2) child _id preservation
            sql2, py2 = self._run_both_tiers(
                conn.c,
                [
                    {"$unwind": "$tags"},
                    {"$replaceWith": "$sub"},
                    {"$match": {"_id": 999}},
                ],
            )
            assert sql2 == py2 == [{"_id": 999, "x": 2}]

            # $$ROOT restores pre-$addFields root in Tier 1, Tier 2, and Tier 3
            sql_r1, py_r1 = self._run_both_tiers(
                conn.c,
                [
                    {"$addFields": {"extra": "tmp"}},
                    {"$replaceRoot": {"newRoot": "$$ROOT"}},
                ],
            )
            assert sql_r1 == py_r1
            assert "extra" not in sql_r1[0]

            sql_r2, py_r2 = self._run_both_tiers(
                conn.c,
                [
                    {"$unwind": "$tags"},
                    {"$addFields": {"extra": "tmp"}},
                    {"$replaceRoot": {"newRoot": "$$ROOT"}},
                ],
            )
            assert sql_r2 == py_r2
            assert "extra" not in sql_r2[0]

    def test_project_objectid_computed_id_and_root_paths(self):
        from neosqlite.objectid import ObjectId

        with neosqlite.Connection(":memory:") as conn:
            conn.u.insert_one({"name": "Alice", "tags": [1]})
            conn.u.insert_one(
                {
                    "_id": "6b069f1db537ce5015147f47",
                    "name": "StrHex",
                    "tags": [1],
                }
            )
            # ObjectId vs 24-hex str preservation in Tier 1, Tier 2, and Tier 3
            for pipe in [
                [{"$project": {"name": 1}}],
                [{"$unwind": "$tags"}, {"$project": {"name": 1}}],
            ]:
                sql_res, py_res = self._run_both_tiers(conn.u, pipe)
                assert sql_res == py_res
                assert isinstance(sql_res[0]["_id"], ObjectId)
                assert isinstance(sql_res[1]["_id"], str)

            # Computed/aliased _id, $$ROOT after $group, dotted $$ROOT/$$CURRENT
            conn.g.insert_many(
                [{"_id": 1, "k": "g1", "val": 10, "a": {"n": 5}}]
            )
            for pipe in [
                [{"$project": {"_id": "$k", "val": 1}}],
                [{"$project": {"_id": {"$toUpper": "$k"}}}],
                [
                    {"$addFields": {"a": {"n": 99}}},
                    {
                        "$project": {
                            "_id": 0,
                            "rx": "$$ROOT.a.n",
                            "cx": "$$CURRENT.a.n",
                        }
                    },
                ],
                [
                    {"$group": {"_id": "$k", "tot": {"$sum": "$val"}}},
                    {"$project": {"_id": 0, "snap": "$$ROOT"}},
                ],
            ]:
                sql_res, py_res = self._run_both_tiers(conn.g, pipe)
                assert sql_res == py_res

    def test_addfields_tier2_id_strings_booleans_and_unset_unwind(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.t.insert_many(
                [
                    {"_id": "custom_abc", "tags": [1], "secret": "s1"},
                    {"_id": "custom_xyz", "tags": [2], "secret": "s2"},
                ]
            )
            # Tier 2 ($unwind forces temp tables) $_id copy, string/list/bool literals
            pipe = [
                {"$unwind": "$tags"},
                {
                    "$addFields": {
                        "copied_id": "$_id",
                        "label": "hello",
                        "flag": True,
                        "arr": [1, 2],
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            sql_res, py_res = self._run_both_tiers(conn.t, pipe)
            assert sql_res == py_res
            assert sql_res[0]["copied_id"] == "custom_abc"
            assert sql_res[0]["label"] == "hello"
            assert sql_res[0]["flag"] is True
            assert type(sql_res[0]["flag"]) is bool

            # $unset: '_id' across Tier 1, Tier 2, Tier 3
            for unset_pipe in [
                [{"$unset": "_id"}, {"$sort": {"secret": 1}}],
                [{"$unwind": "$tags"}, {"$unset": ["_id", "secret"]}],
            ]:
                u_sql, u_py = self._run_both_tiers(conn.t, unset_pipe)
                assert u_sql == u_py
                assert all("_id" not in d for d in u_sql)

            # Nested $unwind preserveNullAndEmptyArrays: True keeps missing & None parents
            conn.w.insert_many(
                [
                    {"_id": 1, "a": {"b": [10]}},
                    {"_id": 2},
                    {"_id": 3, "a": None},
                    {"_id": 4, "a": {"b": []}},
                ]
            )
            w_sql, w_py = self._run_both_tiers(
                conn.w,
                [
                    {
                        "$unwind": {
                            "path": "$a.b",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {"$sort": {"_id": 1}},
                ],
            )
            assert w_sql == w_py
            assert [d["_id"] for d in w_sql] == [1, 2, 3, 4]


class TestIssue198BucketAndGroupFixes:
    """Multi-tier regression tests for #198 ($bucket, $bucketAuto, $group)."""

    @pytest.fixture(autouse=True)
    def _reset_fallback(self):
        yield
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)

    def _run_both_tiers(self, coll, pipeline):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)
        res_sql = list(coll.aggregate(pipeline))
        set_force_fallback(True)
        res_py = list(coll.aggregate(pipeline))
        set_force_fallback(False)
        return res_sql, res_py

    def test_bucket_and_bucket_auto_expressions_across_tiers(self, monkeypatch):
        from neosqlite.collection import jsonb_support

        for force_no_jsonb in (False, True):
            with monkeypatch.context() as mp:
                if force_no_jsonb:
                    mp.setattr(
                        jsonb_support, "supports_jsonb", lambda _db: False
                    )
                with neosqlite.Connection(":memory:") as conn:
                    conn.b.insert_many(
                        [
                            {"_id": 1, "tags": ["x"], "price": 10, "qty": 2},
                            {"_id": 2, "tags": ["x"], "price": 20, "qty": 5},
                            {"_id": 3, "tags": ["x"], "price": 30, "qty": 3},
                            {"_id": 4, "tags": ["x"], "price": None, "qty": 4},
                        ]
                    )

                    # Expression groupBy + expression accumulator in Tier 1 & Tier 2
                    for prefix in ([], [{"$unwind": "$tags"}]):
                        b_pipe = prefix + [
                            {
                                "$bucket": {
                                    "groupBy": {"$add": ["$price", 5]},
                                    "boundaries": [0, 20, 40],
                                    "default": "Other",
                                    "output": {
                                        "count": {"$sum": 1},
                                        "rev": {
                                            "$sum": {
                                                "$multiply": ["$price", "$qty"]
                                            }
                                        },
                                        "avg_qty": {"$avg": "$qty"},
                                    },
                                }
                            }
                        ]
                        b_sql, b_py = self._run_both_tiers(conn.b, b_pipe)
                        assert b_sql == b_py
                        assert {d["_id"]: d for d in b_sql}[0] == {
                            "_id": 0,
                            "count": 1,
                            "rev": 20,
                            "avg_qty": 2.0,
                        }
                        assert {d["_id"]: d for d in b_sql}[20]["rev"] == 190

                        ba_pipe = prefix + [
                            {
                                "$bucketAuto": {
                                    "groupBy": {"$add": ["$price", 5]},
                                    "buckets": 1,
                                    "output": {
                                        "total_qty": {"$sum": "$qty"},
                                        "count": {"$count": {}},
                                    },
                                }
                            }
                        ]
                        ba_sql, ba_py = self._run_both_tiers(conn.b, ba_pipe)
                        assert ba_sql == ba_py
                        assert isinstance(ba_sql[0]["_id"], dict)
                        assert ba_sql[0]["total_qty"] == 14

                    # $bucketAuto preceded by $group (prev_stage has id IS NULL)
                    g_ba_pipe = [
                        {"$group": {"_id": "$price", "q": {"$sum": "$qty"}}},
                        {"$match": {"_id": {"$ne": None}}},
                        {
                            "$bucketAuto": {
                                "groupBy": "$_id",
                                "buckets": 2,
                                "output": {"sum_q": {"$sum": "$q"}},
                            }
                        },
                    ]
                    g_sql, g_py = self._run_both_tiers(conn.b, g_ba_pipe)
                    assert g_sql == g_py
                    assert len(g_sql) == 2
                    assert sum(d["sum_q"] for d in g_sql) == 10

                    # Tied values stay together cleanly across all tiers
                    conn.ties.insert_many([{"v": 10}, {"v": 10}, {"v": 10}])
                    t_sql, t_py = self._run_both_tiers(
                        conn.ties,
                        [{"$bucketAuto": {"groupBy": "$v", "buckets": 2}}],
                    )
                    assert t_sql == t_py
                    assert len(t_sql) == 1

    def test_group_avg_none_user_dict_and_stddev_samp_all_tiers(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.g.insert_many(
                [
                    {
                        "_id": 1,
                        "k": "g1",
                        "val": 10,
                        "obj": {"sum": 100, "count": 4},
                        "tags": ["a"],
                    },
                    {
                        "_id": 2,
                        "k": "g1",
                        "val": None,
                        "obj": {"sum": 200, "count": 5},
                        "tags": ["a"],
                    },
                    {
                        "_id": 3,
                        "k": "g2",
                        "val": None,
                        "obj": {"sum": 50, "count": 2},
                        "tags": ["a"],
                    },
                ]
            )
            for prefix in ([], [{"$unwind": "$tags"}]):
                pipe = prefix + [
                    {
                        "$group": {
                            "_id": "$k",
                            "avg_v": {"$avg": "$val"},
                            "first_obj": {"$first": "$obj"},
                            "sd_samp": {"$stdDevSamp": "$val"},
                            "sd_pop": {"$stdDevPop": "$val"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
                sql_res, py_res = self._run_both_tiers(conn.g, pipe)
                assert sql_res == py_res
                assert sql_res[0] == {
                    "_id": "g1",
                    "avg_v": 10.0,
                    "first_obj": {"sum": 100, "count": 4},
                    "sd_samp": None,
                    "sd_pop": 0.0,
                }
                assert sql_res[1] == {
                    "_id": "g2",
                    "avg_v": None,
                    "first_obj": {"sum": 50, "count": 2},
                    "sd_samp": None,
                    "sd_pop": None,
                }


class TestIssue200AdvancedStagesFixes:
    """Multi-tier regression tests for #200 advanced stage & security fixes."""

    @pytest.fixture(autouse=True)
    def _reset_fallback(self):
        yield
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)

    def _run_both_tiers(self, coll, pipeline):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)
        res_sql = list(coll.aggregate(pipeline))
        set_force_fallback(True)
        res_py = list(coll.aggregate(pipeline))
        set_force_fallback(False)
        return res_sql, res_py

    def test_redact_prunes_completely_and_fails_closed(self):
        from neosqlite.exceptions import MalformedQueryException

        with neosqlite.Connection(":memory:") as conn:
            conn.sec.insert_one(
                {
                    "_id": 1,
                    "lvl": 1,
                    "secret": {"lvl": 5, "pw": "123"},
                    "arr": [{"lvl": 5, "k": "secret"}, {"lvl": 1, "k": "ok"}],
                }
            )
            red = list(
                conn.sec.aggregate(
                    [
                        {
                            "$redact": {
                                "$cond": [
                                    {"$lte": ["$lvl", 2]},
                                    "$$DESCEND",
                                    "$$PRUNE",
                                ]
                            }
                        }
                    ]
                )
            )
            assert red == [{"_id": 1, "lvl": 1, "arr": [{"lvl": 1, "k": "ok"}]}]

            for bad_spec in (
                "BAD_ACTION",
                {"$cond": [True, "INVALID", "$$PRUNE"]},
                {"$divide": [1, 0]},
            ):
                with pytest.raises(MalformedQueryException):
                    list(conn.sec.aggregate([{"$redact": bad_spec}]))

    def test_tier2_limit_before_sort_not_coalesced(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.sl.insert_many(
                [
                    {"_id": 1, "t": [1], "a": 1, "b": 10},
                    {"_id": 2, "t": [1], "a": 2, "b": 40},
                    {"_id": 3, "t": [1], "a": 3, "b": 20},
                    {"_id": 4, "t": [1], "a": 4, "b": 30},
                ]
            )
            p = [
                {"$unwind": "$t"},
                {"$sort": {"a": 1}},
                {"$limit": 2},
                {"$sort": {"b": -1}},
            ]
            sql_res, py_res = self._run_both_tiers(conn.sl, p)
            assert [(d["a"], d["b"]) for d in sql_res] == [(2, 40), (1, 10)]
            assert sql_res == py_res

    def test_facet_unwind_missing_ids_and_large_batch(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.fc.insert_one({"_id": 1, "tags": ["x", "y"], "v": 10})
            for prefix in (
                [{"$unwind": "$tags"}],
                [{"$project": {"_id": 0, "v": 1}}],
                [{"$group": {"_id": "$tags", "tot": {"$sum": "$v"}}}],
            ):
                pipe = prefix + [{"$facet": {"c": [{"$count": "n"}]}}]
                sql_res, py_res = self._run_both_tiers(conn.fc, pipe)
                assert sql_res == py_res

            # > 101 docs inside Tier 3 $facet runs single sub-pipeline (not per chunk)
            conn.big.insert_many([{"_id": i, "v": i} for i in range(150)])
            from neosqlite.collection.query_helper import set_force_fallback

            set_force_fallback(True)
            big_py = list(
                conn.big.aggregate([{"$facet": {"c": [{"$count": "n"}]}}])
            )
            assert big_py == [{"c": [{"n": 150}]}]

            # Tier 2 ($unwind forces Tier 2) with 150 docs
            conn.big2.insert_one({"_id": 1, "tags": list(range(150))})
            set_force_fallback(False)
            big_t2 = list(
                conn.big2.aggregate(
                    [
                        {"$unwind": "$tags"},
                        {"$facet": {"c": [{"$count": "n"}]}},
                    ]
                )
            )
            assert big_t2 == [{"c": [{"n": 150}]}]

    def test_lookup_union_no_collapse_array_local_and_let_pipeline(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.a.insert_one({"_id": 1, "k": "x"})
            conn.b.insert_one({"_id": 1, "k": "y"})
            conn.f.insert_many([{"_id": "x", "v": 10}, {"_id": "y", "v": 20}])

            # $unionWith -> $lookup preserves duplicate t1.id rows
            u_pipe = [
                {"$unionWith": "b"},
                {
                    "$lookup": {
                        "from": "f",
                        "localField": "k",
                        "foreignField": "_id",
                        "as": "matched",
                    }
                },
                {"$sort": {"k": 1}},
            ]
            sql_u, py_u = self._run_both_tiers(conn.a, u_pipe)
            assert sql_u == py_u
            assert len(sql_u) == 2

            # Array localField in Tier 1, Tier 2, and Tier 3
            conn.arr.insert_one({"_id": 1, "tags": ["x", "y"], "t": [1]})
            for prefix in ([], [{"$unwind": "$t"}]):
                arr_pipe = prefix + [
                    {
                        "$lookup": {
                            "from": "f",
                            "localField": "tags",
                            "foreignField": "_id",
                            "as": "matched",
                        }
                    }
                ]
                sql_a, py_a = self._run_both_tiers(conn.arr, arr_pipe)
                assert [d["_id"] for d in sql_a[0]["matched"]] == [
                    d["_id"] for d in py_a[0]["matched"]
                ]

            # let + pipeline in Tier 1/2 -> Tier 3 fallback
            conn.orders.insert_many([{"_id": 1, "dept": "eng", "min_v": 15}])
            let_pipe = [
                {
                    "$lookup": {
                        "from": "f",
                        "let": {"m": "$min_v"},
                        "pipeline": [
                            {"$match": {"$expr": {"$gte": ["$v", "$$m"]}}},
                            {"$addFields": {"thresh": "$$m"}},
                        ],
                        "as": "hi",
                    }
                }
            ]
            sql_l, py_l = self._run_both_tiers(conn.orders, let_pipe)
            assert (
                sql_l
                == py_l
                == [
                    {
                        "_id": 1,
                        "dept": "eng",
                        "min_v": 15,
                        "hi": [{"_id": "y", "v": 20, "thresh": 15}],
                    }
                ]
            )

    def test_window_fields_fill_params_overwrite_and_expr(self):
        with neosqlite.Connection(":memory:") as conn:
            conn.wf.insert_many(
                [
                    {"_id": 1, "g": 1, "x": 10, "t": [1]},
                    {"_id": 2, "g": 1, "x": 20, "t": [1]},
                    {"_id": 3, "g": 1, "x": 30, "t": [1]},
                ]
            )
            # Tier 1: parameterized partitionBy + multiple outputs
            wf_t1 = [
                {
                    "$setWindowFields": {
                        "partitionBy": {"$add": ["$g", 1]},
                        "sortBy": {"_id": 1},
                        "output": {
                            "rk": {"$rank": {}},
                            "drk": {"$denseRank": {}},
                            "fst": {"$first": "$x"},
                        },
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            s1, p1 = self._run_both_tiers(conn.wf, wf_t1)
            assert s1 == p1

            # Tier 2: $shift default + parameterized partitionBy (no param swap!)
            wf_t2 = [
                {"$unwind": "$t"},
                {
                    "$setWindowFields": {
                        "partitionBy": {"$add": ["$g", 1]},
                        "sortBy": {"_id": 1},
                        "output": {
                            "nxt": {
                                "$shift": {
                                    "output": {"$add": ["$x", 5]},
                                    "by": 1,
                                    "default": "END",
                                }
                            }
                        },
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            s2, p2 = self._run_both_tiers(conn.wf, wf_t2)
            assert s2 == p2
            assert [d["nxt"] for d in s2] == [25, 35, "END"]

            # Tier 3 window operator overwriting input field 'x' does not mutate in-place
            wf_ov = [
                {
                    "$setWindowFields": {
                        "sortBy": {"_id": 1},
                        "output": {
                            "x": {
                                "$sum": "$x",
                                "window": {
                                    "documents": ["unbounded", "unbounded"]
                                },
                            }
                        },
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            so, po = self._run_both_tiers(conn.wf, wf_ov)
            assert so == po
            assert [d["x"] for d in po] == [60, 60, 60]

            # Tier 2/3 $fill constant value with parameterized partitionBy & expr value
            conn.fl.insert_many(
                [
                    {"_id": 1, "g": 1, "v": None, "fb": 99, "t": [1]},
                    {"_id": 2, "g": 1, "v": 10, "fb": 88, "t": [1]},
                ]
            )
            fl_const = [
                {"$unwind": "$t"},
                {
                    "$fill": {
                        "partitionBy": {"$add": ["$g", 5]},
                        "sortBy": {"_id": 1},
                        "output": {"v": {"value": "FILLED"}},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            fc_sql, fc_py = self._run_both_tiers(conn.fl, fl_const)
            assert fc_sql == fc_py
            assert fc_sql[0]["v"] == "FILLED"

            fl_expr = [
                {
                    "$fill": {
                        "output": {"v": {"value": "$fb"}},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            fe_sql, fe_py = self._run_both_tiers(conn.fl, fl_expr)
            assert fe_sql == fe_py
            assert fe_sql[0]["v"] == 99

    def test_graph_lookup_start_with_falsy_and_meta_aliases(self):
        from neosqlite.collection.query_helper.meta_validation import (
            normalize_meta_pipeline,
        )
        from neosqlite.collection.query_helper.pipeline_aliases import (
            normalize_pipeline_aliases,
        )
        from neosqlite.exceptions import MalformedQueryException

        with neosqlite.Connection(":memory:") as conn:
            conn.nodes.insert_many([{"_id": 0, "name": "zero", "next": None}])
            conn.src.insert_one({"_id": 1, "start": 0})

            # startWith: 0 executes instead of being skipped as falsy
            gl_pipe = [
                {
                    "$graphLookup": {
                        "from": "nodes",
                        "startWith": 0,
                        "connectFromField": "next",
                        "connectToField": "_id",
                        "as": "found",
                    }
                }
            ]
            s_gl, p_gl = self._run_both_tiers(conn.src, gl_pipe)
            assert s_gl == p_gl
            assert (
                len(p_gl[0]["found"]) == 1 and p_gl[0]["found"][0]["_id"] == 0
            )

            # Tier 2 invalid maxDepth raises NotImplementedError -> falls back / errors vs skipping
            conn.t2.insert_one({"_id": 1, "tags": [1], "s": 0})
            s_gl2, p_gl2 = self._run_both_tiers(
                conn.t2,
                [
                    {"$unwind": "$tags"},
                    {
                        "$graphLookup": {
                            "from": "nodes",
                            "startWith": "$s",
                            "connectFromField": "next",
                            "connectToField": "_id",
                            "as": "found",
                        }
                    },
                ],
            )
            assert s_gl2 == p_gl2

            # pipeline_aliases & meta_validation handle $unionWith.pipeline & $and/$text
            aliased = normalize_pipeline_aliases(
                [
                    {
                        "$unionWith": {
                            "coll": "other",
                            "pipeline": [{"$set": {"k": 1}}],
                        }
                    }
                ]
            )
            assert aliased[0]["$unionWith"]["pipeline"] == [
                {"$addFields": {"k": 1}}
            ]

            with pytest.raises(MalformedQueryException):
                normalize_meta_pipeline(
                    [
                        {
                            "$unionWith": {
                                "coll": "other",
                                "pipeline": [
                                    {
                                        "$addFields": {
                                            "s": {"$meta": "textScore"}
                                        }
                                    }
                                ],
                            }
                        }
                    ]
                )

            norm_and = normalize_meta_pipeline(
                [
                    {
                        "$match": {
                            "$and": [{"a": 1}, {"$text": {"$search": "py"}}]
                        }
                    },
                    {"$addFields": {"s": {"$meta": "textScore"}}},
                ]
            )
            assert len(norm_and) == 2
