"""Section 1 tier-routing and Tier-1/Tier-2 vs Tier-3 parity tests.

Verifies using NeoSQLite's QueryEngine tier tracking (`add_tier_change_callback`,
`get_last_tier`, and `set_force_fallback`) that operations fixed in Section 1
execute natively on Tier 1 / Tier 2 (or fall back intentionally to Tier 3 when
unsupported) and produce identical results to the Tier-3 Python reference.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any
from unittest.mock import patch

import pytest

import neosqlite
from neosqlite import Binary
from neosqlite.collection.query_helper.utils import (
    get_force_fallback,
    set_force_fallback,
)
from neosqlite.objectid import ObjectId


@pytest.fixture(autouse=True)
def _reset_fallback():
    """Ensure kill switch is always reset between tests."""
    set_force_fallback(False)
    yield
    set_force_fallback(False)


@pytest.fixture
def conn():
    """Fresh in-memory NeoSQLite connection."""
    with neosqlite.Connection(":memory:") as connection:
        yield connection


def _normalize_docs(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort documents deterministically without mutating values."""

    def _key(doc: dict[str, Any]) -> str:
        if "_id" in doc:
            return f"id:{doc['_id']!r}"
        return repr(sorted((k, repr(v)) for k, v in doc.items()))

    return sorted(docs, key=_key)


def _assert_aggregate_parity(
    coll,
    pipeline: list[dict[str, Any]],
    expected_tier: str,
    *,
    ordered: bool = False,
) -> list[dict[str, Any]]:
    """Run pipeline normally and under force-fallback, asserting tier and parity."""
    qe = coll.query_engine
    transitions: list[tuple[str | None, str]] = []

    def _cb(prev_tier: str | None, new_tier: str, _pipeline: list[Any]) -> None:
        transitions.append((prev_tier, new_tier))

    qe.add_tier_change_callback(_cb)
    set_force_fallback(False)
    sql_res = list(coll.aggregate(pipeline))
    actual_tier = qe.get_last_tier()
    assert actual_tier == expected_tier

    set_force_fallback(True)
    py_res = list(coll.aggregate(pipeline))
    assert qe.get_last_tier() == "tier3"
    assert transitions[-1][1] == "tier3"

    if ordered:
        assert sql_res == py_res
    else:
        assert _normalize_docs(sql_res) == _normalize_docs(py_res)
    return sql_res


class TestNonAggregationTierTrackingAndQueries:
    """Verify tier callbacks and SQL execution on count, distinct, find, regex, logic."""

    def test_count_and_estimated_count_tiers_and_callbacks(self, conn):
        coll = conn.docs
        coll.insert_many([{"_id": 1, "k": "a"}, {"_id": 2, "k": "b"}])
        qe = coll.query_engine

        events: list[tuple[str | None, str, list[Any]]] = []
        qe.add_tier_change_callback(lambda p, n, pl: events.append((p, n, pl)))

        assert qe.get_last_tier() is None
        assert coll.estimated_document_count() == 2
        assert qe.get_last_tier() == "tier1"

        assert coll.count_documents({"k": "a"}) == 1
        assert qe.get_last_tier() == "tier1"

        set_force_fallback(True)
        assert coll.count_documents({"k": "a"}) == 1
        assert qe.get_last_tier() == "tier3"
        assert events[-1][0] == "tier1"
        assert events[-1][1] == "tier3"

        set_force_fallback(False)
        assert coll.estimated_document_count() == 2
        assert qe.get_last_tier() == "tier1"
        assert events[-1][1] == "tier1"

    def test_distinct_regular_and_id_fields(self, conn):
        coll = conn.items
        oid = ObjectId()
        coll.insert_many(
            [
                {"_id": oid, "cat": "x"},
                {"_id": 10, "cat": "y"},
                {"_id": "custom", "cat": "x"},
                {"_id": 20},
            ]
        )
        qe = coll.query_engine

        set_force_fallback(False)
        ids_sql = coll.distinct("_id")
        assert qe.get_last_tier() == "tier1"
        cats_sql = coll.distinct("cat", {"cat": {"$exists": True}})
        assert qe.get_last_tier() == "tier1"

        set_force_fallback(True)
        ids_py = coll.distinct("_id")
        assert qe.get_last_tier() == "tier3"
        cats_py = coll.distinct("cat", {"cat": {"$exists": True}})
        assert qe.get_last_tier() == "tier3"

        assert {repr(x) for x in ids_sql} == {repr(x) for x in ids_py}
        assert sorted(cats_sql) == sorted(cats_py) == ["x", "y"]

    def test_logical_operators_missing_and_null_fields(self, conn):
        coll = conn.logic
        coll.insert_many(
            [
                {"_id": 1, "a": 1, "b": 10},
                {"_id": 2, "a": None},
                {"_id": 3, "b": 20},
                {"_id": 4, "a": 2, "b": 20},
            ]
        )
        qe = coll.query_engine

        queries = [
            {"$or": [{"a": 1}, {"b": 20}]},
            {"$nor": [{"a": 1}, {"b": 20}]},
            {"a": {"$not": {"$eq": 1}}},
            {"a": {"$not": {"$gt": 1}}},
            {"$and": [{"$nor": [{"a": 1}]}, {"$or": [{"b": 20}, {"a": None}]}]},
        ]
        for query in queries:
            set_force_fallback(False)
            sql_docs = _normalize_docs(list(coll.find(query)))
            assert qe.get_last_tier() == "tier1"

            set_force_fallback(True)
            py_docs = _normalize_docs(list(coll.find(query)))
            assert qe.get_last_tier() == "tier3"
            assert sql_docs == py_docs

        # $not on arrays/nulls/missing/regex parity in Tier 1 vs Tier 3
        coll_not = conn.logic_not_edges
        coll_not.insert_many(
            [
                {"_id": 1, "tags": [1, 10], "txt": "foo"},
                {"_id": 2, "tags": [1, 2], "txt": "bar"},
                {"_id": 3, "tags": [], "txt": None},
                {"_id": 4},
            ]
        )
        qe_not = coll_not.query_engine
        not_edge_queries = [
            {"tags": {"$not": {"$gt": 5}}},
            {"txt": {"$not": re.compile(r"^f")}},
            {"txt": {"$not": {"$regex": "^B", "$options": "i"}}},
            {"tags": {"$not": {"$size": 2}}},
        ]
        for query in not_edge_queries:
            set_force_fallback(False)
            sql_docs = _normalize_docs(list(coll_not.find(query)))
            assert qe_not.get_last_tier() == "tier1", (
                f"fell back for {query}"
            )

            set_force_fallback(True)
            py_docs = _normalize_docs(list(coll_not.find(query)))
            assert qe_not.get_last_tier() == "tier3"
            assert sql_docs == py_docs

    def test_regex_options_re_pattern_and_invalid_option_fallback(self, conn):
        coll = conn.regex_docs
        coll.insert_many(
            [
                {"_id": 1, "txt": "Hello World"},
                {"_id": 2, "txt": "first\nSecond line"},
                {"_id": 3, "txt": "plain text"},
                {"_id": 4},
            ]
        )
        qe = coll.query_engine

        sql_queries = [
            {"txt": {"$regex": "^hello", "$options": "i"}},
            {"txt": {"$regex": "hello", "$options": "iu"}},
            {"txt": {"$regex": "^second", "$options": "im"}},
            {"txt": {"$regex": "hello \\s+ world", "$options": "ix"}},
            {"txt": re.compile(r"^hello", re.IGNORECASE)},
            {"txt": {"$not": re.compile(r"^hello", re.IGNORECASE)}},
        ]
        for q in sql_queries:
            set_force_fallback(False)
            sql_res = _normalize_docs(list(coll.find(q)))
            assert qe.get_last_tier() == "tier1"

            set_force_fallback(True)
            py_res = _normalize_docs(list(coll.find(q)))
            assert qe.get_last_tier() == "tier3"
            assert sql_res == py_res

        # Unsupported regex syntax/options and $eq: re.Pattern fall back cleanly
        fallback_queries = [
            {"txt": {"$regex": "(?z)hello"}},
            {"txt": {"$eq": re.compile(r"Second", re.MULTILINE)}},
        ]
        for q in fallback_queries:
            set_force_fallback(False)
            sql_res = _normalize_docs(list(coll.find(q)))
            assert qe.get_last_tier() == "tier3"

            set_force_fallback(True)
            py_res = _normalize_docs(list(coll.find(q)))
            assert qe.get_last_tier() == "tier3"
            assert sql_res == py_res


class TestObjectIdAndBinaryTierRouting:
    """Verify ObjectId (non-_id and _id) and Binary work across SQL query/agg paths."""

    def test_objectid_non_id_and_binary_find_count_distinct_and_agg(self, conn):
        coll = conn.typed_docs
        oid1, oid2, oid3 = ObjectId(), ObjectId(), ObjectId()
        bin1 = Binary(b"\x00\x01\x02", Binary.UUID_SUBTYPE)
        bin2 = Binary(b"\x03\x04", Binary.BINARY_SUBTYPE)
        bin3 = Binary(b"\x05\x06", Binary.FUNCTION_SUBTYPE)

        coll.insert_many(
            [
                {
                    "_id": oid1,
                    "owner": oid1,
                    "peers": [oid1, oid2],
                    "blob": bin1,
                    "blobs": [bin1, bin2],
                    "items": [{"ref": oid1, "bin": bin1, "code": 1}],
                },
                {
                    "_id": oid2,
                    "owner": oid2,
                    "peers": [oid2],
                    "blob": bin2,
                    "blobs": [bin2],
                    "items": [{"ref": oid2, "bin": bin2, "code": 2}],
                },
                {
                    "_id": oid3,
                    "owner": oid3,
                    "peers": [oid1, oid2],
                    "blob": bin3,
                    "blobs": [bin3],
                    "items": [{"ref": oid3, "bin": bin3, "code": 3}],
                },
            ]
        )
        qe = coll.query_engine

        find_queries = [
            {"owner": oid1},
            {"owner": {"$eq": oid1}},
            {"peers": {"$ne": oid1}},  # array $ne must exclude docs containing oid1
            {"owner": {"$in": [oid1, oid3]}},
            {"owner": {"$nin": [oid2]}},
            {"peers": {"$all": [oid1, oid2]}},
            {"peers": {"$elemMatch": oid1}},
            {"peers": {"$elemMatch": {"$eq": oid1}}},
            {"items": {"$elemMatch": {"ref": oid1}}},
            {"blob": bin1},
            {"blob": {"$eq": bin2}},
            {"blobs": {"$ne": bin1}},
            {"blob": {"$in": [bin1, bin3]}},
            {"blob": {"$nin": [bin2]}},
            {"blobs": {"$all": [bin1, bin2]}},
            {"blobs": {"$elemMatch": bin1}},
            {"blobs": {"$elemMatch": {"$eq": bin1}}},
            {"items": {"$elemMatch": {"bin": bin1}}},
        ]
        for query in find_queries:
            set_force_fallback(False)
            sql_res = _normalize_docs(list(coll.find(query)))
            assert qe.get_last_tier() == "tier1", f"find fell back for {query}"
            sql_count = coll.count_documents(query)
            assert qe.get_last_tier() == "tier1"

            set_force_fallback(True)
            py_res = _normalize_docs(list(coll.find(query)))
            assert qe.get_last_tier() == "tier3"
            py_count = coll.count_documents(query)
            assert sql_res == py_res
            assert sql_count == py_count

            # Same predicate in Tier-1 pipeline $match
            _assert_aggregate_parity(
                coll,
                [{"$addFields": {"tag": 1}}, {"$match": query}],
                expected_tier="tier1",
            )

        # Range comparisons on ObjectId/Binary intentionally fall back to Tier 3
        for range_q in [{"owner": {"$gt": oid1}}, {"blob": {"$lt": bin2}}]:
            set_force_fallback(False)
            sql_res = _normalize_docs(list(coll.find(range_q)))
            assert qe.get_last_tier() == "tier3"
            set_force_fallback(True)
            py_res = _normalize_docs(list(coll.find(range_q)))
            assert sql_res == py_res


class TestExprAndTier1AggregationRouting:
    """Verify $expr Tier-1/2 routing, date arithmetic, $unionWith, and Tier-1 $match."""

    def test_expr_let_round_and_date_arithmetic(self, conn):
        coll = conn.expr_docs
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "price": 12.345,
                    "qty": 3,
                    "d": dt.datetime(2025, 1, 31, tzinfo=dt.UTC),
                },
                {
                    "_id": 2,
                    "price": 5.5,
                    "qty": 2,
                    "d": dt.datetime(2025, 3, 15, tzinfo=dt.UTC),
                },
            ]
        )
        qe = coll.query_engine

        # $let + $multiply in find (Tier 1/2 SQL $expr) and aggregate $project (Tier 1)
        expr_find_q = {
            "$expr": {
                "$let": {
                    "vars": {"sub": {"$multiply": ["$price", "$qty"]}},
                    "in": {"$gt": ["$$sub", 20]},
                }
            }
        }
        set_force_fallback(False)
        sql_docs = _normalize_docs(list(coll.find(expr_find_q)))
        assert qe.get_last_tier() in {"tier1", "tier2"}
        set_force_fallback(True)
        py_docs = _normalize_docs(list(coll.find(expr_find_q)))
        assert qe.get_last_tier() == "tier3"
        assert sql_docs == py_docs

        # $round intentionally falls back to Tier 3 for MongoDB ties-to-even rounding (#116)
        round_find_q = {
            "$expr": {
                "$let": {
                    "vars": {"sub": {"$multiply": ["$price", "$qty"]}},
                    "in": {"$gt": [{"$round": ["$$sub", 1]}, 20]},
                }
            }
        }
        set_force_fallback(False)
        round_sql = _normalize_docs(list(coll.find(round_find_q)))
        assert qe.get_last_tier() == "tier3"
        set_force_fallback(True)
        round_py = _normalize_docs(list(coll.find(round_find_q)))
        assert round_sql == round_py

        # Month-end date arithmetic clamping in Tier-1 aggregation
        _assert_aggregate_parity(
            coll,
            [
                {
                    "$match": {
                        "$expr": {
                            "$gt": [
                                {
                                    "$dateAdd": {
                                        "startDate": "$d",
                                        "unit": "month",
                                        "amount": 1,
                                    }
                                },
                                dt.datetime(2025, 3, 1, tzinfo=dt.UTC),
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "subtotal": {"$multiply": ["$price", "$qty"]},
                        "nextMonth": {
                            "$dateAdd": {
                                "startDate": "$d",
                                "unit": "month",
                                "amount": 1,
                            }
                        },
                    }
                },
            ],
            expected_tier="tier1",
        )

        # $round in aggregation $project also intentionally runs on Tier 3
        _assert_aggregate_parity(
            coll,
            [{"$project": {"rounded": {"$round": ["$price", 1]}}}],
            expected_tier="tier3",
        )

    def test_union_with_and_tier1_match_operators(self, conn):
        c1 = conn.left_coll
        c2 = conn.right_coll
        oid = ObjectId()
        c1.insert_many(
            [
                {"_id": 1, "oid": oid, "arr": [1, 2], "mask": 5, "n": 10},
                {"_id": 2, "arr": [3], "mask": 2, "n": 7},
            ]
        )
        c2.insert_many([{"_id": 3, "oid": oid, "arr": [4, 5], "mask": 6, "n": 20}])

        # $unionWith string shorthand and empty pipeline in Tier 1
        _assert_aggregate_parity(
            c1, [{"$unionWith": "right_coll"}], expected_tier="tier1", ordered=True
        )
        _assert_aggregate_parity(
            c1,
            [{"$unionWith": {"coll": "right_coll", "pipeline": []}}],
            expected_tier="tier1",
            ordered=True,
        )

        # Tier-1 $match operators after $addFields (so match runs inside stage builder)
        for match_filter in [
            {"mask": {"$bitsAllSet": 5}},
            {"mask": {"$bitsAnySet": 4}},
            {"mask": {"$bitsAllClear": 8}},
            {"mask": {"$bitsAnyClear": 1}},
            {"arr": {"$size": 2}},
            {"n": {"$mod": [5, 0]}},
            {"oid": {"$exists": True}},
            {"$or": [{"n": 10}, {"n": 20}]},
            {"$nor": [{"n": 7}]},
            {"n": {"$not": {"$eq": 7}}},
        ]:
            _assert_aggregate_parity(
                c1,
                [{"$addFields": {"seen": True}}, {"$match": match_filter}],
                expected_tier="tier1",
            )


class TestTier2AggregationOptimizations:
    """Verify Section 1 Tier-2 fixes: multi-$unwind, $count, $addFields expr, $group."""

    def test_multi_unwind_and_scalar_unwind_tier2(self, conn):
        coll = conn.unwind_docs
        coll.insert_many(
            [
                {"_id": 1, "tags": ["a", "b"], "flags": [True, False]},
                {"_id": 2, "tags": "scalar", "flags": [True]},
                {"_id": 3, "tags": [], "flags": [False]},
                {"_id": 4, "tags": None, "flags": []},
                {"_id": 5, "flags": [True]},
            ]
        )

        # Consecutive multi-$unwind
        _assert_aggregate_parity(
            coll,
            [{"$unwind": "$tags"}, {"$unwind": "$flags"}],
            expected_tier="tier2",
        )
        # Non-consecutive multi-$unwind with scalar & array items
        _assert_aggregate_parity(
            coll,
            [
                {"$unwind": "$tags"},
                {"$addFields": {"doubleTag": "$tags"}},
                {"$unwind": "$flags"},
            ],
            expected_tier="tier2",
        )
        # Scalar string + preserveNullAndEmptyArrays in Tier 2
        _assert_aggregate_parity(
            coll,
            [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}],
            expected_tier="tier2",
        )

    def test_tier2_count_stage_tail_and_mid_pipeline(self, conn):
        coll = conn.count_docs
        coll.insert_many(
            [
                {"_id": 1, "tags": ["x", "y"]},
                {"_id": 2, "tags": ["z"]},
            ]
        )

        # Tail $count after Tier-2 $unwind
        res_tail = _assert_aggregate_parity(
            coll,
            [{"$unwind": "$tags"}, {"$count": "total"}],
            expected_tier="tier2",
        )
        assert res_tail == [{"total": 3}]

        # Mid-pipeline $count followed by $addFields expression
        res_mid = _assert_aggregate_parity(
            coll,
            [
                {"$unwind": "$tags"},
                {"$count": "total"},
                {"$addFields": {"doubled": {"$multiply": ["$total", 2]}}},
            ],
            expected_tier="tier2",
        )
        assert res_mid == [{"total": 3, "doubled": 6}]

        # Empty input: NeoSQLite $count emits 0 across Tier 1, Tier 2, and Tier 3
        res_empty_tail = _assert_aggregate_parity(
            coll,
            [{"$match": {"_id": 999}}, {"$unwind": "$tags"}, {"$count": "total"}],
            expected_tier="tier2",
        )
        assert res_empty_tail == [{"total": 0}]

        res_empty_mid = _assert_aggregate_parity(
            coll,
            [
                {"$match": {"_id": 999}},
                {"$unwind": "$tags"},
                {"$count": "total"},
                {"$addFields": {"doubled": {"$multiply": ["$total", 2]}}},
            ],
            expected_tier="tier2",
        )
        assert res_empty_mid == [{"total": 0, "doubled": 0}]

    def test_add_fields_sql_expressions_in_tier2(self, conn):
        coll = conn.add_fields_docs
        coll.insert_many(
            [
                {"_id": 1, "a": 2, "b": 3, "name": "alice", "tags": ["x"]},
                {"_id": 2, "a": 5, "b": 7, "name": "bob", "tags": ["y", "z"]},
            ]
        )

        # Dict-form $cond executes directly via SQL inside Tier 2
        _assert_aggregate_parity(
            coll,
            [
                {"$unwind": "$tags"},
                {
                    "$addFields": {
                        "sum": {"$add": ["$a", "$b"]},
                        "upper": {"$toUpper": "$name"},
                        "tier_label": {
                            "$cond": {
                                "if": {"$gt": ["$a", 3]},
                                "then": "high",
                                "else": "low",
                            }
                        },
                    }
                },
            ],
            expected_tier="tier2",
        )

        # Array-form $cond falls back per-stage to Python hybrid while staying in Tier 2
        _assert_aggregate_parity(
            coll,
            [
                {"$unwind": "$tags"},
                {
                    "$addFields": {
                        "sum": {"$add": ["$a", "$b"]},
                        "upper": {"$toUpper": "$name"},
                        "tier_label": {
                            "$cond": [{"$gt": ["$a", 3]}, "high", "low"]
                        },
                    }
                },
            ],
            expected_tier="tier2",
        )

    def test_group_param_keys_dict_accumulators_and_first_last_after_sort(self, conn):
        coll = conn.group_docs
        coll.insert_many(
            [
                {"_id": 1, "grp": "A", "ord": 3, "a": 2, "b": 10},
                {"_id": 2, "grp": "A", "ord": 1, "a": 2, "b": 30},
                {"_id": 3, "grp": "A", "ord": 2, "a": 2, "b": 20},
                {"_id": 4, "grp": "B", "ord": 2, "a": 5, "b": 50},
                {"_id": 5, "grp": "B", "ord": 1, "a": 5, "b": 40},
            ]
        )

        # Parameterized expression _id key + dict expression accumulators
        _assert_aggregate_parity(
            coll,
            [
                {
                    "$group": {
                        "_id": {"$add": ["$a", 1]},
                        "total_prod": {"$sum": {"$multiply": ["$a", "$b"]}},
                        "avg_plus": {"$avg": {"$add": ["$b", 1]}},
                        "min_val": {"$min": {"$add": ["$b", 2]}},
                        "max_val": {"$max": {"$multiply": ["$b", 2]}},
                        "labels": {"$push": {"$concat": ["$grp", "!"]}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
            expected_tier="tier2",
            ordered=True,
        )

        # $first and $last after ascending and descending $sort
        for sort_dir in (1, -1):
            _assert_aggregate_parity(
                coll,
                [
                    {"$sort": {"ord": sort_dir}},
                    {
                        "$group": {
                            "_id": "$grp",
                            "first_b": {"$first": "$b"},
                            "last_b": {"$last": "$b"},
                            "first_id": {"$first": "$_id"},
                            "last_expr": {"$last": {"$add": ["$b", 1]}},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ],
                expected_tier="tier2",
                ordered=True,
            )


class TestBinarySqlUpdates:
    """Verify Binary values take SQL update path where safe and match Python fallback."""

    def test_binary_updates_use_sql_and_match_fallback(self, conn):
        coll = conn.bin_updates
        qh = coll.query_engine.helpers
        b1 = Binary(b"\x01\x02", Binary.BINARY_SUBTYPE)
        b2 = Binary(b"\x03\x04", Binary.UUID_SUBTYPE)
        b3 = Binary(b"\x05\x06", Binary.FUNCTION_SUBTYPE)

        coll.insert_many(
            [
                {"_id": 1, "bin": b1, "arr": [b1]},
                {"_id": 2, "bin": b1, "arr": [b1]},
            ]
        )
        doc1 = coll.find_one({"_id": 1})
        assert doc1 is not None

        assert qh._can_use_sql_updates({"$set": {"bin": b2}}, 1, doc1) is True
        assert (
            qh._can_use_sql_updates(
                {"$push": {"arr": {"$each": [b2], "$position": 0}}}, 1, doc1
            )
            is True
        )
        assert qh._can_use_sql_updates({"$addToSet": {"arr": b2}}, 1, doc1) is True

        # Unsafe comparison / raw-JSON ops still decline SQL cleanly
        assert qh._can_use_sql_updates({"$min": {"bin": b2}}, 1, doc1) is False
        assert qh._can_use_sql_updates({"$pull": {"arr": b1}}, 1, doc1) is False

        ops = [
            {"$set": {"bin": b2}},
            {"$push": {"arr": {"$each": [b2, b3], "$position": 0, "$slice": 2}}},
            {"$addToSet": {"arr": b3}},
            {"$addToSet": {"arr": b1}},
        ]
        for op in ops:
            set_force_fallback(False)
            with patch.object(
                qh,
                "_perform_python_update",
                side_effect=AssertionError("Should not fall back to Python"),
            ):
                coll.update_one({"_id": 1}, op)

            set_force_fallback(True)
            assert get_force_fallback() is True
            coll.update_one({"_id": 2}, op)

        set_force_fallback(False)
        d1 = coll.find_one({"_id": 1})
        d2 = coll.find_one({"_id": 2})
        assert d1 is not None and d2 is not None
        assert {k: v for k, v in d1.items() if k != "_id"} == {
            k: v for k, v in d2.items() if k != "_id"
        }
