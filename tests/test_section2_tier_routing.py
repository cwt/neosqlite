"""Section 2 tier-routing and Tier-1/Tier-2 vs Tier-3 parity tests.

Verifies using NeoSQLite's QueryEngine tier tracking (`add_tier_change_callback`,
`get_last_tier`, and `set_force_fallback`) and `cursor.explain()` that operations
promoted or fixed in Section 2 execute natively on SQL Tier 1 / Tier 2 (or fall
back intentionally to Tier 3 when unsupported) and produce identical results to
the Tier-3 Python reference.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest

import neosqlite
from neosqlite import Binary
from neosqlite._sqlite import sqlite3
from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)
from neosqlite.exceptions import MalformedQueryException
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
    canonicalize: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """Run pipeline in SQL mode and kill-switch Python mode, asserting tier & parity."""
    qe = coll.query_engine
    transitions: list[tuple[str | None, str]] = []

    def _cb(prev_tier: str | None, new_tier: str, _pipeline: list[Any]) -> None:
        transitions.append((prev_tier, new_tier))

    qe.add_tier_change_callback(_cb)
    try:
        set_force_fallback(False)
        sql_res = list(coll.aggregate(pipeline))
        assert qe.get_last_tier() == expected_tier

        set_force_fallback(True)
        py_res = list(coll.aggregate(pipeline))
        assert qe.get_last_tier() == "tier3"
        assert transitions[-1][1] == "tier3"
    finally:
        set_force_fallback(False)

    cmp_sql = canonicalize(copy.deepcopy(sql_res)) if canonicalize else sql_res
    cmp_py = canonicalize(copy.deepcopy(py_res)) if canonicalize else py_res
    if ordered:
        assert cmp_sql == cmp_py
    else:
        assert _normalize_docs(cmp_sql) == _normalize_docs(cmp_py)
    return sql_res


def _assert_find_parity(
    coll,
    query: dict[str, Any],
    expected_tier: str,
    *,
    projection: dict[str, Any] | None = None,
    sort: list[tuple[str, int]] | None = None,
    skip: int = 0,
    limit: int = 0,
    hint: Any = None,
) -> list[dict[str, Any]]:
    """Run find() in SQL mode and kill-switch Python mode, asserting tier & parity."""
    qe = coll.query_engine

    def _run() -> list[dict[str, Any]]:
        cur = coll.find(query, projection)
        if sort:
            cur = cur.sort(sort)
        if skip:
            cur = cur.skip(skip)
        if limit:
            cur = cur.limit(limit)
        if hint is not None:
            cur = cur.hint(hint)
        return list(cur)

    set_force_fallback(False)
    sql_res = _run()
    assert qe.get_last_tier() == expected_tier

    set_force_fallback(True)
    py_res = _run()
    assert qe.get_last_tier() == "tier3"
    set_force_fallback(False)

    if sort:
        assert sql_res == py_res
    else:
        assert _normalize_docs(sql_res) == _normalize_docs(py_res)
    return sql_res


class TestSection2ExprFixes:
    """Verify nested $expr math operators like $log and $sigmoid run in Tier 1."""

    def test_expr_log_and_sigmoid_nested_parameters(self, conn):
        coll = conn.expr_logs
        coll.insert_many(
            [
                {"_id": 1, "a": 6, "b": 2, "val": 8, "base": 2, "s": 0.0},
                {"_id": 2, "a": 14, "b": 2, "val": 27, "base": 3, "s": None},
                {"_id": 3, "a": 0, "b": 2, "val": 10, "base": 10, "s": -5.0},
            ]
        )

        # $log with field references and nested arithmetic in both SQL Tier 1 and Tier 3
        res1 = _assert_find_parity(
            coll,
            {"$expr": {"$gt": [{"$log": ["$val", "$base"]}, 2.5]}},
            "tier1",
        )
        assert [d["_id"] for d in _normalize_docs(res1)] == [1, 2]

        res2 = _assert_find_parity(
            coll,
            {"$expr": {"$eq": [{"$log": [{"$add": ["$a", 2]}, "$b"]}, 3]}},
            "tier1",
        )
        assert [d["_id"] for d in res2] == [1]

        res3 = _assert_find_parity(
            coll,
            {"$expr": {"$gt": [{"$sigmoid": {"input": "$s", "onNull": 0.8}}, 0.6]}},
            "tier1",
        )
        assert [d["_id"] for d in res3] == [2]

        # Also inside $project aggregation pipeline
        agg = _assert_aggregate_parity(
            coll,
            [
                {"$match": {"_id": {"$in": [1, 2]}}},
                {
                    "$project": {
                        "_id": 1,
                        "lg": {"$log": [{"$add": ["$a", 2]}, "$b"]},
                        "sig": {"$sigmoid": {"input": "$s", "onNull": 0.8}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
            "tier1",
            ordered=True,
        )
        assert agg[0]["lg"] == pytest.approx(3.0)
        assert agg[1]["sig"] == pytest.approx(0.8)


class TestSection2AAggregationStages:
    """Verify Section 2.A Tier-1 / Tier-2 aggregation stage routing & parity."""

    def test_facet_and_window_and_fill_and_densify_tier2(self, conn):
        coll = conn.metrics
        coll.insert_many(
            [
                {"_id": 1, "grp": "A", "seq": 1, "val": 10},
                {"_id": 2, "grp": "A", "seq": 2, "val": None},
                {"_id": 3, "grp": "A", "seq": 4, "val": 40},
                {"_id": 4, "grp": "B", "seq": 1, "val": 5},
            ]
        )

        facet_pipe = [
            {
                "$facet": {
                    "totals": [
                        {"$group": {"_id": "$grp", "cnt": {"$sum": 1}}},
                        {"$sort": {"_id": 1}},
                    ],
                    "high": [
                        {"$match": {"val": {"$gte": 10}}},
                        {"$sort": {"_id": 1}},
                    ],
                }
            }
        ]
        # Tier-1 $facet (all sub-pipelines Tier-1 capable) & Tier-2 $facet (with prior Tier-2 stage)
        facet_res = _assert_aggregate_parity(coll, facet_pipe, "tier1", ordered=True)
        assert facet_res[0]["totals"] == [
            {"_id": "A", "cnt": 3},
            {"_id": "B", "cnt": 1},
        ]

        facet_t2_pipe = [
            {
                "$fill": {
                    "sortBy": {"seq": 1},
                    "output": {"val": {"method": "locf"}},
                }
            },
            facet_pipe[0],
        ]
        _assert_aggregate_parity(coll, facet_t2_pipe, "tier2", ordered=True)

        fill_pipe = [
            {"$match": {"grp": "A"}},
            {
                "$fill": {
                    "sortBy": {"seq": 1},
                    "output": {"val": {"method": "linear"}},
                }
            },
            {"$sort": {"seq": 1}},
        ]
        fill_res = _assert_aggregate_parity(coll, fill_pipe, "tier2", ordered=True)
        assert [d["val"] for d in fill_res] == [10, 20.0, 40]

        win_pipe = [
            {"$match": {"grp": "A"}},
            {
                "$setWindowFields": {
                    "partitionBy": "$grp",
                    "sortBy": {"seq": 1},
                    "output": {
                        "running": {
                            "$sum": {"$ifNull": ["$val", 0]},
                            "window": {"documents": ["unbounded", "current"]},
                        }
                    },
                }
            },
            {"$sort": {"seq": 1}},
        ]
        win_res = _assert_aggregate_parity(coll, win_pipe, "tier2", ordered=True)
        assert [d["running"] for d in win_res] == [10, 10, 50]

        dens_pipe = [
            {"$match": {"grp": "A"}},
            {
                "$densify": {
                    "field": "seq",
                    "range": {"step": 1, "bounds": [1, 5]},
                }
            },
            {"$sort": {"seq": 1}},
        ]
        dens_res = _assert_aggregate_parity(coll, dens_pipe, "tier1", ordered=True)
        assert [d["seq"] for d in dens_res] == [1, 2, 3, 4, 5]

        dens_t2_pipe = [
            {
                "$fill": {
                    "sortBy": {"seq": 1},
                    "output": {"val": {"method": "linear"}},
                }
            },
            {
                "$densify": {
                    "field": "seq",
                    "range": {"step": 1, "bounds": [1, 5]},
                }
            },
            {"$sort": {"seq": 1}},
        ]
        _assert_aggregate_parity(coll, dens_t2_pipe, "tier2", ordered=True)

    def test_graph_lookup_tier1_and_tier2(self, conn):
        people = conn.people
        people.insert_many(
            [
                {"_id": 1, "name": "Alice", "reportsTo": None},
                {"_id": 2, "name": "Bob", "reportsTo": "Alice"},
                {"_id": 3, "name": "Carol", "reportsTo": "Bob"},
            ]
        )

        gl_pipe = [
            {"$match": {"name": "Carol"}},
            {
                "$graphLookup": {
                    "from": "people",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "name",
                    "as": "chain",
                }
            },
        ]

        def _sort_chain(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
            for doc in docs:
                if "chain" in doc:
                    doc["chain"] = _normalize_docs(doc["chain"])
            return docs

        res_t1 = _assert_aggregate_parity(
            people, gl_pipe, "tier1", canonicalize=_sort_chain
        )
        assert {d["name"] for d in res_t1[0]["chain"]} == {"Bob", "Alice"}

        gl_pipe_t2 = [
            {"$fill": {"sortBy": {"_id": 1}, "output": {"x": {"value": 1}}}},
            {"$match": {"name": "Carol"}},
            {
                "$graphLookup": {
                    "from": "people",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "name",
                    "as": "chain",
                }
            },
        ]
        _assert_aggregate_parity(people, gl_pipe_t2, "tier2", canonicalize=_sort_chain)

    def test_redact_out_merge_tier2(self, conn):
        src = conn.redact_src
        src.insert_many(
            [
                {"_id": 1, "level": 1, "secret": "s1", "tag": "keep"},
                {"_id": 2, "level": 5, "secret": "s2", "tag": "drop"},
            ]
        )
        # Top-level $$KEEP / $$PRUNE compiles to SQL (Tier 1 standalone, Tier 2 with prior Tier-2 stage)
        redact_sql_stage = {
            "$redact": {
                "$cond": {
                    "if": {"$lte": ["$level", 2]},
                    "then": "$$KEEP",
                    "else": "$$PRUNE",
                }
            }
        }
        red_res_t1 = _assert_aggregate_parity(src, [redact_sql_stage], "tier1")
        assert [d["_id"] for d in red_res_t1] == [1]

        redact_pipe_t2 = [
            {"$fill": {"sortBy": {"_id": 1}, "output": {"level": {"value": 1}}}},
            redact_sql_stage,
        ]
        red_res_t2 = _assert_aggregate_parity(src, redact_pipe_t2, "tier2")
        assert [d["_id"] for d in red_res_t2] == [1]

        # Recursive $$DESCEND falls back cleanly to Tier 3 Python evaluation
        redact_descend = [
            {
                "$redact": {
                    "$cond": {
                        "if": {"$lte": ["$level", 2]},
                        "then": "$$DESCEND",
                        "else": "$$PRUNE",
                    }
                }
            }
        ]
        red_res_t3 = _assert_aggregate_parity(src, redact_descend, "tier3")
        assert [d["_id"] for d in red_res_t3] == [1]

        out_pipe = [{"$match": {"level": 1}}, {"$out": "redact_out"}]
        list(src.aggregate(out_pipe))
        assert src.query_engine.get_last_tier() == "tier2"
        assert [d["_id"] for d in conn.redact_out.find()] == [1]

        merge_pipe = [
            {"$match": {"level": 5}},
            {"$merge": {"into": "redact_out", "whenNotMatched": "insert"}},
        ]
        list(src.aggregate(merge_pipe))
        assert src.query_engine.get_last_tier() == "tier2"
        assert {d["_id"] for d in conn.redact_out.find()} == {1, 2}


class TestSection2BQueriesAndCursors:
    """Verify $type, $jsonSchema, .hint(), SQL projection pushdown, & Tier-2 $expr sort/limit."""

    def test_type_operator_sql_tier1_and_parity(self, conn):
        coll = conn.typed_docs
        oid = ObjectId()
        bin_val = Binary(b"abc")
        coll.insert_many(
            [
                {"_id": 1, "v": 42},
                {"_id": 2, "v": 3.5},
                {"_id": 3, "v": "hi"},
                {"_id": 4, "v": True},
                {"_id": 5, "v": None},
                {"_id": 6, "v": {"nested": 1}},
                {"_id": 7, "v": [1, 2]},
                {"_id": 8, "v": oid},
                {"_id": 9, "v": bin_val},
                {"_id": 10},  # missing v
            ]
        )

        queries = [
            {"v": {"$type": "int"}},
            {"v": {"$type": 16}},
            {"v": {"$type": "double"}},
            {"v": {"$type": "number"}},
            {"v": {"$type": "string"}},
            {"v": {"$type": "bool"}},
            {"v": {"$type": "null"}},
            {"v": {"$type": "object"}},
            {"v": {"$type": "array"}},
            {"v": {"$type": "objectId"}},
            {"v": {"$type": "binData"}},
            {"v": {"$type": ["int", "string"]}},
            {"v": {"$type": [int, bool]}},
        ]
        for q in queries:
            _assert_find_parity(coll, q, "tier1")

        # Bool is NOT numeric/int; missing field does NOT match null; wrapper is NOT object
        assert [d["_id"] for d in coll.find({"v": {"$type": "number"}})] == [
            1,
            2,
        ]
        assert [d["_id"] for d in coll.find({"v": {"$type": "null"}})] == [5]
        assert [d["_id"] for d in coll.find({"v": {"$type": "object"}})] == [6]

        # Unsupported BSON date falls back cleanly to Tier 3
        _assert_find_parity(coll, {"v": {"$type": "date"}}, "tier3")

    def test_jsonschema_sql_tier1_and_parity(self, conn):
        coll = conn.schema_docs
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "age": 30,
                    "role": "admin",
                    "tags": ["a", "b"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "age": 17,
                    "role": "user",
                    "tags": ["a"],
                },
                {
                    "_id": 3,
                    "name": "Eve",
                    "age": True,  # bool must not match minimum numeric bound!
                    "role": "guest",
                    "tags": [],
                },
            ]
        )

        schemas = [
            {
                "$jsonSchema": {
                    "required": ["name", "age"],
                    "properties": {
                        "age": {"bsonType": "int", "minimum": 18},
                        "role": {"enum": ["admin", "user"]},
                    },
                }
            },
            {
                "$jsonSchema": {
                    "properties": {
                        "name": {"bsonType": "string", "minLength": 4},
                        "tags": {
                            "bsonType": "array",
                            "minItems": 2,
                            "items": {"bsonType": "string"},
                        },
                    }
                }
            },
            {
                "$jsonSchema": {
                    "anyOf": [
                        {"properties": {"role": {"enum": ["admin"]}}},
                        {"properties": {"age": {"bsonType": "int", "maximum": 17}}},
                    ]
                }
            },
            {"$jsonSchema": {"not": {"properties": {"role": {"enum": ["guest"]}}}}},
        ]
        for q in schemas:
            res = _assert_find_parity(coll, q, "tier1")
            assert all(d["_id"] != 3 for d in res)

        # JSON Schema numeric bounds alone vacuously allow non-numbers (bool) in SQL & Python
        vacuous_num_res = _assert_find_parity(
            coll,
            {"$jsonSchema": {"properties": {"age": {"maximum": 17}}}},
            "tier1",
        )
        assert {d["_id"] for d in vacuous_num_res} == {2, 3}

        # $jsonSchema inside aggregation $match routes to Tier 1
        _assert_aggregate_parity(coll, [{"$match": schemas[0]}], "tier1")

        # Empty $jsonSchema falls back to Tier 3 without crashing
        _assert_find_parity(coll, {"$jsonSchema": {}}, "tier3")

    def test_hint_emits_indexed_by_and_handles_missing_safely(self, conn):
        coll = conn.hinted
        coll.insert_many(
            [{"_id": 1, "age": 20}, {"_id": 2, "age": 30}, {"_id": 3, "age": 40}]
        )
        coll.create_index("age")

        cur = coll.find({"age": {"$gte": 25}}).hint("age_1")
        plan = str(cur.explain())
        assert "INDEXED BY" in plan or "USING INDEX" in plan
        docs = _assert_find_parity(coll, {"age": {"$gte": 25}}, "tier1", hint="age_1")
        assert [d["_id"] for d in _normalize_docs(docs)] == [2, 3]

        # Non-existent index hint gracefully drops INDEXED BY rather than raising
        docs_missing = list(coll.find({"age": {"$gte": 25}}).hint("does_not_exist"))
        assert [d["_id"] for d in _normalize_docs(docs_missing)] == [2, 3]

    def test_sql_projection_pushdown_and_bool_preservation(self, conn):
        coll = conn.proj_docs
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "active": True,
                    "banned": False,
                    "secret": "xyz",
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "active": False,
                    "secret": "abc",
                },
            ]
        )

        for proj in [
            {"name": 1, "active": 1, "banned": 1},
            {"name": 1, "active": 1, "_id": 0},
            {"secret": 0},
            {"secret": 0, "_id": 0},
            {"_id": 0},
        ]:
            res = _assert_find_parity(coll, {}, "tier1", projection=proj)
            for doc in res:
                if "active" in doc:
                    assert type(doc["active"]) is bool
                if "banned" in doc:
                    assert type(doc["banned"]) is bool

    def test_tier2_expr_sort_skip_limit_with_data_field(self, conn):
        coll = conn.expr_sort
        coll.insert_many(
            [
                {"_id": 1, "a": 10, "b": 2, "c": 5, "data": "row1"},
                {"_id": 2, "a": 30, "b": 1, "c": 10, "data": "row2"},
                {"_id": 3, "a": 20, "b": 3, "c": 20, "data": "row3"},
            ]
        )
        # Tier-1 $expr with document field named 'data' plus sort/skip/limit
        res = _assert_find_parity(
            coll,
            {"$expr": {"$gt": [{"$add": ["$a", "$b"]}, 10]}},
            "tier1",
            sort=[("data", -1)],
            skip=1,
            limit=2,
        )
        assert [d["data"] for d in res] == ["row2", "row1"]

        # Tier-2 $expr (TempTableExprEvaluator) with sort/skip/limit and document field 'data'
        res_t2 = _assert_find_parity(
            coll,
            {
                "$expr": {
                    "$and": [
                        {"$gt": [{"$add": ["$a", "$b"]}, 10]},
                        {"$lt": [{"$multiply": ["$b", "$c"]}, 30]},
                    ]
                }
            },
            "tier2",
            sort=[("data", 1)],
            skip=1,
            limit=1,
        )
        assert [d["data"] for d in res_t2] == ["row2"]


class TestSection2CCRUDAndUpdates:
    """Verify fetch-free update_one, set-based update_many, SQL RETURNING, and update parity."""

    def test_update_one_fetch_free_no_internal_update_loop(self, conn):
        coll = conn.u1
        coll.insert_one({"_id": 1, "a": 1, "b": True, "scores": [10, 50, 90]})

        qh = coll.query_engine.helpers
        qe = coll.query_engine

        with patch.object(
            qh,
            "_internal_update",
            side_effect=AssertionError(
                "Fetch-free update_one must not call _internal_update"
            ),
        ):
            r1 = coll.update_one({"_id": 1}, {"$set": {"a": 2, "b": False}})
            assert r1.matched_count == 1
            assert r1.modified_count == 1
            assert qe.get_last_tier() == "tier1"

            # No-op update -> matched=1, modified=0
            r_noop = coll.update_one({"_id": 1}, {"$set": {"a": 2, "b": False}})
            assert r_noop.matched_count == 1
            assert r_noop.modified_count == 0
            assert qe.get_last_tier() == "tier1"

            # Tier-2 condition $pull without loading into Python
            r2 = coll.update_one({"_id": 1}, {"$pull": {"scores": {"$gte": 50}}})
            assert r2.matched_count == 1
            assert r2.modified_count == 1
            assert qe.get_last_tier() == "tier2"

        doc = coll.find_one({"_id": 1})
        assert doc is not None
        assert doc["a"] == 2
        assert doc["b"] is False
        assert type(doc["b"]) is bool
        assert doc["scores"] == [10]

    def test_update_many_set_based_and_inc_mul_validation(self, conn):
        coll = conn.umany
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "k": "g",
                    "n": 10,
                    "scores": [70, 90],
                    "nest": {"vals": [1, 5, 10]},
                },
                {
                    "_id": 2,
                    "k": "g",
                    "n": 10,
                    "scores": [80, 95],
                    "nest": {"vals": [2, 8]},
                },
                {"_id": 3, "k": "g", "n": 5, "scores": [60], "nest": {"vals": [3]}},
            ]
        )

        qe = coll.query_engine
        qh = coll.query_engine.helpers

        # Compare set-based SQL vs kill-switch Python across two identical collections
        py_coll = conn.umany_py
        py_coll.insert_many(list(coll.find()))

        updates_and_tiers: list[
            tuple[dict[str, Any], list[dict[str, Any]] | None, str]
        ] = [
            ({"$set": {"flag": True}}, None, "tier1"),
            ({"$unset": {"flag": ""}}, None, "tier1"),
            ({"$inc": {"n": 2}, "$mul": {"missing_m": 3}}, None, "tier1"),
            ({"$pull": {"nest.vals": {"$gte": 5}}}, None, "tier2"),
            ({"$set": {"scores.$[]": 100}}, None, "tier2"),
            (
                {"$set": {"scores.$[e]": 50}},
                [{"e": {"$eq": 100}}],
                "tier2",
            ),
            ({"$setOnInsert": {"extra": 9}}, None, "tier1"),
        ]

        for upd, af, expected_tier in updates_and_tiers:
            set_force_fallback(False)
            with patch.object(
                qh,
                "_internal_update",
                side_effect=AssertionError(
                    "Set-based update_many must not loop in Python"
                ),
            ):
                res_sql = coll.update_many({"k": "g"}, upd, array_filters=af)
            assert qe.get_last_tier() == expected_tier

            set_force_fallback(True)
            res_py = py_coll.update_many({"k": "g"}, upd, array_filters=af)
            assert py_coll.query_engine.get_last_tier() == "tier3"
            set_force_fallback(False)

            assert res_sql.matched_count == res_py.matched_count
            assert res_sql.modified_count == res_py.modified_count
            assert _normalize_docs(list(coll.find())) == _normalize_docs(
                list(py_coll.find())
            )

        # $inc validation catches a non-numeric field even on the 2nd matched row!
        coll.insert_one({"_id": 4, "k": "bad", "v": 1})
        coll.insert_one({"_id": 5, "k": "bad", "v": "not_a_number"})
        with pytest.raises(MalformedQueryException):
            coll.update_many({"k": "bad"}, {"$inc": {"v": 1}})

    def test_dotted_and_condition_pull_and_positional_parity(self, conn):
        base_docs = [
            {
                "_id": 1,
                "nested": {"tags": ["a", "ab", "b", 123, True, None]},
                "items": [{"score": 50}, {"score": 90}],
                "grades": [
                    {"type": "A", "scores": [80, 95]},
                    {"type": "B", "scores": [90, 100]},
                ],
                "scores": [80, 90, 100],
            },
            {"_id": 2, "other": 1},  # missing target arrays must stay missing!
        ]

        ops: list[tuple[dict[str, Any], list[dict[str, Any]] | None, str]] = [
            ({"$pull": {"nested.tags": {"$regex": "^a"}}}, None, "tier2"),
            ({"$pull": {"nested.tags": True}}, None, "tier2"),
            ({"$pull": {"items": {"score": {"$gte": 80}}}}, None, "tier2"),
            (
                {"$set": {"grades.$[elem].scores.$[s]": 100}},
                [{"elem": {"type": "A"}}, {"s": {"$lt": 90}}],
                "tier2",
            ),
            (
                {"$set": {"scores.$[m]": 0}},
                [{"m": {"$gte": 90}}],
                "tier2",
            ),
            ({"$set": {"scores.$[]": 1}}, None, "tier2"),
            ({"$addToSet": {"new_set": {"$each": [1, 1, 2]}}}, None, "tier2"),
            ({"$rename": {"other": "renamed_other", "absent": "nope"}}, None, "tier1"),
        ]

        sql_c = conn.cmp_sql
        py_c = conn.cmp_py
        sql_c.insert_many(copy.deepcopy(base_docs))
        py_c.insert_many(copy.deepcopy(base_docs))

        for upd, af, expected_tier in ops:
            set_force_fallback(False)
            r_sql = sql_c.update_many({}, upd, array_filters=af)
            assert sql_c.query_engine.get_last_tier() == expected_tier

            set_force_fallback(True)
            r_py = py_c.update_many({}, upd, array_filters=af)
            assert py_c.query_engine.get_last_tier() == "tier3"
            set_force_fallback(False)

            assert r_sql.matched_count == r_py.matched_count
            assert r_sql.modified_count == r_py.modified_count
            assert _normalize_docs(list(sql_c.find())) == _normalize_docs(
                list(py_c.find())
            )

    def test_set_on_insert_upsert_and_find_one_and_returning(self, conn):
        coll = conn.foa
        coll.insert_many(
            [
                {"_id": 1, "name": "Alice", "age": 30, "scores": [10, 20]},
                {"_id": 2, "name": "Bob", "age": 40, "scores": [30, 40]},
            ]
        )
        qh = coll.query_engine.helpers
        qe = coll.query_engine

        # Single-statement RETURNING find_one_and_update must not call _internal_update
        with patch.object(
            qh,
            "_internal_update",
            side_effect=AssertionError(
                "RETURNING find_one_and_update should run in 1 SQL statement"
            ),
        ):
            after = coll.find_one_and_update(
                {"age": {"$gte": 30}},
                {"$inc": {"age": 1}, "$set": {"active": True}},
                projection={"name": 1, "age": 1, "active": 1},
                sort=[("age", -1)],
                return_document=True,
            )
            assert after == {"_id": 2, "name": "Bob", "age": 41, "active": True}
            assert qe.get_last_tier() == "tier1"

            after_t2 = coll.find_one_and_update(
                {"_id": 1},
                {"$set": {"scores.$[s]": 99}},
                array_filters=[{"s": {"$gte": 20}}],
                return_document=True,
            )
            assert after_t2 is not None and after_t2["scores"] == [10, 99]
            assert qe.get_last_tier() == "tier2"

        # Single-statement RETURNING find_one_and_replace must not call _internal_replace
        with patch.object(
            qh,
            "_internal_replace",
            side_effect=AssertionError(
                "RETURNING find_one_and_replace should run in 1 SQL statement"
            ),
        ):
            replaced = coll.find_one_and_replace(
                {"_id": 1},
                {"name": "AliceReplaced", "v": Binary(b"bin")},
                return_document=True,
            )
            assert replaced == {
                "_id": 1,
                "name": "AliceReplaced",
                "v": Binary(b"bin"),
            }
            assert qe.get_last_tier() == "tier1"

        # Upsert with $setOnInsert: applied on insert, ignored on update
        u_ins = coll.update_one(
            {"_id": 99},
            {"$set": {"name": "New"}, "$setOnInsert": {"created": True}},
            upsert=True,
        )
        assert u_ins.upserted_id == 99
        assert coll.find_one({"_id": 99}) == {
            "_id": 99,
            "name": "New",
            "created": True,
        }

        u_upd = coll.update_one(
            {"_id": 99},
            {"$setOnInsert": {"created": False}},
            upsert=True,
        )
        assert u_upd.matched_count == 1
        assert u_upd.modified_count == 0
        assert coll.find_one({"_id": 99})["created"] is True

        # find_one_and_update upsert with $setOnInsert under kill switch parity
        f_sql = coll.find_one_and_update(
            {"_id": 100},
            {"$set": {"a": 1}, "$setOnInsert": {"meta.origin": "upsert"}},
            upsert=True,
            return_document=True,
        )
        set_force_fallback(True)
        f_py = coll.find_one_and_update(
            {"_id": 101},
            {"$set": {"a": 1}, "$setOnInsert": {"meta.origin": "upsert"}},
            upsert=True,
            return_document=True,
        )
        set_force_fallback(False)
        assert {k: v for k, v in f_sql.items() if k != "_id"} == {
            k: v for k, v in f_py.items() if k != "_id"
        }

        # Positional $ (first-match) intentionally routes to Tier 3 without regressing
        coll.update_one({"_id": 2, "scores": 30}, {"$set": {"scores.$": 35}})
        assert qe.get_last_tier() == "tier3"
        assert coll.find_one({"_id": 2})["scores"] == [35, 40]

    def test_schema_check_constraint_preserved_on_fast_updates(self, conn):
        validated = conn.create_collection(
            "checked",
            validator={"$jsonSchema": {"properties": {"age": {"minimum": 18}}}},
        )
        validated.insert_one({"_id": 1, "age": 20})
        with pytest.raises(sqlite3.IntegrityError):
            validated.update_one({"_id": 1}, {"$set": {"age": 10}})
        with pytest.raises(sqlite3.IntegrityError):
            validated.find_one_and_update(
                {"_id": 1}, {"$set": {"age": 10}}, return_document=True
            )
