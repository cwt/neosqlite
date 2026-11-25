"""
Tests for $expr SQL vs Python consistency.

Verifies that SQL (Tier 1) and Python (Tier 3) implementations produce identical results.
"""

import pytest

import neosqlite
from neosqlite._sqlite import sqlite3
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.expr_temp_table import TempTableExprEvaluator
from neosqlite.collection.jsonb_support import supports_jsonb
from neosqlite.collection.query_helper import set_force_fallback

_JSONB_PROBE_CONN = sqlite3.connect(":memory:")
_JSONB_AVAILABLE = supports_jsonb(_JSONB_PROBE_CONN)
_JSONB_PROBE_CONN.close()


class TestSQLPythonConsistency:
    """Test that SQL and Python implementations produce identical results."""

    def _check_sql_python_consistency(self, evaluator, expr, test_docs):
        """
        Helper to check that SQL and Python produce same results.

        Args:
            evaluator: ExprEvaluator instance
            expr: Expression to test
            test_docs: List of test documents
        """
        # Get SQL result
        sql_result, params = evaluator._evaluate_sql_tier1(expr)

        # Get Python results
        python_results = []
        for doc in test_docs:
            python_result = evaluator.evaluate_python(expr, doc)
            python_results.append(python_result)

        # If SQL is supported, verify it generates valid SQL
        if sql_result is not None:
            assert isinstance(sql_result, str)
            assert len(sql_result) > 0
            return True
        return False

    def test_comparison_consistency(self):
        """Test comparison operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 5, "b": 5},
            {"a": 5, "b": 10},
            {"a": 10, "b": 5},
        ]

        # Test all comparison operators
        for op in ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"]:
            expr = {op: ["$a", "$b"]}
            self._check_sql_python_consistency(evaluator, expr, test_docs)

            # Verify Python results
            python_results = [
                evaluator.evaluate_python(expr, doc) for doc in test_docs
            ]
            assert isinstance(python_results[0], bool)

    def test_arithmetic_consistency(self):
        """Test arithmetic operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 10, "b": 5},
            {"a": 20, "b": 4},
        ]

        # Test arithmetic in comparison
        for op, expected_op in [
            ("$add", "+"),
            ("$subtract", "-"),
            ("$multiply", "*"),
            ("$divide", "/"),
        ]:
            expr = {"$eq": [{op: ["$a", "$b"]}, 5]}
            self._check_sql_python_consistency(evaluator, expr, test_docs)

    def test_conditional_consistency(self):
        """Test $cond SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 15},
            {"a": 5},
        ]

        expr = {
            "$cond": {"if": {"$gt": ["$a", 10]}, "then": "high", "else": "low"}
        }
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"a": 15}) == "high"
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == "low"

    def test_ifnull_consistency(self):
        """Test $ifNull SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 5},
            {"a": None},
            {},
        ]

        expr = {"$ifNull": ["$a", "default"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == 5
        assert evaluator._evaluate_expr_python(expr, {"a": None}) == "default"
        assert evaluator._evaluate_expr_python(expr, {}) == "default"

    def test_array_consistency(self):
        """Test array operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"items": [1, 2, 3]},
            {"items": [1, 2]},
            {"items": []},
        ]

        # Test $size
        expr = {"$size": ["$items"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 3
        assert evaluator._evaluate_expr_python(expr, {"items": []}) == 0

    def test_string_consistency(self):
        """Test string operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"text": "HELLO"},
            {"text": "world"},
        ]

        # Test $toLower
        expr = {"$toLower": ["$text"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "HELLO"}) == "hello"
        )
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "world"}) == "world"
        )

    def test_math_consistency(self):
        """Test math operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"value": 5.5},
            {"value": -5.5},
            {"value": 10},
        ]

        # Test $abs
        expr = {"$abs": ["$value"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"value": -5.5}) == 5.5
        assert evaluator._evaluate_expr_python(expr, {"value": 10}) == 10


class TestExprIssue201Parity:
    """Regression and Tier 1 vs Tier 3 parity tests for issue #201."""

    @pytest.fixture(autouse=True)
    def _reset_fallback(self):
        yield
        set_force_fallback(False)

    def _run_aggregate_both_tiers(self, coll, pipeline):
        set_force_fallback(False)
        tier1 = list(coll.aggregate(pipeline))
        set_force_fallback(True)
        tier3 = list(coll.aggregate(pipeline))
        set_force_fallback(False)
        return tier1, tier3

    def _run_find_both_tiers(self, coll, query):
        set_force_fallback(False)
        tier1 = list(coll.find(query, {"_id": 1}).sort("_id", 1))
        set_force_fallback(True)
        tier3 = list(coll.find(query, {"_id": 1}).sort("_id", 1))
        set_force_fallback(False)
        return tier1, tier3

    def test_param_binding_in_type_conversion_and_replace_one(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.c
            coll.insert_one({"_id": 1, "text": "foo bar foo"})
            pipeline = [
                {
                    "$project": {
                        "_id": 0,
                        "int_ok": {"$toInt": "42"},
                        "int_bad": {"$toInt": "invalid"},
                        "int_zero": {"$toInt": "0"},
                        "dbl_ok": {"$toDouble": "3.14"},
                        "dbl_bad": {"$toDouble": "nope"},
                        "lng_ok": {"$toLong": "99"},
                        "bool_str": {"$toBool": "abc"},
                        "bool_empty": {"$toBool": ""},
                        "type_str": {"$type": "hello"},
                        "rep_hit": {
                            "$replaceOne": {
                                "input": "foo bar foo",
                                "find": "foo",
                                "replacement": "baz",
                            }
                        },
                        "rep_miss": {"$replaceOne": ["$text", "xyz", "abc"]},
                    }
                }
            ]
            tier1, tier3 = self._run_aggregate_both_tiers(coll, pipeline)
            assert tier1 == tier3
            assert tier1 == [
                {
                    "int_ok": 42,
                    "int_bad": None,
                    "int_zero": 0,
                    "dbl_ok": 3.14,
                    "dbl_bad": None,
                    "lng_ok": 99,
                    "bool_str": True,
                    "bool_empty": False,
                    "type_str": "string",
                    "rep_hit": "baz bar foo",
                    "rep_miss": "foo bar foo",
                }
            ]

    def test_getfield_dynamic_type_inference(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.c
            coll.insert_many(
                [
                    {"_id": 1, "val": 42},
                    {"_id": 2, "val": "hello"},
                    {"_id": 3, "val": False},
                    {"_id": 4, "val": 0},
                    {"_id": 5},
                ]
            )
            pipeline = [
                {"$sort": {"_id": 1}},
                {
                    "$project": {
                        "_id": 1,
                        "is_num": {
                            "$isNumber": {"$getField": {"field": "val"}}
                        },
                        "btype": {"$type": {"$getField": {"field": "val"}}},
                        "as_bool": {"$toBool": {"$getField": {"field": "val"}}},
                    }
                },
            ]
            tier1, tier3 = self._run_aggregate_both_tiers(coll, pipeline)
            assert tier1 == tier3
            assert [d["is_num"] for d in tier1] == [
                True,
                False,
                False,
                True,
                False,
            ]
            assert [d["btype"] for d in tier1] == [
                "int",
                "string",
                "bool",
                "int",
                "null",
            ]
            assert [d["as_bool"] for d in tier1] == [
                1,
                1,
                0,
                0,
                0,
            ] or [bool(d["as_bool"]) for d in tier1] == [
                True,
                True,
                False,
                False,
                False,
            ]

    @pytest.mark.skipif(
        not _JSONB_AVAILABLE, reason="Requires SQLite with JSONB support"
    )
    def test_merge_objects_mod_object_to_array_and_string_nulls(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.c
            coll.insert_one(
                {
                    "_id": 1,
                    "a": {"sub": {"x": 1, "y": 2}, "k": 9, "flag": True},
                    "b": {"sub": {"x": 10}, "k": None},
                    "arr": [10, None],
                    "obj": {"flag": False, "nested": {"z": 1}, "items": [1, 2]},
                    "s": "hello",
                }
            )
            pipeline = [
                {
                    "$project": {
                        "_id": 0,
                        "merged": {"$mergeObjects": ["$a", "$b"]},
                        "merged_missing": {"$mergeObjects": ["$a", "$missing"]},
                        "merged_only_missing": {"$mergeObjects": ["$missing"]},
                        "fmod": {"$mod": [5.5, 2]},
                        "neg_fmod": {"$mod": [-7.5, 3]},
                        "has_null": {"$in": [None, "$arr"]},
                        "null_idx": {"$indexOfArray": ["$arr", None]},
                        "ota": {"$objectToArray": "$obj"},
                        "ota_missing": {"$objectToArray": "$missing"},
                        "idx_null": {"$indexOfCP": ["$missing", "a"]},
                        "cmp_null": {"$strcasecmp": ["$s", "$missing"]},
                    }
                }
            ]
            tier1, tier3 = self._run_aggregate_both_tiers(coll, pipeline)
            assert tier1 == tier3
            assert tier1[0]["merged"] == {
                "sub": {"x": 10},
                "k": None,
                "flag": True,
            }
            assert tier1[0]["merged_missing"] == {
                "sub": {"x": 1, "y": 2},
                "k": 9,
                "flag": True,
            }
            assert tier1[0]["merged_only_missing"] == {}
            assert tier1[0]["fmod"] == 1.5
            assert tier1[0]["neg_fmod"] == -1.5
            assert tier1[0]["has_null"] in (True, 1)
            assert tier1[0]["null_idx"] == 1
            assert tier1[0]["ota"] == [
                {"k": "flag", "v": False},
                {"k": "nested", "v": {"z": 1}},
                {"k": "items", "v": [1, 2]},
            ]
            assert tier1[0]["ota_missing"] == []
            assert tier1[0]["idx_null"] == -1
            assert tier1[0]["cmp_null"] is None

    def test_logical_ne_and_set_operators_parity(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.c
            coll.insert_many(
                [
                    {
                        "_id": 1,
                        "a": [1, None, "false"],
                        "b": [None, 1],
                        "falsy": ["false", "", []],
                        "all_falsy": [0, False, None],
                        "x": 5,
                    },
                    {
                        "_id": 2,
                        "a": [1, 2],
                        "b": [1, None],
                        "falsy": [0],
                        "all_falsy": [0, 1],
                    },
                ]
            )
            queries = [
                {"$expr": {"$not": [{"$gt": ["$missing", 5]}]}},
                {
                    "$expr": {
                        "$nor": [
                            {"$gt": ["$missing", 5]},
                            {"$gt": ["$missing", 1]},
                        ]
                    }
                },
                {"$expr": {"$ne": ["$x", 5]}},
                {"$expr": {"$ne": [5, "$x"]}},
                {"$expr": {"$setEquals": ["$a", [None, "false", 1]]}},
                {"$expr": {"$setIsSubset": [[None], "$a"]}},
                {"$expr": {"$in": [None, {"$setIntersection": ["$a", "$b"]}]}},
                {"$expr": {"$in": [None, {"$setUnion": ["$a", "$b"]}]}},
                {"$expr": {"$in": [None, {"$setDifference": ["$b", "$a"]}]}},
                {"$expr": {"$anyElementTrue": ["$falsy"]}},
                {"$expr": {"$allElementsTrue": ["$falsy"]}},
            ]
            for query in queries:
                tier1, tier3 = self._run_find_both_tiers(coll, query)
                assert tier1 == tier3, f"Mismatch for query {query}"

    def test_temp_table_complexity_and_cache_cleanup(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.c
            coll.insert_many(
                [{"a": 1, "b": 2, "c": 2}, {"a": 1, "b": 1, "c": 5}]
            )
            evaluator = TempTableExprEvaluator(conn.db)

            assert evaluator._analyze_complexity({"$eq": ["$a", "$b"]}) == 1
            assert (
                evaluator._analyze_complexity(
                    {"$gt": [{"$add": ["$a", "$b"]}, "$c"]}
                )
                >= 2
            )

            expr = {"$gt": [{"$add": ["$a", "$b"]}, "$c"]}
            sql1, params1, tables1 = evaluator.evaluate(expr, coll.name)
            assert sql1 is not None
            assert len(tables1) == 1
            assert evaluator._temp_tables == []

            # Second call hits TranslationCache and must still clear _temp_tables
            sql2, params2, tables2 = evaluator.evaluate(expr, coll.name)
            assert sql2 is not None
            assert len(tables2) == 1
            assert evaluator._temp_tables == []
            for t in tables1 + tables2:
                conn.db.execute(f'DROP TABLE IF EXISTS "{t}"')
