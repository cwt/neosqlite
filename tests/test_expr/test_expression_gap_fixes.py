"""Regression tests for MongoDB expression gaps.

1. ``$split`` with a field-reference delimiter must be treated as a dynamic
   expression, not as the literal string ``"$delimiter"``.
2. A plain object passed to ``$mergeObjects`` is an object expression whose
   field values are evaluated, including nested ``$sum`` over an array.
"""

import pytest

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.query_helper.utils import set_force_fallback


class TestSplitDynamicDelimiter:
    """``$split`` field-reference delimiter parity."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    def test_sql_converter_declines_field_reference(self):
        evaluator = ExprEvaluator(data_column="data")
        with pytest.raises(NotImplementedError, match="dynamic delimiter"):
            evaluator.evaluate_for_aggregation(
                {"$split": ["$name", "$delimiter"]}
            )

    def test_sql_converter_still_accepts_literal(self):
        evaluator = ExprEvaluator(data_column="data")
        sql, params = evaluator.evaluate_for_aggregation(
            {"$split": ["$name", ","]}
        )
        assert "json_group_array" in sql
        assert params == [","]

    @pytest.mark.parametrize("force_python", [False, True])
    def test_split_with_field_reference(self, force_python):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs
            coll.insert_many(
                [
                    {"_id": 1, "name": "alpha,beta", "delimiter": ","},
                    {"_id": 2, "name": "gamma;delta", "delimiter": ";"},
                ]
            )
            set_force_fallback(force_python)
            results = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "first": {
                                    "$arrayElemAt": [
                                        {"$split": ["$name", "$delimiter"]},
                                        0,
                                    ]
                                }
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            )
            assert [doc["first"] for doc in results] == ["alpha", "gamma"]


class TestMergeObjectsExpressionValues:
    """Nested expression evaluation in ``$mergeObjects``."""

    def test_sql_converter_builds_json_object(self):
        evaluator = ExprEvaluator(data_column="data")
        sql, params = evaluator.evaluate_for_aggregation(
            {"$mergeObjects": ["$nested", {"total": {"$sum": "$values"}}]}
        )
        assert "json_patch(" in sql
        assert "json_object('total'" in sql
        assert "SUM(" in sql
        assert params == []

    def test_python_evaluator_computes_nested_sum(self):
        evaluator = ExprEvaluator(data_column="data")
        result = evaluator._evaluate_expr_python(
            {"$mergeObjects": ["$nested", {"total": {"$sum": "$values"}}]},
            {"_id": 1, "nested": {"a": 1}, "values": [1, 2, 3]},
        )
        assert result == {"a": 1, "total": 6}

    def test_sum_of_empty_array_is_zero(self):
        evaluator = ExprEvaluator(data_column="data")
        sql, _ = evaluator.evaluate_for_aggregation(
            {"$mergeObjects": [{"total": {"$sum": "$values"}}]}
        )
        assert "COALESCE((SELECT SUM(value)" in sql
        result = evaluator._evaluate_expr_python(
            {"$sum": "$values"}, {"values": []}
        )
        assert result == 0

    @pytest.mark.parametrize("force_python", [False, True])
    def test_replace_root_with_nested_sum(self, force_python):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs
            coll.insert_many(
                [
                    {
                        "_id": 1,
                        "nested": {"a": 1},
                        "values": [1, 2, 3],
                    },
                    {
                        "_id": 2,
                        "nested": {"a": 2},
                        "values": [4, 5],
                    },
                ]
            )
            set_force_fallback(force_python)
            results = list(
                coll.aggregate(
                    [
                        {
                            "$replaceRoot": {
                                "newRoot": {
                                    "$mergeObjects": [
                                        "$nested",
                                        {"total": {"$sum": "$values"}},
                                    ]
                                }
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            )
            assert results == [
                {"a": 1, "total": 6, "_id": 1},
                {"a": 2, "total": 9, "_id": 2},
            ]
