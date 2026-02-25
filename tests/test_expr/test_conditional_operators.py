"""
Tests for $expr conditional operators.

Covers: $cond, $ifNull, $switch
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestConditionalOperatorsPython:
    """Test conditional operators Python evaluation."""

    def test_cond_dict_format(self):
        """Test $cond with dict format."""
        evaluator = ExprEvaluator()
        expr = {
            "$cond": {"if": {"$gt": ["$a", 10]}, "then": "high", "else": "low"}
        }

        assert evaluator._evaluate_expr_python(expr, {"a": 15}) == "high"
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == "low"

    def test_cond_array_format(self):
        """Test $cond with array format."""
        evaluator = ExprEvaluator()
        expr = {"$cond": [{"$gt": ["$a", 10]}, "high", "low"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 15}) == "high"
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == "low"

    def test_cond_without_else(self):
        """Test $cond without else."""
        evaluator = ExprEvaluator()
        expr = {"$cond": {"if": {"$gt": ["$a", 10]}, "then": "high"}}

        assert evaluator._evaluate_expr_python(expr, {"a": 15}) == "high"
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) is None

    def test_ifNull_operator(self):
        """Test $ifNull operator."""
        evaluator = ExprEvaluator()
        expr = {"$ifNull": ["$a", "default"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == 5
        assert evaluator._evaluate_expr_python(expr, {}) == "default"
        assert evaluator._evaluate_expr_python(expr, {"a": None}) == "default"

    def test_switch_operator(self):
        """Test $switch operator."""
        evaluator = ExprEvaluator()
        expr = {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$a", 1]}, "then": "one"},
                    {"case": {"$eq": ["$a", 2]}, "then": "two"},
                ],
                "default": "other",
            }
        }

        assert evaluator._evaluate_expr_python(expr, {"a": 1}) == "one"
        assert evaluator._evaluate_expr_python(expr, {"a": 2}) == "two"
        assert evaluator._evaluate_expr_python(expr, {"a": 3}) == "other"


class TestConditionalOperatorsSQL:
    """Test conditional operators SQL conversion."""

    def test_cond_sql(self):
        """Test $cond SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$cond": {"if": {"$gt": ["$a", 10]}, "then": 1, "else": 0}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "CASE" in sql
        assert "WHEN" in sql
        assert "THEN" in sql
        assert "ELSE" in sql
        assert params == [10, 1, 0]

    def test_cond_without_else_sql(self):
        """Test $cond without else SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$cond": {"if": {"$gt": ["$a", 10]}, "then": 1}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "CASE" in sql
        assert "ELSE NULL" in sql
        assert params == [10, 1]

    def test_ifNull_sql(self):
        """Test $ifNull SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$ifNull": ["$a", "default"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "COALESCE" in sql
        assert params == ["default"]


class TestConditionalIntegration:
    """Integration tests for conditional operators."""

    def test_cond_integration(self):
        """Test $cond with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"qty": 5, "price": 10},
                    {"qty": 15, "price": 10},
                ]
            )

            # Find where discounted price < 5
            expr = {
                "$expr": {
                    "$lt": [
                        {
                            "$cond": {
                                "if": {"$gte": ["$qty", 10]},
                                "then": {"$multiply": ["$price", 0.5]},
                                "else": "$price",
                            }
                        },
                        6,
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["qty"] == 15

    def test_ifNull_integration(self):
        """Test $ifNull with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5},
                    {"b": 10},
                ]
            )

            expr = {"$expr": {"$gt": [{"$ifNull": ["$a", 0]}, 3]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["a"] == 5
