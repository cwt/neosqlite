"""
Tests for $expr array aggregation operators.

Covers: $sum, $avg, $min, $max, $slice
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestArrayAggregationPython:
    """Test array aggregation operators Python evaluation."""

    def test_sum_operator(self):
        """Test $sum operator."""
        evaluator = ExprEvaluator()
        expr = {"$sum": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 6
        assert (
            evaluator._evaluate_expr_python(expr, {"items": [1, "a", 2]}) == 3
        )
        assert evaluator._evaluate_expr_python(expr, {"items": []}) == 0

    def test_avg_operator(self):
        """Test $avg operator."""
        evaluator = ExprEvaluator()
        expr = {"$avg": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [10, 20]}) == 15
        assert (
            evaluator._evaluate_expr_python(expr, {"items": [10, "a", 20]})
            == 15
        )
        assert evaluator._evaluate_expr_python(expr, {"items": []}) is None

    def test_min_operator(self):
        """Test $min operator."""
        evaluator = ExprEvaluator()
        expr = {"$min": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [5, 2, 8]}) == 2
        assert evaluator._evaluate_expr_python(expr, {"items": []}) is None

    def test_max_operator(self):
        """Test $max operator."""
        evaluator = ExprEvaluator()
        expr = {"$max": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [5, 2, 8]}) == 8
        assert evaluator._evaluate_expr_python(expr, {"items": []}) is None

    def test_slice_operator(self):
        """Test $slice operator."""
        evaluator = ExprEvaluator()
        expr = {"$slice": ["$items", 2]}
        assert evaluator._evaluate_expr_python(
            expr, {"items": [1, 2, 3, 4]}
        ) == [1, 2]

        expr = {"$slice": ["$items", 2, 1]}
        assert evaluator._evaluate_expr_python(
            expr, {"items": [1, 2, 3, 4]}
        ) == [2, 3]


class TestArrayAggregationSQL:
    """Test array aggregation operators SQL conversion."""

    def test_sum_sql(self):
        """Test $sum SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$sum": ["$items"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "SUM" in sql
        assert "json_each" in sql

    def test_avg_sql(self):
        """Test $avg SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$avg": ["$items"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "AVG" in sql

    def test_slice_sql(self):
        """Test $slice SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$slice": ["$items", 2]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_group_array" in sql
        assert "LIMIT" in sql


class TestArrayAggregationIntegration:
    """Integration tests for array aggregation."""

    def test_sum_integration(self):
        """Test $sum with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"name": "A", "scores": [10, 20, 30]},
                    {"name": "B", "scores": [1, 2]},
                ]
            )

            # Find docs where sum(scores) > 25
            expr = {"$expr": {"$gt": [{"$sum": ["$scores"]}, 25]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["name"] == "A"

    def test_slice_integration(self):
        """Test $slice with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_one({"name": "A", "items": [1, 2, 3, 4, 5]})

            # Find docs where slice(items, 2)[0] == 1
            # Note: MongoDB actually returns the array, so we'd need $arrayElemAt to get the value
            # But here we just test that the slice itself works in a comparison
            expr = {"$expr": {"$eq": [{"$slice": ["$items", 2]}, [1, 2]]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["name"] == "A"
