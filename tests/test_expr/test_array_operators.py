"""
Tests for $expr array operators.

Basic: $size, $in, $isArray, $arrayElemAt, $first, $last
Extended: $slice, $indexOfArray
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestArrayOperatorsPython:
    """Test array operators Python evaluation."""

    def test_size_operator(self):
        """Test $size operator."""
        evaluator = ExprEvaluator()
        expr = {"$size": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 3
        assert evaluator._evaluate_expr_python(expr, {"items": []}) == 0

    def test_in_operator(self):
        """Test $in operator."""
        evaluator = ExprEvaluator()
        expr = {"$in": ["$value", "$allowed"]}
        assert (
            evaluator.evaluate_python(expr, {"value": 2, "allowed": [1, 2, 3]})
            is True
        )
        assert (
            evaluator.evaluate_python(expr, {"value": 5, "allowed": [1, 2, 3]})
            is False
        )

    def test_isArray_operator(self):
        """Test $isArray operator."""
        evaluator = ExprEvaluator()
        expr = {"$isArray": ["$items"]}
        assert evaluator.evaluate_python(expr, {"items": [1, 2, 3]}) is True
        assert evaluator.evaluate_python(expr, {"items": 5}) is False
        assert evaluator.evaluate_python(expr, {"items": "hello"}) is False

    def test_arrayElemAt_operator(self):
        """Test $arrayElemAt operator."""
        evaluator = ExprEvaluator()
        expr = {"$arrayElemAt": ["$items", 1]}
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 2

    def test_first_operator(self):
        """Test $first operator."""
        evaluator = ExprEvaluator()
        expr = {"$first": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 1
        assert evaluator._evaluate_expr_python(expr, {"items": []}) is None

    def test_last_operator(self):
        """Test $last operator."""
        evaluator = ExprEvaluator()
        expr = {"$last": ["$items"]}
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 3
        assert evaluator._evaluate_expr_python(expr, {"items": []}) is None

    def test_slice_operator(self):
        """Test $slice operator."""
        evaluator = ExprEvaluator()
        expr = {"$slice": ["$arr", 2]}
        result = evaluator._evaluate_expr_python(expr, {"arr": [1, 2, 3, 4]})
        assert result == [1, 2]

    def test_slice_negative(self):
        """Test $slice with negative count."""
        evaluator = ExprEvaluator()
        expr = {"$slice": ["$arr", -2]}
        result = evaluator._evaluate_expr_python(expr, {"arr": [1, 2, 3, 4]})
        assert result == [3, 4]

    def test_slice_with_skip(self):
        """Test $slice with skip."""
        evaluator = ExprEvaluator()
        expr = {"$slice": ["$arr", 2, 1]}
        result = evaluator._evaluate_expr_python(expr, {"arr": [1, 2, 3, 4]})
        assert result == [2, 3]

    def test_indexOfArray_operator(self):
        """Test $indexOfArray operator."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfArray": ["$arr", 3]}
        result = evaluator._evaluate_expr_python(expr, {"arr": [1, 2, 3, 4]})
        assert result == 2

    def test_indexOfArray_not_found(self):
        """Test $indexOfArray not found."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfArray": ["$arr", 5]}
        result = evaluator._evaluate_expr_python(expr, {"arr": [1, 2, 3, 4]})
        assert result == -1


class TestArrayOperatorsSQL:
    """Test array operators SQL conversion."""

    def test_size_sql(self):
        """Test $size SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$size": ["$items"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_array_length" in sql

    def test_in_sql(self):
        """Test $in SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$in": [5, "$allowed"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "EXISTS" in sql
        assert "json_each" in sql
        assert params == [5]

    def test_isArray_sql(self):
        """Test $isArray SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$isArray": ["$items"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_type" in sql
        assert "'array'" in sql

    def test_indexOfArray_sql(self):
        """Test $indexOfArray SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfArray": ["$arr", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_each" in sql


class TestArrayIntegration:
    """Integration tests for array operators."""

    def test_size_integration(self):
        """Test $size with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"items": [1, 2, 3]},
                    {"items": [1, 2]},
                    {"items": [1]},
                ]
            )

            expr = {"$expr": {"$gt": [{"$size": ["$items"]}, 2]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert len(results[0]["items"]) == 3

    def test_in_integration(self):
        """Test $in with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"value": 2, "allowed": [1, 2, 3]},
                    {"value": 5, "allowed": [1, 2, 3]},
                ]
            )

            expr = {"$expr": {"$in": ["$value", "$allowed"]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["value"] == 2
