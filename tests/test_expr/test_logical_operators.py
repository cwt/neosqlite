"""
Tests for $expr logical operators.

Covers: $and, $or, $not, $nor
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestLogicalOperatorsPython:
    """Test logical operators Python evaluation."""

    def test_and_operator(self):
        """Test $and operator."""
        evaluator = ExprEvaluator()
        expr = {"$and": [{"$gt": ["$a", 5]}, {"$lt": ["$a", 10]}]}

        assert evaluator.evaluate_python(expr, {"a": 7}) is True
        assert evaluator.evaluate_python(expr, {"a": 3}) is False
        assert evaluator.evaluate_python(expr, {"a": 15}) is False

    def test_or_operator(self):
        """Test $or operator."""
        evaluator = ExprEvaluator()
        expr = {"$or": [{"$lt": ["$a", 5]}, {"$gt": ["$a", 10]}]}

        assert evaluator.evaluate_python(expr, {"a": 3}) is True
        assert evaluator.evaluate_python(expr, {"a": 15}) is True
        assert evaluator.evaluate_python(expr, {"a": 7}) is False

    def test_not_operator(self):
        """Test $not operator."""
        evaluator = ExprEvaluator()
        expr = {"$not": [{"$gt": ["$a", 10]}]}

        assert evaluator.evaluate_python(expr, {"a": 5}) is True
        assert evaluator.evaluate_python(expr, {"a": 15}) is False

    def test_nor_operator(self):
        """Test $nor operator."""
        evaluator = ExprEvaluator()
        expr = {"$nor": [{"$lt": ["$a", 5]}, {"$gt": ["$a", 10]}]}

        assert evaluator.evaluate_python(expr, {"a": 7}) is True
        assert evaluator.evaluate_python(expr, {"a": 3}) is False
        assert evaluator.evaluate_python(expr, {"a": 15}) is False


class TestLogicalOperatorsSQL:
    """Test logical operators SQL conversion."""

    def test_and_sql(self):
        """Test $and SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$and": [{"$gt": ["$a", 5]}, {"$lt": ["$a", 10]}]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "AND" in sql
        assert params == [5, 10]

    def test_or_sql(self):
        """Test $or SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$or": [{"$lt": ["$a", 5]}, {"$gt": ["$a", 10]}]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "OR" in sql
        assert params == [5, 10]

    def test_not_sql(self):
        """Test $not SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$not": [{"$gt": ["$a", 10]}]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "NOT" in sql
        assert params == [10]

    def test_not_single_operand_sql(self):
        """Test $not SQL conversion with single operand (MongoDB format)."""
        evaluator = ExprEvaluator()
        # MongoDB format: {$not: {expression}} without list wrapper
        expr = {"$not": {"$gt": ["$a", 10]}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "NOT" in sql
        assert params == [10]

    def test_nor_sql(self):
        """Test $nor SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$nor": [{"$lt": ["$a", 5]}, {"$gt": ["$a", 10]}]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "NOT" in sql
        assert "OR" in sql
        assert params == [5, 10]


class TestLogicalIntegration:
    """Integration tests for logical operators."""

    def test_and_integration(self):
        """Test $and with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 7},
                    {"a": 3},
                    {"a": 15},
                ]
            )

            expr = {
                "$expr": {"$and": [{"$gt": ["$a", 5]}, {"$lt": ["$a", 10]}]}
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["a"] == 7

    def test_or_integration(self):
        """Test $or with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 3},
                    {"a": 7},
                    {"a": 15},
                ]
            )

            expr = {"$expr": {"$or": [{"$lt": ["$a", 5]}, {"$gt": ["$a", 10]}]}}
            results = list(collection.find(expr))
            assert len(results) == 2

    def test_not_integration(self):
        """Test $not with database (list format)."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5},
                    {"a": 10},
                    {"a": 15},
                ]
            )

            expr = {"$expr": {"$not": [{"$gt": ["$a", 10]}]}}
            results = list(collection.find(expr))
            assert len(results) == 2  # a=5 and a=10

    def test_not_single_operand_integration(self):
        """Test $not with database (single operand MongoDB format)."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5},
                    {"a": 10},
                    {"a": 15},
                ]
            )

            # MongoDB format: {$not: {expression}}
            expr = {"$expr": {"$not": {"$gt": ["$a", 10]}}}
            results = list(collection.find(expr))
            assert len(results) == 2  # a=5 and a=10
