"""
Tests for $expr comparison operators.

Covers: $eq, $ne, $gt, $gte, $lt, $lte, $cmp
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestComparisonOperatorsPython:
    """Test comparison operators Python evaluation."""

    def test_eq_operator(self):
        """Test $eq operator."""
        evaluator = ExprEvaluator()
        expr = {"$eq": ["$a", "$b"]}

        assert evaluator.evaluate_python(expr, {"a": 5, "b": 5}) is True
        assert evaluator.evaluate_python(expr, {"a": 5, "b": 10}) is False

    def test_ne_operator(self):
        """Test $ne operator."""
        evaluator = ExprEvaluator()
        expr = {"$ne": ["$a", "$b"]}

        assert evaluator.evaluate_python(expr, {"a": 5, "b": 10}) is True
        assert evaluator.evaluate_python(expr, {"a": 5, "b": 5}) is False

    def test_gt_operator(self):
        """Test $gt operator."""
        evaluator = ExprEvaluator()
        expr = {"$gt": ["$a", "$b"]}

        assert evaluator.evaluate_python(expr, {"a": 10, "b": 5}) is True
        assert evaluator.evaluate_python(expr, {"a": 5, "b": 10}) is False

    def test_gte_operator(self):
        """Test $gte operator."""
        evaluator = ExprEvaluator()
        expr = {"$gte": ["$a", "$b"]}

        assert evaluator.evaluate_python(expr, {"a": 10, "b": 5}) is True
        assert evaluator.evaluate_python(expr, {"a": 5, "b": 5}) is True
        assert evaluator.evaluate_python(expr, {"a": 5, "b": 10}) is False

    def test_lt_operator(self):
        """Test $lt operator."""
        evaluator = ExprEvaluator()
        expr = {"$lt": ["$a", "$b"]}

        assert evaluator.evaluate_python(expr, {"a": 5, "b": 10}) is True
        assert evaluator.evaluate_python(expr, {"a": 10, "b": 5}) is False

    def test_lte_operator(self):
        """Test $lte operator."""
        evaluator = ExprEvaluator()
        expr = {"$lte": ["$a", "$b"]}

        assert evaluator.evaluate_python(expr, {"a": 5, "b": 10}) is True
        assert evaluator.evaluate_python(expr, {"a": 5, "b": 5}) is True
        assert evaluator.evaluate_python(expr, {"a": 10, "b": 5}) is False

    def test_cmp_operator(self):
        """Test $cmp operator."""
        evaluator = ExprEvaluator()
        expr = {"$cmp": ["$a", "$b"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 5, "b": 10}) == -1
        assert evaluator._evaluate_expr_python(expr, {"a": 10, "b": 5}) == 1
        assert evaluator._evaluate_expr_python(expr, {"a": 5, "b": 5}) == 0


class TestComparisonOperatorsSQL:
    """Test comparison operators SQL conversion."""

    def test_eq_sql(self):
        """Test $eq SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$eq": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "=" in sql
        assert params == [5]

    def test_ne_sql(self):
        """Test $ne SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$ne": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "!=" in sql
        assert params == [5]

    def test_gt_sql(self):
        """Test $gt SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$gt": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert ">" in sql
        assert params == [5]

    def test_gte_sql(self):
        """Test $gte SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$gte": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert ">=" in sql
        assert params == [5]

    def test_lt_sql(self):
        """Test $lt SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$lt": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "<" in sql
        assert params == [5]

    def test_lte_sql(self):
        """Test $lte SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$lte": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "<=" in sql
        assert params == [5]

    def test_cmp_sql(self):
        """Test $cmp SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$cmp": ["$a", "$b"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "CASE" in sql
        assert "THEN -1" in sql
        assert "THEN 1" in sql
        assert "ELSE 0" in sql


class TestComparisonIntegration:
    """Integration tests for comparison operators."""

    def test_eq_integration(self):
        """Test $eq with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5, "b": 5},
                    {"a": 5, "b": 10},
                ]
            )

            results = list(collection.find({"$expr": {"$eq": ["$a", "$b"]}}))
            assert len(results) == 1
            assert results[0]["a"] == 5

    def test_gt_integration(self):
        """Test $gt with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 10, "b": 5},
                    {"a": 5, "b": 10},
                ]
            )

            results = list(collection.find({"$expr": {"$gt": ["$a", "$b"]}}))
            assert len(results) == 1
            assert results[0]["a"] == 10

    def test_cmp_integration(self):
        """Test $cmp with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5, "b": 10},
                    {"a": 10, "b": 5},
                    {"a": 5, "b": 5},
                ]
            )

            # Find where a < b (cmp returns -1)
            expr = {"$expr": {"$eq": [{"$cmp": ["$a", "$b"]}, -1]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["a"] == 5
