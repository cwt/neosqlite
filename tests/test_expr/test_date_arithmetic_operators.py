"""
Tests for $expr date arithmetic operators.

Covers: $dateAdd, $dateSubtract, $dateDiff
"""

import pytest
import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestDateArithmeticSQL:
    """Test date arithmetic operators SQL conversion."""

    def test_date_add_sql(self):
        """Test $dateAdd SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 5, "day"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "datetime" in sql
        assert "+5 days" in sql

    def test_date_subtract_sql(self):
        """Test $dateSubtract SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dateSubtract": ["$date", 3, "hour"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "datetime" in sql
        assert "-3 hours" in sql

    def test_date_diff_sql(self):
        """Test $dateDiff SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dateDiff": ["$date1", "$date2", "day"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "julianday" in sql


class TestDateArithmeticPython:
    """Test date arithmetic operators Python evaluation."""

    def test_date_add_days_python(self):
        """Test $dateAdd with days."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 5, "day"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == "2024-01-20T10:30:00"

    def test_date_add_hours_python(self):
        """Test $dateAdd with hours."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 3, "hour"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == "2024-01-15T13:30:00"

    def test_date_subtract_days_python(self):
        """Test $dateSubtract with days."""
        evaluator = ExprEvaluator()
        expr = {"$dateSubtract": ["$date", 5, "day"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == "2024-01-10T10:30:00"

    def test_date_add_months_python(self):
        """Test $dateAdd with months."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 2, "month"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == "2024-03-15T10:30:00"

    def test_date_add_years_python(self):
        """Test $dateAdd with years."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 1, "year"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == "2025-01-15T10:30:00"

    def test_date_diff_days_python(self):
        """Test $dateDiff with days."""
        evaluator = ExprEvaluator()
        expr = {"$dateDiff": ["$date1", "$date2", "day"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {
                "date1": "2024-01-01T00:00:00",
                "date2": "2024-01-15T00:00:00",
            },
        )
        assert result == 14

    def test_date_diff_hours_python(self):
        """Test $dateDiff with hours."""
        evaluator = ExprEvaluator()
        expr = {"$dateDiff": ["$date1", "$date2", "hour"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {
                "date1": "2024-01-01T00:00:00",
                "date2": "2024-01-01T05:00:00",
            },
        )
        assert result == 5

    def test_date_diff_weeks_python(self):
        """Test $dateDiff with weeks."""
        evaluator = ExprEvaluator()
        expr = {"$dateDiff": ["$date1", "$date2", "week"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {
                "date1": "2024-01-01T00:00:00",
                "date2": "2024-01-22T00:00:00",
            },
        )
        assert result == 3

    def test_date_arithmetic_null_python(self):
        """Test date arithmetic with null value."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 5, "day"]}
        result = evaluator._evaluate_expr_python(expr, {"date": None})
        assert result is None

    def test_date_add_weeks_python(self):
        """Test $dateAdd with weeks."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 2, "week"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == "2024-01-29T10:30:00"


class TestDateArithmeticIntegration:
    """Integration tests with actual collection queries."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        collection = conn["test_collection"]

        collection.insert_many(
            [
                {"name": "doc1", "date": "2024-01-15T10:30:00"},
                {"name": "doc2", "date": "2024-02-20T15:45:00"},
                {"name": "doc3", "date": "2024-03-25T20:00:00"},
            ]
        )

        yield collection
        conn.close()

    def test_date_add_in_query(self, collection):
        """Test $dateAdd in actual query."""
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$eq": [
                            {"$year": [{"$dateAdd": ["$date", 1, "year"]}]},
                            2025,
                        ]
                    }
                }
            )
        )
        assert len(result) == 3
        assert any(doc["name"] == "doc1" for doc in result)

    def test_date_diff_in_query(self, collection):
        """Test $dateDiff in actual query."""
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$gt": [
                            {
                                "$dateDiff": [
                                    "$date",
                                    "2024-12-31T00:00:00",
                                    "day",
                                ]
                            },
                            200,
                        ]
                    }
                }
            )
        )
        assert len(result) >= 1
