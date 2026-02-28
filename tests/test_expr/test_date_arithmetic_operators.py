"""
Tests for $expr date arithmetic operators.

Covers: $dateAdd, $dateSubtract, $dateDiff
"""

import pytest
from datetime import datetime, timezone
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
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == datetime(2024, 1, 20, 10, 30, 0, tzinfo=timezone.utc)

    def test_date_add_hours_python(self):
        """Test $dateAdd with hours."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 3, "hour"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)

    def test_date_subtract_days_python(self):
        """Test $dateSubtract with days."""
        evaluator = ExprEvaluator()
        expr = {"$dateSubtract": ["$date", 5, "day"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == datetime(2024, 1, 10, 10, 30, 0, tzinfo=timezone.utc)

    def test_date_add_months_python(self):
        """Test $dateAdd with months."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 2, "month"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_date_add_years_python(self):
        """Test $dateAdd with years."""
        evaluator = ExprEvaluator()
        expr = {"$dateAdd": ["$date", 1, "year"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_date_diff_days_python(self):
        """Test $dateDiff with days."""
        evaluator = ExprEvaluator()
        expr = {"$dateDiff": ["$date1", "$date2", "day"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {
                "date1": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "date2": datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc),
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
                "date1": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "date2": datetime(2024, 1, 1, 5, 0, 0, tzinfo=timezone.utc),
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
                "date1": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "date2": datetime(2024, 1, 22, 0, 0, 0, tzinfo=timezone.utc),
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
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == datetime(2024, 1, 29, 10, 30, 0, tzinfo=timezone.utc)


class TestDateArithmeticIntegration:
    """Integration tests with actual collection queries."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        collection = conn["test_collection"]

        # Use datetime objects for MongoDB compatibility
        collection.insert_many(
            [
                {
                    "name": "doc1",
                    "date": datetime(
                        2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "name": "doc2",
                    "date": datetime(
                        2024, 2, 20, 15, 45, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "name": "doc3",
                    "date": datetime(
                        2024, 3, 25, 20, 0, 0, tzinfo=timezone.utc
                    ),
                },
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
                                    datetime(
                                        2024,
                                        12,
                                        31,
                                        0,
                                        0,
                                        0,
                                        tzinfo=timezone.utc,
                                    ),
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
