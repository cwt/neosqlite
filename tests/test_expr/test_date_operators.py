"""
Tests for $expr date/time operators.

Covers: $year, $month, $dayOfMonth, $hour, $minute, $second,
        $dayOfWeek, $dayOfYear, $week, $isoDayOfWeek, $isoWeek, $millisecond
"""

from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestDateOperatorsSQL:
    """Test date operators SQL conversion."""

    def test_year_sql(self):
        """Test $year SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$year": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "strftime" in sql
        assert "%Y" in sql

    def test_month_sql(self):
        """Test $month SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$month": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%m" in sql

    def test_day_of_month_sql(self):
        """Test $dayOfMonth SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfMonth": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%d" in sql

    def test_hour_sql(self):
        """Test $hour SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$hour": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%H" in sql

    def test_minute_sql(self):
        """Test $minute SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$minute": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%M" in sql

    def test_second_sql(self):
        """Test $second SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$second": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%S" in sql

    def test_day_of_week_sql(self):
        """Test $dayOfWeek SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfWeek": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%w" in sql

    def test_day_of_year_sql(self):
        """Test $dayOfYear SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfYear": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%j" in sql

    def test_week_sql(self):
        """Test $week SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$week": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%W" in sql

    def test_millisecond_sql(self):
        """Test $millisecond SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$millisecond": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%f" in sql


class TestDateOperatorsPython:
    """Test date operators Python evaluation."""

    def test_year_python(self):
        """Test $year Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$year": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 2024

    def test_month_python(self):
        """Test $month Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$month": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 1

    def test_day_of_month_python(self):
        """Test $dayOfMonth Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfMonth": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 15

    def test_hour_python(self):
        """Test $hour Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$hour": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 10

    def test_minute_python(self):
        """Test $minute Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$minute": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 30

    def test_second_python(self):
        """Test $second Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$second": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:45"}
        )
        assert result == 45

    def test_millisecond_python(self):
        """Test $millisecond Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$millisecond": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:45.123"}
        )
        assert result == 123

    def test_day_of_week_python(self):
        """Test $dayOfWeek Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfWeek": ["$date"]}
        # 2024-01-15 is Monday (weekday() returns 0)
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 0

    def test_day_of_year_python(self):
        """Test $dayOfYear Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfYear": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 15

    def test_week_python(self):
        """Test $week Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$week": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 3  # Week 3 of 2024

    def test_iso_day_of_week_python(self):
        """Test $isoDayOfWeek Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$isoDayOfWeek": ["$date"]}
        # 2024-01-15 is Monday (isocalendar returns 1 for Monday)
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 1

    def test_iso_week_python(self):
        """Test $isoWeek Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$isoWeek": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr, {"date": "2024-01-15T10:30:00"}
        )
        assert result == 3

    def test_date_null_python(self):
        """Test date operators with null value."""
        evaluator = ExprEvaluator()
        expr = {"$year": ["$date"]}
        result = evaluator._evaluate_expr_python(expr, {"date": None})
        assert result is None

    def test_date_invalid_python(self):
        """Test date operators with invalid date."""
        evaluator = ExprEvaluator()
        expr = {"$year": ["$date"]}
        result = evaluator._evaluate_expr_python(expr, {"date": "not-a-date"})
        assert result is None
