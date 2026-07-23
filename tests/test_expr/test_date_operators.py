"""
Tests for $expr date/time operators.

Covers: $year, $month, $dayOfMonth, $hour, $minute, $second,
        $dayOfWeek, $dayOfYear, $week, $isoDayOfWeek, $isoWeek, $millisecond
"""

import json
from datetime import datetime, timezone

import neosqlite
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
        assert "%U" in sql

    def test_millisecond_sql(self):
        """Test $millisecond SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$millisecond": ["$date"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%f" in sql

    def test_dateToString_sql(self):
        """Test $dateToString SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$dateToString": {"format": "%Y-%m-%d", "date": "$dt"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "strftime" in sql
        assert "%Y-%m-%d" in sql


class TestDateOperatorsSingleValueFormat:
    """Test date operators with single-value operand format (bug fix verification).

    These tests verify that date operators work with the single-value format
    {"$year": "$date"} which is the common MongoDB API pattern, not just
    the list format {"$year": ["$date"]}.

    This was a bug where SQL converters failed to normalize single-value operands.
    """

    def test_year_single_value_format_sql(self):
        """Test $year with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$year": "$date"}  # Single-value format, not list
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "strftime" in sql
        assert "%Y" in sql

    def test_month_single_value_format_sql(self):
        """Test $month with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$month": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%m" in sql

    def test_day_of_month_single_value_format_sql(self):
        """Test $dayOfMonth with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfMonth": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%d" in sql

    def test_hour_single_value_format_sql(self):
        """Test $hour with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$hour": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%H" in sql

    def test_minute_single_value_format_sql(self):
        """Test $minute with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$minute": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%M" in sql

    def test_second_single_value_format_sql(self):
        """Test $second with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$second": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%S" in sql

    def test_day_of_week_single_value_format_sql(self):
        """Test $dayOfWeek with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfWeek": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%w" in sql

    def test_day_of_year_single_value_format_sql(self):
        """Test $dayOfYear with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfYear": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%j" in sql

    def test_week_single_value_format_sql(self):
        """Test $week with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$week": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%U" in sql

    def test_iso_day_of_week_single_value_format_sql(self):
        """Test $isoDayOfWeek with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$isoDayOfWeek": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None

    def test_iso_week_single_value_format_sql(self):
        """Test $isoWeek with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$isoWeek": "$date"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None

    def test_millisecond_single_value_format_sql(self):
        """Test $millisecond with single-value operand format in SQL tier."""
        evaluator = ExprEvaluator()
        expr = {"$millisecond": "$date"}
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
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 2024

    def test_month_python(self):
        """Test $month Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$month": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 1

    def test_day_of_month_python(self):
        """Test $dayOfMonth Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfMonth": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 15

    def test_hour_python(self):
        """Test $hour Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$hour": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 10

    def test_minute_python(self):
        """Test $minute Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$minute": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 30

    def test_second_python(self):
        """Test $second Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$second": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)},
        )
        assert result == 45

    def test_millisecond_python(self):
        """Test $millisecond Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$millisecond": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {
                "date": datetime(
                    2024, 1, 15, 10, 30, 45, 123000, tzinfo=timezone.utc
                )
            },
        )
        assert result == 123

    def test_day_of_week_python(self):
        """Test $dayOfWeek Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfWeek": ["$date"]}
        # 2024-01-15 is Monday
        # MongoDB: 1=Sunday, 2=Monday, ..., 7=Saturday
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 2

    def test_day_of_year_python(self):
        """Test $dayOfYear Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$dayOfYear": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 15

    def test_week_python(self):
        """Test $week Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$week": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 2  # Week 2 (strftime %U, Sunday as first day of week)

    def test_iso_day_of_week_python(self):
        """Test $isoDayOfWeek Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$isoDayOfWeek": ["$date"]}
        # 2024-01-15 is Monday (isocalendar returns 1 for Monday)
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
        )
        assert result == 1

    def test_iso_week_python(self):
        """Test $isoWeek Python evaluation."""
        evaluator = ExprEvaluator()
        expr = {"$isoWeek": ["$date"]}
        result = evaluator._evaluate_expr_python(
            expr,
            {"date": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)},
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
        import pytest

        evaluator = ExprEvaluator()
        expr = {"$year": ["$date"]}
        # For MongoDB compatibility, string dates raise ValueError
        with pytest.raises(ValueError):
            evaluator._evaluate_expr_python(expr, {"date": "not-a-date"})


class TestDateFromPartsSQL:
    """Test $dateFromParts SQL conversion."""

    def test_basic_sql(self):
        e = ExprEvaluator()
        expr = {"$dateFromParts": {"year": 2024, "month": 6, "day": 15}}
        sql, params = e._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "strftime" in sql
        assert "printf" in sql

    def test_kill_switch(self):
        e = ExprEvaluator()
        expr = {"$dateFromParts": {"year": 2024, "month": 6}}
        assert e.evaluate(expr, force_python=True) == (None, [])

    def test_python_sql_consistency(self):
        e = ExprEvaluator()
        doc = {}
        for expr in [
            {"$dateFromParts": {"year": 2024, "month": 6, "day": 15}},
            {"$dateFromParts": {"year": 2024}},
            {
                "$dateFromParts": {
                    "year": 2024,
                    "month": 12,
                    "day": 31,
                    "hour": 23,
                    "minute": 59,
                    "second": 45,
                }
            },
        ]:
            py = e._evaluate_expr_python(expr, doc)
            sql, params = e._evaluate_sql_tier1(expr)
            assert sql is not None
            with neosqlite.Connection(":memory:") as conn:
                conn.db.execute("CREATE TEMP TABLE t(data TEXT)")
                conn.db.execute("INSERT INTO t VALUES (?)", [json.dumps(doc)])
                row = conn.db.execute(f"SELECT {sql} FROM t", params).fetchone()
                sq = row[0] if row else None
                if sq:
                    sq_dt = datetime.fromisoformat(sq.replace("Z", "+00:00"))
                    assert sq_dt.year == py.year
                    assert sq_dt.month == py.month
                    assert sq_dt.day == py.day
                    assert sq_dt.hour == py.hour
                    assert sq_dt.minute == py.minute
                    assert sq_dt.second == py.second

    def test_integration(self):
        with neosqlite.Connection(":memory:") as conn:
            c = conn["test"]
            c.insert_one({"y": 2024, "m": 6, "d": 15})
            pipeline = [
                {
                    "$project": {
                        "dt": {
                            "$dateFromParts": {
                                "year": "$y",
                                "month": "$m",
                                "day": "$d",
                            }
                        }
                    }
                }
            ]
            results = list(c.aggregate(pipeline))
            assert len(results) == 1
            dt = results[0]["dt"]
            assert dt.year == 2024 and dt.month == 6 and dt.day == 15

    def test_kill_switch_integration(self):
        with neosqlite.Connection(":memory:") as conn:
            c = conn["test"]
            c.insert_one({"y": 2024, "m": 6, "d": 15})
            results = list(
                c.aggregate(
                    [
                        {
                            "$project": {
                                "dt": {
                                    "$dateFromParts": {
                                        "year": "$y",
                                        "month": "$m",
                                        "day": "$d",
                                    }
                                }
                            }
                        }
                    ],
                    force_python=True,
                )
            )
            assert len(results) == 1
            dt = results[0]["dt"]
            assert dt.year == 2024 and dt.month == 6 and dt.day == 15


class TestDateToPartsSQL:
    """Test $dateToParts SQL conversion."""

    def test_basic_sql(self):
        e = ExprEvaluator()
        expr = {"$dateToParts": {"date": "$dt"}}
        sql, params = e._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "strftime" in sql
        assert "json_object" in sql

    def test_kill_switch(self):
        e = ExprEvaluator()
        expr = {"$dateToParts": {"date": "$dt"}}
        assert e.evaluate(expr, force_python=True) == (None, [])

    def test_python_sql_consistency(self):
        e = ExprEvaluator()
        doc = {"dt": datetime(2024, 6, 15, 9, 30, 45, tzinfo=timezone.utc)}
        expr = {"$dateToParts": {"date": "$dt"}}
        py = e._evaluate_expr_python(expr, doc)
        sql, params = e._evaluate_sql_tier1(expr)
        assert sql is not None
        with neosqlite.Connection(":memory:") as conn:
            conn.db.execute("CREATE TEMP TABLE t(data TEXT)")
            conn.db.execute(
                "INSERT INTO t VALUES (?)",
                [json.dumps(doc, default=str)],
            )
            row = conn.db.execute(f"SELECT {sql} FROM t", params).fetchone()
            sq = json.loads(row[0]) if row[0] else None
        assert sq["year"] == py["year"]
        assert sq["month"] == py["month"]
        assert sq["day"] == py["day"]
        assert sq["hour"] == py["hour"]
        assert sq["minute"] == py["minute"]
        assert sq["second"] == py["second"]

    def test_integration(self):
        with neosqlite.Connection(":memory:") as conn:
            c = conn["test"]
            c.insert_one(
                {"dt": datetime(2024, 6, 15, 9, 30, 45, tzinfo=timezone.utc)}
            )
            pipeline = [
                {"$project": {"parts": {"$dateToParts": {"date": "$dt"}}}}
            ]
            results = list(c.aggregate(pipeline))
            assert len(results) == 1
            parts = results[0]["parts"]
            assert parts["year"] == 2024
            assert parts["month"] == 6
            assert parts["day"] == 15

    def test_kill_switch_integration(self):
        with neosqlite.Connection(":memory:") as conn:
            c = conn["test"]
            c.insert_one(
                {"dt": datetime(2024, 6, 15, 9, 30, 45, tzinfo=timezone.utc)}
            )
            results = list(
                c.aggregate(
                    [
                        {
                            "$project": {
                                "parts": {"$dateToParts": {"date": "$dt"}}
                            }
                        }
                    ],
                    force_python=True,
                )
            )
            assert len(results) == 1
            assert results[0]["parts"]["year"] == 2024


class TestDateFromStringSQL:
    """Test $dateFromString SQL conversion."""

    def test_basic_sql(self):
        e = ExprEvaluator()
        expr = {"$dateFromString": {"dateString": "$s"}}
        sql, params = e._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "strftime" in sql

    def test_kill_switch(self):
        e = ExprEvaluator()
        expr = {"$dateFromString": {"dateString": "$s"}}
        assert e.evaluate(expr, force_python=True) == (None, [])

    def test_python_sql_consistency(self):
        e = ExprEvaluator()
        doc = {"s": "2024-06-15T09:30:45Z"}
        expr = {"$dateFromString": {"dateString": "$s"}}
        py = e._evaluate_expr_python(expr, doc)
        sql, params = e._evaluate_sql_tier1(expr)
        assert sql is not None
        with neosqlite.Connection(":memory:") as conn:
            conn.db.execute("CREATE TEMP TABLE t(data TEXT)")
            conn.db.execute("INSERT INTO t VALUES (?)", [json.dumps(doc)])
            row = conn.db.execute(f"SELECT {sql} FROM t", params).fetchone()
            sq = row[0] if row else None
        if sq:
            sq_dt = datetime.fromisoformat(sq.replace("Z", "+00:00"))
            assert sq_dt.year == py.year
            assert sq_dt.month == py.month
            assert sq_dt.day == py.day

    def test_null_handling(self):
        e = ExprEvaluator()
        doc = {}
        expr = {"$dateFromString": {"dateString": "$s"}}
        py = e._evaluate_expr_python(expr, doc)
        sql, params = e._evaluate_sql_tier1(expr)
        assert sql is not None
        with neosqlite.Connection(":memory:") as conn:
            conn.db.execute("CREATE TEMP TABLE t(data TEXT)")
            conn.db.execute("INSERT INTO t VALUES (?)", [json.dumps(doc)])
            row = conn.db.execute(f"SELECT {sql} FROM t", params).fetchone()
            sq = row[0] if row else None
        assert py is None
        assert sq is None

    def test_integration(self):
        with neosqlite.Connection(":memory:") as conn:
            c = conn["test"]
            c.insert_one({"d": "2024-06-15T09:30:45Z"})
            pipeline = [
                {"$project": {"dt": {"$dateFromString": {"dateString": "$d"}}}}
            ]
            results = list(c.aggregate(pipeline))
            assert len(results) == 1
            dt = results[0]["dt"]
            assert dt.year == 2024 and dt.month == 6 and dt.day == 15

    def test_kill_switch_integration(self):
        with neosqlite.Connection(":memory:") as conn:
            c = conn["test"]
            c.insert_one({"d": "2024-06-15"})
            results = list(
                c.aggregate(
                    [
                        {
                            "$project": {
                                "dt": {"$dateFromString": {"dateString": "$d"}}
                            }
                        }
                    ],
                    force_python=True,
                )
            )
            assert len(results) == 1
            assert results[0]["dt"].year == 2024
