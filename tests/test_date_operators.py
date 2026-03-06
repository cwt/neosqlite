"""
Tests for date operators in $expr.

Covers: $dateFromString, $dateToString, $dateFromParts, $dateToParts, $dateTrunc, $dateDiff
"""

import neosqlite
from datetime import datetime
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


class TestDateFromStringOperator:
    """Test $dateFromString operator."""

    def test_date_from_string_basic(self):
        """Test $dateFromString with ISO 8601 string."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date_str": "2024-01-15T10:30:00"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "date": {
                                    "$dateFromString": {
                                        "dateString": "$date_str"
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert isinstance(result[0]["date"], datetime)
            assert result[0]["date"].year == 2024
            assert result[0]["date"].month == 1
            assert result[0]["date"].day == 15

    def test_date_from_string_with_on_error(self):
        """Test $dateFromString with invalid string and onError."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date_str": "invalid"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "date": {
                                    "$dateFromString": {
                                        "dateString": "$date_str",
                                        "onError": "Invalid date",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["date"] == "Invalid date"

    def test_date_from_string_with_on_null(self):
        """Test $dateFromString with null and onNull."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date_str": None})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "date": {
                                    "$dateFromString": {
                                        "dateString": "$date_str",
                                        "onNull": "No date",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["date"] == "No date"


class TestDateToStringOperator:
    """Test $dateToString operator."""

    def test_date_to_string_basic(self):
        """Test $dateToString with default format."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 0)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "str": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d",
                                        "date": "$date",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["str"] == "2024-01-15"

    def test_date_to_string_with_time(self):
        """Test $dateToString with time format."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "str": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d %H:%M:%S",
                                        "date": "$date",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["str"] == "2024-01-15 10:30:45"


class TestDateFromPartsOperator:
    """Test $dateFromParts operator."""

    def test_date_from_parts_basic(self):
        """Test $dateFromParts with year, month, day."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})  # Need a document to project

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "date": {
                                    "$dateFromParts": {
                                        "year": 2024,
                                        "month": 1,
                                        "day": 15,
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert isinstance(result[0]["date"], datetime)
            assert result[0]["date"].year == 2024
            assert result[0]["date"].month == 1
            assert result[0]["date"].day == 15

    def test_date_from_parts_with_time(self):
        """Test $dateFromParts with hour, minute, second."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})  # Need a document to project

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "date": {
                                    "$dateFromParts": {
                                        "year": 2024,
                                        "month": 1,
                                        "day": 15,
                                        "hour": 10,
                                        "minute": 30,
                                        "second": 45,
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["date"].hour == 10
            assert result[0]["date"].minute == 30
            assert result[0]["date"].second == 45


class TestDateToPartsOperator:
    """Test $dateToParts operator."""

    def test_date_to_parts_basic(self):
        """Test $dateToParts returns all parts."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45, 123000)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "parts": {"$dateToParts": {"date": "$date"}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            parts = result[0]["parts"]
            assert parts["year"] == 2024
            assert parts["month"] == 1
            assert parts["day"] == 15
            assert parts["hour"] == 10
            assert parts["minute"] == 30
            assert parts["second"] == 45
            assert parts["millisecond"] == 123

    def test_date_to_parts_with_unit(self):
        """Test $dateToParts with unit parameter."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "parts": {
                                    "$dateToParts": {
                                        "date": "$date",
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            parts = result[0]["parts"]
            assert "year" in parts
            assert "month" in parts
            assert "day" in parts
            assert "hour" not in parts


class TestDateTruncOperator:
    """Test $dateTrunc operator."""

    def test_date_trunc_to_day(self):
        """Test $dateTrunc to day."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "truncated": {
                                    "$dateTrunc": {
                                        "date": "$date",
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["truncated"] == datetime(2024, 1, 15, 0, 0, 0)

    def test_date_trunc_to_month(self):
        """Test $dateTrunc to month."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "truncated": {
                                    "$dateTrunc": {
                                        "date": "$date",
                                        "unit": "month",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["truncated"] == datetime(2024, 1, 1, 0, 0, 0)

    def test_date_trunc_to_year(self):
        """Test $dateTrunc to year."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "truncated": {
                                    "$dateTrunc": {
                                        "date": "$date",
                                        "unit": "year",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["truncated"] == datetime(2024, 1, 1, 0, 0, 0)

    def test_date_trunc_to_week(self):
        """Test $dateTrunc to week (Monday)."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            # 2024-01-15 is a Monday
            coll.insert_one(
                {"date": datetime(2024, 1, 17, 10, 30, 45)}
            )  # Wednesday

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "truncated": {
                                    "$dateTrunc": {
                                        "date": "$date",
                                        "unit": "week",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Should truncate to Monday 2024-01-15
            assert result[0]["truncated"] == datetime(2024, 1, 15, 0, 0, 0)

    def test_date_trunc_to_hour(self):
        """Test $dateTrunc to hour."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "truncated": {
                                    "$dateTrunc": {
                                        "date": "$date",
                                        "unit": "hour",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["truncated"] == datetime(2024, 1, 15, 10, 0, 0)


class TestDateDiffOperator:
    """Test $dateDiff operator."""

    def test_date_diff_days(self):
        """Test $dateDiff in days."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})  # Need a document to project

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "diff": {
                                    "$dateDiff": {
                                        "startDate": datetime(2024, 1, 1),
                                        "endDate": datetime(2024, 1, 15),
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["diff"] == 14

    def test_date_diff_months(self):
        """Test $dateDiff in months."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})  # Need a document to project

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "diff": {
                                    "$dateDiff": {
                                        "startDate": datetime(2024, 1, 1),
                                        "endDate": datetime(2024, 6, 1),
                                        "unit": "month",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["diff"] == 5

    def test_date_diff_years(self):
        """Test $dateDiff in years."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})  # Need a document to project

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "diff": {
                                    "$dateDiff": {
                                        "startDate": datetime(2020, 1, 1),
                                        "endDate": datetime(2024, 1, 1),
                                        "unit": "year",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["diff"] == 4


class TestDateOperatorsKillSwitch:
    """Test date operators with kill switch."""

    def test_date_operators_with_kill_switch(self):
        """Test date operators work with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"date": datetime(2024, 1, 15, 10, 30, 45)})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "to_str": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d",
                                            "date": "$date",
                                        }
                                    },
                                    "truncated": {
                                        "$dateTrunc": {
                                            "date": "$date",
                                            "unit": "day",
                                        }
                                    },
                                    "parts": {
                                        "$dateToParts": {"date": "$date"}
                                    },
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["to_str"] == "2024-01-15"
                assert result[0]["truncated"] == datetime(2024, 1, 15, 0, 0, 0)
                assert result[0]["parts"]["year"] == 2024
            finally:
                set_force_fallback(original_state)
