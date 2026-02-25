"""
Tests for high-priority $expr operators.

Covers:
- Date Arithmetic: $dateAdd, $dateSubtract, $dateDiff
- Regex Operations: $regexFind, $regexFindAll
- Array Transformation: $filter, $map, $reduce
"""

import pytest
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


class TestRegexOperationsPython:
    """Test regex operations Python evaluation."""

    def test_regex_find_basic_python(self):
        """Test $regexFind basic match."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFind": {
                "input": "$text",
                "regex": "hello",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result is not None
        assert result["match"] == "hello"
        assert result["index"] == 0

    def test_regex_find_with_index_python(self):
        """Test $regexFind with index."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFind": {
                "input": "$text",
                "regex": "world",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result is not None
        assert result["match"] == "world"
        assert result["index"] == 6

    def test_regex_find_no_match_python(self):
        """Test $regexFind with no match."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFind": {
                "input": "$text",
                "regex": "foo",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result is None

    def test_regex_find_case_insensitive_python(self):
        """Test $regexFind with case insensitive option."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFind": {
                "input": "$text",
                "regex": "HELLO",
                "options": "i",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result is not None
        assert result["match"] == "hello"

    def test_regex_find_with_captures_python(self):
        """Test $regexFind with capture groups."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFind": {
                "input": "$text",
                "regex": r"(\w+) (\w+)",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result is not None
        assert result["match"] == "hello world"
        assert "captures" in result
        assert result["captures"] == ["hello", "world"]

    def test_regex_find_all_python(self):
        """Test $regexFindAll basic."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFindAll": {
                "input": "$text",
                "regex": "o",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert isinstance(result, list)
        assert len(result) == 2  # 2 'o's in "hello world"

    def test_regex_find_all_with_matches_python(self):
        """Test $regexFindAll with multiple matches."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFindAll": {
                "input": "$text",
                "regex": r"\w+",
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"text": "hello world foo"}
        )
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["match"] == "hello"
        assert result[1]["match"] == "world"
        assert result[2]["match"] == "foo"

    def test_regex_find_all_no_match_python(self):
        """Test $regexFindAll with no matches."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFindAll": {
                "input": "$text",
                "regex": "xyz",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result == []

    def test_regex_find_null_input_python(self):
        """Test $regexFind with null input."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFind": {
                "input": "$text",
                "regex": "hello",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": None})
        assert result is None

    def test_regex_find_all_case_insensitive_python(self):
        """Test $regexFindAll with case insensitive option."""
        evaluator = ExprEvaluator()
        expr = {
            "$regexFindAll": {
                "input": "$text",
                "regex": "O",
                "options": "i",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert len(result) == 2  # 2 'o's in "hello world"


class TestArrayTransformationPython:
    """Test array transformation operators Python evaluation."""

    def test_filter_basic_python(self):
        """Test $filter basic."""
        evaluator = ExprEvaluator()
        expr = {
            "$filter": {
                "input": "$numbers",
                "as": "n",
                "cond": {"$gt": ["$$n", 5]},
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"numbers": [1, 5, 8, 2, 9, 3]}
        )
        assert result == [8, 9]

    def test_filter_with_index_python(self):
        """Test $filter with index."""
        evaluator = ExprEvaluator()
        expr = {
            "$filter": {
                "input": "$numbers",
                "as": "n",
                "cond": {
                    "$and": [
                        {"$gt": ["$$n", 5]},
                        {"$gt": ["$$nIndex", 1]},
                    ]
                },
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"numbers": [1, 5, 8, 2, 9, 3]}
        )
        assert result == [8, 9]

    def test_filter_empty_array_python(self):
        """Test $filter with empty array."""
        evaluator = ExprEvaluator()
        expr = {
            "$filter": {
                "input": "$numbers",
                "as": "n",
                "cond": {"$gt": ["$$n", 5]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"numbers": []})
        assert result == []

    def test_filter_no_matches_python(self):
        """Test $filter with no matches."""
        evaluator = ExprEvaluator()
        expr = {
            "$filter": {
                "input": "$numbers",
                "as": "n",
                "cond": {"$gt": ["$$n", 100]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"numbers": [1, 2, 3]})
        assert result == []

    def test_map_basic_python(self):
        """Test $map basic."""
        evaluator = ExprEvaluator()
        expr = {
            "$map": {
                "input": "$numbers",
                "as": "n",
                "in": {"$multiply": ["$$n", 2]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"numbers": [1, 2, 3]})
        assert result == [2, 4, 6]

    def test_map_with_index_python(self):
        """Test $map with index."""
        evaluator = ExprEvaluator()
        expr = {
            "$map": {
                "input": "$numbers",
                "as": "n",
                "in": {"$add": ["$$n", "$$nIndex"]},
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"numbers": [10, 20, 30]}
        )
        assert result == [10, 21, 32]

    def test_map_empty_array_python(self):
        """Test $map with empty array."""
        evaluator = ExprEvaluator()
        expr = {
            "$map": {
                "input": "$numbers",
                "as": "n",
                "in": {"$multiply": ["$$n", 2]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"numbers": []})
        assert result == []

    def test_reduce_basic_python(self):
        """Test $reduce basic sum."""
        evaluator = ExprEvaluator()
        expr = {
            "$reduce": {
                "input": "$numbers",
                "initialValue": 0,
                "in": {"$add": ["$$value", "$$this"]},
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"numbers": [1, 2, 3, 4]}
        )
        assert result == 10

    def test_reduce_multiply_python(self):
        """Test $reduce multiply."""
        evaluator = ExprEvaluator()
        expr = {
            "$reduce": {
                "input": "$numbers",
                "initialValue": 1,
                "in": {"$multiply": ["$$value", "$$this"]},
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"numbers": [1, 2, 3, 4]}
        )
        assert result == 24

    def test_reduce_with_index_python(self):
        """Test $reduce with index."""
        evaluator = ExprEvaluator()
        expr = {
            "$reduce": {
                "input": "$numbers",
                "initialValue": 0,
                "in": {"$add": ["$$value", "$$this", "$$index"]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"numbers": [1, 2, 3]})
        # 0 + 1 + 0 = 1, 1 + 2 + 1 = 4, 4 + 3 + 2 = 9
        assert result == 9

    def test_reduce_empty_array_python(self):
        """Test $reduce with empty array."""
        evaluator = ExprEvaluator()
        expr = {
            "$reduce": {
                "input": "$numbers",
                "initialValue": 42,
                "in": {"$add": ["$$value", "$$this"]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"numbers": []})
        assert result == 42

    def test_filter_non_array_input_python(self):
        """Test $filter with non-array input."""
        evaluator = ExprEvaluator()
        expr = {
            "$filter": {
                "input": "$notArray",
                "as": "item",
                "cond": {"$eq": ["$$item", 1]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"notArray": "string"})
        assert result == []

    def test_map_non_array_input_python(self):
        """Test $map with non-array input."""
        evaluator = ExprEvaluator()
        expr = {
            "$map": {
                "input": "$notArray",
                "as": "item",
                "in": "$$item",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"notArray": None})
        assert result == []

    def test_reduce_non_array_input_python(self):
        """Test $reduce with non-array input."""
        evaluator = ExprEvaluator()
        expr = {
            "$reduce": {
                "input": "$notArray",
                "initialValue": 0,
                "in": {"$add": ["$$value", "$$this"]},
            }
        }
        result = evaluator._evaluate_expr_python(expr, {"notArray": "string"})
        assert result is None

    def test_filter_nested_expression_python(self):
        """Test $filter with nested expression."""
        evaluator = ExprEvaluator()
        expr = {
            "$filter": {
                "input": "$items",
                "as": "item",
                "cond": {
                    "$and": [
                        {"$gt": ["$$item", 5]},
                        {"$lt": ["$$item", 15]},
                    ]
                },
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"items": [1, 5, 8, 12, 20, 3]}
        )
        assert result == [8, 12]

    def test_map_nested_expression_python(self):
        """Test $map with nested expression."""
        evaluator = ExprEvaluator()
        expr = {
            "$map": {
                "input": "$numbers",
                "as": "n",
                "in": {
                    "$cond": {
                        "if": {"$gt": ["$$n", 5]},
                        "then": {"$multiply": ["$$n", 2]},
                        "else": "$$n",
                    }
                },
            }
        }
        result = evaluator._evaluate_expr_python(
            expr, {"numbers": [3, 6, 2, 8]}
        )
        assert result == [3, 12, 2, 16]


class TestIntegrationWithQuery:
    """Integration tests with actual collection queries."""

    @pytest.fixture
    def collection(self, tmp_path):
        """Create a test collection."""
        import neosqlite

        conn = neosqlite.Connection(":memory:")
        collection = conn["test_collection"]

        # Insert test documents
        collection.insert_many(
            [
                {
                    "name": "doc1",
                    "date": "2024-01-15T10:30:00",
                    "numbers": [1, 5, 8, 2, 9, 3],
                },
                {
                    "name": "doc2",
                    "date": "2024-02-20T15:45:00",
                    "numbers": [10, 20, 30],
                },
                {
                    "name": "doc3",
                    "date": "2024-03-25T20:00:00",
                    "numbers": [2, 4, 6, 8],
                },
                {"name": "doc4", "text": "hello world", "numbers": [1, 2, 3]},
            ]
        )

        yield collection
        conn.close()

    def test_date_add_in_query(self, collection):
        """Test $dateAdd in actual query."""
        # All documents have dates in 2024, adding 1 year makes them 2025
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
        assert len(result) == 3  # doc1, doc2, doc3 all have 2024 dates
        # Verify doc1 is in the results
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
        # doc2 and doc3 should have more than 200 days until end of 2024
        assert len(result) >= 1

    def test_filter_in_query(self, collection):
        """Test $filter in actual query."""
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$gt": [
                            {
                                "$size": [
                                    {
                                        "$filter": {
                                            "input": "$numbers",
                                            "as": "n",
                                            "cond": {"$gt": ["$$n", 5]},
                                        }
                                    }
                                ]
                            },
                            1,
                        ]
                    }
                }
            )
        )
        # doc1 has [1, 5, 8, 2, 9, 3] -> filtered: [8, 9] (size 2)
        # doc2 has [10, 20, 30] -> filtered: [10, 20, 30] (size 3)
        # doc3 has [2, 4, 6, 8] -> filtered: [6, 8] (size 2)
        assert len(result) == 3

    def test_map_in_query(self, collection):
        """Test $map in actual query."""
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$in": [
                            20,
                            {
                                "$map": {
                                    "input": "$numbers",
                                    "as": "n",
                                    "in": {"$multiply": ["$$n", 2]},
                                }
                            },
                        ]
                    }
                }
            )
        )
        # doc2 has [10, 20, 30] -> mapped: [20, 40, 60]
        assert len(result) == 1
        assert result[0]["name"] == "doc2"

    def test_regex_find_in_query(self, collection):
        """Test $regexFind in actual query."""
        # Note: $regexFind returns dict, so we need to check if it's not null
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$ne": [
                            {
                                "$regexFind": {
                                    "input": "$text",
                                    "regex": "hello",
                                }
                            },
                            None,
                        ]
                    }
                }
            )
        )
        assert len(result) == 1
        assert result[0]["name"] == "doc4"
