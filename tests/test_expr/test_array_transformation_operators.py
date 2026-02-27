"""
Tests for $expr array transformation operators.

Covers: $filter, $map, $reduce
"""

import pytest
import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


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


class TestArrayTransformationIntegration:
    """Integration tests with actual collection queries."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        collection = conn["test_collection"]

        collection.insert_many(
            [
                {"name": "doc1", "numbers": [1, 5, 8, 2, 9, 3]},
                {"name": "doc2", "numbers": [10, 20, 30]},
                {"name": "doc3", "numbers": [2, 4, 6, 8]},
            ]
        )

        yield collection
        conn.close()

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
        assert len(result) == 1
        assert result[0]["name"] == "doc2"
