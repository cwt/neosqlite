"""
Tests for $expr regex operations.

Covers: $regexFind, $regexFindAll
"""

import pytest
import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


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
        assert len(result) == 2

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
        assert len(result) == 2


class TestRegexIntegration:
    """Integration tests with actual collection queries."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        collection = conn["test_collection"]

        collection.insert_many(
            [
                {"name": "doc1", "text": "hello world"},
                {"name": "doc2", "text": "foo bar"},
                {"name": "doc3", "text": "HELLO AGAIN"},
            ]
        )

        yield collection
        conn.close()

    def test_regex_find_in_query(self, collection):
        """Test $regexFind in actual query."""
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
        assert result[0]["name"] == "doc1"

    def test_regex_find_case_insensitive_in_query(self, collection):
        """Test $regexFind case insensitive in actual query."""
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$ne": [
                            {
                                "$regexFind": {
                                    "input": "$text",
                                    "regex": "hello",
                                    "options": "i",
                                }
                            },
                            None,
                        ]
                    }
                }
            )
        )
        assert len(result) == 2
