"""
Tests for $expr string operators.

Basic: $concat, $toLower, $toUpper, $strLenBytes, $substr, $trim
Extended: $ltrim, $rtrim, $indexOfBytes, $regexMatch, $split, $replaceAll
Advanced: $strLenCP, $substrCP, $indexOfCP, $replaceOne
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.query_helper import set_force_fallback


class TestStringOperatorsPython:
    """Test string operators Python evaluation."""

    def test_concat_operator(self):
        """Test $concat operator."""
        evaluator = ExprEvaluator()
        expr = {"$concat": ["$firstName", " ", "$lastName"]}
        result = evaluator._evaluate_expr_python(
            expr, {"firstName": "John", "lastName": "Doe"}
        )
        assert result == "John Doe"

    def test_toLower_operator(self):
        """Test $toLower operator."""
        evaluator = ExprEvaluator()
        expr = {"$toLower": ["$text"]}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "HELLO"}) == "hello"
        )

    def test_toUpper_operator(self):
        """Test $toUpper operator."""
        evaluator = ExprEvaluator()
        expr = {"$toUpper": ["$text"]}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "hello"}) == "HELLO"
        )

    def test_strLenBytes_operator(self):
        """Test $strLenBytes operator."""
        evaluator = ExprEvaluator()
        expr = {"$strLenBytes": ["$text"]}
        assert evaluator._evaluate_expr_python(expr, {"text": "hello"}) == 5

    def test_substr_operator(self):
        """Test $substr operator."""
        evaluator = ExprEvaluator()
        expr = {"$substr": ["$text", 0, 3]}
        assert evaluator._evaluate_expr_python(expr, {"text": "hello"}) == "hel"

    def test_trim_operator(self):
        """Test $trim operator."""
        evaluator = ExprEvaluator()
        expr = {"$trim": {"input": "$text"}}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "  hello  "})
            == "hello"
        )

    def test_ltrim_operator(self):
        """Test $ltrim operator."""
        evaluator = ExprEvaluator()
        expr = {"$ltrim": {"input": "$text"}}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "  hello  "})
            == "hello  "
        )

    def test_ltrim_with_chars(self):
        """Test $ltrim with chars."""
        evaluator = ExprEvaluator()
        expr = {"$ltrim": {"input": "$text", "chars": "xyz"}}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "xyzhelloxyz"})
            == "helloxyz"
        )

    def test_rtrim_operator(self):
        """Test $rtrim operator."""
        evaluator = ExprEvaluator()
        expr = {"$rtrim": {"input": "$text"}}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "  hello  "})
            == "  hello"
        )

    def test_rtrim_with_chars(self):
        """Test $rtrim with chars."""
        evaluator = ExprEvaluator()
        expr = {"$rtrim": {"input": "$text", "chars": "xyz"}}
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "xyzhelloxyz"})
            == "xyzhello"
        )

    def test_indexOfBytes_operator(self):
        """Test $indexOfBytes operator."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfBytes": ["world", "hello world"]}
        assert evaluator._evaluate_expr_python(expr, {}) == 6

    def test_indexOfBytes_not_found(self):
        """Test $indexOfBytes not found."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfBytes": ["xyz", "hello world"]}
        assert evaluator._evaluate_expr_python(expr, {}) == -1

    def test_regexMatch_operator(self):
        """Test $regexMatch operator."""
        evaluator = ExprEvaluator()
        expr = {"$regexMatch": {"input": "$text", "regex": "^[A-Z]"}}
        assert evaluator._evaluate_expr_python(expr, {"text": "Hello"}) is True

    def test_regexMatch_no_match(self):
        """Test $regexMatch no match."""
        evaluator = ExprEvaluator()
        expr = {"$regexMatch": {"input": "$text", "regex": "^[0-9]"}}
        assert evaluator._evaluate_expr_python(expr, {"text": "Hello"}) is False

    def test_split_operator(self):
        """Test $split operator."""
        evaluator = ExprEvaluator()
        expr = {"$split": ["$text", ","]}
        assert evaluator._evaluate_expr_python(expr, {"text": "a,b,c"}) == [
            "a",
            "b",
            "c",
        ]

    def test_replaceAll_operator(self):
        """Test $replaceAll operator."""
        evaluator = ExprEvaluator()
        expr = {"$replaceAll": ["$text", "old", "new"]}
        result = evaluator._evaluate_expr_python(expr, {"text": "old text old"})
        assert result == "new text new"

    def test_strLenCP_operator(self):
        """Test $strLenCP operator."""
        evaluator = ExprEvaluator()
        expr = {"$strLenCP": ["$text"]}
        # For ASCII, code points = bytes
        assert evaluator._evaluate_expr_python(expr, {"text": "hello"}) == 5
        # For Unicode, code points may differ from bytes
        assert evaluator._evaluate_expr_python(expr, {"text": "你好"}) == 2

    def test_indexOfCP_operator(self):
        """Test $indexOfCP operator."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfCP": ["$search", "$text"]}
        result = evaluator._evaluate_expr_python(
            expr, {"text": "hello world", "search": "world"}
        )
        assert result == 6

    def test_replaceOne_operator(self):
        """Test $replaceOne operator."""
        evaluator = ExprEvaluator()
        expr = {"$replaceOne": ["$text", "foo", "bar"]}
        result = evaluator._evaluate_expr_python(expr, {"text": "foo bar foo"})
        # Only first occurrence replaced
        assert result == "bar bar foo"

    def test_replaceOne_no_match(self):
        """Test $replaceOne with no match."""
        evaluator = ExprEvaluator()
        expr = {"$replaceOne": ["$text", "xyz", "abc"]}
        result = evaluator._evaluate_expr_python(expr, {"text": "hello world"})
        assert result == "hello world"


class TestStringIntegration:
    """Integration tests for string operators."""

    def test_strLenCP_integration(self):
        """Test $strLenCP with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_one({"text": "hello world"})

            set_force_fallback(True)
            try:
                expr = {"$expr": {"$eq": [{"$strLenCP": "$text"}, 11]}}
                results = list(collection.find(expr))
                assert len(results) == 1
            finally:
                set_force_fallback(False)

    def test_replaceOne_integration(self):
        """Test $replaceOne with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_one({"text": "foo bar foo"})

            set_force_fallback(True)
            try:
                expr = {
                    "$expr": {
                        "$eq": [
                            {"$replaceOne": ["$text", "foo", "bar"]},
                            "bar bar foo",
                        ]
                    }
                }
                results = list(collection.find(expr))
                assert len(results) == 1
            finally:
                set_force_fallback(False)

    def test_concat_integration(self):
        """Test $concat with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"firstName": "John", "lastName": "Doe"},
                    {"firstName": "Jane", "lastName": "Smith"},
                ]
            )

            expr = {
                "$expr": {
                    "$eq": [
                        {"$concat": ["$firstName", " ", "$lastName"]},
                        "John Doe",
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["firstName"] == "John"

    def test_toLower_integration(self):
        """Test $toLower with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"text": "HELLO"},
                    {"text": "World"},
                ]
            )

            expr = {"$expr": {"$eq": [{"$toLower": ["$text"]}, "hello"]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["text"] == "HELLO"


class TestStringOperatorsSQL:
    """Test string operators SQL conversion."""

    def test_concat_sql(self):
        """Test $concat SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$concat": ["$firstName", " ", "$lastName"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "||" in sql
        assert params == [" "]

    def test_toLower_sql(self):
        """Test $toLower SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$toLower": ["$text"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "lower(" in sql

    def test_toUpper_sql(self):
        """Test $toUpper SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$toUpper": ["$text"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "upper(" in sql

    def test_strLenBytes_sql(self):
        """Test $strLenBytes SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$strLenBytes": ["$text"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "length(" in sql

    def test_substr_sql(self):
        """Test $substr SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$substr": ["$text", 0, 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "substr(" in sql
        assert params == [0, 5]

    def test_trim_sql(self):
        """Test $trim SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$trim": {"input": "$text"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "trim(" in sql
        assert params == []

    def test_ltrim_sql(self):
        """Test $ltrim SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$ltrim": {"input": "$text"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "ltrim" in sql

    def test_rtrim_sql(self):
        """Test $rtrim SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$rtrim": {"input": "$text"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "rtrim" in sql

    def test_indexOfBytes_sql(self):
        """Test $indexOfBytes SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$indexOfBytes": ["world", "hello world"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "instr" in sql

    def test_regexMatch_sql(self):
        """Test $regexMatch SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$regexMatch": {"input": "$text", "regex": "^[A-Z]"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "REGEXP" in sql

    def test_replaceAll_sql(self):
        """Test $replaceAll SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$replaceAll": ["$text", "old", "new"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "replace" in sql
