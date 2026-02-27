"""
Tests for $expr type conversion operators.

Covers: $toLong, $toDecimal, $toObjectId, $toBinData, $toRegex, $convert
"""

import pytest
import re
import neosqlite
from neosqlite.objectid import ObjectId
from neosqlite.binary import Binary
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.query_helper import set_force_fallback


class TestTypeConversionOperators:
    """Test type conversion operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_types
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "num": 42,
                    "str": "123",
                    "hex": "507f1f77bcf86cd799439011",
                },
                {"_id": 2, "num": 3.14, "str": "true", "data": b"binary"},
            ]
        )
        yield coll
        conn.close()

    def test_toLong(self, collection):
        """Test $toLong operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find({"$expr": {"$eq": [{"$toLong": "$num"}, 42]}})
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)

    def test_toDecimal(self, collection):
        """Test $toDecimal operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {"$expr": {"$gt": [{"$toDecimal": "$num"}, 40]}}
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)

    def test_toObjectId(self, collection):
        """Test $toObjectId operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$toObjectId": "$hex"},
                                ObjectId("507f1f77bcf86cd799439011"),
                            ]
                        }
                    }
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)

    def test_toBinData(self, collection):
        """Test $toBinData operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$type": {"$toBinData": "$data"}},
                                "unknown",
                            ]
                        }
                    }
                )
            )
            assert len(result) >= 1
        finally:
            set_force_fallback(False)

    def test_convert(self, collection):
        """Test $convert operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$convert": {"input": "$str", "to": "int"}},
                                123,
                            ]
                        }
                    }
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)


class TestConvertOperatorEdgeCases:
    """Test $convert operator edge cases."""

    def test_convert_objectId(self):
        """Test $convert with objectId type."""
        evaluator = ExprEvaluator()
        expr = {
            "$convert": {
                "input": "507f1f77bcf86cd799439011",
                "to": "objectId",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is not None
        assert isinstance(result, ObjectId)
        assert str(result) == "507f1f77bcf86cd799439011"

    def test_convert_objectId_null_input(self):
        """Test $convert with objectId and null input."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": None, "to": "objectId"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is None

    def test_convert_objectid_with_on_error(self):
        """Test $convert with objectId and on_error handler."""
        evaluator = ExprEvaluator()
        expr = {
            "$convert": {
                "input": "invalid_hex_string",
                "to": "objectId",
                "onError": "conversion_failed",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == "conversion_failed"

    def test_convert_objectid_with_on_null(self):
        """Test $convert with objectId and on_null handler."""
        evaluator = ExprEvaluator()
        expr = {
            "$convert": {
                "input": None,
                "to": "objectId",
                "onNull": "value_was_null",
            }
        }
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == "value_was_null"

    def test_convert_binData(self):
        """Test $convert with binData type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "hello", "to": "binData"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is not None
        assert isinstance(result, Binary)

    def test_convert_bsonBinData(self):
        """Test $convert with bsonBinData type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "world", "to": "bsonBinData"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is not None
        assert isinstance(result, Binary)

    def test_convert_regex(self):
        """Test $convert with regex type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": r"\d+", "to": "regex"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is not None
        assert isinstance(result, re.Pattern)

    def test_convert_bsonRegex(self):
        """Test $convert with bsonRegex type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": r"hello", "to": "bsonRegex"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is not None
        assert isinstance(result, re.Pattern)

    def test_convert_null_type(self):
        """Test $convert with null type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "anything", "to": "null"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is None

    def test_convert_date_type(self):
        """Test $convert with date type (returns as-is)."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "2024-01-01", "to": "date"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == "2024-01-01"

    def test_convert_unknown_type(self):
        """Test $convert with unknown type returns input."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "test", "to": "unknownType"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == "test"

    def test_convert_int(self):
        """Test $convert with int type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "42", "to": "int"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == 42

    def test_convert_double(self):
        """Test $convert with double type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "3.14", "to": "double"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == 3.14

    def test_convert_bool(self):
        """Test $convert with bool type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": "truthy", "to": "bool"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is True

    def test_convert_string(self):
        """Test $convert with string type."""
        evaluator = ExprEvaluator()
        expr = {"$convert": {"input": 123, "to": "string"}}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == "123"
