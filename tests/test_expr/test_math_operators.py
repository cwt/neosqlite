"""
Tests for $expr advanced math operators.

Covers: $log, $log10, $exp
"""

import pytest
import math
import warnings
import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.query_helper import set_force_fallback


class TestAdvancedMathOperators:
    """Test advanced math operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_math
        coll.insert_many(
            [
                {"_id": 1, "value": 10},
                {"_id": 2, "value": 100},
                {"_id": 3, "value": 1},
            ]
        )
        yield coll
        conn.close()

    def test_ln(self, collection):
        """Test $ln (natural logarithm) operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gt": [{"$ln": "$value"}, 2]},
                                {"$lt": [{"$ln": "$value"}, 3]},
                            ]
                        }
                    }
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)

    def test_log_custom_base(self, collection):
        """Test $log with custom base via $expr."""
        set_force_fallback(True)
        try:
            # log base 10 of 100 = 2
            result = list(
                collection.find(
                    {"$expr": {"$eq": [{"$log": ["$value", 10]}, 2]}}
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 2
        finally:
            set_force_fallback(False)

    def test_log10(self, collection):
        """Test $log10 operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find({"$expr": {"$eq": [{"$log10": "$value"}, 2]}})
            )
            assert len(result) == 1
            assert result[0]["_id"] == 2
        finally:
            set_force_fallback(False)

    def test_exp(self, collection):
        """Test $exp operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gt": [{"$exp": "$value"}, 2.5]},
                                {"$lt": [{"$exp": "$value"}, 3]},
                            ]
                        }
                    }
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 3
        finally:
            set_force_fallback(False)


class TestAdvancedMathEdgeCases:
    """Test edge cases for advanced math operators."""

    def test_ln_zero(self):
        """Test $ln with zero returns None."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0})
        assert result is None

    def test_ln_negative(self):
        """Test $ln with negative number returns None."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": -5})
        assert result is None

    def test_ln_null(self):
        """Test $ln with null returns None."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": None})
        assert result is None

    def test_log10_zero(self):
        """Test $log10 with zero returns None."""
        evaluator = ExprEvaluator()
        expr = {"$log10": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0})
        assert result is None

    def test_log10_negative(self):
        """Test $log10 with negative number returns None."""
        evaluator = ExprEvaluator()
        expr = {"$log10": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": -100})
        assert result is None

    def test_exp_zero(self):
        """Test $exp with zero returns 1."""
        evaluator = ExprEvaluator()
        expr = {"$exp": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0})
        assert result == 1.0

    def test_exp_null(self):
        """Test $exp with null returns None."""
        evaluator = ExprEvaluator()
        expr = {"$exp": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": None})
        assert result is None

    def test_ln_e(self):
        """Test $ln(e) returns approximately 1."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": math.e})
        assert result is not None
        assert abs(result - 1.0) < 0.0001

    def test_log10_power_of_10(self):
        """Test $log10 with power of 10."""
        evaluator = ExprEvaluator()
        expr = {"$log10": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 1000})
        assert result == 3.0


class TestLogOperators:
    """Test all logarithm operators for MongoDB compatibility."""

    def test_ln_operator(self):
        """Test $ln (natural log) operator."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": math.e})
        assert result is not None
        assert abs(result - 1.0) < 0.0001

    def test_ln_zero(self):
        """Test $ln with zero returns None."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0})
        assert result is None

    def test_ln_negative(self):
        """Test $ln with negative number returns None."""
        evaluator = ExprEvaluator()
        expr = {"$ln": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": -5})
        assert result is None

    def test_log_custom_base(self):
        """Test $log with custom base."""
        evaluator = ExprEvaluator()
        expr = {"$log": [100, 10]}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == 2.0

    def test_log_custom_base_e(self):
        """Test $log with base e (should equal $ln)."""
        evaluator = ExprEvaluator()
        expr = {"$log": [10, math.e]}
        result_ln = evaluator._evaluate_expr_python({"$ln": 10}, {})
        result_log = evaluator._evaluate_expr_python(expr, {})
        assert abs(result_log - result_ln) < 0.0001

    def test_log_custom_base_invalid(self):
        """Test $log with invalid base (<=1)."""
        evaluator = ExprEvaluator()
        expr = {"$log": [100, 1]}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is None

    def test_log_custom_base_number_negative(self):
        """Test $log with negative number."""
        evaluator = ExprEvaluator()
        expr = {"$log": [-10, 10]}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result is None

    def test_log2_operator(self):
        """Test $log2 (base-2 log) operator."""
        evaluator = ExprEvaluator()
        expr = {"$log2": "$value"}
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = evaluator._evaluate_expr_python(expr, {"value": 8})
            assert result == 3.0
            # Verify warning was issued
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "$log2" in str(w[0].message)
            assert "NeoSQLite extension" in str(w[0].message)

    def test_log2_zero(self):
        """Test $log2 with zero returns None."""
        evaluator = ExprEvaluator()
        expr = {"$log2": "$value"}
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = evaluator._evaluate_expr_python(expr, {"value": 0})
            assert result is None
            # Verify warning was issued
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)

    def test_log2_power_of_2(self):
        """Test $log2 with various powers of 2."""
        evaluator = ExprEvaluator()
        test_cases = [(2, 1), (4, 2), (8, 3), (16, 4), (1024, 10)]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            for value, expected in test_cases:
                expr = {"$log2": value}
                result = evaluator._evaluate_expr_python(expr, {})
                assert result == expected
            # Verify warning was issued only once (per evaluator instance)
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
