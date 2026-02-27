"""
Tests for $expr trigonometric and angle conversion operators.

Covers: $sin, $cos, $tan, $asin, $acos, $atan, $atan2, $degreesToRadians, $radiansToDegrees
"""

import pytest
import math
import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


class TestTrigonometricOperators:
    """Test trigonometric operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with angle data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_trig
        coll.insert_many(
            [
                {"_id": 1, "angle": 0},
                {"_id": 2, "angle": math.pi / 2},
                {"_id": 3, "angle": math.pi},
                {"_id": 4, "angle": math.pi / 4},
            ]
        )
        yield coll
        conn.close()

    def test_sin(self, collection):
        """Test $sin operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find({"$expr": {"$gt": [{"$sin": "$angle"}, 0.9]}})
            )
            assert len(result) == 1
            assert result[0]["_id"] == 2
        finally:
            set_force_fallback(False)

    def test_cos(self, collection):
        """Test $cos operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find({"$expr": {"$gt": [{"$cos": "$angle"}, 0.9]}})
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)

    def test_tan(self, collection):
        """Test $tan operator via $expr."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gt": [{"$tan": "$angle"}, 0.9]},
                                {"$lt": [{"$tan": "$angle"}, 1.1]},
                            ]
                        }
                    }
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 4
        finally:
            set_force_fallback(False)

    def test_atan2(self, collection):
        """Test $atan2 operator via $expr."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_atan2
        coll.insert_one({"_id": 1, "x": 1.0, "y": 1.0})

        set_force_fallback(True)
        try:
            result = list(
                coll.find({"$expr": {"$gt": [{"$atan2": ["$y", "$x"]}, 0.7]}})
            )
            assert len(result) > 0
        finally:
            set_force_fallback(False)
        conn.close()


class TestAngleConversion:
    """Test angle conversion operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_angles
        coll.insert_many(
            [
                {"_id": 1, "degrees": 180, "radians": math.pi},
                {"_id": 2, "degrees": 90, "radians": math.pi / 2},
                {"_id": 3, "degrees": 360, "radians": 2 * math.pi},
            ]
        )
        yield coll
        conn.close()

    def test_degreesToRadians(self, collection):
        """Test $degreesToRadians operator."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {
                        "$expr": {
                            "$eq": [{"$degreesToRadians": "$degrees"}, math.pi]
                        }
                    }
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)

    def test_radiansToDegrees(self, collection):
        """Test $radiansToDegrees operator."""
        set_force_fallback(True)
        try:
            result = list(
                collection.find(
                    {"$expr": {"$eq": [{"$radiansToDegrees": "$radians"}, 180]}}
                )
            )
            assert len(result) == 1
            assert result[0]["_id"] == 1
        finally:
            set_force_fallback(False)


class TestHyperbolicOperators:
    """Test hyperbolic trigonometric operators."""

    def test_sinh_python(self):
        """Test $sinh operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sinh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 1})
        assert result is not None
        assert abs(result - math.sinh(1)) < 0.0001

    def test_cosh_python(self):
        """Test $cosh operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$cosh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 1})
        assert result is not None
        assert abs(result - math.cosh(1)) < 0.0001

    def test_tanh_python(self):
        """Test $tanh operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$tanh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 1})
        assert result is not None
        assert abs(result - math.tanh(1)) < 0.0001

    def test_asinh_python(self):
        """Test $asinh operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$asinh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 1})
        assert result is not None
        assert abs(result - math.asinh(1)) < 0.0001

    def test_acosh_python(self):
        """Test $acosh operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$acosh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 2})
        assert result is not None
        assert abs(result - math.acosh(2)) < 0.0001

    def test_acosh_invalid_input(self):
        """Test $acosh with invalid input (< 1)."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$acosh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0.5})
        assert result is None

    def test_atanh_python(self):
        """Test $atanh operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$atanh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0.5})
        assert result is not None
        assert abs(result - math.atanh(0.5)) < 0.0001

    def test_atanh_invalid_input(self):
        """Test $atanh with invalid input (>= 1 or <= -1)."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$atanh": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 1})
        assert result is None

        result = evaluator._evaluate_expr_python(expr, {"value": -1})
        assert result is None


class TestSigmoidOperator:
    """Test $sigmoid operator."""

    def test_sigmoid_python(self):
        """Test $sigmoid operator."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 0})
        assert result is not None
        assert abs(result - 0.5) < 0.0001

    def test_sigmoid_positive(self):
        """Test $sigmoid with positive value."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": 2})
        assert result is not None
        expected = 1.0 / (1.0 + math.exp(-2))
        assert abs(result - expected) < 0.0001

    def test_sigmoid_negative(self):
        """Test $sigmoid with negative value."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": -2})
        assert result is not None
        expected = 1.0 / (1.0 + math.exp(2))
        assert abs(result - expected) < 0.0001

    def test_sigmoid_null(self):
        """Test $sigmoid with null value."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": "$value"}
        result = evaluator._evaluate_expr_python(expr, {"value": None})
        assert result is None

    def test_sigmoid_onNull(self):
        """Test $sigmoid with onNull option."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": {"input": "$value", "onNull": 0.5}}
        result = evaluator._evaluate_expr_python(expr, {"value": None})
        assert result == 0.5

    def test_sigmoid_onNull_expression(self):
        """Test $sigmoid with onNull as expression."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": {"input": "$value", "onNull": {"$divide": [1, 2]}}}
        result = evaluator._evaluate_expr_python(expr, {"value": None})
        assert result == 0.5

    def test_sigmoid_object_form_with_valid_input(self):
        """Test $sigmoid object form with valid input."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()

        expr = {"$sigmoid": {"input": "$value", "onNull": 0.5}}
        result = evaluator._evaluate_expr_python(expr, {"value": 0})
        assert result is not None
        assert abs(result - 0.5) < 0.0001
