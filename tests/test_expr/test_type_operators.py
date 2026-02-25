"""
Tests for $expr type conversion operators.

Covers: $type, $toString, $toInt, $toDouble, $toBool
"""

from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestTypeOperatorsPython:
    """Test type conversion operators Python evaluation."""

    def test_toString_operator(self):
        """Test $toString operator."""
        evaluator = ExprEvaluator()
        expr = {"$toString": ["$value"]}
        assert evaluator._evaluate_expr_python(expr, {"value": 123}) == "123"

    def test_toInt_operator(self):
        """Test $toInt operator."""
        evaluator = ExprEvaluator()
        expr = {"$toInt": ["$value"]}
        assert evaluator._evaluate_expr_python(expr, {"value": "123"}) == 123
        assert evaluator._evaluate_expr_python(expr, {"value": 12.5}) == 12

    def test_toDouble_operator(self):
        """Test $toDouble operator."""
        evaluator = ExprEvaluator()
        expr = {"$toDouble": ["$value"]}
        assert evaluator._evaluate_expr_python(expr, {"value": "12.5"}) == 12.5

    def test_toBool_operator(self):
        """Test $toBool operator."""
        evaluator = ExprEvaluator()
        expr = {"$toBool": ["$value"]}
        assert evaluator._evaluate_expr_python(expr, {"value": 1}) is True
        assert evaluator._evaluate_expr_python(expr, {"value": 0}) is False
        assert evaluator._evaluate_expr_python(expr, {"value": "hello"}) is True
        assert evaluator._evaluate_expr_python(expr, {"value": ""}) is False

    def test_type_operator(self):
        """Test $type operator."""
        evaluator = ExprEvaluator()
        expr = {"$type": ["$value"]}
        assert evaluator._evaluate_expr_python(expr, {"value": 123}) == "int"
        assert (
            evaluator._evaluate_expr_python(expr, {"value": 12.5}) == "double"
        )
        assert (
            evaluator._evaluate_expr_python(expr, {"value": "hello"})
            == "string"
        )
        assert (
            evaluator._evaluate_expr_python(expr, {"value": [1, 2]}) == "array"
        )
        assert (
            evaluator._evaluate_expr_python(expr, {"value": {"a": 1}})
            == "object"
        )
        assert evaluator._evaluate_expr_python(expr, {"value": None}) == "null"
