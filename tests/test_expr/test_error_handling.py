"""
Tests for $expr error handling.

Verifies that invalid expressions are handled gracefully with proper fallback to Python.
"""

from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestOperatorErrorHandling:
    """Test error handling for operators."""

    def test_invalid_operator_sql(self):
        """Test invalid operator returns None (falls back to Python)."""
        evaluator = ExprEvaluator()
        expr = {"$invalidOperator": ["$field"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_invalid_structure_sql(self):
        """Test invalid expression structure returns None."""
        evaluator = ExprEvaluator()
        expr = {"$eq": [5]}  # Missing second operand
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_date_operator_invalid_operands(self):
        """Test date operator with invalid operands returns None."""
        evaluator = ExprEvaluator()
        expr = {"$year": []}  # Requires exactly 1 operand
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_mergeObjects_invalid_operands(self):
        """Test $mergeObjects with invalid operands returns None."""
        evaluator = ExprEvaluator()
        expr = {"$mergeObjects": "not a list"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_getField_invalid_operands(self):
        """Test $getField with invalid operands returns None."""
        evaluator = ExprEvaluator()
        expr = {"$getField": "not a dict"}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_setField_missing_field(self):
        """Test $setField without field returns None."""
        evaluator = ExprEvaluator()
        expr = {"$setField": {"value": "test"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_pow_invalid_operands(self):
        """Test $pow with invalid operands returns None."""
        evaluator = ExprEvaluator()
        expr = {"$pow": [5]}  # Requires 2 operands
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_sqrt_invalid_operands(self):
        """Test $sqrt with invalid operands returns None."""
        evaluator = ExprEvaluator()
        expr = {"$sqrt": []}  # Requires 1 operand
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is None  # Falls back to Python

    def test_division_by_zero_python(self):
        """Test division by zero handled gracefully in Python."""
        evaluator = ExprEvaluator()
        expr = {"$divide": ["$a", 0]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10})
        assert result is None

    def test_mod_by_zero_python(self):
        """Test modulo by zero handled gracefully in Python."""
        evaluator = ExprEvaluator()
        expr = {"$mod": ["$a", 0]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10})
        assert result is None

    def test_sqrt_negative_python(self):
        """Test sqrt of negative handled gracefully in Python."""
        evaluator = ExprEvaluator()
        expr = {"$sqrt": ["$value"]}
        result = evaluator._evaluate_expr_python(expr, {"value": -4})
        assert result is None

    def test_invalid_date_python(self):
        """Test invalid date handled gracefully in Python."""
        evaluator = ExprEvaluator()
        expr = {"$year": ["$date"]}
        result = evaluator._evaluate_expr_python(expr, {"date": "not-a-date"})
        assert result is None

    def test_missing_field_python(self):
        """Test missing field handled gracefully in Python."""
        evaluator = ExprEvaluator()
        expr = {"$eq": ["$missing", 5]}
        result = evaluator.evaluate_python(expr, {"other": 5})
        assert result is False  # None != 5

    def test_null_values_python(self):
        """Test null values handled gracefully in Python."""
        evaluator = ExprEvaluator()
        expr = {"$eq": ["$a", "$b"]}
        result = evaluator.evaluate_python(expr, {"a": None, "b": None})
        assert result is True  # None == None
