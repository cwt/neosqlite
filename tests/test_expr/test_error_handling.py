"""
Tests for $expr error handling.

Verifies that invalid expressions are handled gracefully with proper fallback to Python.
"""

import pytest
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
        # For MongoDB compatibility, string dates raise ValueError
        # This matches MongoDB behavior where date operators require Date type
        import pytest

        with pytest.raises(ValueError):
            evaluator._evaluate_expr_python(expr, {"date": "not-a-date"})

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


class TestTier2Evaluation:
    """Test Tier 2 evaluation path."""

    def test_tier2_placeholder(self):
        """Test that Tier 2 returns None (placeholder implementation)."""
        evaluator = ExprEvaluator()
        expr = {"$gt": ["$field", 10]}
        result, params = evaluator._evaluate_sql_tier2(expr)
        assert result is None
        assert params == []


class TestSqlConversionErrors:
    """Test error handling paths in SQL conversion."""

    def test_invalid_expr_structure(self):
        """Test invalid expression structure raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="Invalid.*structure"):
            evaluator._convert_expr_to_sql({"key1": 1, "key2": 2})

    def test_non_dict_expr(self):
        """Test non-dict expression raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="Invalid.*structure"):
            evaluator._convert_expr_to_sql("not a dict")

    def test_nor_operator_fewer_than_2_operands(self):
        """Test $nor with fewer than 2 operands raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires at least 2 operands"):
            evaluator._convert_logical_operator("$nor", [{"$gt": ["$a", 1]}])

    def test_unknown_logical_operator(self):
        """Test unknown logical operator raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="Unknown logical operator"):
            evaluator._convert_logical_operator(
                "$unknown", [{"$gt": ["$a", 1]}, {"$lt": ["$b", 2]}]
            )

    def test_comparison_wrong_operand_count(self):
        """Test comparison operator with wrong operand count raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires exactly 2 operands"):
            evaluator._convert_comparison_operator("$gt", [1])

    def test_cmp_wrong_operand_count(self):
        """Test $cmp with wrong operand count raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires exactly 2 operands"):
            evaluator._convert_cmp_operator([1])

    def test_arithmetic_fewer_than_2_operands(self):
        """Test arithmetic operator with fewer than 2 operands raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires at least 2 operands"):
            evaluator._convert_arithmetic_operator("$add", [1])

    def test_cond_invalid_structure(self):
        """Test $cond with invalid structure raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires a dictionary"):
            evaluator._convert_cond_operator("not a dict")

    def test_cond_missing_if_field(self):
        """Test $cond missing 'if' field raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires 'if' and 'then' fields"):
            evaluator._convert_cond_operator({"then": 1})

    def test_cond_missing_then_field(self):
        """Test $cond missing 'then' field raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires 'if' and 'then' fields"):
            evaluator._convert_cond_operator({"if": {"$gt": ["$a", 1]}})

    def test_ifNull_wrong_operand_count(self):
        """Test $ifNull with wrong operand count raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires exactly 2 operands"):
            evaluator._convert_ifNull_operator([1])

    def test_ifNull_non_list_operands(self):
        """Test $ifNull with non-list operands raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="requires exactly 2 operands"):
            evaluator._convert_ifNull_operator("not a list")


class TestPythonEvaluationErrors:
    """Test Python evaluation error handling."""

    def test_invalid_expr_structure_python(self):
        """Test invalid expression structure in Python raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="Invalid.*structure"):
            evaluator._evaluate_expr_python({"key1": 1, "key2": 2}, {})

    def test_non_dict_expr_python(self):
        """Test non-dict expression in Python raises ValueError."""
        evaluator = ExprEvaluator()
        with pytest.raises(ValueError, match="Invalid.*structure"):
            evaluator._evaluate_expr_python("not a dict", {})

    def test_unknown_operator_python(self):
        """Test unknown operator in Python raises NotImplementedError."""
        evaluator = ExprEvaluator()
        with pytest.raises(NotImplementedError, match="not supported"):
            evaluator._evaluate_expr_python({"$unknown": [1]}, {})


class TestOperandEvaluation:
    """Test operand evaluation edge cases."""

    def test_evaluate_operand_literal_value(self):
        """Test evaluating literal value."""
        evaluator = ExprEvaluator()
        result = evaluator._evaluate_operand_python(42, {})
        assert result == 42

    def test_evaluate_operand_dict(self):
        """Test evaluating dict operand."""
        evaluator = ExprEvaluator()
        result = evaluator._evaluate_operand_python({"key": "value"}, {})
        assert result == {"key": "value"}

    def test_evaluate_operand_dict_with_expression(self):
        """Test evaluating dict with nested expression."""
        evaluator = ExprEvaluator()
        result = evaluator._evaluate_operand_python({"$gt": [1, 2]}, {})
        assert result is False

    def test_evaluate_operand_missing_nested_field(self):
        """Test evaluating missing nested field."""
        evaluator = ExprEvaluator()
        result = evaluator._evaluate_operand_python("$a.b.c", {"a": {"x": 1}})
        assert result is None

    def test_evaluate_operand_non_dict_intermediate(self):
        """Test evaluating with non-dict intermediate value."""
        evaluator = ExprEvaluator()
        result = evaluator._evaluate_operand_python("$a.b", {"a": "string"})
        assert result is None


class TestBsonType:
    """Test _get_bson_type method."""

    def test_bson_type_null(self):
        """Test _get_bson_type with None."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type(None)
        assert result == "null"

    def test_bson_type_bool(self):
        """Test _get_bson_type with bool."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type(True)
        assert result == "bool"

    def test_bson_type_int(self):
        """Test _get_bson_type with int."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type(42)
        assert result == "int"

    def test_bson_type_float(self):
        """Test _get_bson_type with float."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type(3.14)
        assert result == "double"

    def test_bson_type_string(self):
        """Test _get_bson_type with string."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type("hello")
        assert result == "string"

    def test_bson_type_list(self):
        """Test _get_bson_type with list."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type([1, 2, 3])
        assert result == "array"

    def test_bson_type_dict(self):
        """Test _get_bson_type with dict."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type({"key": "value"})
        assert result == "object"

    def test_bson_type_unknown(self):
        """Test _get_bson_type with unknown type."""
        evaluator = ExprEvaluator()
        result = evaluator._get_bson_type(object())
        assert result == "unknown"
