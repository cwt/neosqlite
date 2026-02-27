"""
Tests for Phase 1: Aggregation Expression Foundation.

This module tests the new aggregation expression support features:
- AggregationContext class
- Expression type detection helpers
- evaluate_for_aggregation() method
"""

import pytest
from neosqlite.collection.expr_evaluator import (
    ExprEvaluator,
    AggregationContext,
    _is_expression,
    _is_field_reference,
    _is_aggregation_variable,
    _is_literal,
)


class TestAggregationContext:
    """Test AggregationContext class."""

    def test_init_default_values(self):
        """Test AggregationContext initialization."""
        ctx = AggregationContext()
        assert ctx.variables["$$ROOT"] is None
        assert ctx.variables["$$CURRENT"] is None
        assert ctx.variables["$$REMOVE"] is None
        assert ctx.stage_index == 0
        assert ctx.current_field is None
        assert ctx.pipeline_id is None

    def test_bind_document(self):
        """Test binding document to context."""
        ctx = AggregationContext()
        doc = {"_id": 1, "name": "test", "value": 42}
        ctx.bind_document(doc)
        assert ctx.variables["$$ROOT"] is doc
        assert ctx.variables["$$CURRENT"] is doc

    def test_update_current(self):
        """Test updating current document."""
        ctx = AggregationContext()
        original = {"_id": 1, "name": "original"}
        updated = {"_id": 1, "name": "updated", "new_field": 100}

        ctx.bind_document(original)
        assert ctx.variables["$$CURRENT"] is original

        ctx.update_current(updated)
        assert ctx.variables["$$CURRENT"] is updated
        assert ctx.variables["$$ROOT"] is original  # ROOT unchanged

    def test_get_variable(self):
        """Test getting variable values."""
        ctx = AggregationContext()
        doc = {"_id": 1}
        ctx.bind_document(doc)

        assert ctx.get_variable("$$ROOT") is doc
        assert ctx.get_variable("$$CURRENT") is doc
        assert ctx.get_variable("$$REMOVE") is None
        assert ctx.get_variable("$$UNKNOWN") is None

    def test_set_variable(self):
        """Test setting variable values."""
        ctx = AggregationContext()
        ctx.set_variable("$$ROOT", {"_id": 1})
        ctx.set_variable("$$CUSTOM", "custom_value")

        assert ctx.get_variable("$$ROOT") == {"_id": 1}
        assert ctx.get_variable("$$CUSTOM") == "custom_value"


class TestExpressionTypeDetection:
    """Test expression type detection helpers."""

    def test_is_expression_true_cases(self):
        """Test _is_expression with valid expressions."""
        assert _is_expression({"$sin": "$angle"}) is True
        assert _is_expression({"$add": ["$a", "$b"]}) is True
        assert _is_expression({"$multiply": ["$price", "$qty"]}) is True
        assert (
            _is_expression(
                {"$cond": {"if": {"$gt": ["$x", 0]}, "then": 1, "else": 0}}
            )
            is True
        )

    def test_is_expression_false_cases(self):
        """Test _is_expression with non-expressions."""
        # Reserved fields
        assert _is_expression({"$field": "value"}) is False
        assert _is_expression({"$index": 0}) is False

        # Not dicts
        assert _is_expression("$field") is False
        assert _is_expression(42) is False
        assert _is_expression("string") is False
        assert _is_expression(None) is False
        assert _is_expression([1, 2, 3]) is False

        # Dict with multiple keys
        assert _is_expression({"$a": 1, "$b": 2}) is False

        # Dict without $ prefix
        assert _is_expression({"name": "value"}) is False

    def test_is_field_reference_true_cases(self):
        """Test _is_field_reference with valid field references."""
        assert _is_field_reference("$field") is True
        assert _is_field_reference("$nested.field") is True
        assert _is_field_reference("$a.b.c") is True
        assert _is_field_reference("$array[0]") is True

    def test_is_field_reference_false_cases(self):
        """Test _is_field_reference with non-field-references."""
        # Aggregation variables
        assert _is_field_reference("$$ROOT") is False
        assert _is_field_reference("$$CURRENT") is False

        # Not strings
        assert _is_field_reference(42) is False
        assert _is_field_reference({"$sin": "$angle"}) is False

        # Strings not starting with $
        assert _is_field_reference("field") is False
        assert _is_field_reference("") is False

    def test_is_aggregation_variable_true_cases(self):
        """Test _is_aggregation_variable with valid variables."""
        assert _is_aggregation_variable("$$ROOT") is True
        assert _is_aggregation_variable("$$CURRENT") is True
        assert _is_aggregation_variable("$$REMOVE") is True
        assert _is_aggregation_variable("$$CUSTOM_VAR") is True

    def test_is_aggregation_variable_false_cases(self):
        """Test _is_aggregation_variable with non-variables."""
        # Field references (single $)
        assert _is_aggregation_variable("$field") is False
        assert _is_aggregation_variable("$nested.field") is False

        # Not strings
        assert _is_aggregation_variable(42) is False
        assert _is_aggregation_variable({"$sin": "$angle"}) is False

        # Regular strings
        assert _is_aggregation_variable("ROOT") is False
        assert _is_aggregation_variable("") is False

    def test_is_literal_true_cases(self):
        """Test _is_literal with valid literals."""
        assert _is_literal(42) is True
        assert _is_literal(3.14) is True
        assert _is_literal("string") is True
        assert _is_literal(True) is True
        assert _is_literal(False) is True
        assert _is_literal(None) is True
        assert _is_literal([1, 2, 3]) is True
        assert _is_literal({"key": "value"}) is True

    def test_is_literal_false_cases(self):
        """Test _is_literal with non-literals."""
        # Field references
        assert _is_literal("$field") is False
        assert _is_literal("$nested.field") is False

        # Aggregation variables
        assert _is_literal("$$ROOT") is False
        assert _is_literal("$$CURRENT") is False


class TestEvaluateForAggregation:
    """Test evaluate_for_aggregation() method."""

    def test_field_reference(self):
        """Test evaluating field reference."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation("$field")
        assert "json_extract" in sql or "jsonb_extract" in sql
        assert "$.field" in sql
        assert params == []

    def test_nested_field_reference(self):
        """Test evaluating nested field reference."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation("$nested.field")
        assert "json_extract" in sql or "jsonb_extract" in sql
        assert "$.nested.field" in sql
        assert params == []

    def test_literal_number(self):
        """Test evaluating numeric literal."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(42)
        assert sql == "?"
        assert params == [42]

    def test_literal_string(self):
        """Test evaluating string literal."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation("hello")
        assert sql == "?"
        assert params == ["hello"]

    def test_literal_boolean(self):
        """Test evaluating boolean literal."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(True)
        assert sql == "?"
        assert params == [True]

    def test_literal_null(self):
        """Test evaluating null literal."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(None)
        assert sql == "?"
        assert params == [None]

    def test_expression_with_alias(self):
        """Test evaluating expression with alias."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(
            {"$sin": "$angle"}, as_alias="sin_val"
        )
        assert "sin(" in sql
        assert "AS sin_val" in sql
        assert params == []

    def test_arithmetic_expression(self):
        """Test evaluating arithmetic expression."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(
            {"$add": ["$a", "$b", 5]}
        )
        assert "(" in sql
        assert "+" in sql
        # Literal 5 should be in params
        assert params == [5]

    def test_comparison_expression(self):
        """Test evaluating comparison expression."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation({"$gt": ["$a", "$b"]})
        assert ">" in sql
        assert params == []

    def test_conditional_expression(self):
        """Test evaluating conditional expression."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(
            {"$cond": {"if": {"$gt": ["$x", 0]}, "then": 1, "else": 0}}
        )
        assert "CASE" in sql.upper()
        assert "WHEN" in sql.upper()
        assert "THEN" in sql.upper()
        assert "ELSE" in sql.upper()
        assert "END" in sql.upper()
        # Literal values 0, 1, 0 should be in params
        assert len(params) == 3

    def test_aggregation_variable_root(self):
        """Test evaluating $$ROOT variable."""
        evaluator = ExprEvaluator()
        ctx = AggregationContext()
        sql, params = evaluator.evaluate_for_aggregation("$$ROOT", context=ctx)
        assert sql == "data"
        assert params == []

    def test_aggregation_variable_current(self):
        """Test evaluating $$CURRENT variable."""
        evaluator = ExprEvaluator()
        ctx = AggregationContext()
        sql, params = evaluator.evaluate_for_aggregation(
            "$$CURRENT", context=ctx
        )
        assert sql == "data"
        assert params == []

    def test_aggregation_variable_remove_not_implemented(self):
        """Test that $$REMOVE raises NotImplementedError."""
        evaluator = ExprEvaluator()
        ctx = AggregationContext()
        with pytest.raises(NotImplementedError):
            evaluator.evaluate_for_aggregation("$$REMOVE", context=ctx)

    def test_nested_expression(self):
        """Test evaluating nested expression."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation(
            {"$multiply": ["$price", {"$add": [1, 0.1]}]}
        )
        assert "*" in sql
        assert "+" in sql
        # Literal values 1 and 0.1 should be in params
        assert params == [1, 0.1]

    def test_trig_expression(self):
        """Test evaluating trigonometric expression."""
        evaluator = ExprEvaluator()
        sql, params = evaluator.evaluate_for_aggregation({"$sin": "$angle"})
        assert "sin(" in sql
        assert params == []

    def test_jsonb_support_detection(self):
        """Test that JSONB support is properly detected."""
        # Without DB connection, should default to json_extract
        evaluator = ExprEvaluator()
        assert evaluator._jsonb_supported is False

        sql, params = evaluator.evaluate_for_aggregation("$field")
        assert "json_extract" in sql
        assert "jsonb_extract" not in sql


class TestIntegrationWithRealData:
    """Integration tests with real database."""

    def test_simple_field_projection(self, tmp_path):
        """Test simple field projection with aggregation."""
        from neosqlite import Connection

        with Connection(":memory:") as conn:
            collection = conn["test_collection"]

            # Insert test data
            collection.insert_many(
                [
                    {"_id": 1, "name": "Alice", "age": 30},
                    {"_id": 2, "name": "Bob", "age": 25},
                ]
            )

            # Test that evaluate_for_aggregation works
            evaluator = ExprEvaluator(data_column="data", db_connection=conn.db)
            sql, params = evaluator.evaluate_for_aggregation(
                "$name", as_alias="name"
            )

            # Execute the SQL to verify it works
            cursor = conn.db.execute(
                f"SELECT {sql} FROM {collection.name} WHERE id = ?",
                params + [1],
            )
            row = cursor.fetchone()
            assert row[0] == "Alice"

    def test_arithmetic_expression_in_aggregation(self, tmp_path):
        """Test arithmetic expression in aggregation context."""
        from neosqlite import Connection

        with Connection(":memory:") as conn:
            collection = conn["test_collection"]

            # Insert test data
            collection.insert_many(
                [
                    {"_id": 1, "price": 100, "quantity": 5},
                    {"_id": 2, "price": 50, "quantity": 10},
                ]
            )

            evaluator = ExprEvaluator(data_column="data", db_connection=conn.db)
            sql, params = evaluator.evaluate_for_aggregation(
                {"$multiply": ["$price", "$quantity"]}, as_alias="total"
            )

            # Execute the SQL to verify it works
            cursor = conn.db.execute(
                f"SELECT {sql} FROM {collection.name} WHERE id = ?",
                params + [1],
            )
            row = cursor.fetchone()
            assert row[0] == 500  # 100 * 5

    def test_trig_expression_in_aggregation(self, tmp_path):
        """Test trigonometric expression in aggregation context."""
        from neosqlite import Connection
        import math

        with Connection(":memory:") as conn:
            collection = conn["test_collection"]

            # Insert test data with angle in radians
            collection.insert_one(
                {"_id": 1, "angle": math.pi / 2}
            )  # 90 degrees

            evaluator = ExprEvaluator(data_column="data", db_connection=conn.db)
            sql, params = evaluator.evaluate_for_aggregation(
                {"$sin": "$angle"}, as_alias="sin_val"
            )

            # Execute the SQL to verify it works
            cursor = conn.db.execute(
                f"SELECT {sql} FROM {collection.name} WHERE id = ?",
                params + [1],
            )
            row = cursor.fetchone()
            # sin(π/2) should be 1.0 (or very close due to floating point)
            assert abs(row[0] - 1.0) < 0.0001
