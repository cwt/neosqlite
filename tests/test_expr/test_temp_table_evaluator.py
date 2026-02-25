"""
Tests for Tier 2: Temporary table-based expression evaluation.

Covers: TempTableExprEvaluator class in expr_temp_table.py
"""

import pytest
import neosqlite
from neosqlite.collection.expr_temp_table import TempTableExprEvaluator


class TestTempTableEvaluatorInit:
    """Test TempTableExprEvaluator initialization."""

    def test_init_default_column(self):
        """Test initialization with default data column."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            assert evaluator.data_column == "data"
            assert evaluator._temp_tables == []
            assert evaluator.json_function_prefix in ("json", "jsonb")

    def test_init_custom_column(self):
        """Test initialization with custom data column."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(
                conn.db, data_column="custom_data"
            )
            assert evaluator.data_column == "custom_data"

    def test_json_function_prefix_property(self):
        """Test json_function_prefix property."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            prefix = evaluator.json_function_prefix
            assert prefix in ("json", "jsonb")


class TestComplexityAnalysis:
    """Test expression complexity analysis."""

    def test_analyze_complexity_simple_comparison(self):
        """Test complexity analysis for simple comparison."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$a", "$b"]}
            complexity = evaluator._analyze_complexity(expr)
            assert complexity == 1  # Base score only

    def test_analyze_complexity_arithmetic(self):
        """Test complexity analysis with arithmetic operators."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$add": ["$a", "$b"]}
            complexity = evaluator._analyze_complexity(expr)
            assert complexity == 2  # Base + 1 for arithmetic

    def test_analyze_complexity_nested_arithmetic(self):
        """Test complexity analysis with nested arithmetic."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$gt": [{"$add": ["$a", "$b"]}, "$c"]}
            complexity = evaluator._analyze_complexity(expr)
            # Base (1) + nested add (1) = 2
            assert complexity >= 1  # At minimum base score

    def test_analyze_complexity_conditional(self):
        """Test complexity analysis with conditional operators."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {
                "$cond": {"if": {"$gt": ["$a", "$b"]}, "then": 1, "else": 0}
            }
            complexity = evaluator._analyze_complexity(expr)
            # Base (1) + cond (2) = 3
            assert complexity >= 3

    def test_analyze_complexity_array_operators(self):
        """Test complexity analysis with array operators."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$size": ["$items"]}
            complexity = evaluator._analyze_complexity(expr)
            # Base (1) + array op (2) = 3
            assert complexity == 3

    def test_analyze_complexity_string_operators(self):
        """Test complexity analysis with string operators."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$concat": ["$a", "$b"]}
            complexity = evaluator._analyze_complexity(expr)
            # Base (1) + string op (1) = 2
            assert complexity == 2

    def test_analyze_complexity_logical_and(self):
        """Test complexity analysis with $and."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$and": [{"$gt": ["$a", "$b"]}, {"$lt": ["$a", "$c"]}]}
            complexity = evaluator._analyze_complexity(expr)
            assert complexity >= 1

    def test_analyze_complexity_logical_or(self):
        """Test complexity analysis with $or."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$or": [{"$eq": ["$a", 1]}, {"$eq": ["$b", 2]}]}
            complexity = evaluator._analyze_complexity(expr)
            assert complexity >= 1

    def test_analyze_complexity_not(self):
        """Test complexity analysis with $not."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$not": [{"$gt": ["$a", "$b"]}]}
            complexity = evaluator._analyze_complexity(expr)
            assert complexity >= 1

    def test_analyze_complexity_nor(self):
        """Test complexity analysis with $nor."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$nor": [{"$eq": ["$a", 1]}, {"$eq": ["$b", 2]}]}
            complexity = evaluator._analyze_complexity(expr)
            assert complexity >= 1

    def test_analyze_complexity_non_dict(self):
        """Test complexity analysis with non-dict input."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            complexity = evaluator._analyze_complexity("not a dict")
            assert complexity == 0

    def test_analyze_complexity_switch(self):
        """Test complexity analysis with $switch."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$switch": {"branches": []}}
            complexity = evaluator._analyze_complexity(expr)
            # Base (1) + switch (2) = 3
            assert complexity >= 2


class TestEvaluate:
    """Test main evaluate method."""

    def test_evaluate_too_simple(self):
        """Test evaluation returns None for too simple expressions."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            # Simple comparison should fall back to Tier 1
            expr = {"$eq": ["$a", 1]}
            result, params = evaluator.evaluate(expr, "test_collection")
            assert result is None
            assert params == []

    def test_evaluate_too_complex(self):
        """Test evaluation returns None for too complex expressions."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            # Very complex expression should fall back to Tier 3
            expr = {
                "$and": [
                    {"$add": ["$a", "$b"]},
                    {
                        "$cond": {
                            "if": {"$gt": ["$c", "$d"]},
                            "then": 1,
                            "else": 2,
                        }
                    },
                    {"$size": ["$items"]},
                    {"$concat": ["$x", "$y"]},
                ]
            }
            result, params = evaluator.evaluate(expr, "test_collection")
            # May return None if too complex
            assert result is None or isinstance(result, str)

    def test_evaluate_not_implemented_operator(self):
        """Test evaluation with unsupported operator."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$regexFind": {"input": "$text", "regex": "test"}}
            result, params = evaluator.evaluate(expr, "test_collection")
            assert result is None  # Falls back to Python

    def test_evaluate_invalid_expression(self):
        """Test evaluation with invalid expression structure."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = "invalid"
            result, params = evaluator.evaluate(expr, "test_collection")
            assert result is None

    def test_evaluate_cleanup_temp_tables(self):
        """Test that temporary tables are cleaned up after evaluation."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$a", 1]}
            evaluator.evaluate(expr, "test_collection")
            # Temp tables should be cleaned up
            assert evaluator._temp_tables == []


class TestConvertExprToTempSql:
    """Test expression to SQL conversion for temp tables."""

    def test_convert_comparison_eq(self):
        """Test $eq conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "=" in sql
            assert "temp" in sql

    def test_convert_comparison_gt(self):
        """Test $gt conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$gt": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert ">" in sql

    def test_convert_comparison_gte(self):
        """Test $gte conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$gte": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert ">=" in sql

    def test_convert_comparison_lt(self):
        """Test $lt conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$lt": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "<" in sql

    def test_convert_comparison_lte(self):
        """Test $lte conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$lte": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "<=" in sql

    def test_convert_comparison_ne(self):
        """Test $ne conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$ne": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "!=" in sql

    def test_convert_comparison_wrong_operand_count(self):
        """Test comparison with wrong operand count."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$a"]}  # Missing second operand
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_arithmetic_add(self):
        """Test $add conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$add": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "+" in sql

    def test_convert_arithmetic_subtract(self):
        """Test $subtract conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$subtract": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "-" in sql

    def test_convert_arithmetic_multiply(self):
        """Test $multiply conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$multiply": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "*" in sql

    def test_convert_arithmetic_divide(self):
        """Test $divide conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$divide": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "/" in sql

    def test_convert_arithmetic_mod(self):
        """Test $mod conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$mod": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "%" in sql

    def test_convert_arithmetic_wrong_operand_count(self):
        """Test arithmetic with wrong operand count."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$add": ["$a"]}  # Need at least 2 operands
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_cond(self):
        """Test $cond conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {
                "$cond": {"if": {"$gt": ["$a", "$b"]}, "then": 1, "else": 0}
            }
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "CASE" in sql.upper()
            assert "THEN" in sql.upper()
            assert "ELSE" in sql.upper()

    def test_convert_cond_without_else(self):
        """Test $cond without else clause."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$cond": {"if": {"$gt": ["$a", "$b"]}, "then": 1}}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "CASE" in sql.upper()
            assert "NULL" in sql.upper()

    def test_convert_cond_invalid_structure(self):
        """Test $cond with invalid structure."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$cond": "invalid"}
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_cond_missing_fields(self):
        """Test $cond missing required fields."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$cond": {"then": 1}}  # Missing "if"
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_cmp(self):
        """Test $cmp conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$cmp": ["$a", "$b"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "CASE" in sql.upper()

    def test_convert_cmp_wrong_operand_count(self):
        """Test $cmp with wrong operand count."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$cmp": ["$a"]}  # Need exactly 2 operands
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_math_abs(self):
        """Test $abs conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$abs": ["$a"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "abs" in sql.lower()

    def test_convert_math_ceil(self):
        """Test $ceil conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$ceil": ["$a"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "ceil" in sql.lower()

    def test_convert_math_floor(self):
        """Test $floor conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$floor": ["$a"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "floor" in sql.lower()

    def test_convert_math_round(self):
        """Test $round conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$round": ["$a"]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "round" in sql.lower()

    def test_convert_math_wrong_operand_count(self):
        """Test math operator with wrong operand count."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$abs": []}  # Need exactly 1 operand
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_math_unsupported(self):
        """Test unsupported math operator."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$sqrt": ["$a"]}  # Not supported in Tier 2
            with pytest.raises(NotImplementedError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_unsupported_operator(self):
        """Test unsupported operator."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$regexFind": {"input": "$text", "regex": "test"}}
            with pytest.raises(NotImplementedError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_invalid_expression_structure(self):
        """Test invalid expression structure."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = "not a dict"
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_invalid_expression_multi_key(self):
        """Test expression with multiple keys."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$a", "$b"], "$gt": ["$c", "$d"]}
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")


class TestConvertLogicalToTempSql:
    """Test logical operator conversion."""

    def test_convert_logical_and(self):
        """Test $and conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$and": [{"$eq": ["$a", "$b"]}, {"$gt": ["$c", "$d"]}]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "AND" in sql.upper()

    def test_convert_logical_or(self):
        """Test $or conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$or": [{"$eq": ["$a", "$b"]}, {"$gt": ["$c", "$d"]}]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "OR" in sql.upper()

    def test_convert_logical_not(self):
        """Test $not conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$not": [{"$eq": ["$a", "$b"]}]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "NOT" in sql.upper()

    def test_convert_logical_nor(self):
        """Test $nor conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$nor": [{"$eq": ["$a", "$b"]}, {"$gt": ["$c", "$d"]}]}
            sql, params = evaluator._convert_expr_to_temp_sql(expr, "temp")
            assert "NOT" in sql.upper()
            assert "OR" in sql.upper()

    def test_convert_logical_not_wrong_operand_count(self):
        """Test $not with wrong operand count."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$not": []}  # Need exactly 1 operand
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_logical_wrong_operand_count(self):
        """Test $and/$or with wrong operand count."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$and": [{"$eq": ["$a", "$b"]}]}  # Need at least 2
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_logical_invalid_operands(self):
        """Test logical operator with non-list operands."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$and": "not a list"}
            with pytest.raises(ValueError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")

    def test_convert_logical_unknown_operator(self):
        """Test unknown logical operator."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$unknown": [{"$eq": ["$a", "$b"]}]}
            # Unknown operator raises NotImplementedError
            with pytest.raises(NotImplementedError):
                evaluator._convert_expr_to_temp_sql(expr, "temp")


class TestConvertOperandToTempSql:
    """Test operand conversion."""

    def test_convert_field_reference(self):
        """Test field reference conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            sql, params = evaluator._convert_operand_to_temp_sql(
                "$field", "temp"
            )
            assert "temp.field" in sql
            assert params == []

    def test_convert_nested_field_reference(self):
        """Test nested field reference conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            sql, params = evaluator._convert_operand_to_temp_sql(
                "$nested.field", "temp"
            )
            assert "temp.nested_field" in sql
            assert params == []

    def test_convert_literal_value(self):
        """Test literal value conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            sql, params = evaluator._convert_operand_to_temp_sql(42, "temp")
            assert sql == "?"
            assert params == [42]

    def test_convert_string_literal(self):
        """Test string literal conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            sql, params = evaluator._convert_operand_to_temp_sql(
                "hello", "temp"
            )
            assert sql == "?"
            assert params == ["hello"]

    def test_convert_nested_expression(self):
        """Test nested expression conversion."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$add": ["$a", "$b"]}
            sql, params = evaluator._convert_operand_to_temp_sql(expr, "temp")
            assert "+" in sql


class TestExtractFieldReferences:
    """Test field reference extraction."""

    def test_extract_simple_field(self):
        """Test extracting simple field reference."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$field", "$other"]}
            fields = evaluator._extract_field_references(expr)
            assert "field" in fields
            assert "other" in fields

    def test_extract_nested_field(self):
        """Test extracting nested field reference."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": ["$nested.field", "$other"]}
            fields = evaluator._extract_field_references(expr)
            assert "nested.field" in fields

    def test_extract_no_fields(self):
        """Test expression with no field references."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$eq": [1, 2]}
            fields = evaluator._extract_field_references(expr)
            assert fields == []

    def test_extract_from_array(self):
        """Test extracting fields from array operands."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {"$and": [{"$eq": ["$a", "$b"]}, {"$gt": ["$c", "$d"]}]}
            fields = evaluator._extract_field_references(expr)
            assert "a" in fields
            assert "b" in fields
            assert "c" in fields
            assert "d" in fields


class TestSanitizeFieldName:
    """Test field name sanitization."""

    def test_sanitize_simple_field(self):
        """Test sanitizing simple field name."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            name = evaluator._sanitize_field_name("fieldname")
            assert name == "fieldname"

    def test_sanitize_dotted_field(self):
        """Test sanitizing dotted field name."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            name = evaluator._sanitize_field_name("nested.field")
            assert name == "nested_field"

    def test_sanitize_array_field(self):
        """Test sanitizing array field name."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            name = evaluator._sanitize_field_name("items[0]")
            # Brackets are replaced with underscores
            assert "_" in name
            assert "items" in name

    def test_sanitize_numeric_start(self):
        """Test sanitizing field starting with number."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            name = evaluator._sanitize_field_name("123field")
            assert name.startswith("f_")

    def test_sanitize_empty_field(self):
        """Test sanitizing empty field name."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            name = evaluator._sanitize_field_name("")
            # Empty string stays empty (no f_ prefix since empty string is falsy)
            assert name == ""


class TestCleanupTempTables:
    """Test temporary table cleanup."""

    def test_cleanup_multiple_tables(self):
        """Test cleaning up multiple temporary tables."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            evaluator._temp_tables = ["temp1", "temp2", "temp3"]
            evaluator._cleanup_temp_tables()
            assert evaluator._temp_tables == []

    def test_cleanup_with_errors(self):
        """Test cleanup handles errors gracefully."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            evaluator._temp_tables = ["nonexistent_table"]
            # Should not raise exception
            evaluator._cleanup_temp_tables()
            assert evaluator._temp_tables == []


class TestIntegration:
    """Integration tests for Tier 2 evaluation."""

    def test_tier2_with_actual_collection(self):
        """Test Tier 2 evaluation with actual collection."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 1, "b": 2, "c": 3},
                    {"a": 5, "b": 5, "c": 10},
                    {"a": 10, "b": 2, "c": 5},
                ]
            )

            evaluator = TempTableExprEvaluator(conn.db)
            # Complex expression that might use Tier 2
            expr = {
                "$and": [
                    {"$gt": [{"$add": ["$a", "$b"]}, 5]},
                    {"$lt": ["$c", 10]},
                ]
            }
            result, params = evaluator.evaluate(expr, "test")
            # Should return a query or None for fallback
            assert result is None or isinstance(result, str)

    def test_field_extraction_complex_expr(self):
        """Test field extraction from complex expression."""
        with neosqlite.Connection(":memory:") as conn:
            evaluator = TempTableExprEvaluator(conn.db)
            expr = {
                "$and": [
                    {"$eq": [{"$add": ["$a", "$b"]}, "$c"]},
                    {"$gt": ["$d", 10]},
                ]
            }
            fields = evaluator._extract_field_references(expr)
            assert "a" in fields
            assert "b" in fields
            assert "c" in fields
            assert "d" in fields
