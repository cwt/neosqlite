"""
$expr operator evaluator for NeoSQLite.

This module implements the MongoDB $expr operator using a 3-tier approach:
1. Single SQL Query (fastest) - Uses SQLite JSON functions
2. Temporary Tables (intermediate) - For complex expressions
3. Python Fallback (slowest but complete) - Always available, especially for kill switch

The evaluator ensures that SQLite and Python implementations produce identical results.

MongoDB $expr Compatibility:
- Comparison operators: $eq, $ne, $gt, $gte, $lt, $lte, $cmp
- Logical operators: $and, $or, $not, $nor
- Arithmetic operators: $add, $subtract, $multiply, $divide, $mod, $abs, $ceil, $floor, $round, $trunc
- Conditional operators: $cond, $ifNull, $switch
- Array operators: $size, $in, $arrayElemAt, $first, $last, $isArray
- Array aggregation: $sum, $avg, $min, $max
- Array transformation: $filter, $map, $reduce
- String operators: $concat, $toLower, $toUpper, $strLenBytes, $substr, $trim
- String regex: $regexMatch, $regexFind, $regexFindAll
- Date operators: $year, $month, $dayOfMonth, $hour, $minute, $second, $dayOfWeek, $dayOfYear
- Date arithmetic: $dateAdd, $dateSubtract, $dateDiff
- Type operators: $type, $convert, $toString, $toInt, $toDouble, $toBool
- Object operators: $mergeObjects, $getField, $setField
- Other: $literal, $let
- Trigonometric: $sin, $cos, $tan, $asin, $acos, $atan, $atan2
- Hyperbolic: $sinh, $cosh, $tanh, $asinh, $acosh, $atanh
- Logarithmic: $ln, $log, $log10, $log2
- Exponential/Sigmoid: $exp, $sigmoid
- Angle conversion: $degreesToRadians, $radiansToDegrees

Note: NeoSQLite extends MongoDB with $log2 (base-2 log) operator.
"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple, TYPE_CHECKING

# Import from submodules
from .constants import (
    REMOVE_SENTINEL as REMOVE_SENTINEL,
    RESERVED_FIELDS as RESERVED_FIELDS,
    _RemoveSentinel as _RemoveSentinel,
)
from .context import (
    AggregationContext as AggregationContext,
    _is_aggregation_variable as _is_aggregation_variable,
    _is_expression as _is_expression,
    _is_field_reference as _is_field_reference,
    _is_literal as _is_literal,
)
from .python_evaluators import PythonEvaluatorsMixin
from .sql_converters import SqlConvertersMixin
from .type_utils import (
    _convert_to_bindata as _convert_to_bindata,
    _convert_to_bool as _convert_to_bool,
    _convert_to_bsonbindata as _convert_to_bsonbindata,
    _convert_to_bsonregex as _convert_to_bsonregex,
    _convert_to_date as _convert_to_date,
    _convert_to_decimal as _convert_to_decimal,
    _convert_to_double as _convert_to_double,
    _convert_to_int as _convert_to_int,
    _convert_to_long as _convert_to_long,
    _convert_to_null as _convert_to_null,
    _convert_to_objectid as _convert_to_objectid,
    _convert_to_regex as _convert_to_regex,
    _convert_to_string as _convert_to_string,
    get_bson_type as get_bson_type,
)
from ..json_path_utils import (
    build_json_extract_expression as build_json_extract_expression,
    parse_json_path as parse_json_path,
)
from ..jsonb_support import (
    _get_json_each_function as _get_json_each_function,
    _get_json_function_prefix as _get_json_function_prefix,
    _get_json_group_array_function as _get_json_group_array_function,
    supports_jsonb as supports_jsonb,
    supports_jsonb_each as supports_jsonb_each,
)

# Forward reference for PipelineContext to avoid circular import
if TYPE_CHECKING:
    from ..sql_tier_aggregator import PipelineContext

# Public API exports
__all__ = [
    # Classes
    "ExprEvaluator",
    "AggregationContext",
    # Constants
    "RESERVED_FIELDS",
    "REMOVE_SENTINEL",
    "_RemoveSentinel",
    # Helper functions
    "_is_expression",
    "_is_field_reference",
    "_is_aggregation_variable",
    "_is_literal",
    # Type conversion utilities
    "_convert_to_int",
    "_convert_to_long",
    "_convert_to_double",
    "_convert_to_decimal",
    "_convert_to_string",
    "_convert_to_bool",
    "_convert_to_objectid",
    "_convert_to_bindata",
    "_convert_to_bsonbindata",
    "_convert_to_regex",
    "_convert_to_bsonregex",
    "_convert_to_date",
    "_convert_to_null",
    "get_bson_type",
    # JSON utilities
    "parse_json_path",
    "build_json_extract_expression",
    # JSONB support
    "supports_jsonb",
    "supports_jsonb_each",
    "_get_json_function_prefix",
    "_get_json_each_function",
    "_get_json_group_array_function",
]


class ExprEvaluator(SqlConvertersMixin, PythonEvaluatorsMixin):
    """
    Evaluator for MongoDB $expr operator.

    Supports 3-tier evaluation:
    - Tier 1: Direct SQL WHERE clause using JSON functions
    - Tier 2: Temporary tables for complex expressions
    - Tier 3: Python fallback (always available for kill switch)

    JSON/JSONB Support:
    - Automatically uses jsonb_* functions when supported for better performance
    - Falls back to json_* functions when JSONB is not available
    - Detects SQLite 3.51.0+ features (jsonb_each, jsonb_tree) for maximum performance
    """

    def __init__(self, data_column: str = "data", db_connection=None):
        """
        Initialize the expression evaluator.

        Args:
            data_column: Name of the column containing JSON data (default: "data")
            db_connection: Optional SQLite database connection for JSONB detection.
                          If provided, JSONB support will be auto-detected.
                          If None, json_* functions will be used (safe fallback).
        """
        self.data_column = data_column
        self._jsonb_supported = False
        self._jsonb_each_supported = False
        self._log2_warned = False  # Track if we've warned about $log2
        self._current_context = None  # Temporary context for SQL conversion
        if db_connection is not None:
            self._jsonb_supported = supports_jsonb(db_connection)
            self._jsonb_each_supported = supports_jsonb_each(db_connection)

    @property
    def json_function_prefix(self) -> str:
        """Get the appropriate JSON function prefix (json or jsonb)."""
        return _get_json_function_prefix(self._jsonb_supported)

    @property
    def json_each_function(self) -> str:
        """Get the appropriate json_each function name (json_each or jsonb_each)."""
        return _get_json_each_function(
            self._jsonb_supported, self._jsonb_each_supported
        )

    @property
    def json_group_array_function(self) -> str:
        """Get the appropriate json_group_array function name."""
        return _get_json_group_array_function(self._jsonb_supported)

    def evaluate(
        self, expr: Dict[str, Any], tier: int = 1, force_python: bool = False
    ) -> Tuple[str | None, List[Any]]:
        """
        Evaluate a $expr expression.

        Args:
            expr: The $expr expression dictionary
            tier: Complexity tier (1=SQL, 2=TempTable, 3=Python)
            force_python: Force Python evaluation (kill switch)

        Returns:
            Tuple of (SQL WHERE clause, parameters) or (None, []) for Python evaluation
        """
        if force_python or tier >= 3:
            return None, []

        match tier:
            case 1:
                return self._evaluate_sql_tier1(expr)
            case 2:
                return self._evaluate_sql_tier2(expr)
            case _:
                return None, []

    def _evaluate_sql_tier1(
        self, expr: Dict[str, Any]
    ) -> Tuple[str | None, List[Any]]:
        """
        Tier 1: Convert simple expressions to SQL WHERE clauses using JSON functions.

        Supports basic operators and field comparisons.
        """
        try:
            sql_expr, params = self._convert_expr_to_sql(expr)
            return f"({sql_expr})", params
        except (NotImplementedError, ValueError):
            return None, []

    def _evaluate_sql_tier2(
        self, expr: Dict[str, Any]
    ) -> Tuple[str | None, List[Any]]:
        """
        Tier 2: Use temporary tables for complex expressions.

        This tier is used when:
        - Expressions are too complex for Tier 1 (single SQL WHERE clause)
        - Multiple intermediate calculations are needed
        - The expression can benefit from pre-computed field extractions

        Args:
            expr: The $expr expression

        Returns:
            Tuple of (SQL expression, parameters) or (None, []) for Python fallback
        """
        # Tier 2 requires database connection which is passed from query_helper
        # For now, this is a placeholder that will be called from query_helper
        # with the proper database connection
        return None, []

    def evaluate_for_aggregation(
        self,
        expr: Any,
        context: AggregationContext | None = None,
        as_alias: str | None = None,
    ) -> Tuple[str, List[Any]]:
        """
        Evaluate expression for aggregation pipeline.

        This method evaluates expressions for use in aggregation pipeline stages
        like $addFields, $project, $group, etc. Unlike the evaluate() method which
        generates WHERE clause expressions, this method generates SELECT clause
        expressions with optional aliases.

        Args:
            expr: Expression to evaluate. Can be:
                  - Dict: Expression like {"$sin": "$angle"}
                  - Str: Field reference like "$field" or variable like "$$ROOT"
                  - Literal: Number, string, boolean, None, array, or dict
            context: Aggregation context for variable scoping. If None, a new
                     context will be created.
            as_alias: Optional alias for SELECT clause (e.g., "AS field_name")

        Returns:
            Tuple of (SQL expression, parameters). The SQL expression will include
            the alias if as_alias is provided.

        Raises:
            NotImplementedError: If the expression operator is not supported in
                                 SQL tier for aggregation context

        Examples:
            >>> evaluator = ExprEvaluator()
            >>> evaluator.evaluate_for_aggregation({"$sin": "$angle"})
            ("sin(json_extract(data, '$.angle'))", [])

            >>> evaluator.evaluate_for_aggregation({"$sin": "$angle"}, as_alias="sin_val")
            ("sin(json_extract(data, '$.angle')) AS sin_val", [])

            >>> evaluator.evaluate_for_aggregation("$field")
            ("json_extract(data, '$.field')", [])

            >>> evaluator.evaluate_for_aggregation(42)
            ("?", [42])
        """
        if context is None:
            context = AggregationContext()

        # Set temporary context for SQL conversion
        old_context = self._current_context
        self._current_context = context

        try:
            sql, params = self._convert_operand_to_sql_agg(expr, context)

            if as_alias:
                sql = f"{sql} AS {as_alias}"

            return sql, params
        finally:
            # Restore previous context
            self._current_context = old_context

    def _convert_operand_to_sql_agg(
        self, operand: Any, context: AggregationContext
    ) -> Tuple[str, List[Any]]:
        """
        Convert an operand to SQL for aggregation context.

        This method handles different types of operands:
        - Expressions: Recursively evaluate using _convert_expr_to_sql
        - Field references: Convert to json_extract calls
        - Aggregation variables: Handle $$ROOT, $$CURRENT, etc.
        - Literals: Convert to parameterized SQL

        Args:
            operand: The operand to convert
            context: Aggregation context for variable scoping

        Returns:
            Tuple of (SQL expression, parameters)

        Raises:
            NotImplementedError: If the operand type is not supported
        """
        # Handle aggregation variables first ($$ROOT, $$CURRENT, etc.)
        if _is_aggregation_variable(operand):
            return self._handle_aggregation_variable(operand, context)

        # For other types, use the existing _convert_operand_to_sql method
        # which handles expressions, field references, and literals
        return self._convert_operand_to_sql(operand)

    def _handle_aggregation_variable(
        self, var_name: str, context: AggregationContext
    ) -> Tuple[str, List[Any]]:
        """
        Handle aggregation variable references.

        Args:
            var_name: Variable name (e.g., "$$ROOT", "$$CURRENT")
            context: Aggregation context

        Returns:
            Tuple of (SQL expression, parameters)

        Raises:
            NotImplementedError: If the variable is not supported
        """
        match var_name:
            case "$$ROOT":
                # Return the entire document as JSON
                # In aggregation context, this is the data column itself
                return self.data_column, []
            case "$$CURRENT":
                # Return the current document state
                # For SQL tier, this is the same as $$ROOT since we don't
                # modify documents in-place in SQL
                return self.data_column, []
            case "$$REMOVE":
                # Sentinel value for field removal in $project
                # This is handled at the application level, not SQL
                # Return a special marker that can be detected
                raise NotImplementedError(
                    "$$REMOVE is handled at the application level (use REMOVE_SENTINEL)"
                )
            case _:
                # Check for custom variables defined via $let
                if (
                    var_name in context.variables
                    and context.variables[var_name] is not None
                ):
                    var_val = context.variables[var_name]
                    # If it's already a Tuple(sql, params), return it
                    if (
                        isinstance(var_val, tuple)
                        and len(var_val) == 2
                        and isinstance(var_val[0], str)
                    ):
                        return var_val

                # Unknown variable
                raise NotImplementedError(
                    f"Aggregation variable {var_name} not supported in SQL tier"
                )

    def build_select_expression(
        self,
        expr: Any,
        alias: str | None = None,
        context: AggregationContext | None = None,
    ) -> Tuple[str, List[Any]]:
        """
        Build SELECT clause expression for aggregation (SQL Tier 1 optimized).

        This method is similar to evaluate_for_aggregation() but optimized for
        SQL tier usage. It handles field aliasing and context tracking for
        multi-stage pipelines.

        Args:
            expr: Expression to evaluate. Can be:
                  - Dict: Expression like {"$sin": "$angle"}
                  - Str: Field reference like "$field" or variable like "$$ROOT"
                  - Literal: Number, string, boolean, None, array, or dict
            alias: Optional alias for SELECT clause (e.g., "AS field_name")
            context: Aggregation context for variable scoping. If None, creates
                     a new context. This allows tracking computed fields across
                     pipeline stages.

        Returns:
            Tuple of (SQL expression, parameters). The SQL expression will include
            the alias if alias is provided.

        Raises:
            NotImplementedError: If the expression operator is not supported in
                                 SQL tier for aggregation context

        Examples:
            >>> evaluator = ExprEvaluator()
            >>> evaluator.build_select_expression({"$sin": "$angle"})
            ("sin(json_extract(data, '$.angle'))", [])

            >>> evaluator.build_select_expression({"$sin": "$angle"}, alias="sin_val")
            ("sin(json_extract(data, '$.angle')) AS sin_val", [])

            >>> evaluator.build_select_expression("$field")
            ("json_extract(data, '$.field')", [])

            >>> evaluator.build_select_expression(42)
            ("?", [42])
        """
        if context is None:
            context = AggregationContext()

        # Set temporary context for SQL conversion
        old_context = self._current_context
        self._current_context = context

        try:
            sql, params = self._convert_operand_to_sql_agg(expr, context)

            if alias:
                sql = f"{sql} AS {alias}"

            return sql, params
        finally:
            # Restore previous context
            self._current_context = old_context

    def build_group_by_expression(
        self,
        expr: Any,
        context: AggregationContext | None = None,
    ) -> Tuple[str, List[Any]]:
        """
        Build GROUP BY clause expression for aggregation (SQL Tier 1 optimized).

        This method is optimized for grouping operations. It's similar to
        build_select_expression() but doesn't include aliases since GROUP BY
        clauses reference expressions directly.

        Args:
            expr: Expression to evaluate for GROUP BY
            context: Aggregation context for variable scoping

        Returns:
            Tuple of (SQL expression, parameters)

        Examples:
            >>> evaluator.build_group_by_expression("$category")
            ("json_extract(data, '$.category')", [])

            >>> evaluator.build_group_by_expression({"$toLower": "$category"})
            ("lower(json_extract(data, '$.category'))", [])
        """
        if context is None:
            context = AggregationContext()

        # For GROUP BY, we just need the expression without alias
        return self._convert_operand_to_sql_agg(expr, context)

    def build_having_expression(
        self,
        expr: Any,
        context: AggregationContext | None = None,
    ) -> Tuple[str, List[Any]]:
        """
        Build HAVING clause expression for post-aggregation filtering.

        HAVING expressions are evaluated after aggregation, so they can
        reference aggregate results.

        Args:
            expr: Expression to evaluate for HAVING clause
            context: Aggregation context for variable scoping

        Returns:
            Tuple of (SQL expression, parameters)

        Examples:
            >>> evaluator.build_having_expression({"$gt": ["$total", 100]})
            ("(json_extract(data, '$.total') > ?)", [100])
        """
        if context is None:
            context = AggregationContext()

        # HAVING expressions are similar to WHERE expressions
        # Use _convert_expr_to_sql for boolean expressions
        if isinstance(expr, dict) and len(expr) == 1:
            key = next(iter(expr.keys()))
            if key.startswith("$"):
                return self._convert_expr_to_sql(expr)

        # For non-expression operands, convert normally
        return self._convert_operand_to_sql_agg(expr, context)

    def _handle_aggregation_variable_sql_tier(
        self, var_name: str, context: PipelineContext | None = None
    ) -> str:
        """
        Handle aggregation variable references for SQL tier.

        This is a SQL-tier-specific version that works with PipelineContext
        instead of AggregationContext.

        Args:
            var_name: Variable name (e.g., "$$ROOT", "$$CURRENT")
            context: Pipeline context for tracking fields

        Returns:
            SQL expression string

        Raises:
            NotImplementedError: If the variable is not supported
        """
        match var_name:
            case "$$ROOT":
                # In SQL tier, root_data is preserved in a separate column
                return "root_data"
            case "$$CURRENT":
                # Current document state is in the data column
                return "data"
            case "$$REMOVE":
                # Sentinel for field removal - handled at application level
                return "$$REMOVE"
            case _:
                raise NotImplementedError(
                    f"Aggregation variable {var_name} not supported in SQL tier"
                )
