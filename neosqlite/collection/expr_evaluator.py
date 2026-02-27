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
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING
import math
import warnings
from .json_path_utils import build_json_extract_expression
from .jsonb_support import (
    supports_jsonb,
    supports_jsonb_each,
    _get_json_function_prefix,
    _get_json_each_function,
    _get_json_group_array_function,
)

# Forward reference for PipelineContext to avoid circular import
if TYPE_CHECKING:
    from .sql_tier_aggregator import PipelineContext


# Reserved field names that are NOT operators
RESERVED_FIELDS = {
    "$field",
    "$index",  # Used in $let
    # Add other reserved names as needed
}


class _RemoveSentinel:
    """
    Sentinel value for $$REMOVE in $project stage.

    When a field is set to this value, it should be removed from the output document.
    This is a singleton pattern - only one instance should exist.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "$$REMOVE"

    def __bool__(self):
        return False


# Singleton instance for $$REMOVE
REMOVE_SENTINEL = _RemoveSentinel()


class AggregationContext:
    """
    Manages variable scoping for aggregation expressions.

    Aggregation expressions have different variable contexts than query expressions.
    This class manages the lifecycle of aggregation variables like $$ROOT, $$CURRENT,
    and $$REMOVE throughout pipeline execution.

    Attributes:
        variables: Dictionary mapping variable names to their values
        stage_index: Current stage index in the pipeline
        current_field: Name of the field being computed (for context)
        pipeline_id: Unique identifier for the pipeline (for temp table correlation)
    """

    def __init__(self) -> None:
        """Initialize aggregation context with default variables."""
        self.variables: Dict[str, Any] = {
            "$$ROOT": None,  # Original document
            "$$CURRENT": None,  # Current document (may be modified)
            "$$REMOVE": None,  # Sentinel for field removal
        }
        self.stage_index: int = 0
        self.current_field: Optional[str] = None
        self.pipeline_id: Optional[str] = None

    def bind_document(self, doc: Dict[str, Any]) -> None:
        """
        Bind document to context.

        Called at the start of pipeline execution to initialize
        $$ROOT and $$CURRENT with the input document.

        Args:
            doc: The document to bind
        """
        self.variables["$$ROOT"] = doc
        self.variables["$$CURRENT"] = doc

    def update_current(self, doc: Dict[str, Any]) -> None:
        """
        Update current document after stage processing.

        Called after each stage that modifies the document to update
        the $$CURRENT variable.

        Args:
            doc: The updated document
        """
        self.variables["$$CURRENT"] = doc

    def get_variable(self, name: str) -> Any:
        """
        Get variable value.

        Args:
            name: Variable name (e.g., "$$ROOT", "$$CURRENT")

        Returns:
            Variable value or None if not found
        """
        return self.variables.get(name)

    def set_variable(self, name: str, value: Any) -> None:
        """
        Set variable value.

        Args:
            name: Variable name
            value: Value to set
        """
        self.variables[name] = value


def _is_expression(value: Any) -> bool:
    """
    Check if value is an aggregation expression.

    An expression is a dict with exactly one key starting with '$'
    that is not a reserved field name.

    Args:
        value: Value to check

    Returns:
        True if value is an expression, False otherwise

    Examples:
        >>> _is_expression({"$sin": "$angle"})
        True
        >>> _is_expression({"$field": "value"})  # Reserved
        False
        >>> _is_expression("$field")
        False
        >>> _is_expression(42)
        False
    """
    if not isinstance(value, dict):
        return False
    if len(value) != 1:
        return False  # Could be a literal dict
    key = next(iter(value.keys()))
    return key.startswith("$") and key not in RESERVED_FIELDS


def _is_field_reference(value: Any) -> bool:
    """
    Check if value is a field reference.

    Field references start with '$' but are not expressions
    (i.e., they're simple strings like "$field" or "$nested.field").

    Args:
        value: Value to check

    Returns:
        True if value is a field reference, False otherwise

    Examples:
        >>> _is_field_reference("$field")
        True
        >>> _is_field_reference("$nested.field")
        True
        >>> _is_field_reference("$$ROOT")
        False
        >>> _is_field_reference({"$sin": "$angle"})
        False
    """
    return (
        isinstance(value, str)
        and value.startswith("$")
        and not value.startswith("$$")
    )


def _is_aggregation_variable(value: Any) -> bool:
    """
    Check for aggregation variables.

    Aggregation variables start with '$$' (e.g., $$ROOT, $$CURRENT).

    Args:
        value: Value to check

    Returns:
        True if value is an aggregation variable, False otherwise

    Examples:
        >>> _is_aggregation_variable("$$ROOT")
        True
        >>> _is_aggregation_variable("$$CURRENT")
        True
        >>> _is_aggregation_variable("$field")
        False
    """
    return isinstance(value, str) and value.startswith("$$")


def _is_literal(value: Any) -> bool:
    """
    Check if value is a literal (not an expression or field reference).

    Literals include: numbers, strings, booleans, None, arrays, and plain dicts.

    Args:
        value: Value to check

    Returns:
        True if value is a literal, False otherwise

    Examples:
        >>> _is_literal(42)
        True
        >>> _is_literal("string")
        True
        >>> _is_literal(True)
        True
        >>> _is_literal(None)
        True
        >>> _is_literal([1, 2, 3])
        True
        >>> _is_literal("$field")
        False
    """
    if isinstance(value, str):
        # Strings starting with $ are field refs or variables, not literals
        return not value.startswith("$")
    # All other types are literals
    return True


class ExprEvaluator:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
        context: Optional[AggregationContext] = None,
        as_alias: Optional[str] = None,
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

        sql, params = self._convert_operand_to_sql_agg(expr, context)

        if as_alias:
            sql = f"{sql} AS {as_alias}"

        return sql, params

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
                # Unknown variable
                raise NotImplementedError(
                    f"Aggregation variable {var_name} not supported in SQL tier"
                )

    def build_select_expression(
        self,
        expr: Any,
        alias: Optional[str] = None,
        context: Optional[AggregationContext] = None,
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

        sql, params = self._convert_operand_to_sql_agg(expr, context)

        if alias:
            sql = f"{sql} AS {alias}"

        return sql, params

    def build_group_by_expression(
        self,
        expr: Any,
        context: Optional[AggregationContext] = None,
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
        context: Optional[AggregationContext] = None,
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
        self, var_name: str, context: Optional[PipelineContext] = None
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

    def _convert_expr_to_sql(
        self, expr: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """
        Convert a $expr expression to SQL.

        Args:
            expr: Expression dictionary

        Returns:
            Tuple of (SQL expression, parameters)

        Raises:
            NotImplementedError: If operator is not supported in SQL
            ValueError: If expression structure is invalid
        """
        if not isinstance(expr, dict) or len(expr) != 1:
            raise ValueError("Invalid $expr expression structure")

        operator, operands = next(iter(expr.items()))

        # Handle different operator types
        match operator:
            case "$and" | "$or" | "$not" | "$nor":
                return self._convert_logical_operator(operator, operands)
            case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne":
                return self._convert_comparison_operator(operator, operands)
            case "$cmp":
                # $cmp returns -1, 0, or 1, which can be used in comparisons
                # For SQL tier, we convert to a CASE statement
                return self._convert_cmp_operator(operands)
            case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                return self._convert_arithmetic_operator(operator, operands)
            case (
                "$pow"
                | "$sqrt"
                | "$ln"
                | "$log"
                | "$log10"
                | "$log2"
                | "$exp"
                | "$sigmoid"
            ):
                return self._convert_math_operator(operator, operands)
            case "$cond":
                return self._convert_cond_operator(operands)
            case "$ifNull":
                return self._convert_ifNull_operator(operands)
            case (
                "$size"
                | "$in"
                | "$isArray"
                | "$slice"
                | "$indexOfArray"
                | "$sum"
                | "$avg"
                | "$min"
                | "$max"
                | "$setEquals"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
            ):
                return self._convert_array_operator(operator, operands)
            case (
                "$concat"
                | "$toLower"
                | "$toUpper"
                | "$strLenBytes"
                | "$substr"
                | "$trim"
                | "$ltrim"
                | "$rtrim"
                | "$indexOfBytes"
                | "$regexMatch"
                | "$split"
                | "$replaceAll"
                | "$replaceOne"
                | "$strLenCP"
                | "$indexOfCP"
            ):
                return self._convert_string_operator(operator, operands)
            case "$abs" | "$ceil" | "$floor" | "$round" | "$trunc":
                return self._convert_math_operator(operator, operands)
            case (
                "$sin"
                | "$cos"
                | "$tan"
                | "$asin"
                | "$acos"
                | "$atan"
                | "$atan2"
                | "$sinh"
                | "$cosh"
                | "$tanh"
                | "$asinh"
                | "$acosh"
                | "$atanh"
            ):
                return self._convert_trig_operator(operator, operands)
            case "$degreesToRadians" | "$radiansToDegrees":
                return self._convert_angle_operator(operator, operands)
            case (
                "$year"
                | "$month"
                | "$dayOfMonth"
                | "$hour"
                | "$minute"
                | "$second"
                | "$dayOfWeek"
                | "$dayOfYear"
                | "$week"
                | "$isoDayOfWeek"
                | "$isoWeek"
                | "$millisecond"
            ):
                return self._convert_date_operator(operator, operands)
            case "$dateAdd" | "$dateSubtract":
                return self._convert_date_arithmetic_operator(
                    operator, operands
                )
            case "$dateDiff":
                return self._convert_date_diff_operator(operands)
            case (
                "$mergeObjects"
                | "$getField"
                | "$setField"
                | "$unsetField"
                | "$objectToArray"
            ):
                return self._convert_object_operator(operator, operands)
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in SQL tier"
                )

    def _convert_logical_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert logical operators ($and, $or, $not, $nor) to SQL."""
        if not isinstance(operands, list):
            raise ValueError(f"{operator} requires a list of expressions")

        if operator == "$not":
            if len(operands) != 1:
                raise ValueError("$not requires exactly one operand")
            inner_sql, inner_params = self._convert_expr_to_sql(operands[0])
            return f"NOT ({inner_sql})", inner_params

        # $and, $or, $nor
        if len(operands) < 2:
            raise ValueError(f"{operator} requires at least 2 operands")

        sql_parts = []
        all_params = []

        for operand in operands:
            operand_sql, operand_params = self._convert_expr_to_sql(operand)
            sql_parts.append(f"({operand_sql})")
            all_params.extend(operand_params)

        match operator:
            case "$and":
                sql = " AND ".join(sql_parts)
            case "$or":
                sql = " OR ".join(sql_parts)
            case "$nor":
                sql = f"NOT ({' OR '.join(sql_parts)})"
            case _:
                raise ValueError(f"Unknown logical operator: {operator}")

        return sql, all_params

    def _convert_comparison_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert comparison operators to SQL."""
        if len(operands) != 2:
            raise ValueError(f"{operator} requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_sql(operands[0])
        right_sql, right_params = self._convert_operand_to_sql(operands[1])

        sql_operator = self._map_comparison_operator(operator)

        return (
            f"{left_sql} {sql_operator} {right_sql}",
            left_params + right_params,
        )

    def _convert_cmp_operator(
        self, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert $cmp operator to SQL CASE statement."""
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_sql(operands[0])
        right_sql, right_params = self._convert_operand_to_sql(operands[1])

        sql = f"(CASE WHEN {left_sql} < {right_sql} THEN -1 WHEN {left_sql} > {right_sql} THEN 1 ELSE 0 END)"
        return sql, left_params + right_params

    def _convert_arithmetic_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert arithmetic operators to SQL."""
        if len(operands) < 2:
            raise ValueError(f"{operator} requires at least 2 operands")

        sql_parts = []
        all_params = []

        for operand in operands:
            operand_sql, operand_params = self._convert_operand_to_sql(operand)
            sql_parts.append(operand_sql)
            all_params.extend(operand_params)

        sql_operator = self._map_arithmetic_operator(operator)
        sql = f"({f' {sql_operator} '.join(sql_parts)})"

        return sql, all_params

    def _convert_cond_operator(
        self, operands: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Convert $cond operator to SQL CASE statement."""
        if not isinstance(operands, dict):
            raise ValueError("$cond requires a dictionary")

        if "if" not in operands or "then" not in operands:
            raise ValueError("$cond requires 'if' and 'then' fields")

        condition_sql, condition_params = self._convert_expr_to_sql(
            operands["if"]
        )
        then_sql, then_params = self._convert_operand_to_sql(operands["then"])

        if "else" in operands:
            else_sql, else_params = self._convert_operand_to_sql(
                operands["else"]
            )
        else:
            else_sql, else_params = "NULL", []

        sql = f"CASE WHEN {condition_sql} THEN {then_sql} ELSE {else_sql} END"

        return sql, condition_params + then_params + else_params

    def _convert_ifNull_operator(
        self, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert $ifNull operator to SQL COALESCE."""
        if not isinstance(operands, list) or len(operands) != 2:
            raise ValueError("$ifNull requires exactly 2 operands")

        expr_sql, expr_params = self._convert_operand_to_sql(operands[0])
        replacement_sql, replacement_params = self._convert_operand_to_sql(
            operands[1]
        )

        sql = f"COALESCE({expr_sql}, {replacement_sql})"
        return sql, expr_params + replacement_params

    def _convert_array_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert array operators to SQL.

        Note: SQLite's json_each(), json_array_length(), json_type(), and json_group_array()
        work with both JSON and JSONB data types. Starting from SQLite 3.51.0, jsonb_each()
        and jsonb_group_array() are available for better performance with JSONB data.
        """
        # Get the appropriate function names based on SQLite version
        json_each = self.json_each_function
        json_group_array = self.json_group_array_function

        match operator:
            case "$size":
                if len(operands) != 1:
                    raise ValueError("$size requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # SQLite: json_array_length for JSON arrays (works with both JSON and JSONB)
                sql = f"json_array_length({array_sql})"
                return sql, array_params
            case "$in":
                if len(operands) != 2:
                    raise ValueError("$in requires exactly 2 operands")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # Check if value exists in JSON array - use jsonb_each when available
                sql = f"EXISTS (SELECT 1 FROM {json_each}({array_sql}) WHERE value = {value_sql})"
                return sql, value_params + array_params
            case "$isArray":
                if len(operands) != 1:
                    raise ValueError("$isArray requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # Check if value is a JSON array
                sql = f"json_type({value_sql}) = 'array'"
                return sql, value_params
            case "$sum" | "$avg" | "$min" | "$max":
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )

                # Map MongoDB accumulator to SQL aggregator
                sql_agg = operator[1:].upper()

                # Use a correlated subquery to aggregate array elements
                # We filter out non-numeric values for $sum and $avg to match MongoDB
                if operator in ("$sum", "$avg"):
                    sql = f"(SELECT {sql_agg}(value) FROM {json_each}({array_sql}) WHERE typeof(value) IN ('integer', 'real'))"
                else:
                    sql = f"(SELECT {sql_agg}(value) FROM {json_each}({array_sql}))"
                return sql, array_params
            case "$slice":
                if not isinstance(operands, list) or len(operands) < 2:
                    raise ValueError("$slice requires array and count/position")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )

                # Handle count/position parameters
                count = operands[1]
                skip = operands[2] if len(operands) > 2 else 0

                # SQL implementation using json_group_array and LIMIT/OFFSET
                # Use jsonb_group_array when available for better performance
                # Wrap with json() to convert JSONB binary to text for comparison
                if skip != 0:
                    if self._jsonb_supported:
                        sql = f"(SELECT json({json_group_array}(value)) FROM (SELECT value FROM {json_each}({array_sql}) LIMIT {count} OFFSET {skip}))"
                    else:
                        sql = f"(SELECT {json_group_array}(value) FROM (SELECT value FROM {json_each}({array_sql}) LIMIT {count} OFFSET {skip}))"
                else:
                    if self._jsonb_supported:
                        sql = f"(SELECT json({json_group_array}(value)) FROM (SELECT value FROM {json_each}({array_sql}) LIMIT {count}))"
                    else:
                        sql = f"(SELECT {json_group_array}(value) FROM (SELECT value FROM {json_each}({array_sql}) LIMIT {count}))"
                return sql, array_params
            case "$indexOfArray":
                if len(operands) != 2:
                    raise ValueError(
                        "$indexOfArray requires exactly 2 operands"
                    )
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # Use json_each to find index - use jsonb_each when available
                sql = f"(SELECT key FROM {json_each}({array_sql}) WHERE value = {value_sql} LIMIT 1)"
                return sql, array_params + value_params
            case (
                "$setEquals"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
            ):
                # Set operations are complex - fall back to Python for now
                # Can be implemented in SQL using json_each and json_group_array
                raise NotImplementedError(
                    f"Set operator {operator} not supported in SQL tier (use Python fallback)"
                )
            case _:
                raise NotImplementedError(
                    f"Array operator {operator} not supported in SQL tier"
                )

    def _convert_string_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert string operators to SQL."""
        match operator:
            case "$concat":
                if len(operands) < 1:
                    raise ValueError("$concat requires at least 1 operand")
                sql_parts = []
                all_params = []
                for operand in operands:
                    operand_sql, operand_params = self._convert_operand_to_sql(
                        operand
                    )
                    sql_parts.append(operand_sql)
                    all_params.extend(operand_params)
                sql = f"({' || '.join(sql_parts)})"
                return sql, all_params
            case "$toLower":
                if len(operands) != 1:
                    raise ValueError("$toLower requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"lower({value_sql})"
                return sql, value_params
            case "$toUpper":
                if len(operands) != 1:
                    raise ValueError("$toUpper requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"upper({value_sql})"
                return sql, value_params
            case "$strLenBytes":
                if len(operands) != 1:
                    raise ValueError("$strLenBytes requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"length({value_sql})"
                return sql, value_params
            case "$substr":
                if len(operands) != 3:
                    raise ValueError("$substr requires exactly 3 operands")
                str_sql, str_params = self._convert_operand_to_sql(operands[0])
                start_sql, start_params = self._convert_operand_to_sql(
                    operands[1]
                )
                len_sql, len_params = self._convert_operand_to_sql(operands[2])
                sql = f"substr({str_sql}, {start_sql} + 1, {len_sql})"
                return sql, str_params + start_params + len_params
            case "$trim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$trim requires 'input' field")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                if "chars" in operands:
                    chars_sql, chars_params = self._convert_operand_to_sql(
                        operands["chars"]
                    )
                    sql = f"trim({input_sql}, {chars_sql})"
                    return sql, input_params + chars_params
                else:
                    sql = f"trim({input_sql})"
                    return sql, input_params
            case "$ltrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$ltrim requires 'input' field")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                if "chars" in operands:
                    chars_sql, chars_params = self._convert_operand_to_sql(
                        operands["chars"]
                    )
                    sql = f"ltrim({input_sql}, {chars_sql})"
                    return sql, input_params + chars_params
                else:
                    sql = f"ltrim({input_sql})"
                    return sql, input_params
            case "$rtrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$rtrim requires 'input' field")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                if "chars" in operands:
                    chars_sql, chars_params = self._convert_operand_to_sql(
                        operands["chars"]
                    )
                    sql = f"rtrim({input_sql}, {chars_sql})"
                    return sql, input_params + chars_params
                else:
                    sql = f"rtrim({input_sql})"
                    return sql, input_params
            case "$indexOfBytes":
                if len(operands) < 2:
                    raise ValueError(
                        "$indexOfBytes requires substring and string"
                    )
                substr_sql, substr_params = self._convert_operand_to_sql(
                    operands[0]
                )
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[1]
                )
                sql = f"(instr({string_sql}, {substr_sql}) - 1)"
                return sql, substr_params + string_params
            case "$regexMatch":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexMatch requires 'input' and 'regex'")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                regex = operands.get("regex", "")
                sql = f"({input_sql} REGEXP ?)"
                return sql, input_params + [regex]
            case "$split":
                if len(operands) != 2:
                    raise ValueError("$split requires string and delimiter")
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                delimiter_sql, delimiter_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # SQLite doesn't have native split, use recursive CTE (complex)
                # Fall back to Python for now
                raise NotImplementedError("$split requires Python evaluation")
            case "$replaceAll":
                if len(operands) != 3:
                    raise ValueError(
                        "$replaceAll requires string, find, and replacement"
                    )
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                find_sql, find_params = self._convert_operand_to_sql(
                    operands[1]
                )
                replace_sql, replace_params = self._convert_operand_to_sql(
                    operands[2]
                )
                sql = f"replace({string_sql}, {find_sql}, {replace_sql})"
                return sql, string_params + find_params + replace_params
            case "$replaceOne":
                if len(operands) != 3:
                    raise ValueError(
                        "$replaceOne requires string, find, and replacement"
                    )
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                find_sql, find_params = self._convert_operand_to_sql(
                    operands[1]
                )
                replace_sql, replace_params = self._convert_operand_to_sql(
                    operands[2]
                )
                # SQLite replace replaces all occurrences, same as replaceAll
                # For replaceOne, we need a more complex approach - fall back to Python
                raise NotImplementedError(
                    "$replaceOne not supported in SQL tier (use Python fallback)"
                )
            case "$strLenCP":
                if len(operands) != 1:
                    raise ValueError("$strLenCP requires exactly 1 operand")
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # For BMP characters, length in bytes = length in code points
                sql = f"length({string_sql})"
                return sql, string_params
            case "$indexOfCP":
                if len(operands) < 2:
                    raise ValueError("$indexOfCP requires substring and string")
                substr_sql, substr_params = self._convert_operand_to_sql(
                    operands[0]
                )
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # SQLite instr returns 1-based index, convert to 0-based
                sql = f"instr({string_sql}, {substr_sql}) - 1"
                return sql, substr_params + string_params
            case _:
                raise NotImplementedError(
                    f"String operator {operator} not supported in SQL tier"
                )

    def _convert_math_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert math operators to SQL."""
        match operator:
            case "$pow":
                # Handle $pow separately (requires 2 operands)
                if len(operands) != 2:
                    raise ValueError("$pow requires exactly 2 operands")
                base_sql, base_params = self._convert_operand_to_sql(
                    operands[0]
                )
                exp_sql, exp_params = self._convert_operand_to_sql(operands[1])
                sql = f"pow({base_sql}, {exp_sql})"
                return sql, base_params + exp_params
            case "$log":
                # $log with custom base requires 2 operands: [number, base]
                if len(operands) != 2:
                    raise ValueError(
                        "$log requires exactly 2 operands: [number, base]"
                    )
                number_sql, number_params = self._convert_operand_to_sql(
                    operands[0]
                )
                base_sql, base_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # SQLite: log(base, number)
                sql = f"log({base_sql}, {number_sql})"
                return sql, number_params + base_params
            case _:
                # All other math operators require 1 operand
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")

                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )

                match operator:
                    case "$abs":
                        sql = f"abs({value_sql})"
                    case "$ceil":
                        sql = f"ceil({value_sql})"
                    case "$floor":
                        sql = f"floor({value_sql})"
                    case "$round":
                        sql = f"round({value_sql})"
                    case "$trunc":
                        sql = f"cast({value_sql} as integer)"
                    case "$sqrt":
                        sql = f"sqrt({value_sql})"
                    case "$ln":
                        # Natural logarithm (base e)
                        sql = f"ln({value_sql})"
                    case "$log10":
                        # Base-10 logarithm
                        sql = f"log10({value_sql})"
                    case "$log2":
                        # Base-2 logarithm
                        # Warn about NeoSQLite extension (not in MongoDB)
                        if not self._log2_warned:
                            warnings.warn(
                                "$log2 is a NeoSQLite extension (not available in MongoDB). "
                                "For MongoDB compatibility, use { $log: [ <number>, 2 ] } instead.",
                                UserWarning,
                                stacklevel=4,
                            )
                            self._log2_warned = True
                        sql = f"log2({value_sql})"
                    case "$exp":
                        # Exponential function (e^x)
                        sql = f"exp({value_sql})"
                    case "$sigmoid":
                        # Sigmoid function: 1 / (1 + e^(-x))
                        # Handle object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
                        if isinstance(operands, dict):
                            input_sql, input_params = (
                                self._convert_operand_to_sql(
                                    operands.get("input")
                                )
                            )
                            on_null_sql, on_null_params = (
                                self._convert_operand_to_sql(
                                    operands.get("onNull")
                                )
                            )
                            sql = f"(CASE WHEN {input_sql} IS NULL THEN {on_null_sql} ELSE (1.0 / (1.0 + exp(-({input_sql})))) END)"
                            return sql, input_params + on_null_params
                        sql = f"(1.0 / (1.0 + exp(-({value_sql}))))"
                    case _:
                        raise NotImplementedError(
                            f"Math operator {operator} not supported in SQL tier"
                        )

                return sql, value_params

    def _convert_trig_operator(
        self, operator: str, operands: Any
    ) -> Tuple[str, List[Any]]:
        """Convert trigonometric and hyperbolic operators to SQL.

        Args:
            operator: The trig operator ($sin, $cos, etc.)
            operands: The operand(s). Can be:
                      - A single value (string, number) for simple cases like {"$sin": "$angle"}
                      - A list of values for array format like {"$sin": ["$angle"]}
        """
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$sin: "$angle"} and {$sin: ["$angle"]}
        if not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$atan2":
                # Handle $atan2 separately (requires 2 operands)
                if len(operands) != 2:
                    raise ValueError("$atan2 requires exactly 2 operands")
                y_sql, y_params = self._convert_operand_to_sql(operands[0])
                x_sql, x_params = self._convert_operand_to_sql(operands[1])
                sql = f"atan2({y_sql}, {x_sql})"
                return sql, y_params + x_params
            case _:
                # All other trig operators require 1 operand
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")

                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )

                # Standard trigonometric functions
                match operator:
                    case "$sin":
                        sql_func = "sin"
                    case "$cos":
                        sql_func = "cos"
                    case "$tan":
                        sql_func = "tan"
                    case "$asin":
                        sql_func = "asin"
                    case "$acos":
                        sql_func = "acos"
                    case "$atan":
                        sql_func = "atan"
                    # Hyperbolic functions
                    case "$sinh":
                        sql_func = "sinh"
                    case "$cosh":
                        sql_func = "cosh"
                    case "$tanh":
                        sql_func = "tanh"
                    # Inverse hyperbolic functions
                    case "$asinh":
                        sql_func = "asinh"
                    case "$acosh":
                        sql_func = "acosh"
                    case "$atanh":
                        sql_func = "atanh"
                    case _:
                        raise NotImplementedError(
                            f"Trig operator {operator} not supported in SQL tier"
                        )

                sql = f"{sql_func}({value_sql})"
                return sql, value_params

    def _convert_angle_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert angle conversion operators to SQL."""
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        match operator:
            case "$degreesToRadians":
                # radians = degrees * pi() / 180
                sql = f"({value_sql} * pi() / 180.0)"
            case "$radiansToDegrees":
                # degrees = radians * 180 / pi()
                sql = f"({value_sql} * 180.0 / pi())"
            case _:
                raise NotImplementedError(
                    f"Angle operator {operator} not supported in SQL tier"
                )

        return sql, value_params

    def _convert_date_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert date operators to SQL using strftime."""
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        # SQLite strftime format codes
        match operator:
            case "$year":
                fmt = "%Y"
            case "$month":
                fmt = "%m"
            case "$dayOfMonth":
                fmt = "%d"
            case "$hour":
                fmt = "%H"
            case "$minute":
                fmt = "%M"
            case "$second":
                fmt = "%S"
            case "$dayOfWeek":
                fmt = "%w"
            case "$dayOfYear":
                fmt = "%j"
            case "$week":
                fmt = "%W"
            case "$isoDayOfWeek":
                fmt = "%w"  # SQLite doesn't have ISO directly
            case "$isoWeek":
                fmt = "%W"
            case "$millisecond":
                fmt = "%f"
            case _:
                raise NotImplementedError(
                    f"Date operator {operator} not supported in SQL tier"
                )

        # For numeric results, cast to integer
        if operator == "$millisecond":
            sql = (
                f"cast(strftime('{fmt}', {value_sql}) * 1000 as integer) % 1000"
            )
        else:
            sql = f"cast(strftime('{fmt}', {value_sql}) as integer)"

        return sql, value_params

    def _convert_date_arithmetic_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert $dateAdd/$dateSubtract operators to SQL.

        MongoDB syntax: {$dateAdd: [date, amount, unit]}
        SQLite: datetime(date, '+N unit' or '-N unit')
        """
        if len(operands) < 2 or len(operands) > 3:
            raise ValueError(
                f"{operator} requires 2-3 operands: [date, amount, unit]"
            )

        date_sql, date_params = self._convert_operand_to_sql(operands[0])
        amount = operands[1]  # Should be a literal number
        unit = operands[2] if len(operands) > 2 else "day"  # Default to days

        # Validate unit
        valid_units = (
            "day",
            "hour",
            "minute",
            "second",
            "week",
            "month",
            "year",
        )
        if not isinstance(unit, str) or unit not in valid_units:
            raise ValueError(f"{operator} unit must be one of: {valid_units}")

        # Handle year/month specially (SQLite doesn't support directly)
        if unit == "year":
            amount = amount * 12
            unit = "month"

        # Determine sign based on operator
        sign = "+" if operator == "$dateAdd" else "-"

        # Handle week conversion to days
        sqlite_unit = unit
        if unit == "week":
            sqlite_unit = "day"
            if isinstance(amount, (int, float)):
                amount = amount * 7

        # Build the modifier
        if isinstance(amount, (int, float)):
            modifier = f"'{sign}{amount} {sqlite_unit}s'"
            sql = f"datetime({date_sql}, {modifier})"
            return sql, date_params
        else:
            # Amount is a field reference - need to use CASE or build dynamically
            # For simplicity, we'll use printf to build the modifier
            amount_sql, amount_params = self._convert_operand_to_sql(
                operands[1]
            )
            if sign == "-":
                amount_sql = f"-({amount_sql})"
            sql = f"datetime({date_sql}, printf('%+d {sqlite_unit}s', {amount_sql}))"
            return sql, date_params + amount_params

    def _convert_date_diff_operator(
        self, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert $dateDiff operator to SQL.

        MongoDB syntax: {$dateDiff: [date1, date2, unit]}
        SQLite: julianday(date2) - julianday(date1) for days
        """
        if len(operands) < 2 or len(operands) > 3:
            raise ValueError(
                "$dateDiff requires 2-3 operands: [date1, date2, unit]"
            )

        date1_sql, date1_params = self._convert_operand_to_sql(operands[0])
        date2_sql, date2_params = self._convert_operand_to_sql(operands[1])
        unit = operands[2] if len(operands) > 2 else "day"

        # Validate unit
        valid_units = (
            "day",
            "hour",
            "minute",
            "second",
            "week",
            "month",
            "year",
        )
        if not isinstance(unit, str) or unit not in valid_units:
            raise ValueError(f"$dateDiff unit must be one of: {valid_units}")

        # Base calculation: difference in days
        sql = f"(julianday({date2_sql}) - julianday({date1_sql}))"

        # Convert to requested unit
        unit_multipliers = {
            "day": 1,
            "week": 1.0 / 7,
            "month": 1.0 / 30.4375,  # Average days per month
            "year": 1.0 / 365.25,
            "hour": 24,
            "minute": 24 * 60,
            "second": 24 * 60 * 60,
        }

        multiplier = unit_multipliers.get(unit, 1)
        if multiplier != 1:
            sql = f"cast({sql} * {multiplier} as integer)"
        else:
            sql = f"cast({sql} as integer)"

        return sql, date1_params + date2_params

    def _convert_object_operator(
        self, operator: str, operands: Any
    ) -> Tuple[str, List[Any]]:
        """Convert object operators to SQL.

        Note: json_patch() works with both JSON and JSONB data types.
        Only json_extract/jsonb_extract, json_set/jsonb_set have JSONB variants.
        """
        json_prefix = self.json_function_prefix

        match operator:
            case "$mergeObjects":
                if not isinstance(operands, list) or len(operands) < 1:
                    raise ValueError("$mergeObjects requires a list of objects")
                sql_parts = []
                all_params = []
                for obj in operands:
                    obj_sql, obj_params = self._convert_operand_to_sql(obj)
                    sql_parts.append(obj_sql)
                    all_params.extend(obj_params)
                # Use json_patch to merge objects (works with both JSON and JSONB)
                if len(sql_parts) == 1:
                    sql = sql_parts[0]
                else:
                    sql = f"json_patch({sql_parts[0]}, {sql_parts[1]})"
                    for part in sql_parts[2:]:
                        sql = f"json_patch({sql}, {part})"
                return sql, all_params
            case "$getField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError("$getField requires 'field' specification")
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    input_sql, input_params = self._convert_operand_to_sql(
                        input_val
                    )
                else:
                    input_sql, input_params = self.data_column, []
                sql = f"{json_prefix}_extract({input_sql}, '$.{field}')"
                return sql, input_params
            case "$setField":
                if not isinstance(operands, dict):
                    raise ValueError("$setField requires a dictionary")
                field = operands.get("field")
                value = operands.get("value")
                input_val = operands.get("input")
                if field is None:
                    raise ValueError("$setField requires 'field'")
                if input_val is not None:
                    input_sql, input_params = self._convert_operand_to_sql(
                        input_val
                    )
                else:
                    input_sql, input_params = self.data_column, []
                value_sql, value_params = self._convert_operand_to_sql(value)
                sql = (
                    f"{json_prefix}_set({input_sql}, '$.{field}', {value_sql})"
                )
                return sql, input_params + value_params
            case "$unsetField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError(
                        "$unsetField requires 'field' specification"
                    )
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    input_sql, input_params = self._convert_operand_to_sql(
                        input_val
                    )
                else:
                    input_sql, input_params = self.data_column, []
                # Use json_remove to remove field
                sql = f"{json_prefix}_remove({input_sql}, '$.{field}')"
                return sql, input_params
            case "$objectToArray":
                # Complex - convert object keys/values to array of {k, v} objects
                # Fall back to Python for now
                raise NotImplementedError(
                    "$objectToArray not supported in SQL tier (use Python fallback)"
                )
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in SQL tier"
                )

    def _convert_type_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert type conversion operators to SQL."""
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        match operator:
            case "$toString":
                # Cast to text
                sql = f"cast({value_sql} as text)"
            case "$toInt":
                # Cast to integer
                sql = f"cast({value_sql} as integer)"
            case "$toDouble":
                # Cast to real/float
                sql = f"cast({value_sql} as real)"
            case "$toLong":
                # SQLite integers are already 64-bit, same as toInt
                sql = f"cast({value_sql} as integer)"
            case "$toBool":
                # Convert to boolean (0 or 1)
                sql = f"CASE WHEN {value_sql} THEN 1 ELSE 0 END"
            case "$toDecimal":
                # SQLite doesn't have native Decimal128, use REAL
                raise NotImplementedError(
                    "$toDecimal not supported in SQL tier (SQLite lacks Decimal128)"
                )
            case "$toObjectId":
                # Cannot convert to ObjectId in SQL
                raise NotImplementedError(
                    "$toObjectId not supported in SQL tier (use Python fallback)"
                )
            case "$convert":
                # $convert is complex - requires 'to' field specification
                # Fall back to Python
                raise NotImplementedError(
                    "$convert not supported in SQL tier (use Python fallback)"
                )
            case "$toBinData":
                # Cannot convert to binary in SQL
                raise NotImplementedError(
                    "$toBinData not supported in SQL tier (use Python fallback)"
                )
            case "$toRegex":
                # Cannot convert to regex in SQL
                raise NotImplementedError(
                    "$toRegex not supported in SQL tier (use Python fallback)"
                )
            case _:
                raise NotImplementedError(
                    f"Type operator {operator} not supported in SQL tier"
                )

        return sql, value_params

    def _convert_operand_to_sql(self, operand: Any) -> Tuple[str, List[Any]]:
        """
        Convert an operand to SQL expression.

        Handles:
        - Field references: "$field" → json_extract/jsonb_extract expression
        - Literals: numbers, strings, booleans
        - Nested expressions: {"$operator": [...]}
        """
        match operand:
            case str() if operand.startswith("$"):
                # Field reference
                field_path = operand[1:]  # Remove $
                # Use dynamic json/jsonb prefix based on support
                json_path_expr = build_json_extract_expression(
                    self.data_column, field_path
                )
                # Replace hardcoded "json_extract" with dynamic prefix
                if self._jsonb_supported:
                    json_path_expr = json_path_expr.replace(
                        "json_extract", "jsonb_extract"
                    )
                return json_path_expr, []

            case list() | dict():
                # Check if it's an expression (dict with single key starting with $)
                if isinstance(operand, dict) and len(operand) == 1:
                    key = next(iter(operand.keys()))
                    if key.startswith("$"):
                        return self._convert_expr_to_sql(operand)

                # Literal list or dict - convert to JSON for SQL
                from neosqlite.collection.json_helpers import (
                    neosqlite_json_dumps,
                )

                return "json(?)", [neosqlite_json_dumps(operand)]

            case _:
                # Literal value (scalar)
                return "?", [operand]

    def _map_comparison_operator(self, op: str) -> str:
        """Map MongoDB comparison operators to SQL."""
        mapping = {
            "$eq": "=",
            "$gt": ">",
            "$gte": ">=",
            "$lt": "<",
            "$lte": "<=",
            "$ne": "!=",
        }
        return mapping.get(op, op)

    def _map_arithmetic_operator(self, op: str) -> str:
        """Map MongoDB arithmetic operators to SQL."""
        mapping = {
            "$add": "+",
            "$subtract": "-",
            "$multiply": "*",
            "$divide": "/",
            "$mod": "%",
        }
        return mapping.get(op, op)

    def evaluate_python(
        self, expr: Dict[str, Any], document: Dict[str, Any]
    ) -> bool:
        """
        Python fallback evaluation for $expr.

        This ensures identical results to SQL evaluation and provides
        the kill switch functionality.

        Args:
            expr: The $expr expression
            document: Document to evaluate against

        Returns:
            Boolean result of expression evaluation
        """
        result = self._evaluate_expr_python(expr, document)
        # For boolean context, ensure we return a boolean
        if isinstance(result, bool):
            return result
        # For comparison results (like $cmp), convert to boolean context
        return bool(result)

    def _evaluate_expr_python(
        self, expr: Dict[str, Any], document: Dict[str, Any]
    ) -> Any:
        """Recursively evaluate expression in Python."""
        if not isinstance(expr, dict) or len(expr) != 1:
            raise ValueError("Invalid $expr expression structure")

        operator, operands = next(iter(expr.items()))

        # Handle different operator types
        match operator:
            case "$and" | "$or" | "$not" | "$nor":
                return self._evaluate_logical_python(
                    operator, operands, document
                )
            case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne":
                return self._evaluate_comparison_python(
                    operator, operands, document
                )
            case "$cmp":
                return self._evaluate_cmp_python(operands, document)
            case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                return self._evaluate_arithmetic_python(
                    operator, operands, document
                )
            case "$abs" | "$ceil" | "$floor" | "$round" | "$trunc":
                return self._evaluate_math_python(operator, operands, document)
            case "$ln" | "$log" | "$log10" | "$log2" | "$exp" | "$sigmoid":
                return self._evaluate_math_python(operator, operands, document)
            case "$pow":
                return self._evaluate_pow_python(operands, document)
            case "$sqrt":
                return self._evaluate_sqrt_python(operands, document)
            case (
                "$sin"
                | "$cos"
                | "$tan"
                | "$asin"
                | "$acos"
                | "$atan"
                | "$atan2"
                | "$sinh"
                | "$cosh"
                | "$tanh"
                | "$asinh"
                | "$acosh"
                | "$atanh"
            ):
                return self._evaluate_trig_python(operator, operands, document)
            case "$degreesToRadians" | "$radiansToDegrees":
                return self._evaluate_angle_python(operator, operands, document)
            case "$cond":
                return self._evaluate_cond_python(operands, document)
            case "$ifNull":
                return self._evaluate_ifNull_python(operands, document)
            case "$switch":
                return self._evaluate_switch_python(operands, document)
            case (
                "$size"
                | "$in"
                | "$isArray"
                | "$arrayElemAt"
                | "$first"
                | "$last"
                | "$slice"
                | "$indexOfArray"
                | "$sum"
                | "$avg"
                | "$min"
                | "$max"
                | "$setEquals"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
            ):
                return self._evaluate_array_python(operator, operands, document)
            case "$filter" | "$map" | "$reduce":
                return self._evaluate_array_transform_python(
                    operator, operands, document
                )
            case (
                "$concat"
                | "$toLower"
                | "$toUpper"
                | "$strLenBytes"
                | "$substr"
                | "$trim"
                | "$ltrim"
                | "$rtrim"
                | "$indexOfBytes"
                | "$regexMatch"
                | "$regexFind"
                | "$regexFindAll"
                | "$split"
                | "$replaceAll"
                | "$replaceOne"
                | "$strLenCP"
                | "$indexOfCP"
            ):
                return self._evaluate_string_python(
                    operator, operands, document
                )
            case (
                "$year"
                | "$month"
                | "$dayOfMonth"
                | "$hour"
                | "$minute"
                | "$second"
                | "$dayOfWeek"
                | "$dayOfYear"
                | "$week"
                | "$isoDayOfWeek"
                | "$isoWeek"
                | "$millisecond"
            ):
                return self._evaluate_date_python(operator, operands, document)
            case "$dateAdd" | "$dateSubtract" | "$dateDiff":
                return self._evaluate_date_arithmetic_python(
                    operator, operands, document
                )
            case (
                "$mergeObjects"
                | "$getField"
                | "$setField"
                | "$unsetField"
                | "$objectToArray"
            ):
                return self._evaluate_object_python(
                    operator, operands, document
                )
            case (
                "$type"
                | "$toString"
                | "$toInt"
                | "$toDouble"
                | "$toBool"
                | "$toLong"
                | "$toDecimal"
                | "$toObjectId"
                | "$toBinData"
                | "$toRegex"
                | "$convert"
            ):
                return self._evaluate_type_python(operator, operands, document)
            case "$literal":
                return self._evaluate_literal_python(operands, document)
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in Python evaluation"
                )

    def _evaluate_logical_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> bool:
        """Evaluate logical operators in Python."""
        if operator == "$not":
            if len(operands) != 1:
                raise ValueError("$not requires exactly one operand")
            return not self._evaluate_expr_python(operands[0], document)

        results = [self._evaluate_expr_python(op, document) for op in operands]

        match operator:
            case "$and":
                return all(results)
            case "$or":
                return any(results)
            case "$nor":
                return not any(results)
            case _:
                raise ValueError(f"Unknown logical operator: {operator}")

    def _evaluate_comparison_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> bool:
        """Evaluate comparison operators in Python."""
        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        match operator:
            case "$eq":
                return left == right
            case "$gt":
                return left > right
            case "$gte":
                return left >= right
            case "$lt":
                return left < right
            case "$lte":
                return left <= right
            case "$ne":
                return left != right
            case _:
                raise ValueError(f"Unknown comparison operator: {operator}")

    def _evaluate_cmp_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> int:
        """Evaluate $cmp operator in Python."""
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        if left < right:
            return -1
        elif left > right:
            return 1
        else:
            return 0

    def _evaluate_arithmetic_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate arithmetic operators in Python.

        Note: In MongoDB, arithmetic operations with null return null.
        """
        values = [
            self._evaluate_operand_python(op, document) for op in operands
        ]

        # If any operand is None, return None (MongoDB behavior)
        if any(v is None for v in values):
            return None

        match operator:
            case "$add":
                return sum(values)
            case "$subtract":
                return values[0] - sum(values[1:])
            case "$multiply":
                result = 1
                for v in values:
                    result *= v
                return result
            case "$divide":
                result = values[0]
                for v in values[1:]:
                    if v == 0:
                        return None  # Division by zero
                    result /= v
                return result
            case "$mod":
                if len(values) != 2 or values[1] == 0:
                    return None
                return values[0] % values[1]
            case _:
                raise ValueError(f"Unknown arithmetic operator: {operator}")

    def _evaluate_math_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate math operators in Python."""
        # Handle $log with custom base separately (requires 2 operands)
        if operator == "$log":
            if not isinstance(operands, list) or len(operands) != 2:
                raise ValueError(
                    "$log requires exactly 2 operands: [number, base]"
                )
            number = self._evaluate_operand_python(operands[0], document)
            base = self._evaluate_operand_python(operands[1], document)
            if number is None or base is None:
                return None
            if number <= 0 or base <= 1:
                return None
            return math.log(number, base)

        # $sigmoid can be either simple form or object form with onNull
        if operator == "$sigmoid":
            # Object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
            if isinstance(operands, dict):
                # Handled in the operator-specific section below
                pass
            else:
                # Simple format: { $sigmoid: <expr> }
                operands = (
                    [operands] if not isinstance(operands, list) else operands
                )

        # Handle both list and single operand formats for other operators
        if operator != "$sigmoid" and not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1 and operator != "$sigmoid":
            raise ValueError(f"{operator} requires exactly 1 operand")

        if operator != "$sigmoid":
            value = self._evaluate_operand_python(operands[0], document)
        else:
            value = (
                self._evaluate_operand_python(operands[0], document)
                if isinstance(operands, list)
                else None
            )

        match operator:
            case "$abs":
                return abs(value) if value is not None else None
            case "$ceil":
                return math.ceil(value) if value is not None else None
            case "$floor":
                return math.floor(value) if value is not None else None
            case "$round":
                return round(value) if value is not None else None
            case "$trunc":
                return int(value) if value is not None else None
            case "$ln":
                # Natural logarithm (base e)
                return (
                    math.log(value) if value is not None and value > 0 else None
                )
            case "$log10":
                # Base-10 logarithm
                return (
                    math.log10(value)
                    if value is not None and value > 0
                    else None
                )
            case "$log2":
                # Base-2 logarithm
                # Warn about NeoSQLite extension (not in MongoDB)
                if not self._log2_warned:
                    warnings.warn(
                        "$log2 is a NeoSQLite extension (not available in MongoDB). "
                        "For MongoDB compatibility, use { $log: [ <number>, 2 ] } instead.",
                        UserWarning,
                        stacklevel=4,
                    )
                    self._log2_warned = True
                return (
                    math.log2(value)
                    if value is not None and value > 0
                    else None
                )
            case "$exp":
                # Exponential function (e^x)
                return math.exp(value) if value is not None else None
            case "$sigmoid":
                # Sigmoid function: 1 / (1 + e^(-x))
                # Handle object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
                if isinstance(operands, dict):
                    input_val = self._evaluate_operand_python(
                        operands.get("input"), document
                    )
                    on_null = operands.get("onNull")
                    if input_val is None:
                        return self._evaluate_operand_python(on_null, document)
                    return 1.0 / (1.0 + math.exp(-input_val))
                if value is None:
                    return None
                return 1.0 / (1.0 + math.exp(-value))
            case _:
                raise ValueError(f"Unknown math operator: {operator}")

    def _evaluate_pow_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate $pow operator in Python."""
        if len(operands) != 2:
            raise ValueError("$pow requires exactly 2 operands")
        base = self._evaluate_operand_python(operands[0], document)
        exponent = self._evaluate_operand_python(operands[1], document)
        if base is None or exponent is None:
            return None
        return pow(base, exponent)

    def _evaluate_sqrt_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate $sqrt operator in Python."""
        if len(operands) != 1:
            raise ValueError("$sqrt requires exactly 1 operand")
        value = self._evaluate_operand_python(operands[0], document)

        return math.sqrt(value) if value is not None and value >= 0 else None

    def _evaluate_trig_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate trigonometric operators in Python."""

        # Handle both list and single operand formats
        if not isinstance(operands, list):
            operands = [operands]

        # Handle $atan2 separately (requires 2 operands)
        if operator == "$atan2":
            if len(operands) != 2:
                raise ValueError("$atan2 requires exactly 2 operands")
            y = self._evaluate_operand_python(operands[0], document)
            x = self._evaluate_operand_python(operands[1], document)
            if y is None or x is None:
                return None
            return math.atan2(y, x)

        # All other trig operators require 1 operand
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)
        if value is None:
            return None

        match operator:
            case "$sin":
                return math.sin(value)
            case "$cos":
                return math.cos(value)
            case "$tan":
                return math.tan(value)
            case "$asin":
                return math.asin(value) if -1 <= value <= 1 else None
            case "$acos":
                return math.acos(value) if -1 <= value <= 1 else None
            case "$atan":
                return math.atan(value)
            # Hyperbolic functions
            case "$sinh":
                return math.sinh(value)
            case "$cosh":
                return math.cosh(value)
            case "$tanh":
                return math.tanh(value)
            # Inverse hyperbolic functions
            case "$asinh":
                return math.asinh(value)
            case "$acosh":
                return math.acosh(value) if value >= 1 else None
            case "$atanh":
                return math.atanh(value) if -1 < value < 1 else None
            case _:
                raise ValueError(f"Unknown trig operator: {operator}")

    def _evaluate_angle_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate angle conversion operators in Python."""

        # Handle both list and single operand formats
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)
        if value is None:
            return None

        match operator:
            case "$degreesToRadians":
                return math.radians(value)
            case "$radiansToDegrees":
                return math.degrees(value)
            case _:
                raise ValueError(f"Unknown angle operator: {operator}")

    def _evaluate_cond_python(
        self, operands: Dict[str, Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $cond operator in Python."""
        if not isinstance(operands, dict):
            # Handle array format: [condition, true_case, false_case]
            if isinstance(operands, list) and len(operands) == 3:
                condition = self._evaluate_expr_python(operands[0], document)
                if condition:
                    return self._evaluate_operand_python(operands[1], document)
                else:
                    return self._evaluate_operand_python(operands[2], document)
            raise ValueError("$cond requires a dictionary or 3-element array")

        if "if" not in operands or "then" not in operands:
            raise ValueError("$cond requires 'if' and 'then' fields")

        condition = self._evaluate_expr_python(operands["if"], document)
        if condition:
            return self._evaluate_operand_python(operands["then"], document)
        elif "else" in operands:
            return self._evaluate_operand_python(operands["else"], document)
        else:
            return None

    def _evaluate_ifNull_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $ifNull operator in Python."""
        if not isinstance(operands, list) or len(operands) != 2:
            raise ValueError("$ifNull requires exactly 2 operands")

        expr = self._evaluate_operand_python(operands[0], document)
        if expr is not None:
            return expr
        return self._evaluate_operand_python(operands[1], document)

    def _evaluate_switch_python(
        self, operands: Dict[str, Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $switch operator in Python."""
        if not isinstance(operands, dict):
            raise ValueError("$switch requires a dictionary")

        branches = operands.get("branches", [])
        default = operands.get("default")

        for branch in branches:
            if not isinstance(branch, dict):
                continue
            case = branch.get("case")
            then = branch.get("then")
            if case is not None and self._evaluate_expr_python(case, document):
                return self._evaluate_operand_python(then, document)

        if default is not None:
            return self._evaluate_operand_python(default, document)
        return None

    def _evaluate_array_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate array operators in Python."""
        match operator:
            case "$size":
                if len(operands) != 1:
                    raise ValueError("$size requires exactly 1 operand")
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return len(array)
                return None
            case "$in":
                if len(operands) != 2:
                    raise ValueError("$in requires exactly 2 operands")
                value = self._evaluate_operand_python(operands[0], document)
                array = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list):
                    return value in array
                return False
            case "$isArray":
                if len(operands) != 1:
                    raise ValueError("$isArray requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return isinstance(value, list)
            case "$sum" | "$avg" | "$min" | "$max":
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")
                array = self._evaluate_operand_python(operands[0], document)
                if not isinstance(array, list):
                    return 0 if operator == "$sum" else None

                # Filter numeric values for sum/avg
                nums = [
                    v
                    for v in array
                    if isinstance(v, (int, float)) and not isinstance(v, bool)
                ]

                if not nums:
                    if operator == "$sum":
                        return 0
                    return None

                match operator:
                    case "$sum":
                        return sum(nums)
                    case "$avg":
                        return sum(nums) / len(nums)
                    case "$min":
                        return min(array)  # min/max work on all types
                    case "$max":
                        return max(array)
                    case _:
                        return None
            case "$arrayElemAt":
                if len(operands) != 2:
                    raise ValueError("$arrayElemAt requires exactly 2 operands")
                array = self._evaluate_operand_python(operands[0], document)
                index = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list) and isinstance(index, int):
                    try:
                        return array[index]
                    except IndexError:
                        return None
                return None
            case "$first":
                if len(operands) != 1:
                    raise ValueError("$first requires exactly 1 operand")
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list) and len(array) > 0:
                    return array[0]
                return None
            case "$last":
                if len(operands) != 1:
                    raise ValueError("$last requires exactly 1 operand")
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list) and len(array) > 0:
                    return array[-1]
                return None
            case "$slice":
                if not isinstance(operands, list) or len(operands) < 2:
                    raise ValueError("$slice requires array and count/position")
                array = self._evaluate_operand_python(operands[0], document)
                count = self._evaluate_operand_python(operands[1], document)
                if not isinstance(array, list):
                    return []
                if len(operands) >= 3:
                    skip = self._evaluate_operand_python(operands[2], document)
                    return array[skip : skip + count]
                elif isinstance(count, int) and count < 0:
                    return array[count:]
                else:
                    return array[:count]
            case "$indexOfArray":
                if len(operands) != 2:
                    raise ValueError(
                        "$indexOfArray requires exactly 2 operands"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                value = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list):
                    try:
                        return array.index(value)
                    except ValueError:
                        return -1
                return -1
            case "$setEquals":
                if len(operands) != 2:
                    raise ValueError("$setEquals requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return set(set1) == set(set2)
                return False
            case "$setIntersection":
                if len(operands) != 2:
                    raise ValueError(
                        "$setIntersection requires exactly 2 operands"
                    )
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) & set(set2))
                return []
            case "$setUnion":
                if len(operands) != 2:
                    raise ValueError("$setUnion requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) | set(set2))
                return []
            case "$setDifference":
                if len(operands) != 2:
                    raise ValueError(
                        "$setDifference requires exactly 2 operands"
                    )
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) - set(set2))
                return []
            case "$setIsSubset":
                if len(operands) != 2:
                    raise ValueError("$setIsSubset requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return set(set1).issubset(set(set2))
                return False
            case "$anyElementTrue":
                if len(operands) != 1:
                    raise ValueError(
                        "$anyElementTrue requires exactly 1 operand"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return any(array)
                return False
            case "$allElementsTrue":
                if len(operands) != 1:
                    raise ValueError(
                        "$allElementsTrue requires exactly 1 operand"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return all(array)
                return False
            case _:
                raise NotImplementedError(
                    f"Array operator {operator} not supported in Python evaluation"
                )

    def _evaluate_array_transform_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate $filter, $map, $reduce operators in Python.

        These operators use variable scoping:
        - $filter: {input: <array>, as: <var>, cond: <expr>}
        - $map: {input: <array>, as: <var>, in: <expr>}
        - $reduce: {input: <array>, initialValue: <val>, in: <expr>}
        """
        match operator:
            case "$filter":
                if not isinstance(operands, dict):
                    raise ValueError("$filter requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return []

                as_var = operands.get("as", "item")
                cond = operands.get("cond")

                if cond is None:
                    raise ValueError("$filter requires 'cond' expression")

                result = []
                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    ctx = dict(document)
                    ctx[f"$${as_var}"] = item
                    ctx[f"$${as_var}Index"] = i

                    # Evaluate condition in context
                    if self._evaluate_expr_python(cond, ctx):
                        result.append(item)

                return result

            case "$map":
                if not isinstance(operands, dict):
                    raise ValueError("$map requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return []

                as_var = operands.get("as", "item")
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$map requires 'in' expression")

                result = []
                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    ctx = dict(document)
                    ctx[f"$${as_var}"] = item
                    ctx[f"$${as_var}Index"] = i

                    # Evaluate expression in context
                    result.append(self._evaluate_operand_python(in_expr, ctx))

                return result

            case "$reduce":
                if not isinstance(operands, dict):
                    raise ValueError("$reduce requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return None

                initial_value = operands.get("initialValue")
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$reduce requires 'in' expression")

                # Evaluate initial value
                acc = self._evaluate_operand_python(initial_value, document)

                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    # $$value is the accumulator, $$this is the current item
                    ctx = dict(document)
                    ctx["$$value"] = acc
                    ctx["$$this"] = item
                    ctx["$$index"] = i

                    # Evaluate expression in context to get new accumulator value
                    acc = self._evaluate_operand_python(in_expr, ctx)

                return acc

            case _:
                raise NotImplementedError(
                    f"Array transform operator {operator} not supported in Python evaluation"
                )

    def _evaluate_string_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate string operators in Python.

        Args:
            operator: The string operator ($toUpper, $toLower, etc.)
            operands: The operand(s). Can be:
                      - A single value for simple cases like {"$toUpper": "$field"}
                      - A list of values for array format
                      - A dict for operators like $trim, $regexMatch
            document: The document to evaluate against
        """
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$toUpper: "$field"} and {$toUpper: ["$field"]}
        # But some operators like $trim, $regexMatch use dict format
        if operator in (
            "$trim",
            "$ltrim",
            "$rtrim",
            "$regexMatch",
            "$regexFind",
            "$regexFindAll",
        ):
            # These operators use dict format, don't normalize
            pass
        elif not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$concat":
                values = [
                    self._evaluate_operand_python(op, document)
                    for op in operands
                ]
                return "".join(str(v) if v is not None else "" for v in values)
            case "$toLower":
                if len(operands) != 1:
                    raise ValueError("$toLower requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value).lower() if value is not None else None
            case "$toUpper":
                if len(operands) != 1:
                    raise ValueError("$toUpper requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value).upper() if value is not None else None
            case "$strLenBytes":
                if len(operands) != 1:
                    raise ValueError("$strLenBytes requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return (
                    len(str(value).encode("utf-8"))
                    if value is not None
                    else None
                )
            case "$substr":
                if len(operands) != 3:
                    raise ValueError("$substr requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if (
                    string is not None
                    and start is not None
                    and length is not None
                ):
                    return str(string)[int(start) : int(start) + int(length)]
                return None
            case "$trim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$trim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).strip(str(chars_val))
                return str(input_val).strip()
            case "$ltrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$ltrim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).lstrip(str(chars_val))
                return str(input_val).lstrip()
            case "$rtrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$rtrim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).rstrip(str(chars_val))
                return str(input_val).rstrip()
            case "$indexOfBytes":
                if len(operands) < 2:
                    raise ValueError(
                        "$indexOfBytes requires substring and string"
                    )
                substr = self._evaluate_operand_python(operands[0], document)
                string = self._evaluate_operand_python(operands[1], document)
                if substr is None or string is None:
                    return -1
                idx = str(string).find(str(substr))
                return idx
            case "$regexMatch":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexMatch requires 'input' and 'regex'")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                if input_val is None:
                    return False
                import re

                return bool(re.search(regex, str(input_val)))
            case "$split":
                if len(operands) != 2:
                    raise ValueError("$split requires string and delimiter")
                string = self._evaluate_operand_python(operands[0], document)
                delimiter = self._evaluate_operand_python(operands[1], document)
                if string is None or delimiter is None:
                    return []
                return str(string).split(str(delimiter))
            case "$replaceAll":
                if len(operands) != 3:
                    raise ValueError(
                        "$replaceAll requires string, find, and replacement"
                    )
                string = self._evaluate_operand_python(operands[0], document)
                find = self._evaluate_operand_python(operands[1], document)
                replacement = self._evaluate_operand_python(
                    operands[2], document
                )
                if string is None:
                    return None
                return str(string).replace(str(find), str(replacement))
            case "$replaceOne":
                if len(operands) != 3:
                    raise ValueError(
                        "$replaceOne requires string, find, and replacement"
                    )
                string = self._evaluate_operand_python(operands[0], document)
                find = self._evaluate_operand_python(operands[1], document)
                replacement = self._evaluate_operand_python(
                    operands[2], document
                )
                if string is None:
                    return None
                # Replace only first occurrence
                return str(string).replace(str(find), str(replacement), 1)
            case "$strLenCP":
                # String length in code points (Unicode characters)
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$strLenCP requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                return len(str(value))
            case "$substrCP":
                # Substring by code points (not implemented - use $substr)
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 3:
                    raise ValueError("$substrCP requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if (
                    string is not None
                    and start is not None
                    and length is not None
                ):
                    # For BMP characters, this is the same as $substr
                    # For full Unicode support, would need proper code point handling
                    return str(string)[int(start) : int(start) + int(length)]
                return None
            case "$indexOfCP":
                # Find substring by code points
                if len(operands) < 2:
                    raise ValueError("$indexOfCP requires substring and string")
                substr = self._evaluate_operand_python(operands[0], document)
                string = self._evaluate_operand_python(operands[1], document)
                if substr is None or string is None:
                    return -1
                idx = str(string).find(str(substr))
                return idx
            case "$regexFind":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexFind requires 'input' and 'regex'")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return None

                import re

                flags = 0
                if "i" in options:
                    flags |= re.IGNORECASE
                if "m" in options:
                    flags |= re.MULTILINE
                if "s" in options:
                    flags |= re.DOTALL

                match = re.search(regex, str(input_val), flags)
                if match:
                    result = {
                        "match": match.group(),
                        "index": match.start(),
                    }
                    if match.groups():
                        result["captures"] = list(match.groups())
                    return result
                return None
            case "$regexFindAll":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError(
                        "$regexFindAll requires 'input' and 'regex'"
                    )
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return []

                import re

                flags = 0
                if "i" in options:
                    flags |= re.IGNORECASE
                if "m" in options:
                    flags |= re.MULTILINE
                if "s" in options:
                    flags |= re.DOTALL

                matches = list(re.finditer(regex, str(input_val), flags))
                all_results: List[Dict[str, Any]] = []
                for match in matches:
                    match_obj: Dict[str, Any] = {
                        "match": match.group(),
                        "index": match.start(),
                    }
                    if match.groups():
                        match_obj["captures"] = list(match.groups())
                    all_results.append(match_obj)
                return all_results
            case _:
                raise NotImplementedError(
                    f"String operator {operator} not supported in Python evaluation"
                )

    def _evaluate_date_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Optional[int]:
        """Evaluate date operators in Python."""
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)
        if value is None:
            return None

        # Parse date value (handle string or datetime)
        from datetime import datetime

        if isinstance(value, str):
            # Try to parse ISO format
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        elif isinstance(value, datetime):
            dt = value
        else:
            return None

        # Extract date components
        match operator:
            case "$year":
                return dt.year
            case "$month":
                return dt.month
            case "$dayOfMonth":
                return dt.day
            case "$hour":
                return dt.hour
            case "$minute":
                return dt.minute
            case "$second":
                return dt.second
            case "$millisecond":
                return dt.microsecond // 1000
            case "$dayOfWeek":
                return dt.weekday()  # 0=Monday
            case "$dayOfYear":
                return dt.timetuple().tm_yday
            case "$week":
                return dt.isocalendar()[1]
            case "$isoDayOfWeek":
                return dt.isocalendar()[2]  # 1=Monday
            case "$isoWeek":
                return dt.isocalendar()[1]
            case _:
                raise NotImplementedError(
                    f"Date operator {operator} not supported in Python evaluation"
                )

    def _evaluate_date_arithmetic_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $dateAdd, $dateSubtract, $dateDiff operators in Python."""
        from datetime import datetime, timedelta

        match operator:
            case "$dateAdd" | "$dateSubtract":
                if len(operands) < 2 or len(operands) > 3:
                    raise ValueError(
                        f"{operator} requires 2-3 operands: [date, amount, unit]"
                    )

                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None

                amount = self._evaluate_operand_python(operands[1], document)
                if amount is None:
                    return None

                unit = operands[2] if len(operands) > 2 else "day"

                # Parse date value
                if isinstance(value, str):
                    try:
                        dt = datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None
                elif isinstance(value, datetime):
                    dt = value
                else:
                    return None

                # Create timedelta based on unit
                if unit == "year":
                    # Handle years separately (not supported by timedelta directly)
                    years = amount if operator == "$dateAdd" else -amount
                    try:
                        new_dt = dt.replace(year=dt.year + int(years))
                        dt = new_dt
                    except ValueError:
                        # Handle Feb 29 edge case
                        new_dt = dt.replace(year=dt.year + int(years), day=28)
                        dt = new_dt
                elif unit == "month":
                    # Handle months separately
                    months = amount if operator == "$dateAdd" else -amount
                    new_month = dt.month + int(months)
                    new_year = dt.year + (new_month - 1) // 12
                    new_month = ((new_month - 1) % 12) + 1
                    try:
                        dt = dt.replace(year=new_year, month=new_month)
                    except ValueError:
                        # Handle day overflow (e.g., Jan 31 + 1 month)
                        import calendar

                        last_day = calendar.monthrange(new_year, new_month)[1]
                        dt = dt.replace(
                            year=new_year,
                            month=new_month,
                            day=min(dt.day, last_day),
                        )
                else:
                    # Convert to timedelta
                    delta_kwargs = {
                        f"{unit}s": (
                            amount if operator == "$dateAdd" else -amount
                        )
                    }
                    delta = timedelta(**delta_kwargs)
                    dt = dt + delta

                # Return ISO format string
                return dt.isoformat()

            case "$dateDiff":
                if len(operands) < 2 or len(operands) > 3:
                    raise ValueError(
                        "$dateDiff requires 2-3 operands: [date1, date2, unit]"
                    )

                date1 = self._evaluate_operand_python(operands[0], document)
                date2 = self._evaluate_operand_python(operands[1], document)
                unit = operands[2] if len(operands) > 2 else "day"

                if date1 is None or date2 is None:
                    return None

                # Parse dates
                def parse_date(val):
                    if isinstance(val, str):
                        try:
                            return datetime.fromisoformat(
                                val.replace("Z", "+00:00")
                            )
                        except ValueError:
                            return None
                    elif isinstance(val, datetime):
                        return val
                    return None

                dt1 = parse_date(date1)
                dt2 = parse_date(date2)

                if dt1 is None or dt2 is None:
                    return None

                # Calculate difference
                delta = dt2 - dt1

                # Convert to requested unit
                match unit:
                    case "day":
                        return int(delta.days)
                    case "week":
                        return int(delta.days // 7)
                    case "month":
                        # Approximate months
                        return int(
                            (dt2.year - dt1.year) * 12 + (dt2.month - dt1.month)
                        )
                    case "year":
                        return int(dt2.year - dt1.year)
                    case "hour":
                        return int(delta.total_seconds() // 3600)
                    case "minute":
                        return int(delta.total_seconds() // 60)
                    case "second":
                        return int(delta.total_seconds())
                    case _:
                        raise ValueError(f"Unknown unit: {unit}")

            case _:
                raise NotImplementedError(
                    f"Date arithmetic operator {operator} not supported in Python evaluation"
                )

    def _evaluate_object_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate object operators in Python."""
        match operator:
            case "$mergeObjects":
                if not isinstance(operands, list):
                    raise ValueError("$mergeObjects requires a list of objects")
                result = {}
                for obj in operands:
                    obj_val = self._evaluate_operand_python(obj, document)
                    if isinstance(obj_val, dict):
                        result.update(obj_val)
                return result
            case "$getField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError("$getField requires 'field' specification")
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = document
                if not isinstance(obj, dict):
                    return None
                return obj.get(field)
            case "$setField":
                if not isinstance(operands, dict):
                    raise ValueError("$setField requires a dictionary")
                field = operands.get("field")
                value = operands.get("value")
                input_val = operands.get("input")
                if field is None:
                    raise ValueError("$setField requires 'field'")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = dict(document)
                if not isinstance(obj, dict):
                    obj = {}
                result = dict(obj)
                result[field] = self._evaluate_operand_python(value, document)
                return result
            case "$unsetField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError(
                        "$unsetField requires 'field' specification"
                    )
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = dict(document)
                if not isinstance(obj, dict):
                    return {}
                result = dict(obj)
                result.pop(field, None)
                return result
            case "$objectToArray":
                # Convert object to array of {k, v} objects
                obj = self._evaluate_operand_python(operands, document)
                if not isinstance(obj, dict):
                    return []
                return [{"k": k, "v": v} for k, v in obj.items()]
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in Python evaluation"
                )

    def _evaluate_type_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate type conversion operators in Python."""
        # Handle both list and single operand formats (but not for $convert which needs dict)
        if operator != "$convert" and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$type":
                if len(operands) != 1:
                    raise ValueError("$type requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return self._get_bson_type(value)
            case "$toString":
                if len(operands) != 1:
                    raise ValueError("$toString requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value) if value is not None else None
            case "$toInt":
                if len(operands) != 1:
                    raise ValueError("$toInt requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    return int(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toDouble":
                if len(operands) != 1:
                    raise ValueError("$toDouble requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    return float(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toBool":
                if len(operands) != 1:
                    raise ValueError("$toBool requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return False
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return value != 0
                if isinstance(value, str):
                    return len(value) > 0
                return bool(value)
            case "$toLong":
                if len(operands) != 1:
                    raise ValueError("$toLong requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    # Python ints are already 64-bit
                    return int(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toDecimal":
                if len(operands) != 1:
                    raise ValueError("$toDecimal requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    from decimal import Decimal

                    return Decimal(str(value)) if value is not None else None
                except (ValueError, TypeError, ImportError):
                    return None
            case "$toObjectId":
                if len(operands) != 1:
                    raise ValueError("$toObjectId requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                # Convert hex string to ObjectId
                from neosqlite.objectid import ObjectId

                try:
                    if isinstance(value, str) and len(value) == 24:
                        return ObjectId(value)
                    # For other types, try to create from string representation
                    return ObjectId(str(value))
                except Exception:
                    return None
            case "$convert":
                # $convert is complex - requires 'to' field
                if not isinstance(operands, dict):
                    raise ValueError("$convert requires a dictionary")
                input_val = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                to_type = operands.get("to")
                on_error = operands.get("onError")
                on_null = operands.get("onNull")

                if input_val is None:
                    return on_null

                # Import required types upfront
                from neosqlite.objectid import ObjectId
                from neosqlite.binary import Binary
                import re

                # Map conversion types to named converter methods
                conversion_map = {
                    "int": self._convert_to_int,
                    "long": self._convert_to_long,
                    "double": self._convert_to_double,
                    "decimal": self._convert_to_decimal,
                    "string": self._convert_to_string,
                    "bool": self._convert_to_bool,
                    "objectId": self._convert_to_objectid,
                    "binData": self._convert_to_bindata,
                    "bsonBinData": self._convert_to_bsonbindata,
                    "regex": self._convert_to_regex,
                    "bsonRegex": self._convert_to_bsonregex,
                    "date": self._convert_to_date,
                    "null": self._convert_to_null,
                }

                try:
                    converter = conversion_map.get(to_type)
                    if converter:
                        return converter(input_val)
                    return input_val
                except Exception:
                    return on_error
            case "$toBinData":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$toBinData requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                # Convert to Binary
                from neosqlite.binary import Binary

                try:
                    if isinstance(value, str):
                        return Binary(value.encode("utf-8"))
                    elif isinstance(value, bytes):
                        return Binary(value)
                    return Binary(str(value).encode("utf-8"))
                except Exception:
                    return None
            case "$toRegex":
                if len(operands) != 1:
                    raise ValueError("$toRegex requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                # Convert to regex pattern
                try:
                    import re

                    return re.compile(str(value))
                except Exception:
                    return None
            case _:
                raise NotImplementedError(
                    f"Type operator {operator} not supported in Python evaluation"
                )

    # Type converter helper methods for $convert operator
    @staticmethod
    def _convert_to_int(value: Any) -> Any:
        """Convert value to int."""
        return int(value)

    @staticmethod
    def _convert_to_long(value: Any) -> Any:
        """Convert value to long (64-bit int)."""
        return int(value)

    @staticmethod
    def _convert_to_double(value: Any) -> Any:
        """Convert value to double (float)."""
        return float(value)

    @staticmethod
    def _convert_to_decimal(value: Any) -> Any:
        """Convert value to decimal (float, as SQLite lacks Decimal128)."""
        return float(value)

    @staticmethod
    def _convert_to_string(value: Any) -> Any:
        """Convert value to string."""
        return str(value)

    @staticmethod
    def _convert_to_bool(value: Any) -> Any:
        """Convert value to bool."""
        return bool(value)

    @staticmethod
    def _convert_to_objectid(value: Any) -> Any:
        """Convert value to ObjectId."""
        from neosqlite.objectid import ObjectId

        return ObjectId(str(value)) if value else None

    @staticmethod
    def _convert_to_bindata(value: Any) -> Any:
        """Convert value to Binary (binData)."""
        from neosqlite.binary import Binary

        if value is None:
            return None
        if isinstance(value, str):
            return Binary(value.encode("utf-8"))
        return Binary(value)

    @staticmethod
    def _convert_to_bsonbindata(value: Any) -> Any:
        """Convert value to Binary (bsonBinData)."""
        from neosqlite.binary import Binary

        if value is None:
            return None
        if isinstance(value, str):
            return Binary(value.encode("utf-8"))
        return Binary(value)

    @staticmethod
    def _convert_to_regex(value: Any) -> Any:
        """Convert value to regex pattern."""
        import re

        return re.compile(str(value)) if value else None

    @staticmethod
    def _convert_to_bsonregex(value: Any) -> Any:
        """Convert value to regex pattern (bsonRegex)."""
        import re

        return re.compile(str(value)) if value else None

    @staticmethod
    def _convert_to_date(value: Any) -> Any:
        """Convert value to date (returns as-is; proper conversion requires parsing)."""
        return value

    @staticmethod
    def _convert_to_null(value: Any) -> None:
        """Convert any value to None."""
        return None

    def _get_bson_type(self, value: Any) -> str:
        """Get BSON type name for a value."""
        match value:
            case None:
                return "null"
            case bool():
                return "bool"
            case int():
                return "int"
            case float():
                return "double"
            case str():
                return "string"
            case list():
                return "array"
            case dict():
                return "object"
            case _:
                return "unknown"

    def _evaluate_literal_python(
        self, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate $literal operator in Python."""
        # $literal just returns its argument as-is (used to escape special characters)
        return self._evaluate_operand_python(operands, document)

    def _evaluate_operand_python(
        self, operand: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate an operand in Python context."""
        match operand:
            case str() if operand.startswith("$"):
                # Field reference - navigate document
                field_path = operand[1:]  # Remove $

                # Handle $$variable syntax
                if field_path.startswith("$"):
                    # $$var syntax - check for special variables
                    var_name = "$" + field_path  # Reconstruct $$var
                    if var_name == "$$REMOVE":
                        # Special sentinel for field removal in $project
                        return REMOVE_SENTINEL
                    # Otherwise look up directly in document context
                    return document.get(var_name)

                keys = field_path.split(".")
                value: Optional[Any] = document
                for key in keys:
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        return None
                return value

            case dict():
                # Check if it's an expression (single key starting with $) or literal dict
                if len(operand) == 1:
                    key = next(iter(operand.keys()))
                    if key.startswith("$"):
                        # Nested expression
                        return self._evaluate_expr_python(operand, document)
                # Otherwise, it's a literal dict (e.g., for $mergeObjects)
                return operand

            case _:
                # Literal value
                return operand
