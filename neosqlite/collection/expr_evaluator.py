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
from typing import Any, Dict, List, Tuple, Optional
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

        if tier == 1:
            return self._evaluate_sql_tier1(expr)
        elif tier == 2:
            return self._evaluate_sql_tier2(expr)
        else:
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
        if operator in ("$and", "$or", "$not", "$nor"):
            return self._convert_logical_operator(operator, operands)
        elif operator in ("$gt", "$gte", "$lt", "$lte", "$eq", "$ne"):
            return self._convert_comparison_operator(operator, operands)
        elif operator == "$cmp":
            # $cmp returns -1, 0, or 1, which can be used in comparisons
            # For SQL tier, we convert to a CASE statement
            return self._convert_cmp_operator(operands)
        elif operator in ("$add", "$subtract", "$multiply", "$divide", "$mod"):
            return self._convert_arithmetic_operator(operator, operands)
        elif operator in (
            "$pow",
            "$sqrt",
            "$ln",
            "$log",
            "$log10",
            "$log2",
            "$exp",
            "$sigmoid",
        ):
            return self._convert_math_operator(operator, operands)
        elif operator == "$cond":
            return self._convert_cond_operator(operands)
        elif operator == "$ifNull":
            return self._convert_ifNull_operator(operands)
        elif operator in (
            "$size",
            "$in",
            "$isArray",
            "$slice",
            "$indexOfArray",
            "$sum",
            "$avg",
            "$min",
            "$max",
            "$setEquals",
            "$setIntersection",
            "$setUnion",
            "$setDifference",
            "$setIsSubset",
            "$anyElementTrue",
            "$allElementsTrue",
        ):
            return self._convert_array_operator(operator, operands)
        elif operator in (
            "$concat",
            "$toLower",
            "$toUpper",
            "$strLenBytes",
            "$substr",
            "$trim",
            "$ltrim",
            "$rtrim",
            "$indexOfBytes",
            "$regexMatch",
            "$split",
            "$replaceAll",
            "$replaceOne",
            "$strLenCP",
            "$indexOfCP",
        ):
            return self._convert_string_operator(operator, operands)
        elif operator in ("$abs", "$ceil", "$floor", "$round", "$trunc"):
            return self._convert_math_operator(operator, operands)
        elif operator in (
            "$sin",
            "$cos",
            "$tan",
            "$asin",
            "$acos",
            "$atan",
            "$atan2",
            "$sinh",
            "$cosh",
            "$tanh",
            "$asinh",
            "$acosh",
            "$atanh",
        ):
            return self._convert_trig_operator(operator, operands)
        elif operator in ("$degreesToRadians", "$radiansToDegrees"):
            return self._convert_angle_operator(operator, operands)
        elif operator in (
            "$year",
            "$month",
            "$dayOfMonth",
            "$hour",
            "$minute",
            "$second",
            "$dayOfWeek",
            "$dayOfYear",
            "$week",
            "$isoDayOfWeek",
            "$isoWeek",
            "$millisecond",
        ):
            return self._convert_date_operator(operator, operands)
        elif operator in ("$dateAdd", "$dateSubtract"):
            return self._convert_date_arithmetic_operator(operator, operands)
        elif operator == "$dateDiff":
            return self._convert_date_diff_operator(operands)
        elif operator in (
            "$mergeObjects",
            "$getField",
            "$setField",
            "$unsetField",
            "$objectToArray",
        ):
            return self._convert_object_operator(operator, operands)
        else:
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

        if operator == "$and":
            sql = " AND ".join(sql_parts)
        elif operator == "$or":
            sql = " OR ".join(sql_parts)
        elif operator == "$nor":
            sql = f"NOT ({' OR '.join(sql_parts)})"
        else:
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

        if operator == "$size":
            if len(operands) != 1:
                raise ValueError("$size requires exactly 1 operand")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])
            # SQLite: json_array_length for JSON arrays (works with both JSON and JSONB)
            sql = f"json_array_length({array_sql})"
            return sql, array_params
        elif operator == "$in":
            if len(operands) != 2:
                raise ValueError("$in requires exactly 2 operands")
            value_sql, value_params = self._convert_operand_to_sql(operands[0])
            array_sql, array_params = self._convert_operand_to_sql(operands[1])
            # Check if value exists in JSON array - use jsonb_each when available
            sql = f"EXISTS (SELECT 1 FROM {json_each}({array_sql}) WHERE value = {value_sql})"
            return sql, value_params + array_params
        elif operator == "$isArray":
            if len(operands) != 1:
                raise ValueError("$isArray requires exactly 1 operand")
            value_sql, value_params = self._convert_operand_to_sql(operands[0])
            # Check if value is a JSON array
            sql = f"json_type({value_sql}) = 'array'"
            return sql, value_params
        elif operator in ("$sum", "$avg", "$min", "$max"):
            if len(operands) != 1:
                raise ValueError(f"{operator} requires exactly 1 operand")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])

            # Map MongoDB accumulator to SQL aggregator
            sql_agg = operator[1:].upper()

            # Use a correlated subquery to aggregate array elements
            # We filter out non-numeric values for $sum and $avg to match MongoDB
            if operator in ("$sum", "$avg"):
                sql = f"(SELECT {sql_agg}(value) FROM {json_each}({array_sql}) WHERE typeof(value) IN ('integer', 'real'))"
            else:
                sql = f"(SELECT {sql_agg}(value) FROM {json_each}({array_sql}))"
            return sql, array_params
        elif operator == "$slice":
            if not isinstance(operands, list) or len(operands) < 2:
                raise ValueError("$slice requires array and count/position")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])

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
        elif operator == "$indexOfArray":
            if len(operands) != 2:
                raise ValueError("$indexOfArray requires exactly 2 operands")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])
            value_sql, value_params = self._convert_operand_to_sql(operands[1])
            # Use json_each to find index - use jsonb_each when available
            sql = f"(SELECT key FROM {json_each}({array_sql}) WHERE value = {value_sql} LIMIT 1)"
            return sql, array_params + value_params
        elif operator in (
            "$setEquals",
            "$setIntersection",
            "$setUnion",
            "$setDifference",
            "$setIsSubset",
            "$anyElementTrue",
            "$allElementsTrue",
        ):
            # Set operations are complex - fall back to Python for now
            # Can be implemented in SQL using json_each and json_group_array
            raise NotImplementedError(
                f"Set operator {operator} not supported in SQL tier (use Python fallback)"
            )
        else:
            raise NotImplementedError(
                f"Array operator {operator} not supported in SQL tier"
            )

    def _convert_string_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert string operators to SQL."""
        if operator == "$concat":
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
        elif operator == "$toLower":
            if len(operands) != 1:
                raise ValueError("$toLower requires exactly 1 operand")
            value_sql, value_params = self._convert_operand_to_sql(operands[0])
            sql = f"lower({value_sql})"
            return sql, value_params
        elif operator == "$toUpper":
            if len(operands) != 1:
                raise ValueError("$toUpper requires exactly 1 operand")
            value_sql, value_params = self._convert_operand_to_sql(operands[0])
            sql = f"upper({value_sql})"
            return sql, value_params
        elif operator == "$strLenBytes":
            if len(operands) != 1:
                raise ValueError("$strLenBytes requires exactly 1 operand")
            value_sql, value_params = self._convert_operand_to_sql(operands[0])
            sql = f"length({value_sql})"
            return sql, value_params
        elif operator == "$substr":
            if len(operands) != 3:
                raise ValueError("$substr requires exactly 3 operands")
            str_sql, str_params = self._convert_operand_to_sql(operands[0])
            start_sql, start_params = self._convert_operand_to_sql(operands[1])
            len_sql, len_params = self._convert_operand_to_sql(operands[2])
            sql = f"substr({str_sql}, {start_sql} + 1, {len_sql})"
            return sql, str_params + start_params + len_params
        elif operator == "$trim":
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
        elif operator == "$ltrim":
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
        elif operator == "$rtrim":
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
        elif operator == "$indexOfBytes":
            if len(operands) < 2:
                raise ValueError("$indexOfBytes requires substring and string")
            substr_sql, substr_params = self._convert_operand_to_sql(
                operands[0]
            )
            string_sql, string_params = self._convert_operand_to_sql(
                operands[1]
            )
            sql = f"(instr({string_sql}, {substr_sql}) - 1)"
            return sql, substr_params + string_params
        elif operator == "$regexMatch":
            if not isinstance(operands, dict) or "input" not in operands:
                raise ValueError("$regexMatch requires 'input' and 'regex'")
            input_sql, input_params = self._convert_operand_to_sql(
                operands["input"]
            )
            regex = operands.get("regex", "")
            sql = f"({input_sql} REGEXP ?)"
            return sql, input_params + [regex]
        elif operator == "$split":
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
        elif operator == "$replaceAll":
            if len(operands) != 3:
                raise ValueError(
                    "$replaceAll requires string, find, and replacement"
                )
            string_sql, string_params = self._convert_operand_to_sql(
                operands[0]
            )
            find_sql, find_params = self._convert_operand_to_sql(operands[1])
            replace_sql, replace_params = self._convert_operand_to_sql(
                operands[2]
            )
            sql = f"replace({string_sql}, {find_sql}, {replace_sql})"
            return sql, string_params + find_params + replace_params
        elif operator == "$replaceOne":
            if len(operands) != 3:
                raise ValueError(
                    "$replaceOne requires string, find, and replacement"
                )
            string_sql, string_params = self._convert_operand_to_sql(
                operands[0]
            )
            find_sql, find_params = self._convert_operand_to_sql(operands[1])
            replace_sql, replace_params = self._convert_operand_to_sql(
                operands[2]
            )
            # SQLite replace replaces all occurrences, same as replaceAll
            # For replaceOne, we need a more complex approach - fall back to Python
            raise NotImplementedError(
                "$replaceOne not supported in SQL tier (use Python fallback)"
            )
        elif operator == "$strLenCP":
            if len(operands) != 1:
                raise ValueError("$strLenCP requires exactly 1 operand")
            string_sql, string_params = self._convert_operand_to_sql(
                operands[0]
            )
            # For BMP characters, length in bytes = length in code points
            sql = f"length({string_sql})"
            return sql, string_params
        elif operator == "$indexOfCP":
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
        else:
            raise NotImplementedError(
                f"String operator {operator} not supported in SQL tier"
            )

    def _convert_math_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert math operators to SQL."""
        # Handle $pow separately (requires 2 operands)
        if operator == "$pow":
            if len(operands) != 2:
                raise ValueError("$pow requires exactly 2 operands")
            base_sql, base_params = self._convert_operand_to_sql(operands[0])
            exp_sql, exp_params = self._convert_operand_to_sql(operands[1])
            sql = f"pow({base_sql}, {exp_sql})"
            return sql, base_params + exp_params

        # $log with custom base requires 2 operands: [number, base]
        if operator == "$log":
            if len(operands) != 2:
                raise ValueError(
                    "$log requires exactly 2 operands: [number, base]"
                )
            number_sql, number_params = self._convert_operand_to_sql(
                operands[0]
            )
            base_sql, base_params = self._convert_operand_to_sql(operands[1])
            # SQLite: log(base, number)
            sql = f"log({base_sql}, {number_sql})"
            return sql, number_params + base_params

        # All other math operators require 1 operand
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        if operator == "$abs":
            sql = f"abs({value_sql})"
        elif operator == "$ceil":
            sql = f"ceil({value_sql})"
        elif operator == "$floor":
            sql = f"floor({value_sql})"
        elif operator == "$round":
            sql = f"round({value_sql})"
        elif operator == "$trunc":
            sql = f"cast({value_sql} as integer)"
        elif operator == "$sqrt":
            sql = f"sqrt({value_sql})"
        elif operator == "$ln":
            # Natural logarithm (base e)
            sql = f"ln({value_sql})"
        elif operator == "$log10":
            # Base-10 logarithm
            sql = f"log10({value_sql})"
        elif operator == "$log2":
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
        elif operator == "$exp":
            # Exponential function (e^x)
            sql = f"exp({value_sql})"
        elif operator == "$sigmoid":
            # Sigmoid function: 1 / (1 + e^(-x))
            # Handle object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
            if isinstance(operands, dict):
                input_sql, input_params = self._convert_operand_to_sql(
                    operands.get("input")
                )
                on_null_sql, on_null_params = self._convert_operand_to_sql(
                    operands.get("onNull")
                )
                sql = f"(CASE WHEN {input_sql} IS NULL THEN {on_null_sql} ELSE (1.0 / (1.0 + exp(-({input_sql})))) END)"
                return sql, input_params + on_null_params
            sql = f"(1.0 / (1.0 + exp(-({value_sql}))))"
        else:
            raise NotImplementedError(
                f"Math operator {operator} not supported in SQL tier"
            )

        return sql, value_params

    def _convert_trig_operator(
        self, operator: str, operands: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert trigonometric and hyperbolic operators to SQL."""
        # Handle $atan2 separately (requires 2 operands)
        if operator == "$atan2":
            if len(operands) != 2:
                raise ValueError("$atan2 requires exactly 2 operands")
            y_sql, y_params = self._convert_operand_to_sql(operands[0])
            x_sql, x_params = self._convert_operand_to_sql(operands[1])
            sql = f"atan2({y_sql}, {x_sql})"
            return sql, y_params + x_params

        # All other trig operators require 1 operand
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        # Standard trigonometric functions
        trig_map = {
            "$sin": "sin",
            "$cos": "cos",
            "$tan": "tan",
            "$asin": "asin",
            "$acos": "acos",
            "$atan": "atan",
            # Hyperbolic functions
            "$sinh": "sinh",
            "$cosh": "cosh",
            "$tanh": "tanh",
            # Inverse hyperbolic functions
            "$asinh": "asinh",
            "$acosh": "acosh",
            "$atanh": "atanh",
        }

        sql_func = trig_map.get(operator)
        if sql_func is None:
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

        if operator == "$degreesToRadians":
            # radians = degrees * pi() / 180
            sql = f"({value_sql} * pi() / 180.0)"
        elif operator == "$radiansToDegrees":
            # degrees = radians * 180 / pi()
            sql = f"({value_sql} * 180.0 / pi())"
        else:
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
        format_map = {
            "$year": "%Y",
            "$month": "%m",
            "$dayOfMonth": "%d",
            "$hour": "%H",
            "$minute": "%M",
            "$second": "%S",
            "$dayOfWeek": "%w",
            "$dayOfYear": "%j",
            "$week": "%W",
            "$isoDayOfWeek": "%w",  # SQLite doesn't have ISO directly
            "$isoWeek": "%W",
            "$millisecond": "%f",
        }

        fmt = format_map.get(operator)
        if fmt is None:
            raise NotImplementedError(
                f"Date operator {operator} not supported in SQL tier"
            )

        # For numeric results, cast to integer
        if operator in ("$millisecond",):
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

        if operator == "$mergeObjects":
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
        elif operator == "$getField":
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
        elif operator == "$setField":
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
            sql = f"{json_prefix}_set({input_sql}, '$.{field}', {value_sql})"
            return sql, input_params + value_params
        elif operator == "$unsetField":
            if not isinstance(operands, dict) or "field" not in operands:
                raise ValueError("$unsetField requires 'field' specification")
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
        elif operator == "$objectToArray":
            # Complex - convert object keys/values to array of {k, v} objects
            # Fall back to Python for now
            raise NotImplementedError(
                "$objectToArray not supported in SQL tier (use Python fallback)"
            )
        else:
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

        if operator == "$toString":
            # Cast to text
            sql = f"cast({value_sql} as text)"
        elif operator == "$toInt":
            # Cast to integer
            sql = f"cast({value_sql} as integer)"
        elif operator == "$toDouble":
            # Cast to real/float
            sql = f"cast({value_sql} as real)"
        elif operator == "$toLong":
            # SQLite integers are already 64-bit, same as toInt
            sql = f"cast({value_sql} as integer)"
        elif operator == "$toBool":
            # Convert to boolean (0 or 1)
            sql = f"CASE WHEN {value_sql} THEN 1 ELSE 0 END"
        elif operator == "$toDecimal":
            # SQLite doesn't have native Decimal128, use REAL
            raise NotImplementedError(
                "$toDecimal not supported in SQL tier (SQLite lacks Decimal128)"
            )
        elif operator == "$toObjectId":
            # Cannot convert to ObjectId in SQL
            raise NotImplementedError(
                "$toObjectId not supported in SQL tier (use Python fallback)"
            )
        elif operator == "$convert":
            # $convert is complex - requires 'to' field specification
            # Fall back to Python
            raise NotImplementedError(
                "$convert not supported in SQL tier (use Python fallback)"
            )
        elif operator == "$toBinData":
            # Cannot convert to binary in SQL
            raise NotImplementedError(
                "$toBinData not supported in SQL tier (use Python fallback)"
            )
        elif operator == "$toRegex":
            # Cannot convert to regex in SQL
            raise NotImplementedError(
                "$toRegex not supported in SQL tier (use Python fallback)"
            )
        else:
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
        if isinstance(operand, str) and operand.startswith("$"):
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

        elif isinstance(operand, (list, dict)):
            # Check if it's an expression (dict with single key starting with $)
            if isinstance(operand, dict) and len(operand) == 1:
                key = next(iter(operand.keys()))
                if key.startswith("$"):
                    return self._convert_expr_to_sql(operand)

            # Literal list or dict - convert to JSON for SQL
            from neosqlite.collection.json_helpers import neosqlite_json_dumps

            return "json(?)", [neosqlite_json_dumps(operand)]

        else:
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
        if operator in ("$and", "$or", "$not", "$nor"):
            return self._evaluate_logical_python(operator, operands, document)
        elif operator in ("$gt", "$gte", "$lt", "$lte", "$eq", "$ne"):
            return self._evaluate_comparison_python(
                operator, operands, document
            )
        elif operator == "$cmp":
            return self._evaluate_cmp_python(operands, document)
        elif operator in ("$add", "$subtract", "$multiply", "$divide", "$mod"):
            return self._evaluate_arithmetic_python(
                operator, operands, document
            )
        elif operator in ("$abs", "$ceil", "$floor", "$round", "$trunc"):
            return self._evaluate_math_python(operator, operands, document)
        elif operator in ("$ln", "$log", "$log10", "$log2", "$exp", "$sigmoid"):
            return self._evaluate_math_python(operator, operands, document)
        elif operator == "$pow":
            return self._evaluate_pow_python(operands, document)
        elif operator == "$sqrt":
            return self._evaluate_sqrt_python(operands, document)
        elif operator in (
            "$sin",
            "$cos",
            "$tan",
            "$asin",
            "$acos",
            "$atan",
            "$atan2",
            "$sinh",
            "$cosh",
            "$tanh",
            "$asinh",
            "$acosh",
            "$atanh",
        ):
            return self._evaluate_trig_python(operator, operands, document)
        elif operator in ("$degreesToRadians", "$radiansToDegrees"):
            return self._evaluate_angle_python(operator, operands, document)
        elif operator == "$cond":
            return self._evaluate_cond_python(operands, document)
        elif operator == "$ifNull":
            return self._evaluate_ifNull_python(operands, document)
        elif operator == "$switch":
            return self._evaluate_switch_python(operands, document)
        elif operator in (
            "$size",
            "$in",
            "$isArray",
            "$arrayElemAt",
            "$first",
            "$last",
            "$slice",
            "$indexOfArray",
            "$sum",
            "$avg",
            "$min",
            "$max",
            "$setEquals",
            "$setIntersection",
            "$setUnion",
            "$setDifference",
            "$setIsSubset",
            "$anyElementTrue",
            "$allElementsTrue",
        ):
            return self._evaluate_array_python(operator, operands, document)
        elif operator in ("$filter", "$map", "$reduce"):
            return self._evaluate_array_transform_python(
                operator, operands, document
            )
        elif operator in (
            "$concat",
            "$toLower",
            "$toUpper",
            "$strLenBytes",
            "$substr",
            "$trim",
            "$ltrim",
            "$rtrim",
            "$indexOfBytes",
            "$regexMatch",
            "$regexFind",
            "$regexFindAll",
            "$split",
            "$replaceAll",
            "$replaceOne",
            "$strLenCP",
            "$indexOfCP",
        ):
            return self._evaluate_string_python(operator, operands, document)
        elif operator in (
            "$year",
            "$month",
            "$dayOfMonth",
            "$hour",
            "$minute",
            "$second",
            "$dayOfWeek",
            "$dayOfYear",
            "$week",
            "$isoDayOfWeek",
            "$isoWeek",
            "$millisecond",
        ):
            return self._evaluate_date_python(operator, operands, document)
        elif operator in ("$dateAdd", "$dateSubtract", "$dateDiff"):
            return self._evaluate_date_arithmetic_python(
                operator, operands, document
            )
        elif operator in (
            "$mergeObjects",
            "$getField",
            "$setField",
            "$unsetField",
            "$objectToArray",
        ):
            return self._evaluate_object_python(operator, operands, document)
        elif operator in (
            "$type",
            "$toString",
            "$toInt",
            "$toDouble",
            "$toBool",
            "$toLong",
            "$toDecimal",
            "$toObjectId",
            "$toBinData",
            "$toRegex",
            "$convert",
        ):
            return self._evaluate_type_python(operator, operands, document)
        elif operator == "$literal":
            return self._evaluate_literal_python(operands, document)
        else:
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

        if operator == "$and":
            return all(results)
        elif operator == "$or":
            return any(results)
        elif operator == "$nor":
            return not any(results)
        else:
            raise ValueError(f"Unknown logical operator: {operator}")

    def _evaluate_comparison_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> bool:
        """Evaluate comparison operators in Python."""
        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        if operator == "$eq":
            return left == right
        elif operator == "$gt":
            return left > right
        elif operator == "$gte":
            return left >= right
        elif operator == "$lt":
            return left < right
        elif operator == "$lte":
            return left <= right
        elif operator == "$ne":
            return left != right
        else:
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
        """Evaluate arithmetic operators in Python."""
        values = [
            self._evaluate_operand_python(op, document) for op in operands
        ]

        if operator == "$add":
            return sum(values)
        elif operator == "$subtract":
            return values[0] - sum(values[1:])
        elif operator == "$multiply":
            result = 1
            for v in values:
                result *= v
            return result
        elif operator == "$divide":
            result = values[0]
            for v in values[1:]:
                if v == 0:
                    return None  # Division by zero
                result /= v
            return result
        elif operator == "$mod":
            if len(values) != 2 or values[1] == 0:
                return None
            return values[0] % values[1]
        else:
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

        if operator == "$abs":
            return abs(value) if value is not None else None
        elif operator == "$ceil":
            return math.ceil(value) if value is not None else None
        elif operator == "$floor":
            return math.floor(value) if value is not None else None
        elif operator == "$round":
            return round(value) if value is not None else None
        elif operator == "$trunc":
            return int(value) if value is not None else None
        elif operator == "$ln":
            # Natural logarithm (base e)
            return math.log(value) if value is not None and value > 0 else None
        elif operator == "$log10":
            # Base-10 logarithm
            return (
                math.log10(value) if value is not None and value > 0 else None
            )
        elif operator == "$log2":
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
            return math.log2(value) if value is not None and value > 0 else None
        elif operator == "$exp":
            # Exponential function (e^x)
            return math.exp(value) if value is not None else None
        elif operator == "$sigmoid":
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
        else:
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

        if operator == "$sin":
            return math.sin(value)
        elif operator == "$cos":
            return math.cos(value)
        elif operator == "$tan":
            return math.tan(value)
        elif operator == "$asin":
            return math.asin(value) if -1 <= value <= 1 else None
        elif operator == "$acos":
            return math.acos(value) if -1 <= value <= 1 else None
        elif operator == "$atan":
            return math.atan(value)
        # Hyperbolic functions
        elif operator == "$sinh":
            return math.sinh(value)
        elif operator == "$cosh":
            return math.cosh(value)
        elif operator == "$tanh":
            return math.tanh(value)
        # Inverse hyperbolic functions
        elif operator == "$asinh":
            return math.asinh(value)
        elif operator == "$acosh":
            return math.acosh(value) if value >= 1 else None
        elif operator == "$atanh":
            return math.atanh(value) if -1 < value < 1 else None
        else:
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

        if operator == "$degreesToRadians":
            return math.radians(value)
        elif operator == "$radiansToDegrees":
            return math.degrees(value)
        else:
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
        if operator == "$size":
            if len(operands) != 1:
                raise ValueError("$size requires exactly 1 operand")
            array = self._evaluate_operand_python(operands[0], document)
            if isinstance(array, list):
                return len(array)
            return None
        elif operator == "$in":
            if len(operands) != 2:
                raise ValueError("$in requires exactly 2 operands")
            value = self._evaluate_operand_python(operands[0], document)
            array = self._evaluate_operand_python(operands[1], document)
            if isinstance(array, list):
                return value in array
            return False
        elif operator == "$isArray":
            if len(operands) != 1:
                raise ValueError("$isArray requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            return isinstance(value, list)
        elif operator in ("$sum", "$avg", "$min", "$max"):
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

            if operator == "$sum":
                return sum(nums)
            elif operator == "$avg":
                return sum(nums) / len(nums)
            elif operator == "$min":
                return min(array)  # min/max work on all types
            elif operator == "$max":
                return max(array)
            return None
        elif operator == "$arrayElemAt":
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
        elif operator == "$first":
            if len(operands) != 1:
                raise ValueError("$first requires exactly 1 operand")
            array = self._evaluate_operand_python(operands[0], document)
            if isinstance(array, list) and len(array) > 0:
                return array[0]
            return None
        elif operator == "$last":
            if len(operands) != 1:
                raise ValueError("$last requires exactly 1 operand")
            array = self._evaluate_operand_python(operands[0], document)
            if isinstance(array, list) and len(array) > 0:
                return array[-1]
            return None
        elif operator == "$slice":
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
        elif operator == "$indexOfArray":
            if len(operands) != 2:
                raise ValueError("$indexOfArray requires exactly 2 operands")
            array = self._evaluate_operand_python(operands[0], document)
            value = self._evaluate_operand_python(operands[1], document)
            if isinstance(array, list):
                try:
                    return array.index(value)
                except ValueError:
                    return -1
            return -1
        elif operator == "$setEquals":
            if len(operands) != 2:
                raise ValueError("$setEquals requires exactly 2 operands")
            set1 = self._evaluate_operand_python(operands[0], document)
            set2 = self._evaluate_operand_python(operands[1], document)
            if isinstance(set1, list) and isinstance(set2, list):
                return set(set1) == set(set2)
            return False
        elif operator == "$setIntersection":
            if len(operands) != 2:
                raise ValueError("$setIntersection requires exactly 2 operands")
            set1 = self._evaluate_operand_python(operands[0], document)
            set2 = self._evaluate_operand_python(operands[1], document)
            if isinstance(set1, list) and isinstance(set2, list):
                return list(set(set1) & set(set2))
            return []
        elif operator == "$setUnion":
            if len(operands) != 2:
                raise ValueError("$setUnion requires exactly 2 operands")
            set1 = self._evaluate_operand_python(operands[0], document)
            set2 = self._evaluate_operand_python(operands[1], document)
            if isinstance(set1, list) and isinstance(set2, list):
                return list(set(set1) | set(set2))
            return []
        elif operator == "$setDifference":
            if len(operands) != 2:
                raise ValueError("$setDifference requires exactly 2 operands")
            set1 = self._evaluate_operand_python(operands[0], document)
            set2 = self._evaluate_operand_python(operands[1], document)
            if isinstance(set1, list) and isinstance(set2, list):
                return list(set(set1) - set(set2))
            return []
        elif operator == "$setIsSubset":
            if len(operands) != 2:
                raise ValueError("$setIsSubset requires exactly 2 operands")
            set1 = self._evaluate_operand_python(operands[0], document)
            set2 = self._evaluate_operand_python(operands[1], document)
            if isinstance(set1, list) and isinstance(set2, list):
                return set(set1).issubset(set(set2))
            return False
        elif operator == "$anyElementTrue":
            if len(operands) != 1:
                raise ValueError("$anyElementTrue requires exactly 1 operand")
            array = self._evaluate_operand_python(operands[0], document)
            if isinstance(array, list):
                return any(array)
            return False
        elif operator == "$allElementsTrue":
            if len(operands) != 1:
                raise ValueError("$allElementsTrue requires exactly 1 operand")
            array = self._evaluate_operand_python(operands[0], document)
            if isinstance(array, list):
                return all(array)
            return False
        else:
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
        if operator == "$filter":
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

        elif operator == "$map":
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

        elif operator == "$reduce":
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

        else:
            raise NotImplementedError(
                f"Array transform operator {operator} not supported in Python evaluation"
            )

    def _evaluate_string_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate string operators in Python."""
        if operator == "$concat":
            values = [
                self._evaluate_operand_python(op, document) for op in operands
            ]
            return "".join(str(v) if v is not None else "" for v in values)
        elif operator == "$toLower":
            if len(operands) != 1:
                raise ValueError("$toLower requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            return str(value).lower() if value is not None else None
        elif operator == "$toUpper":
            if len(operands) != 1:
                raise ValueError("$toUpper requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            return str(value).upper() if value is not None else None
        elif operator == "$strLenBytes":
            if len(operands) != 1:
                raise ValueError("$strLenBytes requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            return (
                len(str(value).encode("utf-8")) if value is not None else None
            )
        elif operator == "$substr":
            if len(operands) != 3:
                raise ValueError("$substr requires exactly 3 operands")
            string = self._evaluate_operand_python(operands[0], document)
            start = self._evaluate_operand_python(operands[1], document)
            length = self._evaluate_operand_python(operands[2], document)
            if string is not None and start is not None and length is not None:
                return str(string)[int(start) : int(start) + int(length)]
            return None
        elif operator == "$trim":
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
        elif operator == "$ltrim":
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
        elif operator == "$rtrim":
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
        elif operator == "$indexOfBytes":
            if len(operands) < 2:
                raise ValueError("$indexOfBytes requires substring and string")
            substr = self._evaluate_operand_python(operands[0], document)
            string = self._evaluate_operand_python(operands[1], document)
            if substr is None or string is None:
                return -1
            idx = str(string).find(str(substr))
            return idx
        elif operator == "$regexMatch":
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
        elif operator == "$split":
            if len(operands) != 2:
                raise ValueError("$split requires string and delimiter")
            string = self._evaluate_operand_python(operands[0], document)
            delimiter = self._evaluate_operand_python(operands[1], document)
            if string is None or delimiter is None:
                return []
            return str(string).split(str(delimiter))
        elif operator == "$replaceAll":
            if len(operands) != 3:
                raise ValueError(
                    "$replaceAll requires string, find, and replacement"
                )
            string = self._evaluate_operand_python(operands[0], document)
            find = self._evaluate_operand_python(operands[1], document)
            replacement = self._evaluate_operand_python(operands[2], document)
            if string is None:
                return None
            return str(string).replace(str(find), str(replacement))
        elif operator == "$replaceOne":
            if len(operands) != 3:
                raise ValueError(
                    "$replaceOne requires string, find, and replacement"
                )
            string = self._evaluate_operand_python(operands[0], document)
            find = self._evaluate_operand_python(operands[1], document)
            replacement = self._evaluate_operand_python(operands[2], document)
            if string is None:
                return None
            # Replace only first occurrence
            return str(string).replace(str(find), str(replacement), 1)
        elif operator == "$strLenCP":
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
        elif operator == "$substrCP":
            # Substring by code points (not implemented - use $substr)
            # Handle both list and single operand formats
            if not isinstance(operands, list):
                operands = [operands]
            if len(operands) != 3:
                raise ValueError("$substrCP requires exactly 3 operands")
            string = self._evaluate_operand_python(operands[0], document)
            start = self._evaluate_operand_python(operands[1], document)
            length = self._evaluate_operand_python(operands[2], document)
            if string is not None and start is not None and length is not None:
                # For BMP characters, this is the same as $substr
                # For full Unicode support, would need proper code point handling
                return str(string)[int(start) : int(start) + int(length)]
            return None
        elif operator == "$indexOfCP":
            # Find substring by code points
            if len(operands) < 2:
                raise ValueError("$indexOfCP requires substring and string")
            substr = self._evaluate_operand_python(operands[0], document)
            string = self._evaluate_operand_python(operands[1], document)
            if substr is None or string is None:
                return -1
            idx = str(string).find(str(substr))
            return idx
        elif operator == "$regexFind":
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
        elif operator == "$regexFindAll":
            if not isinstance(operands, dict) or "input" not in operands:
                raise ValueError("$regexFindAll requires 'input' and 'regex'")
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
            result = []
            for match in matches:
                match_obj = {
                    "match": match.group(),
                    "index": match.start(),
                }
                if match.groups():
                    match_obj["captures"] = list(match.groups())
                result.append(match_obj)
            return result
        else:
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
        if operator == "$year":
            return dt.year
        elif operator == "$month":
            return dt.month
        elif operator == "$dayOfMonth":
            return dt.day
        elif operator == "$hour":
            return dt.hour
        elif operator == "$minute":
            return dt.minute
        elif operator == "$second":
            return dt.second
        elif operator == "$millisecond":
            return dt.microsecond // 1000
        elif operator == "$dayOfWeek":
            return dt.weekday()  # 0=Monday
        elif operator == "$dayOfYear":
            return dt.timetuple().tm_yday
        elif operator == "$week":
            return dt.isocalendar()[1]
        elif operator == "$isoDayOfWeek":
            return dt.isocalendar()[2]  # 1=Monday
        elif operator == "$isoWeek":
            return dt.isocalendar()[1]
        else:
            raise NotImplementedError(
                f"Date operator {operator} not supported in Python evaluation"
            )

    def _evaluate_date_arithmetic_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $dateAdd, $dateSubtract, $dateDiff operators in Python."""
        from datetime import datetime, timedelta

        if operator in ("$dateAdd", "$dateSubtract"):
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
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
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
                    f"{unit}s": amount if operator == "$dateAdd" else -amount
                }
                delta = timedelta(**delta_kwargs)
                dt = dt + delta

            # Return ISO format string
            return dt.isoformat()

        elif operator == "$dateDiff":
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
            if unit == "day":
                return int(delta.days)
            elif unit == "week":
                return int(delta.days // 7)
            elif unit == "month":
                # Approximate months
                return int((dt2.year - dt1.year) * 12 + (dt2.month - dt1.month))
            elif unit == "year":
                return int(dt2.year - dt1.year)
            elif unit == "hour":
                return int(delta.total_seconds() // 3600)
            elif unit == "minute":
                return int(delta.total_seconds() // 60)
            elif unit == "second":
                return int(delta.total_seconds())
            else:
                raise ValueError(f"Unknown unit: {unit}")

        else:
            raise NotImplementedError(
                f"Date arithmetic operator {operator} not supported in Python evaluation"
            )

    def _evaluate_object_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate object operators in Python."""
        if operator == "$mergeObjects":
            if not isinstance(operands, list):
                raise ValueError("$mergeObjects requires a list of objects")
            result = {}
            for obj in operands:
                obj_val = self._evaluate_operand_python(obj, document)
                if isinstance(obj_val, dict):
                    result.update(obj_val)
            return result
        elif operator == "$getField":
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
        elif operator == "$setField":
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
        elif operator == "$unsetField":
            if not isinstance(operands, dict) or "field" not in operands:
                raise ValueError("$unsetField requires 'field' specification")
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
        elif operator == "$objectToArray":
            # Convert object to array of {k, v} objects
            obj = self._evaluate_operand_python(operands, document)
            if not isinstance(obj, dict):
                return []
            return [{"k": k, "v": v} for k, v in obj.items()]
        else:
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

        if operator == "$type":
            if len(operands) != 1:
                raise ValueError("$type requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            return self._get_bson_type(value)
        elif operator == "$toString":
            if len(operands) != 1:
                raise ValueError("$toString requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            return str(value) if value is not None else None
        elif operator == "$toInt":
            if len(operands) != 1:
                raise ValueError("$toInt requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            try:
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        elif operator == "$toDouble":
            if len(operands) != 1:
                raise ValueError("$toDouble requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        elif operator == "$toBool":
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
        elif operator == "$toLong":
            if len(operands) != 1:
                raise ValueError("$toLong requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            try:
                # Python ints are already 64-bit
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        elif operator == "$toDecimal":
            if len(operands) != 1:
                raise ValueError("$toDecimal requires exactly 1 operand")
            value = self._evaluate_operand_python(operands[0], document)
            try:
                from decimal import Decimal

                return Decimal(str(value)) if value is not None else None
            except (ValueError, TypeError, ImportError):
                return None
        elif operator == "$toObjectId":
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
        elif operator == "$convert":
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
        elif operator == "$toBinData":
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
        elif operator == "$toRegex":
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
        else:
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
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "bool"
        elif isinstance(value, int):
            return "int"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
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
        if isinstance(operand, str) and operand.startswith("$"):
            # Field reference - navigate document
            field_path = operand[1:]  # Remove $

            # Handle $$variable syntax (for $filter, $map, $reduce)
            if field_path.startswith("$"):
                # $$var syntax - look up directly in document context
                # field_path is now "$var", we need to look up "$$var"
                var_name = "$" + field_path  # Reconstruct $$var
                return document.get(var_name)

            keys = field_path.split(".")
            value: Optional[Any] = document
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None
            return value

        elif isinstance(operand, dict):
            # Check if it's an expression (single key starting with $) or literal dict
            if len(operand) == 1:
                key = next(iter(operand.keys()))
                if key.startswith("$"):
                    # Nested expression
                    return self._evaluate_expr_python(operand, document)
            # Otherwise, it's a literal dict (e.g., for $mergeObjects)
            return operand
        else:
            # Literal value
            return operand
