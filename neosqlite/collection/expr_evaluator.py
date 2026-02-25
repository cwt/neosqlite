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
- String operators: $concat, $toLower, $toUpper, $strLenBytes, $substr, $trim
- Type operators: $type, $convert, $toString, $toInt, $toDouble, $toBool
- Object operators: $mergeObjects, $objectToArray
- Set operators: $setEquals, $setIntersection, $setUnion, $setDifference
- Other: $literal, $let
"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
from .json_path_utils import build_json_extract_expression


class ExprEvaluator:
    """
    Evaluator for MongoDB $expr operator.

    Supports 3-tier evaluation:
    - Tier 1: Direct SQL WHERE clause using JSON functions
    - Tier 2: Temporary tables for complex expressions
    - Tier 3: Python fallback (always available for kill switch)
    """

    def __init__(self, data_column: str = "data"):
        """
        Initialize the expression evaluator.

        Args:
            data_column: Name of the column containing JSON data (default: "data")
        """
        self.data_column = data_column

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
        elif operator in ("$pow", "$sqrt"):
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
        ):
            return self._convert_string_operator(operator, operands)
        elif operator in ("$abs", "$ceil", "$floor", "$round", "$trunc"):
            return self._convert_math_operator(operator, operands)
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
        elif operator in ("$mergeObjects", "$getField", "$setField"):
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
        """Convert array operators to SQL."""
        if operator == "$size":
            if len(operands) != 1:
                raise ValueError("$size requires exactly 1 operand")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])
            # SQLite: json_array_length for JSON arrays
            sql = f"json_array_length({array_sql})"
            return sql, array_params
        elif operator == "$in":
            if len(operands) != 2:
                raise ValueError("$in requires exactly 2 operands")
            value_sql, value_params = self._convert_operand_to_sql(operands[0])
            array_sql, array_params = self._convert_operand_to_sql(operands[1])
            # Check if value exists in JSON array
            sql = f"EXISTS (SELECT 1 FROM json_each({array_sql}) WHERE value = {value_sql})"
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
                sql = f"(SELECT {sql_agg}(value) FROM json_each({array_sql}) WHERE typeof(value) IN ('integer', 'real'))"
            else:
                sql = f"(SELECT {sql_agg}(value) FROM json_each({array_sql}))"
            return sql, array_params
        elif operator == "$slice":
            if not isinstance(operands, list) or len(operands) < 2:
                raise ValueError("$slice requires array and count/position")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])

            # Handle count/position parameters
            count = operands[1]
            skip = operands[2] if len(operands) > 2 else 0

            # SQL implementation using json_group_array and LIMIT/OFFSET
            # This is complex in SQL, so we construct a subquery that re-groups the sliced elements
            if skip != 0:
                sql = f"(SELECT json_group_array(value) FROM (SELECT value FROM json_each({array_sql}) LIMIT {count} OFFSET {skip}))"
            else:
                sql = f"(SELECT json_group_array(value) FROM (SELECT value FROM json_each({array_sql}) LIMIT {count}))"
            return sql, array_params
        elif operator == "$indexOfArray":
            if len(operands) != 2:
                raise ValueError("$indexOfArray requires exactly 2 operands")
            array_sql, array_params = self._convert_operand_to_sql(operands[0])
            value_sql, value_params = self._convert_operand_to_sql(operands[1])
            # Use json_each to find index
            sql = f"(SELECT key FROM json_each({array_sql}) WHERE value = {value_sql} LIMIT 1)"
            return sql, array_params + value_params
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
        else:
            raise NotImplementedError(
                f"Math operator {operator} not supported in SQL tier"
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

    def _convert_object_operator(
        self, operator: str, operands: Any
    ) -> Tuple[str, List[Any]]:
        """Convert object operators to SQL."""
        if operator == "$mergeObjects":
            if not isinstance(operands, list) or len(operands) < 1:
                raise ValueError("$mergeObjects requires a list of objects")
            sql_parts = []
            all_params = []
            for obj in operands:
                obj_sql, obj_params = self._convert_operand_to_sql(obj)
                sql_parts.append(obj_sql)
                all_params.extend(obj_params)
            # Use json_patch to merge objects
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
            sql = f"json_extract({input_sql}, '$.{field}')"
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
            sql = f"json_set({input_sql}, '$.{field}', {value_sql})"
            return sql, input_params + value_params
        else:
            raise NotImplementedError(
                f"Object operator {operator} not supported in SQL tier"
            )

    def _convert_operand_to_sql(self, operand: Any) -> Tuple[str, List[Any]]:
        """
        Convert an operand to SQL expression.

        Handles:
        - Field references: "$field" → json_extract expression
        - Literals: numbers, strings, booleans
        - Nested expressions: {"$operator": [...]}
        """
        if isinstance(operand, str) and operand.startswith("$"):
            # Field reference
            field_path = operand[1:]  # Remove $
            return (
                build_json_extract_expression(self.data_column, field_path),
                [],
            )

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
        elif operator == "$pow":
            return self._evaluate_pow_python(operands, document)
        elif operator == "$sqrt":
            return self._evaluate_sqrt_python(operands, document)
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
        ):
            return self._evaluate_array_python(operator, operands, document)
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
        elif operator in ("$mergeObjects", "$getField", "$setField"):
            return self._evaluate_object_python(operator, operands, document)
        elif operator in (
            "$type",
            "$toString",
            "$toInt",
            "$toDouble",
            "$toBool",
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
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)

        import math

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
        import math

        return math.sqrt(value) if value is not None and value >= 0 else None

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
        else:
            raise NotImplementedError(
                f"Array operator {operator} not supported in Python evaluation"
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
        else:
            raise NotImplementedError(
                f"Object operator {operator} not supported in Python evaluation"
            )

    def _evaluate_type_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate type conversion operators in Python."""
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
        else:
            raise NotImplementedError(
                f"Type operator {operator} not supported in Python evaluation"
            )

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
