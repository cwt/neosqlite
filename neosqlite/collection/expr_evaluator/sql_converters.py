"""
SQL converters for expression evaluation.

This module contains the SQL tier conversion methods for the ExprEvaluator.
These methods convert MongoDB $expr operators to SQL expressions.

This is designed as a mixin class to be composed into ExprEvaluator.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from ..json_path_utils import build_json_extract_expression, parse_json_path

if TYPE_CHECKING:
    # Avoid circular import by using TYPE_CHECKING
    from .context import AggregationContext


class SqlConvertersMixin:
    """
    Mixin class providing SQL conversion methods for expression evaluation.

    This class is designed to be composed into ExprEvaluator and provides
    all the _convert_* methods for converting MongoDB operators to SQL.

    Required attributes from parent class:
        - data_column: Name of the JSON data column
        - json_function_prefix: 'json' or 'jsonb' based on support
        - json_each_function: 'json_each' or 'jsonb_each'
        - json_group_array_function: 'json_group_array' or 'jsonb_group_array'
        - _jsonb_supported: Boolean indicating JSONB support
        - _log2_warned: Boolean tracking if $log2 warning was issued
    """

    # Type annotations for simple attributes expected from parent class
    # (Properties like json_function_prefix are handled separately)
    data_column: str
    _jsonb_supported: bool
    _log2_warned: bool
    _current_context: AggregationContext | None

    def _convert_expr_to_sql(
        self, expr: dict[str, Any]
    ) -> tuple[str, list[Any]]:
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
            case "$let":
                return self._convert_let_operator(operands)
            case (
                "$type"
                | "$toString"
                | "$toInt"
                | "$toDouble"
                | "$toLong"
                | "$toBool"
                | "$toDecimal"
                | "$toObjectId"
                | "$isNumber"
                | "$convert"
            ):
                return self._convert_type_operator(operator, operands)
            case "$binarySize" | "$bsonSize":
                return self._convert_data_size_operator(operator, operands)
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in SQL tier"
                )

    def _convert_logical_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert logical operators ($and, $or, $not, $nor) to SQL."""
        # Normalize operands for $not to handle both formats:
        # {$not: {expression}} and {$not: [expression]}
        if operator == "$not" and not isinstance(operands, list):
            operands = [operands]

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
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
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
        self, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert $cmp operator to SQL CASE statement."""
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_sql(operands[0])
        right_sql, right_params = self._convert_operand_to_sql(operands[1])

        sql = f"(CASE WHEN {left_sql} < {right_sql} THEN -1 WHEN {left_sql} > {right_sql} THEN 1 ELSE 0 END)"
        return sql, left_params + right_params

    def _convert_arithmetic_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
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
        self, operands: dict[str, Any]
    ) -> tuple[str, list[Any]]:
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
        self, operands: list[Any]
    ) -> tuple[str, list[Any]]:
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
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert array operators to SQL."""
        # Get the appropriate function names based on SQLite version
        json_each = self.json_each_function  # type: ignore[attr-defined]
        json_group_array = self.json_group_array_function  # type: ignore[attr-defined]

        # Normalize operands for operators that accept single values
        if operator in (
            "$size",
            "$isArray",
            "$sum",
            "$avg",
            "$min",
            "$max",
        ) and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$size":
                if len(operands) != 1:
                    raise ValueError("$size requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
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
                sql = f"EXISTS (SELECT 1 FROM {json_each}({array_sql}) WHERE value = {value_sql})"
                return sql, value_params + array_params
            case "$isArray":
                if len(operands) != 1:
                    raise ValueError("$isArray requires exactly 1 operand")

                operand = operands[0]
                if isinstance(operand, str) and operand.startswith("$"):
                    field_path = operand[1:]
                    sql = f"json_type({self.data_column}, '{parse_json_path(field_path)}') = 'array'"
                    return sql, []
                else:
                    value_sql, value_params = self._convert_operand_to_sql(
                        operand
                    )
                    sql = f"json_type({value_sql}) = 'array'"
                    return sql, value_params
            case "$sum" | "$avg" | "$min" | "$max":
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql_agg = operator[1:].upper()
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
                count = operands[1]
                skip = operands[2] if len(operands) > 2 else 0
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
                sql = f"(SELECT COALESCE((SELECT key FROM {json_each}({array_sql}) WHERE value = {value_sql} LIMIT 1), -1))"
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
                return self._convert_set_operator(operator, operands)
            case _:
                raise NotImplementedError(
                    f"Array operator {operator} not supported in SQL tier"
                )

    def _convert_set_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert set operators to SQL using json_each."""
        # Get the appropriate json_each function based on SQLite version
        json_each = self.json_each_function  # type: ignore[attr-defined]
        json_group_array = self.json_group_array_function  # type: ignore[attr-defined]

        # Normalize operands for operators that accept single values
        if operator in (
            "$anyElementTrue",
            "$allElementsTrue",
        ) and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$setEquals":
                # NOT EXISTS clauses duplicate ? placeholders causing param mismatch.
                raise NotImplementedError(
                    "Operator $setEquals not supported in SQL tier"
                )

            case "$setIntersection":
                if len(operands) != 2:
                    raise ValueError(
                        "$setIntersection requires exactly 2 operands"
                    )

                array1_sql, array1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array2_sql, array2_params = self._convert_operand_to_sql(
                    operands[1]
                )

                # SELECT elements from array1 that exist in array2
                sql = f"""
                (SELECT json({json_group_array}(DISTINCT a1.value))
                 FROM {json_each}({array1_sql}) AS a1
                 WHERE EXISTS (SELECT 1 FROM {json_each}({array2_sql}) AS a2 WHERE a2.value = a1.value))
                """
                return sql, array1_params + array2_params

            case "$setUnion":
                if len(operands) != 2:
                    raise ValueError("$setUnion requires exactly 2 operands")

                array1_sql, array1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array2_sql, array2_params = self._convert_operand_to_sql(
                    operands[1]
                )

                # SELECT DISTINCT elements from both arrays
                sql = f"""
                (SELECT json({json_group_array}(DISTINCT value))
                 FROM (
                   SELECT value FROM {json_each}({array1_sql})
                   UNION
                   SELECT value FROM {json_each}({array2_sql})
                 ))
                """
                return sql, array1_params + array2_params

            case "$setDifference":
                if len(operands) != 2:
                    raise ValueError(
                        "$setDifference requires exactly 2 operands"
                    )

                array1_sql, array1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array2_sql, array2_params = self._convert_operand_to_sql(
                    operands[1]
                )

                # SELECT elements from array1 that don't exist in array2
                sql = f"""
                (SELECT json({json_group_array}(a1.value))
                 FROM {json_each}({array1_sql}) AS a1
                 WHERE NOT EXISTS (SELECT 1 FROM {json_each}({array2_sql}) AS a2 WHERE a2.value = a1.value))
                """
                return sql, array1_params + array2_params

            case "$setIsSubset":
                if len(operands) != 2:
                    raise ValueError("$setIsSubset requires exactly 2 operands")

                array1_sql, array1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array2_sql, array2_params = self._convert_operand_to_sql(
                    operands[1]
                )

                # Check if all elements of array1 exist in array2
                sql = f"""
                (
                  NOT EXISTS (
                    SELECT 1 FROM {json_each}({array1_sql}) AS a1
                    WHERE NOT EXISTS (SELECT 1 FROM {json_each}({array2_sql}) AS a2 WHERE a2.value = a1.value)
                  )
                )
                """
                return sql, array1_params + array2_params

            case "$anyElementTrue":
                if len(operands) != 1:
                    raise ValueError(
                        "$anyElementTrue requires exactly 1 operand"
                    )

                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )

                # Check if any element is truthy (not false, null, or 0)
                sql = f"""
                (
                  EXISTS (
                    SELECT 1 FROM {json_each}({array_sql}) AS a
                    WHERE a.value IS NOT NULL AND a.value != 0 AND a.value != json('false') AND a.value != json('null')
                  )
                )
                """
                return sql, array_params

            case "$allElementsTrue":
                if len(operands) != 1:
                    raise ValueError(
                        "$allElementsTrue requires exactly 1 operand"
                    )

                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )

                # Check if all elements are truthy (no false, null, or 0 elements)
                # Empty array returns True (vacuous truth, matching Python's all([]))
                sql = f"""
                (
                  NOT EXISTS (
                    SELECT 1 FROM {json_each}({array_sql}) AS a
                    WHERE a.value IS NULL OR a.value = 0 OR a.value = json('false') OR a.value = json('null')
                  )
                )
                """
                return sql, array_params

            case _:
                raise NotImplementedError(
                    f"Set operator {operator} not supported in SQL tier"
                )

    def _convert_string_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
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
                        "$indexOfBytes requires string and substring"
                    )
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                substr_sql, substr_params = self._convert_operand_to_sql(
                    operands[1]
                )
                sql = f"(instr({string_sql}, {substr_sql}) - 1)"
                return sql, string_params + substr_params
            case "$strcasecmp":
                # Case-insensitive string comparison using SQLite's COLLATE NOCASE
                if len(operands) != 2:
                    raise ValueError("$strcasecmp requires exactly 2 operands")
                str1_sql, str1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                str2_sql, str2_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # Use CASE expression to return -1, 0, or 1
                sql = f"""
                    CASE
                        WHEN {str1_sql} COLLATE NOCASE < {str2_sql} COLLATE NOCASE THEN -1
                        WHEN {str1_sql} COLLATE NOCASE > {str2_sql} COLLATE NOCASE THEN 1
                        ELSE 0
                    END
                """
                return sql, str1_params + str2_params
            case "$substrBytes":
                # Substring by bytes - SQLite's substr works on characters, not bytes
                # For ASCII this is the same, for UTF-8 we need special handling
                if len(operands) != 3:
                    raise ValueError("$substrBytes requires exactly 3 operands")
                str_sql, str_params = self._convert_operand_to_sql(operands[0])
                start_sql, start_params = self._convert_operand_to_sql(
                    operands[1]
                )
                len_sql, len_params = self._convert_operand_to_sql(operands[2])
                # Use substr - note this works on characters in SQLite
                # For true byte-level operations, would need hex/unescape
                sql = f"substr({str_sql}, {start_sql} + 1, {len_sql})"
                return sql, str_params + start_params + len_params
            case "$regexMatch":
                # SQLite has no built-in REGEXP function unless registered by user.
                raise NotImplementedError(
                    "Operator $regexMatch not supported in SQL tier"
                )
            case "$split":
                # Recursive CTE duplicates ? placeholders causing param mismatch.
                raise NotImplementedError(
                    "Operator $split not supported in SQL tier"
                )
            case "$replaceAll":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string_operand = operands.get("input")
                    find_operand = operands.get("find")
                    replace_operand = operands.get("replacement")
                else:
                    # Handle list format
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceAll requires string, find, and replacement"
                        )
                    string_operand = operands[0]
                    find_operand = operands[1]
                    replace_operand = operands[2]

                string_sql, string_params = self._convert_operand_to_sql(
                    string_operand
                )
                find_sql, find_params = self._convert_operand_to_sql(
                    find_operand
                )
                replace_sql, replace_params = self._convert_operand_to_sql(
                    replace_operand
                )
                sql = f"replace({string_sql}, {find_sql}, {replace_sql})"
                return sql, string_params + find_params + replace_params
            case "$replaceOne":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string_operand = operands.get("input")
                    find_operand = operands.get("find")
                    replace_operand = operands.get("replacement")
                else:
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceOne requires string, find, and replacement"
                        )
                    string_operand = operands[0]
                    find_operand = operands[1]
                    replace_operand = operands[2]

                string_sql, string_params = self._convert_operand_to_sql(
                    string_operand
                )
                find_sql, find_params = self._convert_operand_to_sql(
                    find_operand
                )
                replace_sql, replace_params = self._convert_operand_to_sql(
                    replace_operand
                )
                # Use instr() and substr() to replace only first occurrence
                # Note: string_sql and find_sql are used multiple times, so we
                # need to duplicate params for each occurrence
                sql = (
                    f"CASE WHEN instr({string_sql}, {find_sql}) > 0 THEN "
                    f"substr({string_sql}, 1, instr({string_sql}, {find_sql}) - 1) || "
                    f"{replace_sql} || "
                    f"substr({string_sql}, instr({string_sql}, {find_sql}) + length({find_sql})) "
                    f"ELSE {string_sql} END"
                )
                # Duplicate params to match SQL order:
                # 1. instr(string, find) - string_params + find_params
                # 2. instr(string, find) - string_params + find_params
                # 3. replace - replace_params
                # 4. instr(string, find) - string_params + find_params
                # 5. length(find) - find_params
                all_params = (
                    string_params
                    + find_params  # 1st instr
                    + string_params
                    + find_params  # 2nd instr
                    + replace_params  # replacement
                    + string_params
                    + find_params  # 3rd instr
                    + find_params  # length
                )
                return sql, all_params
            case "$strLenCP":
                # Normalize operands
                if not isinstance(operands, list):
                    operands = [operands]
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
                    raise ValueError("$indexOfCP requires string and substring")
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                substr_sql, substr_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # SQLite instr(haystack, needle) returns 1-based index, convert to 0-based
                # Note: The haystack comes first, needle second (opposite of MongoDB's order)
                sql = f"instr({string_sql}, {substr_sql}) - 1"
                return sql, string_params + substr_params
            case _:
                raise NotImplementedError(
                    f"String operator {operator} not supported in SQL tier"
                )

    def _convert_math_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert math operators to SQL."""
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$exp: 1} and {$exp: [1]}
        # Note: $pow, $log, $sigmoid, and $round can have multiple operands
        if operator not in (
            "$pow",
            "$log",
            "$sigmoid",
            "$round",
        ) and not isinstance(operands, list):
            operands = [operands]

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
            case "$round":
                # $round can have 1 or 2 operands: [number] or [number, precision]
                if len(operands) < 1 or len(operands) > 2:
                    raise ValueError("$round requires 1 or 2 operands")
                number_sql, number_params = self._convert_operand_to_sql(
                    operands[0]
                )
                if len(operands) == 2:
                    precision_sql, precision_params = (
                        self._convert_operand_to_sql(operands[1])
                    )
                    sql = f"round({number_sql}, {precision_sql})"
                    return sql, number_params + precision_params
                else:
                    sql = f"round({number_sql})"
                    return sql, number_params
            case "$sigmoid":
                # Sigmoid function: 1 / (1 + e^(-x))
                # Handle object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
                if isinstance(operands, dict):
                    input_sql, input_params = self._convert_operand_to_sql(
                        operands.get("input")
                    )
                    on_null_value = operands.get("onNull")
                    if on_null_value is not None:
                        on_null_sql, on_null_params = (
                            self._convert_operand_to_sql(on_null_value)
                        )
                        sql = f"(CASE WHEN {input_sql} IS NULL THEN {on_null_sql} ELSE (1.0 / (1.0 + exp(-({input_sql})))) END)"
                        return sql, input_params + on_null_params
                    else:
                        sql = f"(1.0 / (1.0 + exp(-({input_sql}))))"
                        return sql, input_params
                else:
                    # Simple format: { $sigmoid: <expr> } or { $sigmoid: [<expr>] }
                    if not isinstance(operands, list):
                        operands = [operands]
                    if len(operands) != 1:
                        raise ValueError("$sigmoid requires exactly 1 operand")
                    value_sql, value_params = self._convert_operand_to_sql(
                        operands[0]
                    )
                    sql = f"(1.0 / (1.0 + exp(-({value_sql}))))"
                    return sql, value_params
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
                    case _:
                        raise NotImplementedError(
                            f"Math operator {operator} not supported in SQL tier"
                        )

                return sql, value_params

    def _convert_trig_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
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
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert angle conversion operators to SQL."""
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$degreesToRadians: 180} and {$degreesToRadians: [180]}
        if not isinstance(operands, list):
            operands = [operands]

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
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert date operators to SQL using strftime."""
        # Normalize operands to handle both single values and lists
        if not isinstance(operands, list):
            operands = [operands]

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
                sql = f"(CAST(strftime('{fmt}', {value_sql}) AS INTEGER) + 1)"
                return sql, value_params
            case "$dayOfYear":
                fmt = "%j"
            case "$week":
                fmt = "%U"
            case "$isoDayOfWeek":
                fmt = "%u"
            case "$isoWeek":
                fmt = "%V"
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
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert $dateAdd/$dateSubtract operators to SQL.

        MongoDB syntax: {$dateAdd: [date, amount, unit]} or
                        {$dateAdd: {startDate: date, amount: N, unit: "day"}}
        SQLite: datetime(date, '+N unit' or '-N unit')
        """
        # Handle MongoDB dict format: {startDate, amount, unit}
        if isinstance(operands, dict):
            operands = [
                operands.get("startDate"),
                operands.get("amount"),
                operands.get("unit", "day"),
            ]

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
            # Use strftime with 'T' separator and 'Z' suffix so
            # neosqlite_json_loads recognizes the result as a UTC ISO
            # date and converts it back to a timezone-aware datetime
            sql = f"strftime('%Y-%m-%dT%H:%M:%SZ', {date_sql}, {modifier})"
            return sql, date_params
        else:
            # Amount is a field reference - need to use CASE or build dynamically
            # For simplicity, we'll use printf to build the modifier
            amount_sql, amount_params = self._convert_operand_to_sql(
                operands[1]
            )
            if sign == "-":
                amount_sql = f"-({amount_sql})"

            # Use strftime with 'T' separator and 'Z' suffix so
            # neosqlite_json_loads recognizes the result as a UTC ISO
            # date and converts it back to a timezone-aware datetime
            sql = f"strftime('%Y-%m-%dT%H:%M:%SZ', {date_sql}, printf('%+d {sqlite_unit}s', {amount_sql}))"
            return sql, date_params + amount_params

    def _convert_date_diff_operator(
        self, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert $dateDiff operator to SQL.

        MongoDB syntax: {$dateDiff: [date1, date2, unit]} or
                        {$dateDiff: {startDate: date1, endDate: date2, unit: "day"}}
        SQLite: julianday(date2) - julianday(date1) for days
        """
        # Handle MongoDB dict format: {startDate, endDate, unit}
        if isinstance(operands, dict):
            operands = [
                operands.get("startDate"),
                operands.get("endDate"),
                operands.get("unit", "day"),
            ]

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

        # For month and year, use SQLite strftime to extract components
        # and compute the difference directly (julianday-based division
        # is inaccurate for month/year units).
        if unit in ("month", "year"):
            sql = f"""(
                (strftime('%Y', {date2_sql}) - strftime('%Y', {date1_sql})) * 12
                + (strftime('%m', {date2_sql}) - strftime('%m', {date1_sql}))
            )"""
            if unit == "year":
                sql = f"cast({sql} / 12 as integer)"
            return sql, date2_params + date1_params

        # Base calculation: difference in days
        sql = f"(julianday({date2_sql}) - julianday({date1_sql}))"

        # Convert to requested unit
        unit_multipliers = {
            "day": 1,
            "week": 1.0 / 7,
            "hour": 24,
            "minute": 24 * 60,
            "second": 24 * 60 * 60,
        }

        multiplier = unit_multipliers.get(unit, 1)
        if multiplier != 1:
            sql = f"cast({sql} * {multiplier} as integer)"
        else:
            sql = f"cast({sql} as integer)"

        # Params must match placeholder order: date2 first, then date1
        return sql, date2_params + date1_params

    def _convert_object_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert object operators to SQL.

        Note: json_patch() works with both JSON and JSONB data types.
        Only json_extract/jsonb_extract, json_set/jsonb_set have JSONB variants.
        """
        json_prefix = self.json_function_prefix  # type: ignore[attr-defined]

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
                sql = f"{json_prefix}_extract({input_sql}, '{parse_json_path(field)}')"
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
                sql = f"{json_prefix}_set({input_sql}, '{parse_json_path(field)}', {value_sql})"
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
                sql = f"{json_prefix}_remove({input_sql}, '{parse_json_path(field)}')"
                return sql, input_params
            case "$objectToArray":
                # Convert object to array of {k, v} objects
                # Syntax: { $objectToArray: <object> }
                sql_input, params = self._convert_operand_to_sql(operands)
                json_group_array = self.json_group_array_function  # type: ignore[attr-defined]
                json_each = self.json_each_function  # type: ignore[attr-defined]

                # Use a subquery with json_each to build the array
                sql = f"(SELECT json({json_group_array}(json_object('k', key, 'v', value))) FROM {json_each}({sql_input}))"
                return sql, params
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in SQL tier"
                )

    def _convert_let_operator(self, operands: Any) -> tuple[str, list[Any]]:
        """
        Convert $let operator to SQL by inlining variables.

        MongoDB syntax: { $let: { vars: { <var1>: <expr1>, ... }, in: <expr> } }
        """
        if not isinstance(operands, dict):
            raise ValueError("$let requires a dictionary")

        vars_spec = operands.get("vars", {})
        in_expr = operands.get("in")

        if in_expr is None:
            raise ValueError("$let requires 'in' expression")

        # We need the current context to store variables for inlining
        if (
            not hasattr(self, "_current_context")
            or self._current_context is None
        ):
            # Fallback to Python if no context is available
            raise NotImplementedError(
                "$let requires an aggregation context in SQL tier"
            )

        context = self._current_context
        # Create a new context for nested scoping
        nested_context = context.clone()

        for var_name, var_expr in vars_spec.items():
            # Evaluate the variable expression to SQL
            var_sql, var_params = self._convert_operand_to_sql(var_expr)
            # Store the SQL and params in the nested context
            # var_name should be prefixed with $$
            nested_context.set_variable("$$" + var_name, (var_sql, var_params))

        # Now evaluate 'in' using the nested context by temporarily swapping it
        old_context = self._current_context
        self._current_context = nested_context
        try:
            return self._convert_operand_to_sql(in_expr)
        finally:
            self._current_context = old_context

    def _convert_data_size_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $binarySize and $bsonSize operators to SQL."""
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        if operator == "$binarySize":
            # In NeoSQLite, binary data is stored as base64 in a JSON object:
            # {"__neosqlite_binary__": true, "data": "...", "subtype": 0}
            # The 'data' field is base64 encoded.

            # Use 'json_extract' (not jsonb) to ensure we get a text string
            # if the value is extracted from a JSON document.

            # If value_sql is a field reference, it might be jsonb_extract.
            # We want the text version for base64 length calculation.
            text_value_sql = value_sql.replace("jsonb_extract", "json_extract")

            # Extract the base64 string if it's a binary object
            base64_data = f"CASE WHEN typeof({text_value_sql}) = 'text' AND json_extract({text_value_sql}, '$.__neosqlite_binary__') = 1 THEN json_extract({text_value_sql}, '$.data') ELSE {text_value_sql} END"

            # Simple base64 decoded length approximation: (len * 3 / 4)
            # We use CAST AS TEXT to ensure we don't have any JSONB weirdness
            return (
                f"((length(CAST({base64_data} AS TEXT)) * 3) / 4)",
                value_params,
            )

        else:  # $bsonSize
            # MongoDB $bsonSize returns the size of the document in BSON bytes.
            # In NeoSQLite, we return the size of the JSON representation.
            # Use json() to ensure we are measuring the serialized string size,
            # and octet_length/length to get the byte count.
            return f"length(json({value_sql}))", value_params

    def _get_operator_return_type(self, operator: str) -> str | None:
        """
        Infer the BSON return type of a MongoDB operator.

        Returns:
            BSON type name (e.g., 'number', 'bool', 'string', 'array', 'object')
            or None if the return type is ambiguous or unknown.
        """
        match operator:
            # Operators returning numbers
            case (
                "$add"
                | "$subtract"
                | "$multiply"
                | "$divide"
                | "$mod"
                | "$abs"
                | "$ceil"
                | "$floor"
                | "$round"
                | "$trunc"
                | "$pow"
                | "$sqrt"
                | "$ln"
                | "$log"
                | "$log10"
                | "$log2"
                | "$exp"
                | "$sin"
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
                | "$size"
                | "$indexOfArray"
                | "$sum"
                | "$avg"
                | "$min"
                | "$max"
                | "$strLenBytes"
                | "$strLenCP"
                | "$indexOfBytes"
                | "$indexOfCP"
                | "$year"
                | "$month"
                | "$dayOfMonth"
                | "$hour"
                | "$minute"
                | "$second"
                | "$millisecond"
                | "$dayOfWeek"
                | "$dayOfYear"
                | "$week"
                | "$isoDayOfWeek"
                | "$isoWeek"
                | "$dateDiff"
                | "$binarySize"
                | "$bsonSize"
                | "$toInt"
                | "$toDouble"
                | "$toLong"
                | "$toDecimal"
            ):
                return "number"

            # Operators returning booleans
            case (
                "$eq"
                | "$ne"
                | "$gt"
                | "$gte"
                | "$lt"
                | "$lte"
                | "$and"
                | "$or"
                | "$not"
                | "$nor"
                | "$in"
                | "$isArray"
                | "$setEquals"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
                | "$regexMatch"
                | "$isNumber"
                | "$toBool"
            ):
                return "bool"

            # Operators returning strings
            case (
                "$concat"
                | "$toLower"
                | "$toUpper"
                | "$substr"
                | "$substrBytes"
                | "$trim"
                | "$ltrim"
                | "$rtrim"
                | "$replaceAll"
                | "$replaceOne"
                | "$toString"
                | "$type"
            ):
                return "string"

            # Operators returning arrays
            case (
                "$slice"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$split"
                | "$objectToArray"
            ):
                return "array"

            # Operators returning objects
            case "$mergeObjects" | "$getField" | "$setField" | "$unsetField":
                return "object"

            case _:
                return None

    def _get_literal_bson_type(self, value: Any) -> str | None:
        """Get the BSON type name for a literal value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, str):
            # Check if it's a field reference, which is not a literal
            if value.startswith("$"):
                return None
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            # Check if it's an expression
            if len(value) == 1 and next(iter(value.keys())).startswith("$"):
                return None
            return "object"
        return None

    def _convert_type_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert type conversion operators to SQL."""
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$isNumber: "$field"} and {$isNumber: ["$field"]}
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        operand = operands[0]
        value_sql, value_params = self._convert_operand_to_sql(operand)

        match operator:
            case "$toString":
                # Cast to text
                sql = f"cast({value_sql} as text)"
            case "$toInt":
                # Cast to integer, handle non-numeric strings by returning NULL
                # SQLite CAST('abc' AS INTEGER) returns 0, we want NULL for compatibility
                sql = (
                    f"CASE WHEN typeof({value_sql}) IN ('integer', 'real') THEN CAST({value_sql} AS INTEGER) "
                    f"WHEN typeof({value_sql}) = 'text' AND (CAST({value_sql} AS INTEGER) != 0 OR {value_sql} IN ('0', '0.0')) "
                    f"THEN CAST({value_sql} AS INTEGER) ELSE NULL END"
                )
            case "$toDouble":
                # Cast to real/float, handle non-numeric strings by returning NULL
                sql = (
                    f"CASE WHEN typeof({value_sql}) IN ('integer', 'real') THEN CAST({value_sql} AS REAL) "
                    f"WHEN typeof({value_sql}) = 'text' AND (CAST({value_sql} AS REAL) != 0.0 OR {value_sql} IN ('0', '0.0')) "
                    f"THEN CAST({value_sql} AS REAL) ELSE NULL END"
                )
            case "$toLong":
                # SQLite integers are already 64-bit, same as toInt logic
                sql = (
                    f"CASE WHEN typeof({value_sql}) IN ('integer', 'real') THEN CAST({value_sql} AS INTEGER) "
                    f"WHEN typeof({value_sql}) = 'text' AND (CAST({value_sql} AS INTEGER) != 0 OR {value_sql} IN ('0', '0.0')) "
                    f"THEN CAST({value_sql} AS INTEGER) ELSE NULL END"
                )
            case "$toBool":
                # Convert to boolean with MongoDB truthiness semantics
                if isinstance(operand, str) and operand.startswith("$"):
                    # Direct field reference - we can use json_type on the data column
                    field_path = operand[1:]
                    from ..json_path_utils import parse_json_path

                    json_path = parse_json_path(field_path)
                    type_expr = f"json_type({self.data_column}, '{json_path}')"
                    sql = (
                        f"CASE WHEN {type_expr} = 'null' THEN 0 "
                        f"WHEN {type_expr} = 'false' THEN 0 "
                        f"WHEN {type_expr} = 'true' THEN 1 "
                        f"WHEN {type_expr} IN ('integer', 'real') THEN {value_sql} != 0 "
                        f"WHEN {type_expr} = 'text' THEN length({value_sql}) > 0 "
                        f"WHEN {type_expr} IN ('array', 'object') THEN 1 "
                        f"ELSE 0 END"
                    )
                else:
                    # Computed expression or literal - try to infer type
                    inferred_type = None
                    if isinstance(operand, dict) and len(operand) == 1:
                        op_name = next(iter(operand.keys()))
                        if op_name.startswith("$"):
                            inferred_type = self._get_operator_return_type(
                                op_name
                            )
                    else:
                        inferred_type = self._get_literal_bson_type(operand)

                    if inferred_type == "bool":
                        sql = f"{value_sql}"
                    elif inferred_type == "number":
                        sql = f"{value_sql} != 0"
                    elif inferred_type == "string":
                        sql = f"length({value_sql}) > 0"
                    elif inferred_type in ("array", "object"):
                        sql = "1"
                        value_params = []
                    elif inferred_type == "null":
                        sql = "0"
                        value_params = []
                    else:
                        # Fallback to typeof
                        sql = (
                            f"CASE WHEN typeof({value_sql}) = 'text' THEN length({value_sql}) > 0 "
                            f"WHEN typeof({value_sql}) = 'null' THEN 0 "
                            f"ELSE {value_sql} != 0 END"
                        )
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
            case "$isNumber":
                if isinstance(operand, str) and operand.startswith("$"):
                    # Direct field reference
                    field_path = operand[1:]
                    from ..json_path_utils import parse_json_path

                    json_path = parse_json_path(field_path)
                    type_expr = f"json_type({self.data_column}, '{json_path}')"
                    sql = f"CASE WHEN {type_expr} IN ('integer', 'real') THEN 1 ELSE 0 END"
                else:
                    # Computed expression or literal - try to infer type
                    inferred_type = None
                    if isinstance(operand, dict) and len(operand) == 1:
                        op_name = next(iter(operand.keys()))
                        if op_name.startswith("$"):
                            inferred_type = self._get_operator_return_type(
                                op_name
                            )
                    else:
                        inferred_type = self._get_literal_bson_type(operand)

                    if inferred_type == "number":
                        sql = "1"
                        value_params = []
                    elif inferred_type is not None:
                        # Known non-number type (bool, string, array, object, null)
                        sql = "0"
                        value_params = []
                    else:
                        # Truly ambiguous, fall back to Python
                        raise NotImplementedError(
                            "Ambiguous type for $isNumber in SQL tier"
                        )
            case "$type":
                if isinstance(operand, str) and operand.startswith("$"):
                    # Direct field reference
                    field_path = operand[1:]
                    from ..json_path_utils import parse_json_path

                    json_path = parse_json_path(field_path)
                    type_expr = f"json_type({self.data_column}, '{json_path}')"
                    sql = (
                        f"CASE WHEN {type_expr} = 'null' THEN 'null' "
                        f"WHEN {type_expr} IN ('true', 'false') THEN 'bool' "
                        f"WHEN {type_expr} = 'integer' THEN 'int' "
                        f"WHEN {type_expr} = 'real' THEN 'double' "
                        f"WHEN {type_expr} = 'text' THEN 'string' "
                        f"WHEN {type_expr} = 'array' THEN 'array' "
                        f"WHEN {type_expr} = 'object' THEN "
                        f"  CASE WHEN json_extract({self.data_column}, '{json_path}.__neosqlite_binary__') = 1 THEN 'binData' "
                        f"  WHEN json_extract({self.data_column}, '{json_path}.__neosqlite_objectid__') = 1 THEN 'objectId' "
                        f"  ELSE 'object' END "
                        f"ELSE 'unknown' END"
                    )
                else:
                    # Computed expression or literal
                    inferred_type = None
                    if isinstance(operand, dict) and len(operand) == 1:
                        op_name = next(iter(operand.keys()))
                        if op_name.startswith("$"):
                            inferred_type = self._get_operator_return_type(
                                op_name
                            )
                    else:
                        inferred_type = self._get_literal_bson_type(operand)

                    if inferred_type == "number":
                        sql = f"CASE WHEN typeof({value_sql}) = 'integer' THEN 'int' ELSE 'double' END"
                    elif inferred_type == "bool":
                        sql = "'bool'"
                        value_params = []
                    elif inferred_type == "string":
                        sql = "'string'"
                        value_params = []
                    elif inferred_type == "array":
                        sql = "'array'"
                        value_params = []
                    elif inferred_type == "object":
                        sql = (
                            f"CASE WHEN typeof({value_sql}) = 'text' THEN "
                            f"  CASE WHEN json_extract({value_sql}, '$.__neosqlite_binary__') = 1 THEN 'binData' "
                            f"  WHEN json_extract({value_sql}, '$.__neosqlite_objectid__') = 1 THEN 'objectId' "
                            f"  ELSE 'object' END "
                            f"ELSE 'object' END"
                        )
                    elif inferred_type == "null":
                        sql = "'null'"
                        value_params = []
                    else:
                        # Fallback to typeof
                        sql = (
                            f"CASE WHEN typeof({value_sql}) = 'null' THEN 'null' "
                            f"WHEN typeof({value_sql}) = 'integer' THEN 'int' "
                            f"WHEN typeof({value_sql}) = 'real' THEN 'double' "
                            f"WHEN typeof({value_sql}) = 'text' THEN 'string' "
                            f"ELSE 'unknown' END"
                        )
            case "$convert":
                # $convert is complex - requires 'to' field specification
                # Fall back to Python
                raise NotImplementedError(
                    "$convert not supported in SQL tier (use Python fallback)"
                )
            case _:
                raise NotImplementedError(
                    f"Type operator {operator} not supported in SQL tier"
                )

        return sql, value_params

    def _convert_operand_to_sql(self, operand: Any) -> tuple[str, list[Any]]:
        """
        Convert an operand to SQL expression.

        Handles:
        - Field references: "$field" → json_extract/jsonb_extract expression
        - Literals: numbers, strings, booleans
        - Nested expressions: {"$operator": [...]}
        """
        # Check for aggregation variables if context is available
        from .context import _is_aggregation_variable

        if (
            _is_aggregation_variable(operand)
            and hasattr(self, "_current_context")
            and self._current_context is not None
        ):
            # We are inside an aggregator that set the context
            # Use the handle_aggregation_variable method from the parent/mixin
            # which is mixed into ExprEvaluator
            return self._handle_aggregation_variable(operand, self._current_context)  # type: ignore[attr-defined]

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
