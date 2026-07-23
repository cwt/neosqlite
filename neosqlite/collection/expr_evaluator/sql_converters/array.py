"""SQL converters for array operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...json_path_utils import parse_json_path

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ArrayMixin(BaseSqlMixin):
    """$size / $in / $isArray / $slice / $indexOfArray / $sum / $avg → SQL."""

    def _convert_array_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert array operators to SQL."""
        # Get the appropriate function names based on SQLite version
        json_each = self.json_each_function
        json_group_array = self.json_group_array_function

        # Normalize operands for operators that accept single values
        if operator in (
            "$size",
            "$isArray",
            "$sum",
            "$avg",
            "$min",
            "$max",
            "$first",
            "$last",
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
                    sql = f"CASE WHEN json_type({self.data_column}, '{parse_json_path(field_path)}') = 'array' THEN json('true') ELSE json('false') END"
                    return sql, []
                else:
                    value_sql, value_params = self._convert_operand_to_sql(
                        operand
                    )
                    sql = f"CASE WHEN json_type({value_sql}) = 'array' THEN json('true') ELSE json('false') END"
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
                    if self.jsonb.jsonb_supported:
                        sql = f"(SELECT json({json_group_array}(value)) FROM (SELECT value FROM {json_each}({array_sql}) LIMIT {count} OFFSET {skip}))"
                    else:
                        sql = f"(SELECT {json_group_array}(value) FROM (SELECT value FROM {json_each}({array_sql}) LIMIT {count} OFFSET {skip}))"
                else:
                    if self.jsonb.jsonb_supported:
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
            case "$arrayElemAt":
                if not isinstance(operands, list) or len(operands) != 2:
                    raise ValueError("$arrayElemAt requires exactly 2 operands")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # Literal index: use SQLite JSON path $[N] / $[#-N] directly
                if isinstance(operands[1], int):
                    idx = operands[1]
                    if idx >= 0:
                        sql = f"json_extract({array_sql}, '$[{idx}]')"
                    else:
                        sql = f"json_extract({array_sql}, '$[#-{abs(idx)}]')"
                    return sql, array_params
                # Dynamic index: use json_each to avoid parameter duplication
                idx_sql, idx_params = self._convert_operand_to_sql(operands[1])
                json_each = self.json_each_function
                sql = (
                    f"(SELECT value FROM {json_each}({array_sql})"
                    f" WHERE CAST(key AS INTEGER) = CAST({idx_sql} AS INTEGER)"
                    f" LIMIT 1)"
                )
                return sql, array_params + idx_params
            case "$first":
                if len(operands) != 1:
                    raise ValueError("$first requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"json_extract({array_sql}, '$[0]')"
                return sql, array_params
            case "$last":
                if len(operands) != 1:
                    raise ValueError("$last requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # Negative index -1 for last element (SQLite JSON path supports #-1)
                sql = f"json_extract({array_sql}, '$[#-1]')"
                return sql, array_params
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
