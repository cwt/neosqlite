"""SQL converters for set operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class SetMixin(BaseSqlMixin):

    def _convert_set_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert set operators to SQL using json_each."""
        # Get the appropriate json_each function based on SQLite version
        json_each = self.json_each_function
        json_group_array = self.json_group_array_function

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
