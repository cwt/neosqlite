"""SQL converters for set operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class SetMixin(BaseSqlMixin):
    """SQL converters for MongoDB aggregation array set operators.

    Translates ``$setEquals``, ``$setIntersection``, ``$setUnion``,
    ``$setDifference``, ``$setIsSubset``, ``$anyElementTrue``, and
    ``$allElementsTrue`` into SQLite queries using ``json_each`` or
    ``jsonb_each`` and ``json_group_array`` or ``jsonb_group_array``.
    """

    def _convert_set_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert set operators to SQL using ``json_each``/``jsonb_each``.

        Supports ``$setEquals``, ``$setIntersection``, ``$setUnion``,
        ``$setDifference``, ``$setIsSubset``, ``$anyElementTrue``, and
        ``$allElementsTrue``. Normalizes single-value operands for the
        boolean element-truthiness operators.

        Args:
            operator: Set operator name (e.g., ``"$setUnion"``).
            operands: Two-element operand list for binary set operators, or a
                single operand (or one-element list) for element truth checks.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.

        Raises:
            ValueError: If the operand count does not match the operator.
            NotImplementedError: If ``operator`` is not supported in SQL.
        """
        # Get the appropriate json_each function based on SQLite version
        json_each = self.json_each_function
        json_group_array = self.json_group_array_function

        # Normalize operands for operators that accept single values
        if operator in (
            "$anyElementTrue",
            "$allElementsTrue",
        ) and not isinstance(operands, list):
            operands = [operands]

        # MongoDB element projection preserving booleans and nested containers
        def _elem_sql(alias: str = "") -> str:
            p = f"{alias}." if alias else ""
            return (
                f"CASE WHEN {p}type = 'true' THEN json('true') "
                f"WHEN {p}type = 'false' THEN json('false') "
                f"WHEN {p}type IN ('object', 'array') THEN json({p}value) "
                f"ELSE {p}value END"
            )

        elem_eq = "(a1.type = a2.type AND a1.value IS a2.value)"

        match operator:
            case "$setEquals":
                if len(operands) != 2:
                    raise ValueError("$setEquals requires exactly 2 operands")
                array1_sql, array1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array2_sql, array2_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # Bidirectional subset check: A ⊆ B AND B ⊆ A.
                # Each array reference appears twice, so its parameters
                # must appear twice in the binding list, in SQL order:
                # array1, array2, array2, array1.
                sql = f"""
                (
                  NOT EXISTS (
                    SELECT 1 FROM {json_each}({array1_sql}) AS a1
                    WHERE NOT EXISTS (
                      SELECT 1 FROM {json_each}({array2_sql}) AS a2
                      WHERE {elem_eq}
                    )
                  )
                  AND
                  NOT EXISTS (
                    SELECT 1 FROM {json_each}({array2_sql}) AS a2
                    WHERE NOT EXISTS (
                      SELECT 1 FROM {json_each}({array1_sql}) AS a1
                      WHERE {elem_eq}
                    )
                  )
                )
                """
                return (
                    sql,
                    array1_params
                    + array2_params
                    + array2_params
                    + array1_params,
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
                json((SELECT {json_group_array}(DISTINCT {_elem_sql('a1')})
                 FROM {json_each}({array1_sql}) AS a1
                 WHERE EXISTS (SELECT 1 FROM {json_each}({array2_sql}) AS a2 WHERE {elem_eq})))
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
                json((SELECT {json_group_array}({_elem_sql()})
                 FROM (
                   SELECT value, type FROM {json_each}({array1_sql})
                   UNION
                   SELECT value, type FROM {json_each}({array2_sql})
                 )))
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
                json((SELECT {json_group_array}({_elem_sql('a1')})
                 FROM {json_each}({array1_sql}) AS a1
                 WHERE NOT EXISTS (SELECT 1 FROM {json_each}({array2_sql}) AS a2 WHERE {elem_eq})))
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
                    WHERE NOT EXISTS (SELECT 1 FROM {json_each}({array2_sql}) AS a2 WHERE {elem_eq})
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

                # MongoDB falsy elements: false, null, or numeric 0.
                # Strings like "false", "", and empty containers are truthy.
                sql = f"""
                (
                  EXISTS (
                    SELECT 1 FROM {json_each}({array_sql}) AS a
                    WHERE a.type NOT IN ('null', 'false')
                      AND NOT (a.type IN ('integer', 'real') AND a.value = 0)
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

                # Empty array returns True; only false, null, and numeric 0 are falsy.
                sql = f"""
                (
                  NOT EXISTS (
                    SELECT 1 FROM {json_each}({array_sql}) AS a
                    WHERE a.type IN ('null', 'false')
                       OR (a.type IN ('integer', 'real') AND a.value = 0)
                  )
                )
                """
                return sql, array_params

            case _:
                raise NotImplementedError(
                    f"Set operator {operator} not supported in SQL tier"
                )
