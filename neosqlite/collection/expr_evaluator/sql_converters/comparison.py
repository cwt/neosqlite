"""SQL converters for comparison operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ComparisonMixin(BaseSqlMixin):
    """$eq / $gt / $lt / $gte / $lte / $ne / $cmp → SQL."""

    def _convert_comparison_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert comparison operators to SQL."""
        if len(operands) != 2:
            raise ValueError(f"{operator} requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_sql(operands[0])
        right_sql, right_params = self._convert_operand_to_sql(operands[1])

        # $eq/$ne with a None literal: use IS / IS NOT.
        # Also handle missing-field-vs-null distinction via json_type:
        # a missing JSON key (json_type IS NULL) is NOT equal to null.
        if operator in ("$eq", "$ne") and (
            operands[0] is None or operands[1] is None
        ):
            field_op = operands[1] if operands[0] is None else operands[0]
            field_sql = right_sql if operands[0] is None else left_sql
            field_params = right_params if operands[0] is None else left_params
            # Use json_type to distinguish missing key (returns SQL NULL)
            # from present null value (returns 'null').
            # Only do this for simple $field references where the SQL
            # is just json_extract(data, '$.field').
            if (
                isinstance(field_op, str)
                and field_op.startswith("$")
                and "json_extract" in field_sql
            ):
                # $eq: missing->0, null->1, else->0
                # $ne: missing->1, null->0, else->1
                m_val = "0" if operator == "$eq" else "1"
                n_val = "1" if operator == "$eq" else "0"
                e_val = "0" if operator == "$eq" else "1"
                sql = (
                    f"(CASE WHEN json_type({self.data_column},"
                    f"'$." + field_op[1:] + f"') IS NULL THEN {m_val}"
                    f" WHEN {field_sql} IS NULL THEN {n_val}"
                    f" ELSE {e_val} END)"
                )
                return sql, field_params
            # Fallback: simple IS / IS NOT (can't distinguish missing from null)
            sql_operator = "IS" if operator == "$eq" else "IS NOT"
            return (
                f"{left_sql} {sql_operator} {right_sql}",
                left_params + right_params,
            )

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
