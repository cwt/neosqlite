"""SQL converters for conditional operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ConditionalMixin(BaseSqlMixin):
    """$cond / $ifNull → SQL CASE / COALESCE."""

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
