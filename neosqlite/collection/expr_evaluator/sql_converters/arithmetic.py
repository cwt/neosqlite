"""SQL converters for arithmetic operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ArithmeticMixin(BaseSqlMixin):

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
