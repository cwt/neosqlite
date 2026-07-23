"""SQL converters for logical operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class LogicalMixin(BaseSqlMixin):
    """$and / $or / $not / $nor → SQL."""

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
