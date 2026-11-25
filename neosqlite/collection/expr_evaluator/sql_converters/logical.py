"""SQL converters for logical operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class LogicalMixin(BaseSqlMixin):
    """Mixin converting MongoDB logical operators to SQL expressions.

    Translates ``$and``, ``$or``, ``$not``, and ``$nor`` expressions inside
    ``$expr`` into SQL boolean clauses by delegating each operand to
    ``_convert_expr_to_sql`` and combining the resulting fragments and
    bound parameters.
    """

    def _convert_logical_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert a MongoDB logical operator to a SQL boolean expression.

        Handles ``$and``, ``$or``, ``$not``, and ``$nor``. For ``$not``,
        normalizes a single non-list expression into a one-element list and
        returns ``NOT (<expr>)``. For ``$and``, ``$or``, and ``$nor``,
        requires at least two operand expressions.

        Args:
            operator: Logical operator name (``$and``, ``$or``, ``$not``,
                or ``$nor``).
            operands: List of operand expressions, or a single expression
                dict when ``operator`` is ``"$not"``.

        Returns:
            tuple[str, list[Any]]: A ``(sql_expression, parameters)`` pair.

        Raises:
            ValueError: If ``operands`` has an invalid type or arity, or if
                ``operator`` is unrecognized.
        """
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
            return f"NOT COALESCE(({inner_sql}), 0)", inner_params

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
                coalesced = " OR ".join(
                    f"COALESCE({part}, 0)" for part in sql_parts
                )
                sql = f"NOT ({coalesced})"
            case _:
                raise ValueError(f"Unknown logical operator: {operator}")

        return sql, all_params
