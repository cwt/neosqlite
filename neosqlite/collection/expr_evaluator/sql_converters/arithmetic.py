"""SQL converters for arithmetic operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ArithmeticMixin(BaseSqlMixin):
    """SQL converter mixin for arithmetic aggregation operators.

    Converts ``$add``, ``$subtract``, ``$multiply``, ``$divide``, and ``$mod``
    expressions into parenthesized SQLite arithmetic expressions using
    ``BaseSqlMixin`` operand and operator helpers.
    """

    def _convert_arithmetic_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert a MongoDB arithmetic operator expression to SQL.

        Converts each operand with ``_convert_operand_to_sql`` and joins them
        using the mapped SQL operator. For ``$divide``, multiplies the first
        operand by ``1.0`` so SQLite performs floating-point division instead
        of integer truncation.

        Args:
            operator: Arithmetic operator name (e.g., ``"$add"``,
                ``"$divide"``).
            operands: List of at least two operand expressions to combine.

        Returns:
            tuple[str, list[Any]]: A ``(sql, params)`` tuple containing the
            parenthesized SQL expression and bound parameters.

        Raises:
            ValueError: If fewer than two operands are provided.
        """
        if len(operands) < 2:
            raise ValueError(f"{operator} requires at least 2 operands")

        sql_parts = []
        all_params = []

        for operand in operands:
            operand_sql, operand_params = self._convert_operand_to_sql(operand)
            sql_parts.append(operand_sql)
            all_params.extend(operand_params)

        if operator == "$mod":
            # SQLite '%' truncates operands to integers; fold via CTE so each
            # operand is evaluated once while computing floating-point fmod.
            mod_id = getattr(self, "_mod_counter", 0) + 1
            self._mod_counter = mod_id
            acc_sql = sql_parts[0]
            for idx, part in enumerate(sql_parts[1:]):
                cte = f"_mod_{mod_id}_{idx}"
                acc_sql = (
                    f"(WITH {cte}(l, r) AS (SELECT {acc_sql}, {part}) "
                    f"SELECT l - r * CAST(l / r AS INTEGER) FROM {cte})"
                )
            return acc_sql, all_params

        sql_operator = self._map_arithmetic_operator(operator)
        if operator == "$divide":
            # SQLite '/' on two integers truncates; MongoDB always returns
            # a double. Force REAL division (#116).
            sql_parts = [
                f"({part}) * 1.0" if idx == 0 else f"({part})"
                for idx, part in enumerate(sql_parts)
            ]
        sql = f"({f' {sql_operator} '.join(sql_parts)})"

        return sql, all_params
