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

    def _convert_switch_operator(
        self, operands: dict[str, Any]
    ) -> tuple[str, list[Any]]:
        """Convert $switch operator to SQL CASE statement.

        MongoDB syntax:
            { $switch: {
                branches: [
                    { case: <expr>, then: <expr> },
                    ...
                ],
                default: <expr>   // optional
            } }

        SQL equivalent:
            CASE
                WHEN <case1> THEN <then1>
                WHEN <case2> THEN <then2>
                ...
                ELSE <default>    // NULL if omitted
            END
        """
        if not isinstance(operands, dict):
            raise ValueError("$switch requires a dictionary")

        branches = operands.get("branches")
        if not branches or not isinstance(branches, list) or len(branches) == 0:
            raise ValueError("$switch requires a non-empty 'branches' array")

        default_val = operands.get("default")

        all_params: list[Any] = []
        when_clauses: list[str] = []

        for branch in branches:
            if not isinstance(branch, dict):
                raise ValueError("Each $switch branch must be a dictionary")

            case_expr = branch.get("case")
            then_expr = branch.get("then")

            if case_expr is None:
                raise ValueError(
                    "Each $switch branch requires a 'case' expression"
                )
            if then_expr is None:
                raise ValueError(
                    "Each $switch branch requires a 'then' expression"
                )

            # The 'case' is a boolean expression — use _convert_expr_to_sql
            case_sql, case_params = self._convert_expr_to_sql(case_expr)
            # The 'then' can be any value — use _convert_operand_to_sql
            then_sql, then_params = self._convert_operand_to_sql(then_expr)

            when_clauses.append(f"WHEN {case_sql} THEN {then_sql}")
            all_params.extend(case_params)
            all_params.extend(then_params)

        if default_val is not None:
            default_sql, default_params = self._convert_operand_to_sql(
                default_val
            )
            else_clause = f"ELSE {default_sql}"
            all_params.extend(default_params)
        else:
            else_clause = "ELSE NULL"

        sql = f"CASE {' '.join(when_clauses)} {else_clause} END"
        return sql, all_params
