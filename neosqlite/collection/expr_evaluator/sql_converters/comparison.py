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

        # $eq between two FIELD references: missing-vs-missing counts as
        # equal (Python tier semantics, #121); null-vs-null handled by the
        # literal branch below.
        if (
            operator == "$eq"
            and isinstance(operands[0], str)
            and operands[0].startswith("$")
            and not operands[0].startswith("$$")
            and isinstance(operands[1], str)
            and operands[1].startswith("$")
            and not operands[1].startswith("$$")
        ):
            from ...json_path_utils import parse_json_path

            p1 = "'" + parse_json_path(operands[0][1:]) + "'"
            p2 = "'" + parse_json_path(operands[1][1:]) + "'"
            d = self.data_column
            sql = (
                f"(CASE "
                f"WHEN json_type({d}, {p1}) IS NULL AND "
                f"json_type({d}, {p2}) IS NULL THEN 1 "
                f"WHEN {left_sql} IS {right_sql} THEN 1 ELSE 0 END)"
            )
            return sql, []

        if (
            operator == "$ne"
            and isinstance(operands[0], str)
            and operands[0].startswith("$")
            and not operands[0].startswith("$$")
            and not (operands[1] is None)
            and not (
                isinstance(operands[1], str) and operands[1].startswith("$")
            )
        ):
            # $ne field-vs-scalar: missing fields DO match ($ne semantics,
            # #121); null-vs-missing does not. Use the two-arg json_type to
            # separate absent keys from explicit nulls.
            from ...json_path_utils import parse_json_path

            p1 = "'" + parse_json_path(operands[0][1:]) + "'"
            d = self.data_column
            lit_sql, lit_params = self._convert_operand_to_sql(operands[1])
            sql = (
                f"(CASE WHEN json_type({d}, {p1}) IS NULL THEN 1"
                f" WHEN {left_sql} IS NULL THEN 0"
                f" ELSE ({left_sql} IS NOT {lit_sql}) END)"
            )
            return sql, lit_params

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
            # is a json/jsonb_extract call.
            both_fields = (
                isinstance(operands[0], str)
                and operands[0].startswith("$")
                and isinstance(field_op, str)
                and field_op.startswith("$")
            )
            if both_fields and operator == "$eq":
                # Both operands are fields: missing-vs-missing compares as
                # equal (both absent) — Python tier semantics (#121). Escape
                # the path via parse_json_path instead of raw concatenation.
                from ...json_path_utils import parse_json_path

                p1 = "'" + parse_json_path(field_op[1:]) + "'"
                p2 = "'" + parse_json_path(operands[0][1:]) + "'"
                t1 = f"json_type({self.data_column}, {p1})"
                t2 = f"json_type({self.data_column}, {p2})"
                sql = (
                    f"(CASE WHEN {t1} IS NULL AND {t2} IS NULL THEN 1"
                    f" WHEN {t1} IS NOT NULL AND {t2} IS NOT NULL"
                    f" THEN CASE WHEN {field_sql} IS {right_sql} THEN 1 ELSE 0 END"
                    f" ELSE 0 END)"
                )
                return sql, field_params
            if (
                isinstance(field_op, str)
                and field_op.startswith("$")
                and (
                    "json_extract" in field_sql or "jsonb_extract" in field_sql
                )
            ):
                from ...json_path_utils import parse_json_path

                safe_path = "'" + parse_json_path(field_op[1:]) + "'"
                # $eq: missing->0, null->1, else->0
                # $ne: missing->1, null->0, else->1
                m_val = "0" if operator == "$eq" else "1"
                n_val = "1" if operator == "$eq" else "0"
                e_val = "0" if operator == "$eq" else "1"
                sql = (
                    f"(CASE WHEN json_type({self.data_column},"
                    f"{safe_path}) IS NULL THEN {m_val}"
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
        # Both fragments appear twice; duplicate params in SQL order:
        # left, right, left, right.
        return sql, left_params + right_params + left_params + right_params
