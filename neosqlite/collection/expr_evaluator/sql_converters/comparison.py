"""SQL converters for comparison operators."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ComparisonMixin(BaseSqlMixin):
    """SQL converters for MongoDB aggregation comparison operators.

    Translates ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, and
    ``$cmp`` into SQLite expressions and parameter lists while preserving
    MongoDB semantics for missing fields vs. explicit ``null`` and normalized
    ISO-8601 date comparisons.
    """

    def _convert_comparison_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert comparison operators to SQL expressions and parameters.

        Handles ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, and ``$lte``.
        Distinguishes missing JSON keys from explicit ``null`` values via
        ``json_type`` for field comparisons, normalizes date expressions via
        ``strftime``, and falls back to Python evaluation when ordering
        against ``None`` or comparing mixed date and non-date operands.

        Args:
            operator: Comparison operator name (e.g., ``"$eq"``, ``"$gt"``).
            operands: Two-element list of left and right operands.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.

        Raises:
            ValueError: If ``operands`` does not contain two elements.
            NotImplementedError: If ordering involves ``None`` or a mixed
                date/non-date comparison requires Python evaluation.
        """
        if len(operands) != 2:
            raise ValueError(f"{operator} requires exactly 2 operands")

        # Force Python fallback for ordering comparisons involving null so that
        # MongoDB's missing-vs-null ordering (missing < null) is respected.
        if operator in ("$gt", "$gte", "$lt", "$lte") and (
            operands[0] is None or operands[1] is None
        ):
            raise NotImplementedError(
                f"{operator} with null operand requires Python evaluation"
            )

        left_sql, left_params = self._convert_operand_to_sql(operands[0])
        right_sql, right_params = self._convert_operand_to_sql(operands[1])

        # $eq/$ne between two FIELD references: missing-vs-missing counts as
        # equal ($eq -> 1, $ne -> 0), while missing-vs-present (including null)
        # is not equal ($eq -> 0, $ne -> 1).
        def _is_doc_field(op: Any) -> bool:
            return (
                isinstance(op, str)
                and op.startswith("$")
                and not op.startswith("$$")
            )

        if (
            operator in ("$eq", "$ne")
            and _is_doc_field(operands[0])
            and _is_doc_field(operands[1])
        ):
            from ...json_path_utils import parse_json_path

            p1 = "'" + parse_json_path(operands[0][1:]) + "'"
            p2 = "'" + parse_json_path(operands[1][1:]) + "'"
            d = self.data_column
            if operator == "$eq":
                sql = (
                    f"(CASE "
                    f"WHEN json_type({d}, {p1}) IS NULL AND "
                    f"json_type({d}, {p2}) IS NULL THEN 1 "
                    f"WHEN json_type({d}, {p1}) IS NULL OR "
                    f"json_type({d}, {p2}) IS NULL THEN 0 "
                    f"WHEN {left_sql} IS {right_sql} THEN 1 ELSE 0 END)"
                )
            else:
                sql = (
                    f"(CASE "
                    f"WHEN json_type({d}, {p1}) IS NULL AND "
                    f"json_type({d}, {p2}) IS NULL THEN 0 "
                    f"WHEN json_type({d}, {p1}) IS NULL OR "
                    f"json_type({d}, {p2}) IS NULL THEN 1 "
                    f"WHEN {left_sql} IS NOT {right_sql} THEN 1 ELSE 0 END)"
                )
            return sql, []

        if operator == "$ne" and (
            (
                _is_doc_field(operands[0])
                and operands[1] is not None
                and not _is_doc_field(operands[1])
            )
            or (
                _is_doc_field(operands[1])
                and operands[0] is not None
                and not _is_doc_field(operands[0])
            )
        ):
            # $ne field-vs-non-null-scalar/expression (symmetric regardless of
            # operand order): missing fields DO match ($ne semantics, #121).
            from ...json_path_utils import parse_json_path

            if _is_doc_field(operands[0]):
                field_op, field_sql = operands[0], left_sql
                other_sql, other_params = right_sql, right_params
            else:
                field_op, field_sql = operands[1], right_sql
                other_sql, other_params = left_sql, left_params
            p1 = "'" + parse_json_path(field_op[1:]) + "'"
            d = self.data_column
            sql = (
                f"(CASE WHEN json_type({d}, {p1}) IS NULL THEN 1"
                f" ELSE ({field_sql} IS NOT {other_sql}) END)"
            )
            return sql, other_params

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

        # Date comparison normalization: when either operand is a datetime literal
        # or a date-producing SQL expression ($dateAdd/$dateSubtract/$dateFromParts/
        # $dateFromString/$dateTrunc), stored {"$date": "+00:00"} values and SQLite
        # "...Z" strings must be canonicalized via strftime before comparison.
        _date_producing_ops = (
            "$dateAdd",
            "$dateSubtract",
            "$dateFromParts",
            "$dateFromString",
            "$dateTrunc",
        )

        def _is_date_producing(op_val: Any, op_sql: str) -> bool:
            """Return whether an operand represents or emits a date value.

            Args:
                op_val: Raw aggregation operand value.
                op_sql: Converted SQL expression for ``op_val``.

            Returns:
                bool: ``True`` if ``op_val`` or ``op_sql`` produces a date.
            """
            if isinstance(op_val, datetime):
                return True
            if isinstance(op_val, dict) and len(op_val) == 1:
                if next(iter(op_val)) in _date_producing_ops:
                    return True
            return any(
                marker in op_sql
                for marker in (
                    "json_object('$date',",
                    'json_object("$date",',
                    "jsonb_object('$date',",
                    'jsonb_object("$date",',
                )
            )

        is_date_cmp = _is_date_producing(
            operands[0], left_sql
        ) or _is_date_producing(operands[1], right_sql)
        if is_date_cmp and operator in (
            "$eq",
            "$ne",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
        ):
            from ...json_path_utils import parse_json_path

            prefix = self.jsonb.function("extract")

            def _date_norm_sql(
                op_val: Any, op_sql: str
            ) -> tuple[str, list[Any] | None]:
                """Normalize a date operand to canonical UTC ``strftime`` SQL.

                Args:
                    op_val: Raw aggregation operand value.
                    op_sql: Converted SQL expression for ``op_val``.

                Returns:
                    tuple[str, list[Any] | None]: Normalized SQL expression
                        and optional parameter override (``None`` keeps
                        existing parameters).

                Raises:
                    NotImplementedError: If the operand is not a date value.
                """
                if (
                    isinstance(op_val, str)
                    and op_val.startswith("$")
                    and not op_val.startswith("$$")
                ):
                    path = f"'{parse_json_path(op_val[1:], preserve_id=False)}.\"$date\"'"
                    iso = f"{prefix}({self.data_column}, {path})"
                    return f"strftime('%Y-%m-%dT%H:%M:%fZ', {iso})", []
                if any(
                    marker in op_sql
                    for marker in (
                        "json_object('$date',",
                        'json_object("$date",',
                        "jsonb_object('$date',",
                        'jsonb_object("$date",',
                    )
                ):
                    iso = f"json_extract({op_sql}, '$.$date')"
                    return f"strftime('%Y-%m-%dT%H:%M:%fZ', {iso})", None
                raise NotImplementedError(
                    "Mixed date/non-date comparison requires Python evaluation"
                )

            norm_left, override_left = _date_norm_sql(operands[0], left_sql)
            norm_right, override_right = _date_norm_sql(operands[1], right_sql)
            final_left_params = (
                override_left if override_left is not None else left_params
            )
            final_right_params = (
                override_right if override_right is not None else right_params
            )
            if operator == "$eq":
                sql_op = "IS"
            elif operator == "$ne":
                sql_op = "IS NOT"
            else:
                sql_op = self._map_comparison_operator(operator)
            return (
                f"({norm_left} {sql_op} {norm_right})",
                final_left_params + final_right_params,
            )

        sql_operator = self._map_comparison_operator(operator)

        return (
            f"{left_sql} {sql_operator} {right_sql}",
            left_params + right_params,
        )

    def _convert_cmp_operator(
        self, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert ``$cmp`` operator to a SQL ``CASE`` expression.

        Replicates MongoDB three-way comparison ordering (``-1``, ``0``,
        ``1``), treating ``NULL`` as less than non-null values and equal to
        ``NULL``.

        Args:
            operands: Two-element list of operands to compare.

        Returns:
            tuple[str, list[Any]]: SQL ``CASE`` expression and repeated bind
                parameter list matching each operand occurrence.

        Raises:
            ValueError: If ``operands`` does not contain two elements.
        """
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_sql(operands[0])
        right_sql, right_params = self._convert_operand_to_sql(operands[1])

        sql = (
            f"(CASE "
            f"WHEN ({left_sql}) IS NULL AND ({right_sql}) IS NULL THEN 0 "
            f"WHEN ({left_sql}) IS NULL THEN -1 "
            f"WHEN ({right_sql}) IS NULL THEN 1 "
            f"WHEN ({left_sql}) < ({right_sql}) THEN -1 "
            f"WHEN ({left_sql}) > ({right_sql}) THEN 1 "
            f"ELSE 0 END)"
        )
        return sql, (left_params + right_params) * 4
