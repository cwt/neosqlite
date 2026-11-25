from __future__ import annotations

import json
import logging
from collections.abc import Callable
from itertools import pairwise
from typing import Any

from ...sql_utils import quote_table_name
from ..expr_evaluator import AggregationContext, _is_expression
from ..json_path_utils import parse_json_path
from ..query_helper.utils import _check_sqlite_version
from .operators_base import OperatorsBaseMixin
from .operators_hybrid import python_hybrid_fallback

logger = logging.getLogger(__name__)


class OperatorsGroupMixin(OperatorsBaseMixin):
    """Mixin providing temporary-table processors for grouping stages.

    Implements SQL temporary-table evaluation for ``$group``, ``$bucket``,
    and ``$bucketAuto`` aggregation pipeline stages, along with ranked
    ``$first``/``$last`` CTE builders and typed result extraction.
    """

    @python_hybrid_fallback("$group")
    def _process_group_stage(
        self,
        create_temp: Callable,
        current_table: str,
        group_spec: dict[str, Any],
    ) -> str:
        """Process a ``$group`` stage using temporary tables.

        Groups input documents from ``current_table`` by the ``_id`` key
        expression and evaluates accumulator fields into a new temporary
        table.

        Supported accumulators in the SQL tier:
        - ``$sum``, ``$avg``, ``$min``, ``$max``: Standard SQL aggregates.
        - ``$count``: ``COUNT(*)``.
        - ``$first``, ``$last``: Ranked window CTE preserving input row order.
        - ``$addToSet``: ``json_group_array(DISTINCT ...)`` or JSONB variant.
        - ``$push``: ``json_group_array(...)`` or JSONB equivalent.
        - Expression keys: Evaluated via ``self.expr_evaluator``.

        Args:
            create_temp: Callback used to materialize the stage's SQL query
                into a temporary table.
            current_table: Name of the current temporary table holding input
                documents.
            group_spec: The ``$group`` stage specification dictionary.

        Returns:
            str: Name of the newly created temporary table with grouped rows.

        Raises:
            NotImplementedError: If force fallback is active or an unsupported
                group key, accumulator, or expression is encountered.
        """
        # Check kill switch FIRST (Bug 010 fix)
        from ..query_helper import get_force_fallback

        if get_force_fallback():
            raise NotImplementedError(
                "Force fallback - use Tier 3 Python evaluation"
            )

        # Reset per-stage state
        self._group_key_expr: str | None = None
        self._group_key_params: list[Any] = []

        # Inspect available columns on current_table (post-$group tables omit _id)
        table_cols = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        has_underscore_id = "_id" in {col[1] for col in table_cols}
        underscore_id_expr = (
            "_id"
            if has_underscore_id
            else f"{self.jsonb.json_function_prefix}_extract(data, '$._id')"
        )

        group_id_expr = group_spec.get("_id")
        select_parts = []
        group_by_parts = []
        select_params: list[Any] = []
        first_last_params: list[Any] = []
        array_fields = []  # Track fields that are arrays (from $push/$addToSet)
        object_fields = (
            []
        )  # Track fields that are JSON objects (from $first/$last with $$ROOT)

        # Handle _id (group key)
        if group_id_expr is None:
            # Group all documents together
            select_parts.append("NULL AS _id")
        elif isinstance(group_id_expr, str) and group_id_expr.startswith("$"):
            field_name = group_id_expr[1:]
            if field_name == "_id":
                # Special case: grouping by _id column or embedded _id
                select_parts.append(f"{underscore_id_expr} AS _id")
                group_by_parts.append(underscore_id_expr)
            else:
                # Group by extracted field
                json_extract = f"{self.jsonb.json_function_prefix}_extract"
                select_parts.append(
                    f"{json_extract}(data, '{parse_json_path(field_name)}') AS _id"
                )
                group_by_parts.append(
                    f"{json_extract}(data, '{parse_json_path(field_name)}')"
                )
        else:
            # Support expression keys using ExprEvaluator
            # This allows grouping by computed fields like {$concat: ["$firstName", " ", "$lastName"]}
            try:
                key_expr, key_params = (
                    self.expr_evaluator.build_select_expression(
                        group_id_expr, context=AggregationContext()
                    )
                )
                if key_expr:
                    # Precompute expression key once in a CTE so ? params aren't duplicated
                    self._group_key_expr = key_expr
                    self._group_key_params = list(key_params or [])
                    select_parts.append("__neosqlite_gk AS _id")
                    group_by_parts.append("__neosqlite_gk")
                else:
                    raise NotImplementedError(
                        f"$group with expression key {group_id_expr} not supported in SQL tier"
                    )
            except NotImplementedError:
                raise
            except Exception as e:
                logger.debug(
                    f"$group with expression key {group_id_expr} not supported in SQL tier: {e}"
                )
                raise NotImplementedError(
                    f"$group with expression key {group_id_expr} not supported in SQL tier: {e}"
                )

        # $first/$last accumulators are materialized via a ranked CTE
        self._first_last_fields: list[tuple[str, str, bool, str, bool]] = []
        # Map (output_doc_field, internal_sql_alias) to avoid illegal or
        # reserved-word column aliases in the inner grouped SELECT.
        self._group_output_aliases: list[tuple[str, str]] = [("_id", "_id")]
        next_alias_idx = 0

        def _alloc_alias(doc_field: str) -> str:
            """Allocate a safe internal SQL column alias for a $group field."""
            nonlocal next_alias_idx
            sql_alias = f"__acc_{next_alias_idx}"
            next_alias_idx += 1
            self._group_output_aliases.append((doc_field, sql_alias))
            return sql_alias

        def _format_literal_sql(val: Any) -> str:
            """Format a Python literal value as an inline SQL expression.

            Converts ``None``, booleans, numbers, single-quote-escaped strings,
            and JSON containers into SQL literal expressions suitable for
            ``CREATE TABLE ... AS SELECT``.

            Args:
                val: Python literal value to convert.

            Returns:
                str: Inline SQL literal representation of ``val``.
            """
            if val is None:
                return "NULL"
            if isinstance(val, bool):
                return "1" if val else "0"
            if isinstance(val, (int, float)):
                return str(val)
            if isinstance(val, str):
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
            if isinstance(val, (dict, list)):
                json_str = json.dumps(val, separators=(",", ":")).replace(
                    "'", "''"
                )
                return f"json('{json_str}')"
            return f"'{val}'"

        def _build_template_object_sql(
            template: dict[str, Any], op_name: str
        ) -> str:
            """Build a JSON object SQL expression for a $push/$addToSet dict."""
            json_extract = f"{self.jsonb.json_function_prefix}_extract"
            json_object_func = f"{self.jsonb.json_function_prefix}_object"
            obj_args: list[str] = []
            for key, val in template.items():
                escaped_key = key.replace("'", "''")
                if isinstance(val, str) and val in ("$$ROOT", "$$CURRENT"):
                    obj_args.append(f"'{escaped_key}', json(data)")
                elif isinstance(val, str) and val.startswith("$$ROOT."):
                    obj_args.append(
                        f"'{escaped_key}', {json_extract}(data, '{parse_json_path(val[7:])}')"
                    )
                elif isinstance(val, str) and val.startswith("$$CURRENT."):
                    obj_args.append(
                        f"'{escaped_key}', {json_extract}(data, '{parse_json_path(val[10:])}')"
                    )
                elif isinstance(val, str) and val.startswith("$"):
                    field_name = val[1:]
                    if field_name == "_id":
                        obj_args.append(f"'{escaped_key}', _id")
                    else:
                        obj_args.append(
                            f"'{escaped_key}', {json_extract}(data, '{parse_json_path(field_name)}')"
                        )
                elif isinstance(val, (int, float, str, bool, dict, list)) or (
                    val is None
                ):
                    if isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        obj_args.append(
                            f"'{escaped_key}', json_quote('{escaped_val}')"
                        )
                    elif isinstance(val, (dict, list)):
                        json_str = json.dumps(
                            val, separators=(",", ":")
                        ).replace("'", "''")
                        obj_args.append(f"'{escaped_key}', json('{json_str}')")
                    elif val is None:
                        obj_args.append(f"'{escaped_key}', NULL")
                    elif isinstance(val, bool):
                        obj_args.append(f"'{escaped_key}', {1 if val else 0}")
                    else:
                        obj_args.append(f"'{escaped_key}', {val}")
                else:
                    raise NotImplementedError(
                        f"{op_name} with complex expression {template} not supported in SQL tier"
                    )
            return f"{json_object_func}({', '.join(obj_args)})"

        # Handle accumulators
        for field, accumulator in group_spec.items():
            if field == "_id":
                continue

            if not isinstance(accumulator, dict) or len(accumulator) != 1:
                raise NotImplementedError(
                    f"$group accumulator {field} must be a single operator"
                )

            op, expr = next(iter(accumulator.items()))

            # Check for unsupported operators
            if op == "$accumulator":
                raise NotImplementedError(
                    "The '$accumulator' operator is not supported in NeoSQLite. "
                    "Please use built-in accumulators ($sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last), "
                    "or post-process results in Python."
                )

            # Extract field name from expression or identify literal values
            is_literal = False
            literal_value = None
            expr_sql: str | None = None
            expr_params: list[Any] = []
            if isinstance(expr, str) and expr in ("$$ROOT", "$$CURRENT"):
                expr_field = expr
            elif isinstance(expr, str) and expr.startswith("$$ROOT."):
                expr_field = expr[7:]
            elif isinstance(expr, str) and expr.startswith("$$CURRENT."):
                expr_field = expr[10:]
            elif isinstance(expr, str) and expr.startswith("$"):
                expr_field = expr[1:]
            elif (
                isinstance(expr, (int, float, bool))
                or expr is None
                or (isinstance(expr, str) and not expr.startswith("$"))
            ):
                expr_field = None
                is_literal = True
                literal_value = expr
            elif isinstance(expr, dict):
                # Either an arithmetic/conditional/string expression dict ($multiply, $cond, ...)
                # or a subdocument object template for $push / $addToSet
                expr_field = None
                if _is_expression(expr):
                    try:
                        expr_sql, expr_params = (
                            self.expr_evaluator.build_select_expression(
                                expr, context=AggregationContext()
                            )
                        )
                        expr_params = list(expr_params or [])
                    except NotImplementedError:
                        expr_sql, expr_params = None, []
            else:
                # Complex expression - fall back to Python
                raise NotImplementedError(
                    f"$group accumulator {op} with expression {expr} not supported in SQL tier"
                )

            # Map accumulator to SQL
            json_extract = f"{self.jsonb.json_function_prefix}_extract"
            json_group_array = self.jsonb.json_group_array_function

            match op:
                case "$sum" | "$avg" | "$min" | "$max":
                    sql_alias = _alloc_alias(field)
                    sql_func = op[1:].upper()
                    if op == "$sum" and is_literal and literal_value == 1:
                        select_parts.append(f"COUNT(*) AS {sql_alias}")
                    elif is_literal:
                        select_parts.append(
                            f"{sql_func}({_format_literal_sql(literal_value)}) AS {sql_alias}"
                        )
                    elif expr_field:
                        val_expr = (
                            underscore_id_expr
                            if expr_field == "_id"
                            else f"{json_extract}(data, '{parse_json_path(expr_field)}')"
                        )
                        select_parts.append(
                            f"{sql_func}({val_expr}) AS {sql_alias}"
                        )
                    elif expr_sql is not None:
                        select_parts.append(
                            f"{sql_func}({expr_sql}) AS {sql_alias}"
                        )
                        select_params.extend(expr_params)
                    else:
                        raise NotImplementedError(
                            f"$group accumulator {op} with expression {expr} not supported in SQL tier"
                        )

                case "$count":
                    sql_alias = _alloc_alias(field)
                    select_parts.append(f"COUNT(*) AS {sql_alias}")

                case "$first" | "$last":
                    sql_alias = _alloc_alias(field)
                    if is_literal:
                        select_parts.append(
                            f"{_format_literal_sql(literal_value)} AS {sql_alias}"
                        )
                    elif expr_field:
                        if expr_field in ("$$ROOT", "$$CURRENT"):
                            object_fields.append(field)
                        self._first_last_fields.append(
                            (
                                field,
                                sql_alias,
                                op == "$last",
                                expr_field,
                                False,
                            )
                        )
                    elif expr_sql is not None:
                        self._first_last_fields.append(
                            (
                                field,
                                sql_alias,
                                op == "$last",
                                expr_sql,
                                True,
                            )
                        )
                        first_last_params.extend(expr_params)
                    else:
                        raise NotImplementedError(
                            f"$group accumulator {op} with expression {expr} not supported in SQL tier"
                        )

                case "$addToSet" | "$push":
                    # Use json_group_array (with DISTINCT for $addToSet)
                    # Track this field for post-processing
                    sql_alias = _alloc_alias(field)
                    array_fields.append(field)
                    distinct_prefix = "DISTINCT " if op == "$addToSet" else ""

                    if expr_sql is not None:
                        select_parts.append(
                            f"{json_group_array}({distinct_prefix}{expr_sql}) AS {sql_alias}"
                        )
                        select_params.extend(expr_params)
                    elif isinstance(expr, dict):
                        template_sql = _build_template_object_sql(expr, op)
                        select_parts.append(
                            f"{json_group_array}({distinct_prefix}{template_sql}) AS {sql_alias}"
                        )
                    elif is_literal:
                        select_parts.append(
                            f"{json_group_array}({distinct_prefix}{_format_literal_sql(literal_value)}) AS {sql_alias}"
                        )
                    elif expr_field:
                        if expr_field == "_id":
                            select_parts.append(
                                f"{json_group_array}({distinct_prefix}_id) AS {sql_alias}"
                            )
                        elif expr_field in ("$$ROOT", "$$CURRENT"):
                            select_parts.append(
                                f"{json_group_array}({distinct_prefix}json(data)) AS {sql_alias}"
                            )
                        else:
                            select_parts.append(
                                f"{json_group_array}({distinct_prefix}{json_extract}(data, '{parse_json_path(expr_field)}')) AS {sql_alias}"
                            )

                case _:
                    # Unsupported accumulator
                    raise NotImplementedError(
                        f"$group accumulator ${op} not supported in SQL tier"
                    )

        # Build GROUP BY clause
        group_by_clause = ""
        if group_by_parts:
            group_by_clause = f"GROUP BY {', '.join(group_by_parts)}"

        # Create group temporary table
        group_stage = {"$group": group_spec}

        # For grouped results, we need to properly construct the output
        # The _id field should be the group key, and other fields are accumulators
        # We'll create a JSON object with all the fields
        json_args = ", ".join(
            (
                f"'{doc_key.replace(chr(39), chr(39) * 2)}', "
                f"CASE WHEN json_valid({sql_alias}) THEN "
                f"CASE WHEN json_type({sql_alias}) IN ('array', 'object') "
                f"THEN json({sql_alias}) ELSE {sql_alias} END ELSE {sql_alias} END"
                if doc_key == "_id" or doc_key in array_fields
                else f"'{doc_key.replace(chr(39), chr(39) * 2)}', {sql_alias}"
            )
            for doc_key, sql_alias in self._group_output_aliases
        )
        json_object_func = f"{self.jsonb.json_function_prefix}_object"
        # Wrap with json() to ensure text output for Python consumption
        # (jsonb_object returns binary JSONB which Python can't read directly)
        json_output_func = f"json({json_object_func}"

        group_params = list(getattr(self, "_group_key_params", []) or [])

        if self._first_last_fields:
            query, _ = self._ranked_group_query(
                current_table=current_table,
                select_parts=select_parts,
                group_by_parts=group_by_parts,
                json_args=json_args,
                json_output_func=json_output_func,
                has_underscore_id=has_underscore_id,
            )
            all_params = group_params + select_params + first_last_params
        else:
            if self._group_key_expr is not None:
                cte_prefix = (
                    f"WITH __src AS (SELECT *, {self._group_key_expr} AS __neosqlite_gk "
                    f"FROM {current_table}) "
                )
                source_table = "__src"
            else:
                cte_prefix = ""
                source_table = current_table
            query = (
                cte_prefix
                + "SELECT ROW_NUMBER() OVER () as id, "
                + f"{json_output_func}({json_args})) as data "
                + self._grouped_source_sql(
                    source_table, select_parts, group_by_clause
                )
            )
            all_params = group_params + select_params
        new_table = create_temp(group_stage, query, params=all_params or None)

        # Store array fields metadata for efficient post-processing
        # This avoids scanning all fields in _get_results_from_table
        if not hasattr(self, "_array_fields_map"):
            self._array_fields_map = {}
        self._array_fields_map[new_table] = array_fields

        if not hasattr(self, "_object_fields_map"):
            self._object_fields_map = {}
        self._object_fields_map[new_table] = object_fields

        return new_table

    @staticmethod
    def _select_aliases(select_parts: list[str]) -> list[str]:
        """Extract output aliases from ``expression AS alias`` fragments.

        The grouped ``SELECT`` is wrapped in an outer projection (#104), so
        ``json_object`` must reference the inner aliases instead of
        re-evaluating aggregate expressions against the source ``data`` column.

        Args:
            select_parts: List of SQL projection fragments, typically of the
                form ``"<expr> AS <alias>"``.

        Returns:
            list[str]: Extracted alias names (defaulting to ``"column"`` when
                ``" AS "`` is absent).
        """
        aliases = []
        for part in select_parts:
            if " AS " in part:
                alias = part.rsplit(" AS ", 1)[1].strip().strip('"').strip("'")
                aliases.append(alias)
            else:
                aliases.append("column")
        return aliases

    def _ranked_group_query(
        self,
        current_table: str,
        select_parts: list[str],
        group_by_parts: list[str],
        json_args: str,
        json_output_func: str,
        has_underscore_id: bool = True,
    ) -> tuple[str, list]:
        """Build the ``$group`` query when ``$first`` or ``$last`` are present.

        Uses a ranked CTE (#105) to assign per-group row numbers by input
        stream ``rowid`` order (preserving preceding ``$sort`` or ``$unwind``
        ordering), partitioned by the group key, and converts ``$first`` and
        ``$last`` into ``MAX(CASE WHEN rn/rd = 1 ...)`` aggregates.

        Args:
            current_table: Name of the temporary table containing input rows.
            select_parts: Non-``$first``/``$last`` SQL projection expressions.
            group_by_parts: SQL expressions used for ``GROUP BY`` partitioning.
            json_args: Prebuilt ``json_object`` argument string (unused here
                because ``local_json_args`` is built with JSONB blob guards).
            json_output_func: Prefix opening the outer JSON output wrapper
                (for example ``"json(json_object"``).
            has_underscore_id: Whether ``current_table`` includes an ``_id``
                column. Defaults to True.

        Returns:
            tuple[str, list]: Pair of ``(sql_query, extra_params)`` where
                ``extra_params`` is currently empty.
        """
        # Text extraction prevents SQLite from returning a raw JSONB blob when
        # aggregate MAX() discards the subtype on $first/$last fields.
        gk_expr = getattr(self, "_group_key_expr", None) or (
            group_by_parts[0] if group_by_parts else None
        )

        src_cols = ["rowid AS __row_order"]
        if has_underscore_id:
            src_cols.append("_id")
        src_cols.append("data AS data")
        if gk_expr is not None:
            src_cols.append(f"{gk_expr} AS __neosqlite_gk")
        partition = "PARTITION BY __neosqlite_gk" if gk_expr is not None else ""
        cte = (
            f"WITH src AS (SELECT {', '.join(src_cols)} FROM {current_table}), "
            f"ranked AS (SELECT src.*, "
            f"ROW_NUMBER() OVER ({partition} ORDER BY __row_order ASC) AS rn, "
            f"ROW_NUMBER() OVER ({partition} ORDER BY __row_order DESC) AS rd "
            f"FROM src)"
        )

        agg_parts: list[str] = []
        for idx, part in enumerate(select_parts):
            if idx == 0 and gk_expr is not None:
                # Reference precomputed __neosqlite_gk once to avoid duplicating params
                agg_parts.append("__neosqlite_gk AS _id")
            else:
                agg_parts.append(part)
        for _, sql_alias, is_last, expr_val, is_sql in self._first_last_fields:
            if is_sql:
                value_sql = expr_val
            elif expr_val == "_id":
                value_sql = (
                    "_id"
                    if has_underscore_id
                    else "json_extract(data, '$._id')"
                )
            elif expr_val in ("$$ROOT", "$$CURRENT"):
                value_sql = "json(data)"
            else:
                value_sql = f"json_extract(data, '{parse_json_path(expr_val)}')"
            flag = "rd" if is_last else "rn"
            agg_parts.append(
                f"MAX(CASE WHEN {flag} = 1 THEN {value_sql} END) AS {sql_alias}"
            )

        inner = f"SELECT {', '.join(agg_parts)} FROM ranked"
        if gk_expr is not None:
            inner += " GROUP BY __neosqlite_gk"

        outer_from = f"FROM ({inner})"
        if not group_by_parts:
            # Constant-key aggregate: guard the phantom single row (#104)
            outer_from += " WHERE (SELECT COUNT(*) FROM ranked) > 0"

        aliases = getattr(self, "_group_output_aliases", None) or [
            (a, a)
            for a in self._select_aliases(select_parts)
            + [f for f, _, _, _, _ in self._first_last_fields]
        ]
        if self.jsonb.jsonb_supported:
            local_json_args = ", ".join(
                f"'{doc_key.replace(chr(39), chr(39) * 2)}', "
                f"CASE WHEN typeof({sql_alias}) = 'blob' AND json_valid({sql_alias}, 4) "
                f"THEN json({sql_alias}) "
                f"WHEN typeof({sql_alias}) = 'text' AND json_valid({sql_alias}) "
                f"AND json_type({sql_alias}) IN ('array', 'object') THEN json({sql_alias}) "
                f"ELSE {sql_alias} END"
                for doc_key, sql_alias in aliases
            )
        else:
            local_json_args = ", ".join(
                f"'{doc_key.replace(chr(39), chr(39) * 2)}', "
                f"CASE WHEN typeof({sql_alias}) = 'text' AND json_valid({sql_alias}) "
                f"AND json_type({sql_alias}) IN ('array', 'object') THEN json({sql_alias}) "
                f"ELSE {sql_alias} END"
                for doc_key, sql_alias in aliases
            )

        query = (
            f"{cte} SELECT ROW_NUMBER() OVER () as id, "
            f"{json_output_func}({local_json_args})) as data {outer_from}"
        )
        return query, []

    @staticmethod
    def _grouped_source_sql(
        current_table: str, select_parts: list[str], group_by_clause: str
    ) -> str:
        """Build the ``FROM`` subquery for grouped output (#104).

        With a ``GROUP BY`` clause, empty input naturally yields zero rows. A
        constant-key aggregate (empty ``group_by_clause``) always emits one row
        in SQLite even over zero input rows, so this adds ``COUNT(*) AS __cnt``
        and filters ``WHERE __cnt > 0``.

        Args:
            current_table: Source table or CTE name to select from.
            select_parts: List of SQL aggregate/key projection expressions.
            group_by_clause: ``GROUP BY ...`` clause or empty string.

        Returns:
            str: SQL ``FROM (...)`` clause for the outer projection query.
        """
        if group_by_clause:
            return (
                f"FROM (SELECT {', '.join(select_parts)} "
                f"FROM {current_table} {group_by_clause})"
            )
        return (
            f"FROM (SELECT {', '.join(select_parts)}, COUNT(*) AS __cnt "
            f"FROM {current_table}) WHERE __cnt > 0"
        )

    def _get_results_from_table(
        self,
        table_name: str,
        is_count: bool = False,
        count_field: str | None = None,
        batch_size: int = 101,
    ) -> list[dict[str, Any]]:
        """Retrieve documents from a temporary aggregation table.

        Reads rows in batches using ``fetchmany(batch_size)`` and decodes JSON
        ``data`` columns (wrapping with ``json(data)`` when JSONB is enabled)
        into Python dictionaries. When ``is_count`` and ``count_field`` are
        given, runs ``SELECT COUNT(*)`` instead of loading rows. Also parses
        stringified JSON arrays and objects recorded in ``_array_fields_map``
        and ``_object_fields_map``.

        Args:
            table_name: Name of the temporary table to read from.
            is_count: If True and ``count_field`` is provided, return a single
                document containing the SQL row count. Defaults to False.
            count_field: Output field name for the count document when
                ``is_count`` is True. Defaults to None.
            batch_size: Maximum number of rows fetched per ``fetchmany()``
                call. Defaults to 101.

        Returns:
            list[dict[str, Any]]: List of result documents as Python dicts.
        """
        if is_count and count_field:
            # Optimized path for $count: use SQL COUNT instead of loading all documents
            cursor = self.db.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return [{count_field: count}]

        # When data is stored as JSONB (binary), we need to convert it to text JSON for Python
        # Since temp tables created with CREATE TABLE ... AS SELECT don't preserve column types,
        # we check if the source collection has JSONB support instead
        use_json_wrapper = self.jsonb.jsonb_supported

        # Check if the table has id and _id columns
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(table_name)})"
        ).fetchall()
        column_names = [col[1] for col in columns]
        has_id_column = "id" in column_names
        has_underscore_id_column = "_id" in column_names
        has_data_column = "data" in column_names

        # Check if this is a non-standard table (e.g., from $bucket, $bucketAuto, $group)
        # These tables have custom columns like _id, count, etc. but no 'data' column
        is_standard_table = has_data_column or (
            has_id_column and has_underscore_id_column
        )

        if not is_standard_table:
            # Non-standard table - return rows as dictionaries with column names as keys
            select_clause = ", ".join(
                f'"{col.replace(chr(34), chr(34) * 2)}"' for col in column_names
            )
            cursor = self.db.execute(
                f"SELECT {select_clause} FROM {quote_table_name(table_name)}"
            )
            results = []
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    doc = {}
                    for i, col_name in enumerate(column_names):
                        doc[col_name] = row[i]
                    results.append(doc)
            return results

        # Build SELECT statement based on available columns for standard tables
        if use_json_wrapper:
            if has_id_column and has_underscore_id_column and has_data_column:
                cursor = self.db.execute(
                    f"SELECT id, _id, json(data) as data FROM {table_name}"
                )
            elif has_id_column and has_data_column:
                # Table has id but not _id - select id and wrap data
                cursor = self.db.execute(
                    f"SELECT id, json(data) as data FROM {table_name}"
                )
                has_underscore_id_column = False
            elif has_data_column:
                # Only data column available
                cursor = self.db.execute(
                    f"SELECT json(data) as data FROM {table_name}"
                )
                has_id_column = False
                has_underscore_id_column = False
            else:
                # No standard columns - this is an edge case, return empty
                logger.warning(f"Table {table_name} has no id/_id/data columns")
                return []
        else:
            if has_id_column and has_underscore_id_column and has_data_column:
                cursor = self.db.execute(
                    f"SELECT id, _id, data FROM {table_name}"
                )
            elif has_id_column and has_data_column:
                cursor = self.db.execute(f"SELECT id, data FROM {table_name}")
                has_underscore_id_column = False
            elif has_data_column:
                cursor = self.db.execute(f"SELECT data FROM {table_name}")
                has_id_column = False
                has_underscore_id_column = False
            else:
                # No standard columns - this is an edge case, return empty
                logger.warning(f"Table {table_name} has no id/_id/data columns")
                return []

        # Use fetchmany to avoid loading all results into memory at once
        results = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                # For grouped results, we need to preserve the _id from the JSON data
                # instead of using the row id. Parse the JSON directly.
                from neosqlite.collection.json_helpers import (
                    neosqlite_json_loads,
                )

                # Handle different column counts based on what columns exist
                # 3 columns: id, _id, data
                # 2 columns: id, data OR _id, data (depending on has_id_column)
                # 1 column: data only
                if has_id_column and has_underscore_id_column and len(row) == 3:
                    # _id is provided as a separate column, use it when non-null
                    doc = neosqlite_json_loads(row[2])
                    # Only set _id from column if it's not already in the JSON
                    if "_id" not in doc and row[1] is not None:
                        doc["_id"] = self.collection._parse_stored_id(row[1])
                elif has_id_column and len(row) == 2:
                    # Only id column, no separate _id column
                    doc = neosqlite_json_loads(row[1])
                elif len(row) == 2 and not has_id_column:
                    # _id and data columns (no id)
                    doc = neosqlite_json_loads(row[1])
                    if "_id" not in doc and row[0] is not None:
                        doc["_id"] = self.collection._parse_stored_id(row[0])
                else:
                    # Only data column
                    doc = neosqlite_json_loads(row[0])

                # Parse array fields that were created with json_group_array
                # These are stored as JSON strings and need to be parsed
                # Optimization: Only check fields we know are arrays (from $push/$addToSet)
                array_fields = getattr(self, "_array_fields_map", {}).get(
                    table_name, []
                )
                for key in array_fields:
                    if key in doc:
                        value = doc[key]
                        if (
                            isinstance(value, str)
                            and value.startswith("[")
                            and value.endswith("]")
                        ):
                            try:
                                doc[key] = neosqlite_json_loads(value)
                            except Exception as e:
                                logger.debug(
                                    f"Failed to parse array field '{key}' JSON: {e}"
                                )
                                pass  # Keep as string if parsing fails

                object_fields = getattr(self, "_object_fields_map", {}).get(
                    table_name, []
                )
                for key in object_fields:
                    if key in doc:
                        value = doc[key]
                        if (
                            isinstance(value, str)
                            and value.startswith("{")
                            and value.endswith("}")
                        ):
                            try:
                                doc[key] = neosqlite_json_loads(value)
                            except Exception as e:
                                logger.debug(
                                    f"Failed to parse object field '{key}' JSON: {e}"
                                )
                                pass

                results.append(doc)
        return results

    def _compile_bucket_expr(
        self,
        expr: Any,
        agg_ctx: AggregationContext,
        has_underscore_id: bool,
    ) -> tuple[str, list[Any]]:
        """Compile a ``$bucket``/``$bucketAuto`` expression or field ref.

        Args:
            expr: MongoDB field reference, variable, or expression dict.
            agg_ctx: Active aggregation expression context.
            has_underscore_id: Whether the input table includes an ``_id``
                column as a fallback for ``$_id``.

        Returns:
            tuple[str, list[Any]]: Compiled SQL expression and bound params.

        Raises:
            NotImplementedError: If the expression is unsupported in SQL tier.
        """
        if expr == "$_id" and has_underscore_id:
            json_extract = f"{self.jsonb.json_function_prefix}_extract"
            return f"COALESCE({json_extract}(data, '$._id'), _id)", []
        expr_sql, expr_params = self.expr_evaluator.build_select_expression(
            expr, context=agg_ctx
        )
        if expr_sql is None:
            raise NotImplementedError(
                f"Bucket expression {expr!r} not supported in SQL tier"
            )
        return expr_sql, list(expr_params)

    def _compile_bucket_accumulators(
        self,
        output_spec: dict[str, Any],
        agg_ctx: AggregationContext,
        has_underscore_id: bool,
    ) -> tuple[list[str], list[Any], list[tuple[str, str]], list[str]]:
        """Compile ``$bucket``/``$bucketAuto`` output accumulators for Tier 2.

        Args:
            output_spec: Mapping of output field names to accumulator specs.
            agg_ctx: Active aggregation expression context.
            has_underscore_id: Whether the input table has an ``_id`` column.

        Returns:
            tuple: ``(acc_parts, acc_params, output_aliases, array_fields)``.

        Raises:
            NotImplementedError: If any accumulator requires Python fallback.
        """
        acc_parts: list[str] = []
        acc_params: list[Any] = []
        output_aliases: list[tuple[str, str]] = []
        array_fields: list[str] = []
        op_map = {
            "$sum": ("SUM", False),
            "$avg": ("AVG", False),
            "$min": ("MIN", False),
            "$max": ("MAX", False),
            "$push": ("json_group_array", False),
            "$addToSet": ("json_group_array", True),
        }

        for field_name, acc_spec in output_spec.items():
            if not isinstance(acc_spec, dict) or len(acc_spec) != 1:
                raise NotImplementedError(
                    f"Bucket accumulator {acc_spec!r} requires Python fallback"
                )
            op, expr = next(iter(acc_spec.items()))
            sql_alias = f"__acc_{len(output_aliases)}"
            if op == "$count":
                acc_parts.append(f"COUNT(*) AS {sql_alias}")
            elif op == "$sum" and expr == 1 and not isinstance(expr, bool):
                acc_parts.append(f"COUNT(*) AS {sql_alias}")
            elif op in op_map:
                sql_agg, use_distinct = op_map[op]
                expr_sql, expr_params = self._compile_bucket_expr(
                    expr, agg_ctx, has_underscore_id
                )
                acc_params.extend(expr_params)
                distinct_str = "DISTINCT " if use_distinct else ""
                agg_call = f"{sql_agg}({distinct_str}{expr_sql})"
                if op == "$sum":
                    agg_call = f"COALESCE({agg_call}, 0)"
                acc_parts.append(f"{agg_call} AS {sql_alias}")
                if sql_agg == "json_group_array":
                    array_fields.append(field_name)
            else:
                raise NotImplementedError(
                    f"Bucket accumulator {op} requires Python fallback"
                )
            output_aliases.append((field_name, sql_alias))

        return acc_parts, acc_params, output_aliases, array_fields

    @python_hybrid_fallback("$bucket")
    def _process_bucket_stage(self, create_temp, current_table, bucket_spec):
        """Process a ``$bucket`` stage grouping documents by boundary ranges.

        Compiles ``groupBy`` and ``output`` expressions with ``ExprEvaluator``,
        evaluates ``groupBy`` once in an inner subquery, maps values into
        ``[lower, upper)`` intervals (or the ``default`` label, defaulting to
        ``"Other"``), and materializes a standard ``(id, _id, data)`` table.

        Args:
            create_temp: Callback used to create the stage's temporary table.
            current_table: Name of the current temporary table containing input
                documents.
            bucket_spec: Dictionary specifying ``groupBy``, ``boundaries``,
                optional ``default``, and optional ``output``.

        Returns:
            str: Name of the newly created temporary table.
        """
        group_by = bucket_spec.get("groupBy")
        boundaries = bucket_spec.get("boundaries", [])
        default_label = bucket_spec.get("default", "Other")
        output_spec = bucket_spec.get("output", {"count": {"$sum": 1}})

        if group_by is None or group_by == "" or not boundaries:
            return create_temp(
                {"$bucket": bucket_spec},
                "SELECT 1 AS id, NULL AS _id, '{}' AS data WHERE 0",
            )

        sorted_boundaries = sorted(boundaries)

        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        has_underscore_id = any(col[1] == "_id" for col in columns)

        agg_ctx = AggregationContext()
        key_sql, key_params = self._compile_bucket_expr(
            group_by, agg_ctx, has_underscore_id
        )
        acc_parts, acc_params, output_aliases, array_fields = (
            self._compile_bucket_accumulators(
                output_spec, agg_ctx, has_underscore_id
            )
        )

        # Build CASE expression for bucketing using bound parameters
        case_parts = []
        bucket_params: list[Any] = []
        for lower, upper in pairwise(sorted_boundaries):
            case_parts.append("WHEN __gval >= ? AND __gval < ? THEN ?")
            bucket_params.extend([lower, upper, lower])

        if not case_parts:
            if default_label is not None:
                bucket_id_sql = "?"
                bucket_params.append(default_label)
            else:
                bucket_id_sql = "NULL"
        else:
            if default_label is not None:
                case_parts.append("ELSE ?")
                bucket_params.append(default_label)
            bucket_id_sql = f"CASE {' '.join(case_parts)} END"

        array_field_set = set(array_fields)
        json_args = ", ".join(
            ["'_id', __bucket_id"]
            + [
                (
                    f"'{field_name.replace(chr(39), chr(39) * 2)}', json({alias})"
                    if field_name in array_field_set
                    else f"'{field_name.replace(chr(39), chr(39) * 2)}', {alias}"
                )
                for field_name, alias in output_aliases
            ]
        )

        id_expr = (
            "ROW_NUMBER() OVER () AS id"
            if _check_sqlite_version("3.25.0")
            else "NULL AS id"
        )
        data_expr = self.jsonb.data_column(f"json_object({json_args})")
        sub_parts = ["__bucket_id"] + acc_parts
        all_params = acc_params + bucket_params + key_params

        new_table = create_temp(
            {"$bucket": bucket_spec},
            f"SELECT {id_expr}, __bucket_id AS _id, {data_expr} AS data "
            f"FROM ("
            f"SELECT {', '.join(sub_parts)} FROM ("
            f"SELECT __bsrc.*, {bucket_id_sql} AS __bucket_id FROM ("
            f"SELECT *, ({key_sql}) AS __gval FROM {current_table}"
            f") AS __bsrc"
            f") GROUP BY __bucket_id"
            f") ORDER BY __bucket_id",
            params=all_params or None,
        )

        if not hasattr(self, "_array_fields_map"):
            self._array_fields_map = {}
        self._array_fields_map[new_table] = array_fields

        return new_table

    def _build_group_by_expr(self, group_by):
        """Build a SQL expression for a ``$bucket`` ``groupBy`` spec.

        Delegates to ``expr_evaluator.build_select_expression`` when possible.

        Args:
            group_by: Field reference string or expression dict.

        Returns:
            str: SQL expression evaluating the bucket key.
        """
        expr_sql, _ = self.expr_evaluator.build_select_expression(
            group_by, context=AggregationContext()
        )
        return expr_sql

    @python_hybrid_fallback("$bucketAuto")
    def _process_bucket_auto_stage(
        self, create_temp, current_table, bucket_auto_spec
    ):
        """Process a ``$bucketAuto`` stage into equal-count buckets.

        Compiles ``groupBy`` and ``output`` accumulators via
        ``ExprEvaluator``, partitions rows using ``DENSE_RANK`` so tied values
        stay together, and materializes a standard ``(id, _id, data)`` table
        with ``{"min": ..., "max": ...}`` encoded in JSON ``data``.

        Args:
            create_temp: Callback used to create the stage's temporary table.
            current_table: Name of the current temporary table containing input
                documents.
            bucket_auto_spec: Dictionary specifying ``groupBy``, optional
                ``buckets`` (default 10), and optional ``output``.

        Returns:
            str: Name of the newly created temporary table.

        Raises:
            ValueError: If ``buckets`` is not a positive non-boolean integer.
        """
        group_by = bucket_auto_spec.get("groupBy")
        num_buckets = bucket_auto_spec.get("buckets", 10)
        output_spec = bucket_auto_spec.get("output", {"count": {"$sum": 1}})
        granularity = bucket_auto_spec.get("granularity")

        if (
            not isinstance(num_buckets, int)
            or isinstance(num_buckets, bool)
            or num_buckets <= 0
        ):
            raise ValueError(
                "The $bucketAuto 'buckets' option must be a positive integer, "
                f"got {num_buckets!r}"
            )

        if group_by is None or group_by == "":
            return create_temp(
                {"$bucketAuto": bucket_auto_spec},
                "SELECT 1 AS id, NULL AS _id, '{}' AS data WHERE 0",
            )

        if granularity or not _check_sqlite_version("3.25.0"):
            raise NotImplementedError(
                "$bucketAuto options require Python fallback"
            )

        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        has_underscore_id = any(col[1] == "_id" for col in columns)

        agg_ctx = AggregationContext()
        key_sql, key_params = self._compile_bucket_expr(
            group_by, agg_ctx, has_underscore_id
        )
        acc_parts, acc_params, output_aliases, array_fields = (
            self._compile_bucket_accumulators(
                output_spec, agg_ctx, has_underscore_id
            )
        )

        array_field_set = set(array_fields)
        json_args = ", ".join(
            ["'_id', json_object('min', __bmin, 'max', __bmax)"]
            + [
                (
                    f"'{field_name.replace(chr(39), chr(39) * 2)}', json({alias})"
                    if field_name in array_field_set
                    else f"'{field_name.replace(chr(39), chr(39) * 2)}', {alias}"
                )
                for field_name, alias in output_aliases
            ]
        )

        acc_select_sql = f", {', '.join(acc_parts)}" if acc_parts else ""
        data_expr = self.jsonb.data_column(f"json_object({json_args})")
        sql = f"""
            SELECT
                ROW_NUMBER() OVER () AS id,
                json_object('min', __bmin, 'max', __bmax) AS _id,
                {data_expr} AS data
            FROM (
                SELECT
                    __bucket_id,
                    CASE WHEN MAX(CASE WHEN __gval IS NULL THEN 1 ELSE 0 END) = 1 THEN NULL ELSE MIN(__gval) END AS __bmin,
                    MAX(__gval) AS __bmax{acc_select_sql}
                FROM (
                    SELECT
                        __dr.*,
                        ((__dr.__drank - 1) * ?) / NULLIF(MAX(__dr.__drank) OVER (), 0) AS __bucket_id
                    FROM (
                        SELECT
                            __bsrc.*,
                            DENSE_RANK() OVER (ORDER BY __bsrc.__gval) AS __drank
                        FROM (
                            SELECT
                                *,
                                ({key_sql}) AS __gval
                            FROM {current_table}
                        ) AS __bsrc
                    ) AS __dr
                )
                GROUP BY __bucket_id
            )
            ORDER BY __bmin
        """
        all_params = acc_params + [num_buckets] + key_params

        new_table = create_temp(
            {"$bucketAuto": bucket_auto_spec},
            sql,
            params=all_params or None,
        )

        if not hasattr(self, "_array_fields_map"):
            self._array_fields_map = {}
        self._array_fields_map[new_table] = array_fields

        return new_table
