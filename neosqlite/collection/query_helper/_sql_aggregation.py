"""SQL-based aggregation pipeline builder.

Extracted from aggregation.py — provides _build_aggregation_query and
related SQL generation methods for $match, $sort, $group, $unwind.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ...sql_utils import quote_table_name
from ..cursor import DESCENDING
from ..json_path_utils import parse_json_path
from .utils import get_force_fallback

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


class SqlAggregationMixin:
    """Mixin providing SQL-based aggregation pipeline methods.

    Designed to be composed into ``AggregationMixin``. Generates single-query
    SQL for supported ``$match``, ``$sort``, ``$skip``, ``$limit``,
    ``$group``, and ``$unwind`` + ``$group`` pipelines, returning ``None``
    when higher-tier or Python fallback is required.

    Attributes:
        collection: Host ``Collection`` with ``db`` and ``name``.
        jsonb: ``JSONBContext`` describing JSON/JSONB capabilities.
    """

    collection: Collection
    jsonb: JSONBContext
    _build_simple_where_clause: Any
    _estimate_pipeline_cost: Any
    _optimize_match_pushdown: Any
    _reorder_pipeline_for_indexes: Any
    _is_datetime_indexed_field: Any
    _apply_query: Any

    def _build_aggregation_query(
        self,
        pipeline: list[dict[str, Any]],
    ) -> tuple[str, list[Any], list[str] | None] | None:
        """Build a SQL query for a MongoDB-style aggregation pipeline.

        Returns ``None`` immediately if forced fallback is enabled. Otherwise
        compares index-reordered and original pipeline costs, pushes down
        ``$match`` filters when possible, and translates ``$match``,
        ``$sort``, ``$skip``, ``$limit``, ``$group``, and single
        ``$unwind`` + ``$group`` stages into a single SQL SELECT statement.

        Args:
            pipeline: Aggregation pipeline stages to compile into SQL.

        Returns:
            tuple[str, list[Any], list[str] | None] | None: Tuple of
                ``(sql, params, output_fields)`` where ``output_fields`` is
                a list of projected column names for ``$group`` queries or
                ``None`` for raw document queries, or ``None`` when fallback
                to temporary-table or Python execution is required.

        Raises:
            ValueError: If a ``$limit`` stage value is not a non-negative
                integer.
        """
        # Check if we should force fallback for benchmarking/debugging
        if get_force_fallback():
            return None  # Force fallback to Python implementation

        # Try to optimize the pipeline by reordering for better index usage
        optimized_pipeline = self._reorder_pipeline_for_indexes(pipeline)

        # Estimate costs for both original and optimized pipelines
        original_cost = self._estimate_pipeline_cost(pipeline)
        optimized_cost = self._estimate_pipeline_cost(optimized_pipeline)

        # Use the better pipeline based on cost estimation
        if optimized_cost < original_cost:
            # Use optimized pipeline
            effective_pipeline = optimized_pipeline
        else:
            # Use original pipeline
            effective_pipeline = pipeline

        # Additional optimization: Check if we can push match filters down into SQL operations
        effective_pipeline = self._optimize_match_pushdown(effective_pipeline)

        where_clause = ""
        params: list[Any] = []
        order_by = ""
        limit = ""
        offset = ""
        group_by = ""
        select_clause = "SELECT id, data"
        output_fields: list[str] | None = None

        for i, stage in enumerate(effective_pipeline):
            stage_name = next(iter(stage.keys()))
            match stage_name:
                case "$match":
                    query = stage["$match"]
                    where_result = self._build_simple_where_clause(query)
                    if where_result is None:
                        return None  # Fallback for complex queries
                    where_clause, params, tables = where_result
                case "$sort":
                    sort_spec = stage["$sort"]
                    sort_clauses = []
                    for key, direction in sort_spec.items():
                        # When sorting after a group stage, we sort by the output field name
                        if group_by:
                            sort_clauses.append(
                                f"{key} {'DESC' if direction == DESCENDING else 'ASC'}"
                            )
                        else:
                            sort_clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(key)}') "
                                f"{'DESC' if direction == DESCENDING else 'ASC'}"
                            )
                    order_by = "ORDER BY " + ", ".join(sort_clauses)
                case "$skip":
                    count = stage["$skip"]
                    # SQLite requires LIMIT when OFFSET is present (#166)
                    if not limit:
                        limit = "LIMIT -1"
                    offset = f"OFFSET {count}"
                case "$limit":
                    count = stage["$limit"]
                    if (
                        not isinstance(count, int)
                        or isinstance(count, bool)
                        or count < 0
                    ):
                        raise ValueError(
                            f"The $limit stage requires a non-negative integer, got {count}"
                        )
                    limit = f"LIMIT {count}"
                case "$group":
                    # Check if this is a $unwind + $group pattern we can optimize
                    optimization_result = self._optimize_unwind_group_pattern(
                        i, pipeline
                    )
                    if optimization_result is not None:
                        return optimization_result

                    # A group stage must be the first stage or after a match stage
                    if i > 1 or (i == 1 and "$match" not in pipeline[0]):
                        return None
                    group_spec = stage["$group"]
                    group_result = self._build_group_query(group_spec)
                    if group_result is None:
                        return None
                    select_clause, group_by, output_fields = group_result
                case "$unwind":
                    # Check if this is followed by $match with $text - fall back to temp tables
                    # The text search on unwound elements requires special handling that
                    # the single-SQL optimization cannot provide
                    # Also check for multiple consecutive $unwind stages
                    if len(pipeline) > i + 1:
                        # Count consecutive $unwind stages starting from position i
                        unwind_count = 0
                        match_with_text_idx = -1
                        j = i
                        while j < len(pipeline) and "$unwind" in pipeline[j]:
                            unwind_count += 1
                            j += 1
                        # Check if next stage after unwinds is $match with $text
                        if (
                            j < len(pipeline)
                            and "$match" in pipeline[j]
                            and "$text" in pipeline[j]["$match"]
                        ):
                            match_with_text_idx = j

                        # Fall back for: single unwind + text, multiple unwinds, or multiple unwinds + text
                        if match_with_text_idx >= 0 or unwind_count > 1:
                            return None  # Fall back to temp table approach

                    # Check if this is part of an $unwind + $group pattern we can optimize
                    # Case 1: $unwind is first stage followed by $group
                    if i == 0 and len(pipeline) > 1 and "$group" in pipeline[1]:
                        # $unwind followed by $group - try to optimize with SQL
                        group_stage = pipeline[1]["$group"]
                        unwind_field = stage["$unwind"]

                        if (
                            isinstance(unwind_field, str)
                            and unwind_field.startswith("$")
                            and isinstance(group_stage.get("_id"), str)
                            and group_stage.get("_id").startswith("$")
                        ):
                            unwind_field_name = unwind_field[
                                1:
                            ]  # Remove leading $
                            group_id_field = group_stage["_id"][
                                1:
                            ]  # Remove leading $

                            # Build SELECT and GROUP BY clauses for unwound data
                            select_expressions = []
                            output_fields = ["_id"]

                            # Handle _id field
                            if group_id_field == unwind_field_name:
                                # Grouping by the unwound field
                                select_expressions.append("je.value AS _id")
                                group_by_clause = "GROUP BY je.value"
                            else:
                                # Grouping by another field
                                select_expressions.append(
                                    f"{self.jsonb.json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(group_id_field)}') AS _id"
                                )
                                group_by_clause = f"GROUP BY {self.jsonb.json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(group_id_field)}')"

                            # Try to build the group query using the general method
                            # This supports all accumulator operations including $avg, $min, $max
                            group_result = self._build_group_query(group_stage)
                            if group_result is not None:
                                (
                                    select_clause,
                                    group_by_clause,
                                    group_output_fields,
                                ) = group_result

                                # Modify the SELECT clause to work with the unwound data
                                # Replace json_extract(data, '$.field') with appropriate expressions
                                table_name = quote_table_name(
                                    self.collection.name
                                )

                                # For the _id field, if it matches the unwind field, use je.value
                                group_id_field = group_stage["_id"][
                                    1:
                                ]  # Remove leading $
                                if group_id_field == unwind_field_name:
                                    # Replace the _id extraction with je.value
                                    modified_select = select_clause.replace(
                                        f"{self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(unwind_field_name)}') AS _id",
                                        "je.value AS _id",
                                    )
                                    # For GROUP BY clause, use je.value when grouping by the unwind field
                                    group_by_clause = "GROUP BY je.value"
                                else:
                                    modified_select = select_clause
                                    # Keep the original GROUP BY but ensure it references the correct table
                                    group_by_clause = group_by_clause.replace(
                                        f"{self.jsonb.json_function_prefix}_extract(data,",
                                        f"{self.jsonb.json_function_prefix}_extract({table_name}.data,",
                                    )

                                # Replace all other json_extract(data, ...) with json_extract(table.data, ...)
                                # to properly reference the table column in the JOIN context
                                modified_select = modified_select.replace(
                                    f"{self.jsonb.json_function_prefix}_extract(data,",
                                    f"{self.jsonb.json_function_prefix}_extract({table_name}.data,",
                                )

                                # For fields that reference the unwind field (e.g., $push: "$tags" when unwinding "$tags"),
                                # replace with je.value to get the unwound value instead of the full array
                                modified_select = modified_select.replace(
                                    f"{self.jsonb.json_function_prefix}_extract({table_name}.data, '{parse_json_path(unwind_field_name)}')",
                                    "je.value",
                                )

                                # Build the FROM clause with json_each for unwinding
                                from_clause = f"FROM {table_name}, {self.jsonb.json_each_function}({self.jsonb.json_function_prefix}_extract({table_name}.data, '{parse_json_path(unwind_field_name)}')) as je"

                                # Add ordering by _id for consistent results
                                order_by_clause = "ORDER BY _id"

                                # Construct the full SQL command
                                cmd = f"{modified_select} {from_clause} {group_by_clause} {order_by_clause}"
                                return cmd, [], group_output_fields

                    # Fall back to Python for complex unwind scenarios
                    return None
                case _:
                    return None  # Fallback for unsupported stages

        cmd = f"{select_clause} FROM {quote_table_name(self.collection.name)} {where_clause} {group_by} {order_by} {limit} {offset}"
        return cmd, params, output_fields

    def _optimize_unwind_group_pattern(
        self, group_stage_index: int, pipeline: list[dict[str, Any]]
    ) -> tuple[str, list[Any], list[str]] | None:
        """Optimize a ``$unwind`` + ``$group`` pattern into SQL.

        Handles pipelines where stage 0 is a simple string ``$unwind``
        (``"$field"``) and stage 1 is a ``$group`` with a simple string
        ``_id`` (``"$field"``). Rewrites SQL expressions produced by
        ``_build_group_query`` to read unwound elements from
        ``json_each``/``jsonb_each`` aliased as ``je``.

        Args:
            group_stage_index: Index of the ``$group`` stage in the pipeline.
            pipeline: Complete aggregation pipeline being compiled.

        Returns:
            tuple[str, list[Any], list[str]] | None: Tuple of
                ``(sql, params, output_fields)`` with an empty parameter list
                if optimization applies, or ``None`` otherwise.
        """
        # Check if this is a $unwind + $group pattern we can optimize
        if group_stage_index == 1 and "$unwind" in pipeline[0]:
            # $unwind followed by $group - try to optimize with SQL
            unwind_stage = pipeline[0]["$unwind"]
            group_spec = pipeline[group_stage_index]["$group"]

            if (
                isinstance(unwind_stage, str)
                and unwind_stage.startswith("$")
                and isinstance(group_spec.get("_id"), str)
                and group_spec.get("_id").startswith("$")
            ):
                unwind_field = unwind_stage[1:]  # Remove leading $
                table_name = quote_table_name(self.collection.name)

                # Try to build the group query using the general method
                group_result = self._build_group_query(group_spec)
                if group_result is not None:
                    select_clause, group_by_clause, output_fields = group_result

                    # Modify the SELECT clause to work with the unwound data
                    # Replace json_extract(data, '$.field') with appropriate expressions

                    # For the _id field, if it matches the unwind field, use je.value
                    group_id_field = group_spec["_id"][1:]  # Remove leading $
                    if group_id_field == unwind_field:
                        # Replace the _id extraction with je.value
                        modified_select = select_clause.replace(
                            f"{self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(unwind_field)}') AS _id",
                            "je.value AS _id",
                        )
                        # Also replace any other references to the unwind field in the SELECT clause
                        modified_select = modified_select.replace(
                            f"{self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(unwind_field)}')",
                            "je.value",
                        )
                        # For GROUP BY clause, use je.value when grouping by the unwind field
                        modified_group_by = "GROUP BY je.value"
                    else:
                        modified_select = select_clause
                        # Keep the original GROUP BY but ensure it references the correct table
                        modified_group_by = group_by_clause.replace(
                            f"{self.jsonb.json_function_prefix}_extract(data,",
                            f"{self.jsonb.json_function_prefix}_extract({table_name}.data,",
                        )

                    # Replace all other json_extract(data, ...) with json_extract(table.data, ...)
                    # to properly reference the table column in the JOIN context
                    # This is needed for fields that aren't the unwind field (e.g., $push: $name)
                    modified_select = modified_select.replace(
                        f"{self.jsonb.json_function_prefix}_extract(data,",
                        f"{self.jsonb.json_function_prefix}_extract({table_name}.data,",
                    )

                    # Build the FROM clause with json_each for unwinding
                    from_clause = f"FROM {table_name}, {self.jsonb.json_each_function}({self.jsonb.json_function_prefix}_extract({table_name}.data, '{parse_json_path(unwind_field)}')) as je"

                    # Add ordering by _id for consistent results
                    order_by_clause = "ORDER BY _id"

                    # Construct the full SQL command
                    cmd = f"{modified_select} {from_clause} {modified_group_by} {order_by_clause}"
                    return cmd, [], output_fields
                else:
                    # If we can't build the group query, fall back to Python
                    return None

        return None

    def _build_group_query(
        self, group_spec: dict[str, Any]
    ) -> tuple[str, str, list[str]] | None:
        """Build SELECT and GROUP BY clauses for a ``$group`` stage.

        Supports ``_id: None`` or single-field ``"$field"`` grouping along
        with ``$count``, ``$sum`` (field or literal ``1``), ``$avg``,
        ``$min``, ``$max``, ``$push``, and ``$addToSet`` accumulators on
        simple ``"$field"`` expressions.

        Args:
            group_spec: Dictionary representing the ``$group`` stage
                specification containing ``_id`` and accumulator fields.

        Returns:
            tuple[str, str, list[str]] | None: Tuple of
                ``(select_clause, group_by_clause, output_fields)``, or
                ``None`` if unsupported expressions require Python fallback.
        """
        group_id_expr = group_spec.get("_id")
        if group_id_expr is None:
            group_by_clause = ""
            select_expressions = ["NULL AS _id"]
            output_fields = ["_id"]
        elif isinstance(group_id_expr, str) and group_id_expr.startswith("$"):
            group_by_field = group_id_expr[1:]
            group_by_clause = f"GROUP BY {self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(group_by_field)}')"
            select_expressions = [
                f"{self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(group_by_field)}') AS _id"
            ]
            output_fields = ["_id"]
        else:
            return None  # Fallback for complex _id expressions

        for field, accumulator in group_spec.items():
            if field == "_id":
                continue

            if not isinstance(accumulator, dict) or len(accumulator) != 1:
                return None

            op, expr = next(iter(accumulator.items()))

            if op == "$count":
                select_expressions.append(f"COUNT(*) AS {field}")
                output_fields.append(field)
                continue

            if op == "$push":
                # Handle $push accumulator
                if not isinstance(expr, str) or not expr.startswith("$"):
                    return None  # Fallback for complex accumulator expressions
                field_name = expr[1:]
                select_expressions.append(
                    f"json_group_array({self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(field_name)}')) AS \"{field}\""
                )
                output_fields.append(field)
                continue

            if op == "$addToSet":
                # Handle $addToSet accumulator
                if not isinstance(expr, str) or not expr.startswith("$"):
                    return None  # Fallback for complex accumulator expressions
                field_name = expr[1:]
                select_expressions.append(
                    f"json_group_array(DISTINCT {self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(field_name)}')) AS \"{field}\""
                )
                output_fields.append(field)
                continue

            # Handle special case for $sum with integer literal 1 (count operation)
            if op == "$sum" and isinstance(expr, int) and expr == 1:
                select_expressions.append(f"COUNT(*) AS {field}")
                output_fields.append(field)
                continue

            # Handle field-based operations
            if not isinstance(expr, str) or not expr.startswith("$"):
                return None  # Fallback for complex accumulator expressions

            field_name = expr[1:]
            sql_func = {
                "$sum": "SUM",
                "$avg": "AVG",
                "$min": "MIN",
                "$max": "MAX",
            }.get(op)

            if not sql_func:
                return None  # Unsupported accumulator

            select_expressions.append(
                f"{sql_func}({self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(field_name)}')) AS {field}"
            )
            output_fields.append(field)

        select_clause = "SELECT " + ", ".join(select_expressions)
        return select_clause, group_by_clause, output_fields
