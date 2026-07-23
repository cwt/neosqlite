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

    Designed to be composed into AggregationMixin.
    """

    collection: "Collection"
    jsonb: "JSONBContext"
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
        """
        Builds a SQL query for the given MongoDB-like aggregation pipeline.

        This method constructs a SQL query based on the stages provided in the
        aggregation pipeline. It currently handles $match, $sort, $skip,
        and $limit stages, while $group stages are handled in Python. The method
        returns a tuple containing the SQL command and a list of parameters.

        Args:
            pipeline (list[dict[str, Any]]): A list of aggregation pipeline stages.

        Returns:
            tuple[str, list[Any]] | None: A tuple containing the SQL command and
                                          a list of parameters, or None if the
                                          pipeline contains unsupported stages
                                          or complex queries.
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
                    offset = f"OFFSET {count}"
                case "$limit":
                    count = stage["$limit"]
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
        """
        Optimize $unwind + $group pattern with SQL-based processing.

        This method handles the specific optimization pattern where a $unwind stage
        is immediately followed by a $group stage. It supports all accumulator
        operations by leveraging the general _build_group_query method while
        handling the $unwind optimization.

        Args:
            group_stage_index: Index of the $group stage in the pipeline
            pipeline: The complete aggregation pipeline

        Returns:
            tuple[str, list[Any], list[str]] | None: SQL command, params, and output fields
            if optimization is possible, None otherwise
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

    def _build_unwind_query(
        self,
        pipeline_index: int,
        pipeline: list[dict[str, Any]],
        unwind_stages: list[str],
    ) -> tuple[str, list[Any], list[str] | None] | None:
        """
        Builds a SQL query for a sequence of $unwind stages.

        This method constructs a SQL query to handle one or more consecutive $unwind
        stages in an aggregation pipeline. It processes array fields by joining
        with SQLite's `json_each`/`jsonb_each` function to "unwind" the arrays into separate rows.
        The method also handles necessary array type checks and integrates with
        other pipeline stages like $match, $sort, $skip, and $limit.

        Args:
            pipeline_index (int): The index of the first $unwind stage in the pipeline.
            pipeline (list[dict[str, Any]]): The full aggregation pipeline.
            unwind_stages (list[str]): A list of field paths to unwind,
                                       each prefixed with '$'.

        Returns:
            tuple[str, list[Any], list[str] | None] | None: A tuple containing:
                - The constructed SQL command string.
                - A list of parameters for the SQL query.
                - A list of output field names (None if not applicable).
            Returns None if the unwind stages cannot be processed with SQL and a
            fallback to Python is required.
        """
        field_names = []
        for field in unwind_stages:
            if (
                not isinstance(field, str)
                or not field.startswith("$")
                or len(field) == 1
            ):
                return None  # Fallback to Python implementation
            field_names.append(field[1:])

        # Build SELECT clause with nested json_set calls
        select_parts = [f"{quote_table_name(self.collection.name)}.data"]
        for i, field_name in enumerate(field_names):
            select_parts.insert(0, "json_set(")
            select_parts.append(
                f", '{parse_json_path(field_name)}', je{i + 1}.value)"
            )
        select_expr = "".join(select_parts)
        select_clause = f"SELECT {quote_table_name(self.collection.name)}.id, {select_expr} as data"

        # Build FROM clause with multiple json_each calls
        from_clause, unwound_fields = self._build_unwind_from_clause(
            field_names
        )

        # Handle $match stage and array type checks
        all_where_clauses = []
        params: list[Any] = []
        if pipeline_index == 1 and "$match" in pipeline[0]:
            match_query = pipeline[0]["$match"]
            where_result = self._build_simple_where_clause(match_query)
            if where_result and where_result[0]:
                all_where_clauses.append(
                    where_result[0].replace("WHERE ", "", 1)
                )
                params.extend(where_result[1])

        for field_name in field_names:
            parent_field, parent_alias = self._find_parent_unwind(
                field_name, unwound_fields
            )
            if parent_field and parent_alias:
                nested_path = field_name[len(parent_field) + 1 :]
                all_where_clauses.append(
                    f"json_type({self.jsonb.json_function_prefix}_extract({parent_alias}.value, '{parse_json_path(nested_path)}')) = 'array'"
                )
            else:
                all_where_clauses.append(
                    f"json_type({self.jsonb.json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) = 'array'"
                )

        where_clause = ""
        if all_where_clauses:
            where_clause = "WHERE " + " AND ".join(all_where_clauses)

        # Handle sort, skip, and limit operations
        start_index = pipeline_index + len(unwind_stages)
        end_index = len(pipeline)
        order_by, limit, offset = self._build_sort_skip_limit_clauses(
            pipeline, start_index, end_index, unwound_fields
        )

        cmd = f"{select_clause} {from_clause} {where_clause} {order_by} {limit} {offset}"
        return cmd, params, None

    def _build_unwind_from_clause(
        self, field_names: list[str]
    ) -> tuple[str, dict[str, str]]:
        """
        Builds the FROM clause for a SQL query with one or more $unwind stages.

        This method constructs the FROM clause needed to handle multiple $unwind
        operations in an aggregation pipeline. It creates joins with SQLite's
        `json_each`/`jsonb_each` function for each field to be unwound, allowing array elements
        to be processed as separate rows. It also manages nested unwinds by
        identifying parent-child relationships between fields.

        Args:
            field_names (list[str]): A list of field paths to unwind. Each path
                                     should be a string without the leading '$'.

        Returns:
            tuple[str, dict[str, str]]: A tuple containing:
                - The constructed FROM clause as a string.
                - A dictionary mapping each unwound field path to its corresponding
                  alias (e.g., 'je1', 'je2').
        """
        from_clause, unwound_fields = self._build_unwind_from_clause_impl(
            field_names
        )
        return from_clause, unwound_fields

    def _build_unwind_from_clause_impl(
        self, field_names: list[str]
    ) -> tuple[str, dict[str, str]]:
        """
        Internal implementation for building the FROM clause.

        Args:
            field_names (list[str]): A list of field paths to unwind.

        Returns:
            tuple[str, dict[str, str]]: A tuple containing the FROM clause and
                                        unwound fields mapping.
        """
        from_parts = [f"FROM {quote_table_name(self.collection.name)}"]
        unwound_fields: dict[str, str] = {}

        for i, field_name in enumerate(field_names):
            je_alias = f"je{i + 1}"
            parent_field, parent_alias = self._find_parent_unwind(
                field_name, unwound_fields
            )

            if parent_field and parent_alias:
                nested_path = field_name[len(parent_field) + 1 :]
                from_parts.append(
                    f", {self.jsonb.json_each_function}({self.jsonb.json_function_prefix}_extract({parent_alias}.value, '{parse_json_path(nested_path)}')) as {je_alias}"
                )
            else:
                from_parts.append(
                    f", {self.jsonb.json_each_function}({self.jsonb.json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) as {je_alias}"
                )
            unwound_fields[field_name] = je_alias

        return " ".join(from_parts), unwound_fields

    def _find_parent_unwind(
        self, field_name: str, unwound_fields: dict[str, str]
    ) -> tuple[str | None, str | None]:
        """
        Find the parent unwind field for a nested unwind.

        This method searches through already processed unwind fields to find a
        parent field that the current field is nested within. This is used to
        properly construct SQL joins for nested array unwinding operations.

        Args:
            field_name (str): The field name to find the parent for.
            unwound_fields (dict[str, str]): A dictionary mapping field paths to
                                             their aliases.

        Returns:
            tuple[str | None, str | None]: A tuple containing the parent field
                                           name and its alias, or (None, None)
                                           if no parent is found.
        """
        parent_field = None
        parent_alias = None
        longest_match_len = -1

        for p_field, p_alias in unwound_fields.items():
            prefix = p_field + "."
            if field_name.startswith(prefix):
                if len(p_field) > longest_match_len:
                    longest_match_len = len(p_field)
                    parent_field = p_field
                    parent_alias = p_alias
        return parent_field, parent_alias

    def _build_sort_skip_limit_clauses(
        self,
        pipeline: list[dict[str, Any]],
        start_index: int,
        end_index: int,
        unwound_fields: dict[str, str],
    ) -> tuple[str, str, str]:
        """
        Build ORDER BY, LIMIT, and OFFSET clauses for aggregation queries.

        This method constructs the SQL clauses for sorting, skipping, and limiting
        results in an aggregation pipeline. It handles both regular fields and
        fields that have been unwound from arrays, ensuring proper SQL generation
        for nested array elements.

        Args:
            pipeline (list[dict[str, Any]]): The aggregation pipeline stages.
            start_index (int): The starting index in the pipeline to process stages from.
            end_index (int): The ending index in the pipeline to process stages to.
            unwound_fields (dict[str, str]): A mapping of field names to their aliases
                                             for unwound fields.

        Returns:
            tuple[str, str, str]: A tuple containing:
                - The ORDER BY clause (empty string if no sorting)
                - The LIMIT clause (empty string if no limit)
                - The OFFSET clause (empty string if no offset)
        """
        local_order_by = ""
        local_limit = ""
        local_offset = ""

        sort_stages = []
        skip_value = 0
        limit_value = None

        for stage_idx in range(start_index, end_index):
            stage = pipeline[stage_idx]
            if "$sort" in stage:
                sort_stages.append(stage["$sort"])
            elif "$skip" in stage:
                skip_value = stage["$skip"]
            elif "$limit" in stage:
                limit_value = stage["$limit"]

        if sort_stages:
            sort_clauses = []
            for sort_spec in sort_stages:
                for key, direction in sort_spec.items():
                    parent_field, parent_alias = self._find_parent_unwind(
                        key, unwound_fields
                    )
                    if parent_field and parent_alias:
                        nested_path = key[len(parent_field) + 1 :]
                        sort_clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract({parent_alias}.value, '{parse_json_path(nested_path)}') "
                            f"{'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    elif key in unwound_fields:
                        unwound_alias = unwound_fields[key]
                        sort_clauses.append(
                            f"{unwound_alias}.value {'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    else:
                        sort_clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(key)}') "
                            f"{'DESC' if direction == DESCENDING else 'ASC'}"
                        )
            if sort_clauses:
                local_order_by = "ORDER BY " + ", ".join(sort_clauses)

        if limit_value is not None:
            local_limit = f"LIMIT {limit_value}"
            if skip_value > 0:
                local_offset = f"OFFSET {skip_value}"
        elif skip_value > 0:
            # SQLite requires LIMIT when using OFFSET
            local_limit = "LIMIT -1"
            local_offset = f"OFFSET {skip_value}"

        return local_order_by, local_limit, local_offset

    def _build_group_query(
        self, group_spec: dict[str, Any]
    ) -> tuple[str, str, list[str]] | None:
        """
        Builds the SELECT and GROUP BY clauses for a $group stage.

        This method constructs SQL SELECT and GROUP BY clauses for MongoDB-like
        $group aggregation stages that can be handled directly with SQL. It supports
        grouping by a single field and various accumulator operations like $sum,
        $avg, $min, $max, $count, $push, and $addToSet.

        Args:
            group_spec (dict[str, Any]): A dictionary representing the $group stage
                                         specification. It should contain an "_id"
                                         field for grouping and accumulator operations
                                         for other fields.

        Returns:
            tuple[str, str, list[str]] | None: A tuple containing:
                - The SELECT clause string with all required expressions
                - The GROUP BY clause string
                - A list of output field names
            Returns None if the group specification contains unsupported operations
            that require Python-based processing.
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
