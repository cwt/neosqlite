"""
Aggregation pipeline methods for NeoSQLite.

This module contains the AggregationMixin class, which provides methods for
building and executing MongoDB-like aggregation pipelines using SQL.
"""

import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from ...sql_utils import quote_table_name
from ..cursor import DESCENDING
from ..expr_evaluator import (
    AggregationContext,
    ExprEvaluator,
    _is_expression,
)
from ..json_path_utils import (
    parse_json_path,
)

logger = logging.getLogger(__name__)


# Import utility functions
from .utils import (
    get_force_fallback,
)

if TYPE_CHECKING:
    from .. import Collection


class AggregationMixin:
    """
    Mixin class providing aggregation pipeline methods.

    This mixin assumes it will be used with a class that has the following:

    Attributes:
        self.collection: A collection instance with:
            - db: Database connection
            - name: Collection name
            - _load: Method to load documents
            - _get_val: Method to get values from documents
            - _set_val: Method to set values in documents
        self._jsonb_supported: Whether JSONB is supported
        self._json_function_prefix: "json" or "jsonb"
        self._json_each_function: "json_each" or "jsonb_each"
        self._build_simple_where_clause: Method to build WHERE clauses
        self._reorder_pipeline_for_indexes: Method to reorder pipelines
        self._estimate_pipeline_cost: Method to estimate costs
        self._optimize_match_pushdown: Method to optimize match pushdown
        self._is_datetime_indexed_field: Method to check datetime indexes
        self._build_group_query: Method to build group queries
        self._apply_query: Method to apply queries to documents
    """

    collection: "Collection"
    _jsonb_supported: bool
    _json_function_prefix: str
    _json_each_function: str
    _build_simple_where_clause: Any
    _reorder_pipeline_for_indexes: Any
    _estimate_pipeline_cost: Any
    _optimize_match_pushdown: Any
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
                                f"{self._json_function_prefix}_extract(data, '{parse_json_path(key)}') "
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
                                    f"{self._json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(group_id_field)}') AS _id"
                                )
                                group_by_clause = f"GROUP BY {self._json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(group_id_field)}')"

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
                                        f"{self._json_function_prefix}_extract(data, '{parse_json_path(unwind_field_name)}') AS _id",
                                        "je.value AS _id",
                                    )
                                    # For GROUP BY clause, use je.value when grouping by the unwind field
                                    group_by_clause = "GROUP BY je.value"
                                else:
                                    modified_select = select_clause
                                    # Keep the original GROUP BY but ensure it references the correct table
                                    group_by_clause = group_by_clause.replace(
                                        f"{self._json_function_prefix}_extract(data,",
                                        f"{self._json_function_prefix}_extract({table_name}.data,",
                                    )

                                # Replace all other json_extract(data, ...) with json_extract(table.data, ...)
                                # to properly reference the table column in the JOIN context
                                modified_select = modified_select.replace(
                                    f"{self._json_function_prefix}_extract(data,",
                                    f"{self._json_function_prefix}_extract({table_name}.data,",
                                )

                                # For fields that reference the unwind field (e.g., $push: "$tags" when unwinding "$tags"),
                                # replace with je.value to get the unwound value instead of the full array
                                modified_select = modified_select.replace(
                                    f"{self._json_function_prefix}_extract({table_name}.data, '{parse_json_path(unwind_field_name)}')",
                                    "je.value",
                                )

                                # Build the FROM clause with json_each for unwinding
                                from_clause = f"FROM {table_name}, {self._json_each_function}({self._json_function_prefix}_extract({table_name}.data, '{parse_json_path(unwind_field_name)}')) as je"

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
                            f"{self._json_function_prefix}_extract(data, '{parse_json_path(unwind_field)}') AS _id",
                            "je.value AS _id",
                        )
                        # Also replace any other references to the unwind field in the SELECT clause
                        modified_select = modified_select.replace(
                            f"{self._json_function_prefix}_extract(data, '{parse_json_path(unwind_field)}')",
                            "je.value",
                        )
                        # For GROUP BY clause, use je.value when grouping by the unwind field
                        modified_group_by = "GROUP BY je.value"
                    else:
                        modified_select = select_clause
                        # Keep the original GROUP BY but ensure it references the correct table
                        modified_group_by = group_by_clause.replace(
                            f"{self._json_function_prefix}_extract(data,",
                            f"{self._json_function_prefix}_extract({table_name}.data,",
                        )

                    # Replace all other json_extract(data, ...) with json_extract(table.data, ...)
                    # to properly reference the table column in the JOIN context
                    # This is needed for fields that aren't the unwind field (e.g., $push: $name)
                    modified_select = modified_select.replace(
                        f"{self._json_function_prefix}_extract(data,",
                        f"{self._json_function_prefix}_extract({table_name}.data,",
                    )

                    # Build the FROM clause with json_each for unwinding
                    from_clause = f"FROM {table_name}, {self._json_each_function}({self._json_function_prefix}_extract({table_name}.data, '{parse_json_path(unwind_field)}')) as je"

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
                    f"json_type({self._json_function_prefix}_extract({parent_alias}.value, '{parse_json_path(nested_path)}')) = 'array'"
                )
            else:
                all_where_clauses.append(
                    f"json_type({self._json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) = 'array'"
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
                    f", {self._json_each_function}({self._json_function_prefix}_extract({parent_alias}.value, '{parse_json_path(nested_path)}')) as {je_alias}"
                )
            else:
                from_parts.append(
                    f", {self._json_each_function}({self._json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) as {je_alias}"
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
                            f"{self._json_function_prefix}_extract({parent_alias}.value, '{parse_json_path(nested_path)}') "
                            f"{'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    elif key in unwound_fields:
                        unwound_alias = unwound_fields[key]
                        sort_clauses.append(
                            f"{unwound_alias}.value {'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    else:
                        sort_clauses.append(
                            f"{self._json_function_prefix}_extract({quote_table_name(self.collection.name)}.data, '{parse_json_path(key)}') "
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
            group_by_clause = f"GROUP BY {self._json_function_prefix}_extract(data, '{parse_json_path(group_by_field)}')"
            select_expressions = [
                f"{self._json_function_prefix}_extract(data, '{parse_json_path(group_by_field)}') AS _id"
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
                    f"json_group_array({self._json_function_prefix}_extract(data, '{parse_json_path(field_name)}')) AS \"{field}\""
                )
                output_fields.append(field)
                continue

            if op == "$addToSet":
                # Handle $addToSet accumulator
                if not isinstance(expr, str) or not expr.startswith("$"):
                    return None  # Fallback for complex accumulator expressions
                field_name = expr[1:]
                select_expressions.append(
                    f"json_group_array(DISTINCT {self._json_function_prefix}_extract(data, '{parse_json_path(field_name)}')) AS \"{field}\""
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
                f"{sql_func}({self._json_function_prefix}_extract(data, '{parse_json_path(field_name)}')) AS {field}"
            )
            output_fields.append(field)

        select_clause = "SELECT " + ", ".join(select_expressions)
        return select_clause, group_by_clause, output_fields

    def _process_group_stage(
        self,
        group_query: dict[str, Any],
        docs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Process the $group stage of an aggregation pipeline.

        This method groups documents by a specified field and performs specified
        accumulator operations on other fields.

        Args:
            group_query (dict[str, Any]): A dictionary representing the $group
                                          stage of the aggregation pipeline.
            docs (list[dict[str, Any]]): A list of documents to be grouped.

        Returns:
            list[dict[str, Any]]: A list of grouped documents with applied
                                  accumulator operations.
        """

        grouped_docs: dict[Any, dict[str, Any]] = {}
        group_id_key = group_query.get("_id")

        # Create a copy of group_query without _id for processing accumulator operations
        accumulators = {k: v for k, v in group_query.items() if k != "_id"}

        # Create expression evaluator for evaluating expressions in accumulators
        evaluator = ExprEvaluator(
            data_column="data", db_connection=self.collection.db
        )

        for doc in docs:
            if group_id_key is None:
                group_id = None
            elif _is_expression(group_id_key):
                # Evaluate expression for group key
                group_id = evaluator._evaluate_expr_python(group_id_key, doc)
            else:
                group_id = self.collection._get_val(doc, group_id_key)

            group = grouped_docs.setdefault(group_id, {"_id": group_id})

            for field, accumulator in accumulators.items():
                # Check if accumulator is a valid dictionary format
                if not isinstance(accumulator, dict) or len(accumulator) != 1:
                    # Invalid accumulator format, skip this field
                    continue

                op, key = next(iter(accumulator.items()))

                # Check for unsupported operators
                if op == "$accumulator":
                    raise NotImplementedError(
                        "The '$accumulator' operator is not supported in NeoSQLite. "
                        "Please use built-in accumulators ($sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last), "
                        "or post-process results in Python."
                    )

                if op == "$count":
                    group[field] = group.get(field, 0) + 1
                    continue

                # Handle expressions in accumulators
                if _is_expression(key):
                    # Evaluate expression for each document
                    value = evaluator._evaluate_expr_python(key, doc)
                # Handle literal values (e.g., $sum: 1 for counting)
                elif isinstance(key, (int, float)):
                    value = key
                elif isinstance(key, dict):
                    # Check if this is one of our new N-value operators
                    if op in {"$firstN", "$lastN", "$minN", "$maxN"}:
                        # These operators use dict format with "input" field
                        # Extract the input field and get its value
                        input_field = key.get("input", key.get("values", ""))
                        if input_field:
                            value = self.collection._get_val(doc, input_field)
                        else:
                            value = None
                    else:
                        # Complex expression like {"$multiply": [...]}, not supported in Python fallback
                        continue
                else:
                    value = self.collection._get_val(doc, key)

                match op:
                    case "$sum":
                        group[field] = (group.get(field, 0) or 0) + (value or 0)
                    case "$avg":
                        avg_info = group.get(field, {"sum": 0, "count": 0})
                        avg_info["sum"] += value or 0
                        avg_info["count"] += 1
                        group[field] = avg_info
                    case "$min":
                        current = group.get(field, value)
                        if current is not None and value is not None:
                            group[field] = min(current, value)
                        elif value is not None:
                            group[field] = value
                        elif current is not None:
                            group[field] = current
                        else:
                            group[field] = None
                    case "$max":
                        current = group.get(field, value)
                        if current is not None and value is not None:
                            group[field] = max(current, value)
                        elif value is not None:
                            group[field] = value
                        elif current is not None:
                            group[field] = current
                        else:
                            group[field] = None
                    case "$push":
                        group.setdefault(field, []).append(value)
                    case "$addToSet":
                        # Initialize the list if it doesn't exist
                        if field not in group:
                            group[field] = []
                        # Only add the value if it's not already in the list
                        if value not in group[field]:
                            group[field].append(value)
                    case "$first":
                        # Only set the value if it hasn't been set yet (first document in group)
                        if field not in group:
                            group[field] = value
                    case "$last":
                        # Always update with the latest value (last document in group)
                        group[field] = value
                    case "$mergeObjects":
                        # Merge objects from all documents in the group
                        # Last value wins for conflicting fields
                        if field not in group:
                            group[field] = {}
                        if isinstance(value, dict):
                            group[field] |= value
                    case "$stdDevPop":
                        # Track sum, sum of squares, and count for population standard deviation
                        if field not in group:
                            group[field] = {
                                "sum": 0,
                                "sum_squares": 0,
                                "count": 0,
                                "type": "stdDevPop",
                            }
                        if value is not None:
                            group[field]["sum"] += value
                            group[field]["sum_squares"] += value * value
                            group[field]["count"] += 1
                    case "$stdDevSamp":
                        # Track sum, sum of squares, and count for sample standard deviation
                        if field not in group:
                            group[field] = {
                                "sum": 0,
                                "sum_squares": 0,
                                "count": 0,
                                "type": "stdDevSamp",
                            }
                        if value is not None:
                            group[field]["sum"] += value
                            group[field]["sum_squares"] += value * value
                            group[field]["count"] += 1
                    case "$firstN" | "$lastN" | "$minN" | "$maxN":
                        # Handle N-value operators
                        if not isinstance(key, dict) or "n" not in key:
                            continue

                        n_value = key["n"]

                        if field not in group:
                            group[field] = {
                                "type": op,
                                "n": n_value,
                                "values": [],
                            }

                        # Add value to the list
                        if value is not None:
                            group[field]["values"].append(value)

                            # Keep only the top N values based on operator type
                            if len(group[field]["values"]) > n_value:
                                if op == "$firstN":
                                    # Keep first N values (already in order)
                                    group[field]["values"] = group[field][
                                        "values"
                                    ][:n_value]
                                elif op == "$lastN":
                                    # Keep last N values
                                    group[field]["values"] = group[field][
                                        "values"
                                    ][-n_value:]
                                elif op == "$minN":
                                    # Keep N smallest values
                                    group[field]["values"] = sorted(
                                        group[field]["values"]
                                    )[:n_value]
                                elif op == "$maxN":
                                    # Keep N largest values
                                    group[field]["values"] = sorted(
                                        group[field]["values"], reverse=True
                                    )[:n_value]

        # Finalize $avg calculations
        for group in grouped_docs.values():
            for field, value in group.items():
                if field == "_id":
                    continue
                # Skip if this is a std dev calculation (has "type" key)
                if isinstance(value, dict) and value.get("type") in {
                    "stdDevPop",
                    "stdDevSamp",
                }:
                    continue
                # Finalize $avg calculations
                if (
                    isinstance(value, dict)
                    and "sum" in value
                    and "count" in value
                ):
                    if value["count"] > 0:
                        group[field] = value["sum"] / value["count"]
                    else:
                        group[field] = None

        # Finalize standard deviation calculations
        import math

        for group in grouped_docs.values():
            for field, value in group.items():
                if field == "_id":
                    continue
                if isinstance(value, dict) and value.get("type") in {
                    "stdDevPop",
                    "stdDevSamp",
                }:
                    n = value["count"]
                    if n > 0:
                        mean = value["sum"] / n
                        variance = (value["sum_squares"] / n) - (mean * mean)
                        if value["type"] == "stdDevSamp" and n > 1:
                            # Sample standard deviation uses Bessel's correction
                            variance = (
                                value["sum_squares"] - (value["sum"] ** 2) / n
                            ) / (n - 1)
                        if variance < 0:
                            # Handle floating point errors
                            variance = 0
                        group[field] = math.sqrt(variance)
                    else:
                        group[field] = None

        # Finalize N-value operators
        for group in grouped_docs.values():
            for field, value in group.items():
                if field == "_id":
                    continue
                if isinstance(value, dict) and value.get("type") in {
                    "$firstN",
                    "$lastN",
                    "$minN",
                    "$maxN",
                }:
                    if value["type"] == "$minN":
                        # Sort in ascending order and take first N values
                        sorted_values = sorted(value["values"])
                        group[field] = sorted_values[: value["n"]]
                    elif value["type"] == "$maxN":
                        # Sort in descending order and take first N values
                        sorted_values = sorted(value["values"], reverse=True)
                        group[field] = sorted_values[: value["n"]]
                    else:
                        # For firstN and lastN, values are already in correct order
                        group[field] = value["values"]

        return list(grouped_docs.values())

    def _run_subpipeline(
        self,
        sub_pipeline: list[dict[str, Any]],
        docs: list[dict[str, Any]],
        batch_size: int = 101,
    ) -> str:
        """
        Run a sub-pipeline (e.g., for $facet) on a list of documents.

        Uses tier optimization (Tier-1/Tier-2/Tier-3) for each sub-pipeline.
        Results are streamed to a temporary table in batches to avoid memory issues.

        Args:
            sub_pipeline: List of pipeline stages to execute
            docs: Input documents
            batch_size: Number of documents to process in each batch

        Returns:
            Name of the temporary table containing results
        """
        # Create a temporary in-memory collection to run the sub-pipeline
        # This allows each sub-pipeline to use Tier-1/Tier-2 optimization
        import uuid

        from .. import Collection

        # Create temp collection for processing this batch
        temp_collection_name = f"_facet_batch_{uuid.uuid4().hex[:12]}"
        temp_collection = Collection(
            db=self.collection.db,
            name=temp_collection_name,
            create=True,
            database=self.collection._database,
        )

        # Create result temp table to store sub-pipeline results
        result_table = f"_facet_result_{uuid.uuid4().hex[:12]}"
        self.collection.db.execute(f"""
            CREATE TEMP TABLE {result_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT
            )
        """)

        try:
            # Process input docs in batches
            for i in range(0, len(docs), batch_size):
                batch = docs[i : i + batch_size]

                # Strip __doc__ wrapper if present
                docs_to_insert = []
                for doc in batch:
                    if isinstance(doc, dict) and "__doc__" in doc:
                        docs_to_insert.append(doc["__doc__"])
                    else:
                        docs_to_insert.append(doc)

                if not docs_to_insert:
                    continue

                # Insert batch into temp collection
                temp_collection.insert_many(docs_to_insert)

                # Run sub-pipeline through normal aggregation (uses Tier-1/Tier-2/Tier-3)
                result = list(
                    temp_collection.aggregate(
                        sub_pipeline, batchSize=batch_size
                    )
                )

                # Insert results into result temp table
                for doc in result:
                    from neosqlite.collection.json_helpers import (
                        neosqlite_json_dumps,
                    )

                    self.collection.db.execute(
                        f"INSERT INTO {result_table} (data) VALUES (?)",
                        (neosqlite_json_dumps(doc),),
                    )

                # Clear temp collection for next batch
                temp_collection.delete_many({})

            return result_table

        finally:
            # Clean up temporary collection
            try:
                temp_collection.drop()
            except Exception as e:
                logger.debug(
                    f"Failed to drop temporary collection '{temp_collection.name}': {e}"
                )
                pass  # Ignore cleanup errors

    def _apply_projection(
        self,
        projection: dict[str, Any],
        document: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Applies the projection to the document, selecting or excluding fields
        based on the projection criteria.

        Args:
            projection (dict[str, Any]): A dictionary specifying which fields to
                                         include or exclude.
            document (dict[str, Any]): The document to apply the projection to.

        Returns:
            dict[str, Any]: The document with fields applied based on the projection.
        """
        from ..expr_evaluator import (
            REMOVE_SENTINEL,
        )

        if not projection:
            return document

        doc = deepcopy(document)
        projected_doc: dict[str, Any] = {}
        include_id = projection.get("_id", 1) == 1

        # Check if this is an inclusion projection with expressions or aggregation variables
        has_expressions = any(
            _is_expression(value)
            or (isinstance(value, str) and value.startswith("$"))
            for value in projection.values()
        )

        if has_expressions:
            # Inclusion mode with expressions - evaluate each field
            evaluator = ExprEvaluator(
                data_column="data", db_connection=self.collection.db
            )
            ctx = AggregationContext()
            ctx.bind_document(document)

            for key, value in projection.items():
                if key == "_id":
                    if include_id and "_id" in doc:
                        projected_doc["_id"] = doc["_id"]
                    continue

                if _is_expression(value):
                    # Evaluate expression
                    projected_value = evaluator._evaluate_expr_python(
                        value, document
                    )
                    # Check for $$REMOVE sentinel
                    if projected_value is REMOVE_SENTINEL:
                        # Skip this field (remove it)
                        continue
                    projected_doc[key] = projected_value
                elif isinstance(value, str) and value.startswith("$"):
                    # Field reference or aggregation variable
                    if value.startswith("$$"):
                        # Aggregation variable
                        if value == "$$ROOT":
                            projected_doc[key] = document.copy()
                        elif value == "$$CURRENT":
                            projected_doc[key] = document.copy()
                        elif value == "$$REMOVE":
                            # Skip this field (remove it)
                            continue
                        else:
                            projected_doc[key] = None
                    else:
                        # Regular field reference
                        field_name = value[1:]
                        projected_doc[key] = self.collection._get_val(
                            document, field_name
                        )
                elif value == 1:
                    # Simple inclusion
                    if key in doc:
                        projected_doc[key] = doc[key]
                # value == 0 is exclusion, skip it

            return projected_doc

        # Inclusion mode (no expressions)
        if any(v == 1 for v in projection.values()):
            for key, value in projection.items():
                if value == 1 and key in doc:
                    projected_doc[key] = doc[key]
            if include_id and "_id" in doc:
                projected_doc["_id"] = doc["_id"]
            return projected_doc

        # Exclusion mode
        for key, value in projection.items():
            if value == 0 and key in doc:
                doc.pop(key, None)
        if not include_id and "_id" in doc:
            doc.pop("_id", None)
        return doc
