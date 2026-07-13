from __future__ import annotations

import logging
from typing import Any, Callable

from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from ..jsonb_support import (
    _get_json_group_array_function,
)
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)



class OperatorsGroupMixin(OperatorsBaseMixin):
    def _process_group_stage(
        self,
        create_temp: Callable,
        current_table: str,
        group_spec: dict[str, Any],
    ) -> str:
        """
        Process a $group stage using temporary tables.

        This method implements the $group aggregation stage which groups documents
        by a specified key and performs accumulator operations.

        Supports these accumulators in SQL tier:
        - $sum, $avg, $min, $max: Standard SQL aggregators
        - $count: COUNT(*)
        - $first, $last: Using subqueries (LIMITATION: requires no preceding $sort)
        - $addToSet: Using json_group_array(DISTINCT ...)
        - $push: Using json_group_array(...)
        - Expression keys: Using SQLTranslator for expression evaluation

        Limitation:
        - $first/$last with preceding $sort stage falls back to Python for correctness.
          The current implementation uses correlated subqueries that don't preserve
          sort order across groups.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            group_spec (dict[str, Any]): The $group stage specification

        Returns:
            str: Name of the newly created temporary table with grouped results
        """
        # Check kill switch FIRST (Bug 010 fix)
        from ..query_helper import get_force_fallback

        if get_force_fallback():
            raise NotImplementedError(
                "Force fallback - use Tier 3 Python evaluation"
            )

        # Check for $first/$last with preceding $sort - fall back to Python
        if self._has_sort_stage:
            for field, accumulator in group_spec.items():
                if field == "_id":
                    continue
                if isinstance(accumulator, dict) and len(accumulator) == 1:
                    op = next(
                        iter(accumulator.keys())
                    )  # Get the key (operator name), not value
                    if op in ("$first", "$last"):
                        raise NotImplementedError(
                            "$first/$last with preceding $sort not supported in SQL tier for correctness"
                        )

        group_id_expr = group_spec.get("_id")
        select_parts = []
        group_by_parts = []
        array_fields = []  # Track fields that are arrays (from $push/$addToSet)

        # Handle _id (group key)
        if group_id_expr is None:
            # Group all documents together
            select_parts.append("NULL AS _id")
        elif isinstance(group_id_expr, str) and group_id_expr.startswith("$"):
            field_name = group_id_expr[1:]
            if field_name == "_id":
                # Special case: grouping by _id column
                select_parts.append("_id AS _id")
                group_by_parts.append("_id")
            else:
                # Group by extracted field
                json_extract = f"{self._json_function_prefix}_extract"
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
                # Use ExprEvaluator to build the SQL expression
                key_expr, key_params = (
                    self.expr_evaluator.build_select_expression(group_id_expr)
                )
                if key_expr:
                    select_parts.append(f"{key_expr} AS _id")
                    group_by_parts.append(key_expr)
                    # Store params for later use (though currently not used in CREATE TABLE AS SELECT)
                    self._group_key_params = key_params
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

            # Extract field name from expression
            if isinstance(expr, str) and expr.startswith("$"):
                expr_field = expr[1:]
            elif isinstance(expr, (int, float)):
                expr_field = None  # Literal value
            elif isinstance(expr, dict):
                # Expression object (e.g., {'title': '$title', 'author': '$author'})
                # This is valid for $push and $addToSet
                expr_field = (
                    None  # Will be handled specially in the accumulator logic
                )
            else:
                # Complex expression - fall back to Python
                raise NotImplementedError(
                    f"$group accumulator {op} with expression {expr} not supported in SQL tier"
                )

            # Map accumulator to SQL
            json_extract = f"{self._json_function_prefix}_extract"
            json_group_array = self.json_group_array_function

            match op:
                case "$sum":
                    if expr == 1:
                        # Count operation
                        select_parts.append(f"COUNT(*) AS {field}")
                    elif expr_field:
                        if expr_field == "_id":
                            select_parts.append(f"SUM(_id) AS {field}")
                        else:
                            select_parts.append(
                                f"SUM({json_extract}(data, '{parse_json_path(expr_field)}')) AS {field}"
                            )
                    else:
                        select_parts.append(f"SUM({expr}) AS {field}")

                case "$avg":
                    if expr_field:
                        if expr_field == "_id":
                            select_parts.append(f"AVG(_id) AS {field}")
                        else:
                            select_parts.append(
                                f"AVG({json_extract}(data, '{parse_json_path(expr_field)}')) AS {field}"
                            )
                    else:
                        select_parts.append(f"AVG({expr}) AS {field}")

                case "$min":
                    if expr_field:
                        if expr_field == "_id":
                            select_parts.append(f"MIN(_id) AS {field}")
                        else:
                            select_parts.append(
                                f"MIN({json_extract}(data, '{parse_json_path(expr_field)}')) AS {field}"
                            )
                    else:
                        select_parts.append(f"MIN({expr}) AS {field}")

                case "$max":
                    if expr_field:
                        if expr_field == "_id":
                            select_parts.append(f"MAX(_id) AS {field}")
                        else:
                            select_parts.append(
                                f"MAX({json_extract}(data, '{parse_json_path(expr_field)}')) AS {field}"
                            )
                    else:
                        select_parts.append(f"MAX({expr}) AS {field}")

                case "$count":
                    select_parts.append(f"COUNT(*) AS {field}")

                case "$first":
                    # $first gets the first value in the group (by insertion order / minimum id)
                    # When grouping by $_id, each document is its own group, so just return the value
                    if expr_field:
                        if group_id_expr == "$_id" or (
                            isinstance(group_id_expr, str)
                            and group_id_expr.lstrip("$") == "_id"
                        ):
                            # Special case: grouping by $_id, each doc is its own group
                            if expr_field == "_id":
                                select_parts.append(f"_id AS {field}")
                            else:
                                select_parts.append(
                                    f"{json_extract}(data, '{parse_json_path(expr_field)}') AS {field}"
                                )
                        elif expr_field == "_id":
                            select_parts.append(
                                f"(SELECT first_t._id FROM {current_table} first_t "
                                f"INNER JOIN (SELECT MIN(sub_t.id) as min_id FROM {current_table} sub_t "
                                f"WHERE sub_t.{group_by_parts[0]} = {group_by_parts[0]}) first_sub "
                                f"ON first_t.id = first_sub.min_id) AS {field}"
                            )
                        else:
                            select_parts.append(
                                f"(SELECT {json_extract}(first_t.data, '{parse_json_path(expr_field)}') "
                                f"FROM {current_table} first_t "
                                f"INNER JOIN (SELECT MIN(sub_t.id) as min_id FROM {current_table} sub_t "
                                f"WHERE sub_t.{group_by_parts[0]} = {group_by_parts[0]}) first_sub "
                                f"ON first_t.id = first_sub.min_id) AS {field}"
                            )
                    # Note: This is a simplified implementation
                    # A full implementation would need proper ordering within groups

                case "$last":
                    # $last gets the last value in the group (by insertion order / maximum id)
                    if expr_field:
                        if group_id_expr == "$_id" or (
                            isinstance(group_id_expr, str)
                            and group_id_expr.lstrip("$") == "_id"
                        ):
                            # Special case: grouping by $_id, each doc is its own group
                            if expr_field == "_id":
                                select_parts.append(f"_id AS {field}")
                            else:
                                select_parts.append(
                                    f"{json_extract}(data, '{parse_json_path(expr_field)}') AS {field}"
                                )
                        elif expr_field == "_id":
                            select_parts.append(
                                f"(SELECT last_t._id FROM {current_table} last_t "
                                f"INNER JOIN (SELECT MAX(sub_t.id) as max_id FROM {current_table} sub_t "
                                f"WHERE sub_t.{group_by_parts[0]} = {group_by_parts[0]}) last_sub "
                                f"ON last_t.id = last_sub.max_id) AS {field}"
                            )
                        else:
                            select_parts.append(
                                f"(SELECT {json_extract}(last_t.data, '{parse_json_path(expr_field)}') "
                                f"FROM {current_table} last_t "
                                f"INNER JOIN (SELECT MAX(sub_t.id) as max_id FROM {current_table} sub_t "
                                f"WHERE sub_t.{group_by_parts[0]} = {group_by_parts[0]}) last_sub "
                                f"ON last_t.id = last_sub.max_id) AS {field}"
                            )

                case "$addToSet":
                    # Use json_group_array with DISTINCT
                    # Track this field for post-processing
                    array_fields.append(field)

                    # Check if expr is a dict expression
                    if isinstance(expr, dict):
                        # Build json_object for the expression (same as $push)
                        json_object_func = (
                            f"{self._json_function_prefix}_object"
                        )
                        obj_args = []
                        for key, val in expr.items():
                            if isinstance(val, str) and val.startswith("$"):
                                field_name = val[1:]
                                if field_name == "_id":
                                    obj_args.append(f"'{key}', _id")
                                else:
                                    obj_args.append(
                                        f"'{key}', {json_extract}(data, '{parse_json_path(field_name)}')"
                                    )
                            elif isinstance(val, (int, float, str)):
                                # Literal value - inline directly
                                if isinstance(val, str):
                                    escaped_val = val.replace("'", "''")
                                    obj_args.append(
                                        f"'{key}', json_quote('{escaped_val}')"
                                    )
                                else:
                                    obj_args.append(f"'{key}', {val}")
                            else:
                                raise NotImplementedError(
                                    f"$addToSet with complex expression {expr} not supported in SQL tier"
                                )

                        select_parts.append(
                            f"{json_group_array}(DISTINCT {json_object_func}({', '.join(obj_args)})) AS {field}"
                        )
                    elif expr_field:
                        if expr_field == "_id":
                            select_parts.append(
                                f"{json_group_array}(DISTINCT _id) AS {field}"
                            )
                        else:
                            select_parts.append(
                                f"{json_group_array}(DISTINCT {json_extract}(data, '{parse_json_path(expr_field)}')) AS {field}"
                            )

                case "$push":
                    # Use json_group_array
                    # Track this field for post-processing
                    array_fields.append(field)

                    # Check if expr is a dict expression (e.g., {'title': '$title', 'author': '$author'})
                    if isinstance(expr, dict):
                        # Build json_object for the expression
                        json_object_func = (
                            f"{self._json_function_prefix}_object"
                        )
                        obj_args = []
                        for key, val in expr.items():
                            if isinstance(val, str) and val.startswith("$"):
                                field_name = val[1:]
                                if field_name == "_id":
                                    obj_args.append(f"'{key}', _id")
                                else:
                                    obj_args.append(
                                        f"'{key}', {json_extract}(data, '{parse_json_path(field_name)}')"
                                    )
                            elif isinstance(val, (int, float, str)):
                                # Literal value - inline directly (CREATE TABLE AS SELECT doesn't support params)
                                if isinstance(val, str):
                                    # Escape single quotes for SQL
                                    escaped_val = val.replace("'", "''")
                                    obj_args.append(
                                        f"'{key}', json_quote('{escaped_val}')"
                                    )
                                else:
                                    obj_args.append(f"'{key}', {val}")
                            else:
                                raise NotImplementedError(
                                    f"$push with complex expression {expr} not supported in SQL tier"
                                )

                        select_parts.append(
                            f"{json_group_array}({json_object_func}({', '.join(obj_args)})) AS {field}"
                        )
                        # Store literal values as params (though they can't be used in CREATE TABLE AS SELECT)
                        # For now, we inline literal values
                    elif expr_field:
                        if expr_field == "_id":
                            select_parts.append(
                                f"{json_group_array}(_id) AS {field}"
                            )
                        else:
                            select_parts.append(
                                f"{json_group_array}({json_extract}(data, '{parse_json_path(expr_field)}')) AS {field}"
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
        json_args = self._id_to_json_object_args(select_parts)
        json_object_func = f"{self._json_function_prefix}_object"
        # Wrap with json() to ensure text output for Python consumption
        # (jsonb_object returns binary JSONB which Python can't read directly)
        json_output_func = f"json({json_object_func}"

        # Check if we have params from expression keys
        # Note: CREATE TABLE AS SELECT doesn't support params, so we inline them
        group_params = getattr(self, "_group_key_params", [])
        if group_params:
            # For expression keys with params, we need to inline them
            # This is a limitation - for now, fall back to Python if params are needed
            # A future enhancement could use a different approach (e.g., CTEs)
            raise NotImplementedError(
                "$group with parameterized expression key not supported in SQL tier"
            )

        new_table = create_temp(
            group_stage,
            "SELECT ROW_NUMBER() OVER () as id, "
            + f"{json_output_func}({json_args})) as data "
            + f"FROM {current_table} {group_by_clause}",
        )

        # Store array fields metadata for efficient post-processing
        # This avoids scanning all fields in _get_results_from_table
        if not hasattr(self, "_array_fields_map"):
            self._array_fields_map = {}
        self._array_fields_map[new_table] = array_fields

        return new_table

    def _id_to_json_object_args(self, select_parts: list[str]) -> str:
        """
        Convert SELECT parts to json_object arguments.

        Args:
            select_parts: List of SELECT column expressions (e.g., ["expr1 AS field1", "expr2 AS field2"])

        Returns:
            Comma-separated list of 'key', value pairs for json_object
        """
        args = []
        for part in select_parts:
            # Parse "expression AS alias"
            if " AS " in part:
                expr, alias = part.rsplit(" AS ", 1)
                expr = expr.strip()
                alias = alias.strip().strip('"').strip("'")
                args.append(f"'{alias}', {expr}")
            else:
                # No alias, use the expression as-is (shouldn't happen normally)
                args.append(f"'column', {part}")
        return ", ".join(args)

    def _get_results_from_table(
        self,
        table_name: str,
        is_count: bool = False,
        count_field: str | None = None,
        batch_size: int = 101,
    ) -> list[dict[str, Any]]:
        """
        Get results from a temporary table.

        This method retrieves all documents from a temporary table and converts
        them back into their Python dictionary representation using the collection's
        document loading mechanism.

        For $count optimization, if is_count is True, it returns a single document
        with the count from the table using SQL COUNT(*) instead of loading all documents.

        Args:
            table_name (str): Name of the temporary table to retrieve results from
            is_count (bool): If True, return count document instead of all documents
            count_field (str | None): The field name for the count if is_count is True

        Returns:
            list[dict[str, Any]]: List of documents retrieved from the temporary table,
                                  with each document represented as a dictionary
        """
        if is_count and count_field:
            # Optimized path for $count: use SQL COUNT instead of loading all documents
            cursor = self.db.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return [{count_field: count}]

        # When data is stored as JSONB (binary), we need to convert it to text JSON for Python
        # Since temp tables created with CREATE TABLE ... AS SELECT don't preserve column types,
        # we check if the source collection has JSONB support instead
        use_json_wrapper = self._jsonb_supported

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
                quote_table_name(col) for col in column_names
            )
            cursor = self.db.execute(
                f"SELECT {select_clause} FROM {table_name}"
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
                    # _id is provided as a separate column, use it directly
                    doc = neosqlite_json_loads(row[2])
                    # Only set _id from column if it's not already in the JSON
                    if "_id" not in doc:
                        doc["_id"] = self.collection._parse_stored_id(row[1])
                elif has_id_column and len(row) == 2:
                    # Only id column, no separate _id column
                    doc = neosqlite_json_loads(row[1])
                elif len(row) == 2 and not has_id_column:
                    # _id and data columns (no id)
                    doc = neosqlite_json_loads(row[1])
                    if "_id" not in doc:
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

                results.append(doc)
        return results

    def _process_bucket_stage(self, create_temp, current_table, bucket_spec):
        """
        Process $bucket stage - groups documents by boundaries.

        MongoDB syntax:
        {
          $bucket: {
            groupBy: <expression>,
            boundaries: [<lowerbound1>, <lowerbound2>, ...],
            default: <literal>,  // optional
            output: { <output1>: { <$accumulator expression> }, ... }
          }
        }
        """
        group_by = bucket_spec.get("groupBy")
        boundaries = bucket_spec.get("boundaries", [])
        default_label = bucket_spec.get("default", "Other")
        output_spec = bucket_spec.get("output", {"count": {"$sum": 1}})

        if not group_by or not boundaries:
            return current_table

        # Sort boundaries
        sorted_boundaries = sorted(boundaries)

        # Build CASE expression for bucketing
        # MongoDB uses the lower boundary as the _id value
        case_parts = []
        for i in range(len(sorted_boundaries) - 1):
            lower = sorted_boundaries[i]
            upper = sorted_boundaries[i + 1]
            case_parts.append(
                f"WHEN {self._build_group_by_expr(group_by)} >= {lower} AND {self._build_group_by_expr(group_by)} < {upper} THEN {lower}"
            )
        # Last bucket (inclusive upper bound) - use the last boundary as _id
        last_lower = sorted_boundaries[-1]
        case_parts.append(
            f"WHEN {self._build_group_by_expr(group_by)} >= {last_lower} THEN {last_lower}"
        )
        # Default case - use the default label
        case_parts.append(f"ELSE '{default_label}'")

        case_expr = "CASE " + " ".join(case_parts) + " END"

        # Build output expressions
        output_fields = []
        output_fields.append(f"{case_expr} AS _id")

        for field_name, accumulator in output_spec.items():
            match accumulator:
                case {"$sum": sum_expr}:
                    output_fields.append(f"SUM({sum_expr}) AS {field_name}")
                case {"$avg": avg_expr}:
                    output_fields.append(f"AVG({avg_expr}) AS {field_name}")
                case {"$count": _}:
                    output_fields.append(f"COUNT(*) AS {field_name}")
                case {"$min": min_expr}:
                    output_fields.append(f"MIN({min_expr}) AS {field_name}")
                case {"$max": max_expr}:
                    output_fields.append(f"MAX({max_expr}) AS {field_name}")
                case {"$first": first_expr}:
                    output_fields.append(f"MIN({first_expr}) AS {field_name}")
                case {"$last": last_expr}:
                    output_fields.append(f"MAX({last_expr}) AS {field_name}")
                case {"$push": push_expr}:
                    # Use json_group_array for push
                    json_group_func = _get_json_group_array_function(
                        self._jsonb_supported
                    )
                    output_fields.append(
                        f"{json_group_func}({push_expr}) AS {field_name}"
                    )
                case _:
                    # Default to count
                    output_fields.append(f"COUNT(*) AS {field_name}")

        select_clause = ", ".join(output_fields)

        # Note: We must repeat the CASE expression in GROUP BY because SQLite
        # doesn't allow column aliases in GROUP BY clause
        new_table = create_temp(
            {"$bucket": bucket_spec},
            f"SELECT {select_clause} FROM {current_table} GROUP BY {case_expr} ORDER BY _id",
        )

        return new_table

    def _build_group_by_expr(self, group_by):
        """Build SQL expression for groupBy field."""
        if isinstance(group_by, str) and group_by.startswith("$"):
            field = group_by[1:]
            json_path = parse_json_path(field)
            json_extract = f"{self._json_function_prefix}_extract"
            return f"CAST({json_extract}(data, '{json_path}') AS REAL)"
        return "1"

    def _process_bucket_auto_stage(
        self, create_temp, current_table, bucket_auto_spec
    ):
        """
        Process $bucketAuto stage - auto-sized buckets.

        MongoDB syntax:
        {
          $bucketAuto: {
            groupBy: <expression>,
            buckets: <number>,
            output: { <output1>: { <$accumulator expression> }, ... },
            granularity: <string>  // optional
          }
        }
        """
        group_by = bucket_auto_spec.get("groupBy")
        num_buckets = bucket_auto_spec.get("buckets", 10)
        output_spec = bucket_auto_spec.get("output", {"count": {"$sum": 1}})

        if not group_by or num_buckets <= 0:
            return current_table

        # For bucketAuto, we need to calculate min/max and divide into buckets
        # This is a simplified implementation using NTILE window function
        json_extract = f"{self._json_function_prefix}_extract"
        field = (
            group_by[1:]
            if isinstance(group_by, str) and group_by.startswith("$")
            else group_by
        )
        json_path = parse_json_path(field)

        # Use NTILE for automatic bucketing
        # MongoDB returns _id as {min: <value>, max: <value>}
        agg_fields = []
        for field_name, accumulator in output_spec.items():
            match accumulator:
                case {"$sum": 1}:
                    # Special case: $sum: 1 is a count
                    agg_fields.append(f"COUNT(*) AS {field_name}")
                case {"$sum": _}:
                    agg_fields.append(f"SUM(s.val) AS {field_name}")
                case {"$avg": _}:
                    agg_fields.append(f"AVG(s.val) AS {field_name}")
                case {"$count": _}:
                    agg_fields.append(f"COUNT(*) AS {field_name}")
                case {"$min": _}:
                    agg_fields.append(f"MIN(s.val) AS {field_name}")
                case {"$max": _}:
                    agg_fields.append(f"MAX(s.val) AS {field_name}")
                case _:
                    agg_fields.append(f"COUNT(*) AS {field_name}")

        # Create subquery with NTILE bucketing
        subquery = f"""
            SELECT
                NTILE({num_buckets}) OVER (ORDER BY {json_extract}(data, '{json_path}')) AS bucket,
                CAST({json_extract}(data, '{json_path}') AS REAL) AS val
            FROM {current_table}
        """

        # Group by bucket and create the _id object with min/max using json_object
        # Wrap with json() to ensure text output (not JSONB binary)
        json_set_func = (
            "jsonb_object" if self._jsonb_supported else "json_object"
        )
        select_clause = f"json({json_set_func}('min', MIN(s.val), 'max', MAX(s.val))) AS _id"
        if agg_fields:
            select_clause += ", " + ", ".join(agg_fields)

        new_table = create_temp(
            {"$bucketAuto": bucket_auto_spec},
            f"SELECT {select_clause} FROM ({subquery}) s GROUP BY bucket ORDER BY MIN(s.val)",
        )

        return new_table

