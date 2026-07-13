from __future__ import annotations

import logging
from typing import Any, Callable

from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from .operators_base import OperatorsBaseMixin
from .utils import (
    _contains_text_search,
    _sanitize_params,
)

logger = logging.getLogger(__name__)



class OperatorsMatchMixin(OperatorsBaseMixin):
    def _process_match_stage(
        self,
        create_temp: Callable,
        current_table: str,
        match_spec: dict[str, Any],
    ) -> str:
        """
        Process a $match stage using temporary tables.

        This method creates a temporary table that contains only documents matching
        the specified criteria. It translates the MongoDB-style match specification
        into SQL WHERE conditions using json_extract for field access.

        The method supports these match operators:
        - $eq, $gt, $lt, $gte, $lte: Comparison operators
        - $in, $nin: Array membership operators
        - $ne: Not equal operator
        - $text: Text search operator (handled with special logic for unwound elements)

        For the special _id field, it uses the table's id column directly rather
        than json_extract.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing
                                 input data
            match_spec (dict[str, Any]): The $match stage specification

        Returns:
            str: Name of the newly created temporary table with matched documents
        """
        # Check if text search is involved
        if _contains_text_search(match_spec):
            return self._process_text_search_stage(
                create_temp, current_table, match_spec
            )

        # Try to use SQLTranslator to build WHERE clause
        # If it returns (None, []), it means an unsupported operator is involved
        where_clause, params = self.sql_translator.translate_match(match_spec)

        # Check if translation failed (SQLTranslator returns None for unsupported operators)
        if where_clause is None:
            # Check if it's text search (which has special handling)
            if _contains_text_search(match_spec):
                # For text search on unwound elements, we currently fall back to
                # returning all documents from the temporary table.
                # This preserves the behavior where text search falls back to Python
                # processing when it can't be handled efficiently with SQL.
                # A future enhancement could implement proper text search on temporary tables.
                match_stage = {"$match": match_spec}
                new_table = create_temp(
                    match_stage, f"SELECT * FROM {current_table}"
                )
                return new_table
            else:
                # Unsupported operator (e.g., $elemMatch, $in on arrays)
                # Raise NotImplementedError to trigger Python fallback
                raise NotImplementedError(
                    f"$match stage contains unsupported operators: {match_spec}"
                )

        # Remove "WHERE " prefix if present for easier manipulation
        if where_clause.startswith("WHERE "):
            where_clause = where_clause[6:]

        # Create filtered temporary table for regular match operations
        match_stage = {"$match": match_spec}
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        # Check what columns the current table has (similar to _process_add_fields_stage)
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names
        has_data = "data" in column_names

        # If table doesn't have _id column but has data column (e.g., after $group),
        # rewrite WHERE clause to extract _id from JSON
        if not has_underscore_id and has_data:
            # Replace references to _id with json_extract(data, '$._id')
            json_extract = f"{self._json_function_prefix}_extract"
            import re

            # Replace _id when it's used as a column reference (not inside a string)
            where_clause = re.sub(
                r"(?<!\.)\b_id\b(?!\s*=)",
                f"{json_extract}(data, '$._id')",
                where_clause,
            )

        # Build SELECT clause based on available columns
        # After $group, tables have id and data (with _id embedded in JSON)
        # Regular tables have id, _id, and data
        if has_id and has_underscore_id and has_data:
            # Standard table with _id column
            sql = (
                f"SELECT id, _id, "
                f"json({json_set_func}(data, '$._id', _id)) AS data "
                f"FROM {current_table} WHERE {where_clause}"
            )
        elif has_id and has_data:
            # Table without _id column (e.g., after $group)
            # Extract _id from JSON data for consistency
            json_extract = f"{self._json_function_prefix}_extract"
            sql = (
                f"SELECT id, "
                f"json({json_set_func}(data, '$._id', {json_extract}(data, '$._id'))) AS data "
                f"FROM {current_table} WHERE {where_clause}"
            )
        else:
            # Fallback: just select from the table
            sql = f"SELECT * FROM {current_table} WHERE {where_clause}"

        # Sanitize parameters to convert ObjectId to strings
        sanitized_params = _sanitize_params(params)
        new_table = create_temp(match_stage, sql, sanitized_params)
        return new_table

    def _process_unwind_stages(
        self, create_temp: Callable, current_table: str, unwind_specs: list[Any]
    ) -> str:
        """
        Process one or more consecutive $unwind stages using temporary tables.

        This method handles the $unwind stage which deconstructs an array field
        from input documents to output a document for each element. It can process
        either a single unwind stage or multiple consecutive unwind stages.

        For a single unwind, it uses SQLite's json_each function to expand the
        array into separate rows. For multiple consecutive unwinds, it processes
        them sequentially (one at a time) rather than trying to process them all
        together, which doesn't work for nested arrays that depend on previous
        unwind operations.

        The method properly handles array validation, ensuring that only documents
        with array fields are processed. It also supports the special _id field
        handling if it were to be unwound (though this would be unusual).

        Supports these $unwind options:
        - path: The array field to unwind (required)
        - preserveNullAndEmptyArrays: If true, includes documents where the array is missing/null/empty
        - includeArrayIndex: If specified, includes the array index in the output

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing
                                 input data
            unwind_specs (list[Any]): List of $unwind stage specifications to
                                      process consecutively

        Returns:
            str: Name of the newly created temporary table with unwound documents

        Raises:
            ValueError: If an invalid unwind specification is encountered
        """

        # Process unwind stages one at a time to handle nested dependencies correctly
        current_temp_table = current_table

        for unwind_spec in unwind_specs:
            # Handle both simple string format and dict format
            field_path: str
            preserve_null: bool
            include_index: str | None

            if isinstance(unwind_spec, str):
                field_path = unwind_spec
                preserve_null = False
                include_index = None
            elif isinstance(unwind_spec, dict):
                field_path = str(unwind_spec.get("path", ""))
                preserve_null = bool(
                    unwind_spec.get("preserveNullAndEmptyArrays", False)
                )
                include_index = unwind_spec.get("includeArrayIndex")
            else:
                raise ValueError(f"Invalid unwind specification: {unwind_spec}")

            if not isinstance(field_path, str) or not field_path.startswith(
                "$"
            ):
                raise ValueError(f"Invalid unwind path: {field_path}")

            field_name = field_path[1:]  # Remove leading $

            # Build SQL based on options
            # Use appropriate JSON functions based on support
            json_extract_func = f"{self._json_function_prefix}_extract"

            # Build the SELECT clause
            select_parts = [
                f"{quote_table_name(self.collection.name)}.id",
                f"{quote_table_name(self.collection.name)}._id as _id",
            ]

            # Handle includeArrayIndex option
            if include_index:
                # Add array index as a new field in the data
                index_field = parse_json_path(include_index.lstrip("$"))
                # Use CAST to ensure key is treated as integer for proper indexing
                select_parts.append(
                    f"{self._json_function_prefix}_set("
                    f"  {self._json_function_prefix}_set("
                    f"    {quote_table_name(self.collection.name)}.data,"
                    f"    '{parse_json_path(field_name)}',"
                    f"    je.value"
                    f"  ),"
                    f"  '{index_field}',"
                    f"  CAST(je.key AS INTEGER)"
                    f") as data"
                )
            else:
                # Standard unwind - just set the unwound value
                select_parts.append(
                    f"{self._json_function_prefix}_set("
                    f"  {quote_table_name(self.collection.name)}.data,"
                    f"  '{parse_json_path(field_name)}',"
                    f"  je.value"
                    f") as data"
                )

            select_clause = ", ".join(select_parts)

            # Build FROM clause with json_each
            from_clause = (
                f"FROM {current_table} as {quote_table_name(self.collection.name)}, "
                f"{self._json_each_function}({json_extract_func}("
                f"  {quote_table_name(self.collection.name)}.data,"
                f"  '{parse_json_path(field_name)}'"
                f")) as je"
            )

            # Build WHERE clause based on preserveNullAndEmptyArrays
            if preserve_null:
                # Include documents where array is missing/null/empty
                # Use LEFT JOIN approach with UNION for null/empty cases
                where_clause = ""

                # Create temp table with two parts:
                # 1. Documents with arrays (unwound)
                # 2. Documents without arrays (preserved as-is)

                # For JSONB, we need to use json() to convert binary JSON to text for comparisons
                json_wrapper = "json(" if self._jsonb_supported else ""
                json_wrapper_close = ")" if self._jsonb_supported else ""

                # For preserved documents, MongoDB sets the unwound field to null (not empty array)
                # We need to handle three cases:
                # 1. Missing field (json_type IS NULL) - keep as-is
                # 2. Null value (json_type IS NULL but field exists) - keep as-is
                # 3. Empty array (json_type = 'array' AND value = '[]') - set field to null

                # Build the data expression for preserved documents
                if include_index:
                    index_field = parse_json_path(include_index.lstrip("$"))
                    # For empty arrays, remove the array field but keep index as None (MongoDB behavior)
                    # For null/missing fields, keep the field with None value and index as None
                    preserved_data_expr = f"""
                        CASE
                            WHEN json_type({json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) = 'array'
                                 AND {json_wrapper}{json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}'){json_wrapper_close} = '[]'
                            THEN {self._json_function_prefix}_set(
                                    {self._json_function_prefix}_remove({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}'),
                                    '{index_field}',
                                    NULL
                                  )
                            ELSE {self._json_function_prefix}_set(
                                    {quote_table_name(self.collection.name)}.data,
                                    '{index_field}',
                                    NULL
                                  )
                        END
                    """
                else:
                    # For empty arrays, remove the field entirely (MongoDB behavior)
                    preserved_data_expr = f"""
                        CASE
                            WHEN json_type({json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) = 'array'
                                 AND {json_wrapper}{json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}'){json_wrapper_close} = '[]'
                            THEN {self._json_function_prefix}_remove({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')
                            ELSE {quote_table_name(self.collection.name)}.data
                        END
                    """

                unwind_query = f"""
                    SELECT {select_clause}
                    {from_clause}
                    WHERE json_type({json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) = 'array'

                    UNION ALL

                    SELECT {quote_table_name(self.collection.name)}.id,
                           {quote_table_name(self.collection.name)}._id as _id,
                           {preserved_data_expr} as data
                    FROM {current_table} as {quote_table_name(self.collection.name)}
                    WHERE json_type({json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) IS NULL
                       OR json_type({json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}')) != 'array'
                       OR {json_wrapper}{json_extract_func}({quote_table_name(self.collection.name)}.data, '{parse_json_path(field_name)}'){json_wrapper_close} = '[]'
                """
            else:
                # Only include documents where the field is a non-empty array
                where_clause = (
                    f"WHERE json_type({json_extract_func}("
                    f"  {quote_table_name(self.collection.name)}.data,"
                    f"  '{parse_json_path(field_name)}'"
                    f")) = 'array'"
                )
                unwind_query = (
                    f"SELECT {select_clause} {from_clause} {where_clause}"
                )

            # Create the unwind stage spec for naming
            unwind_stage: dict[str, Any] = {"$unwind": field_path}
            if preserve_null:
                unwind_stage["preserveNullAndEmptyArrays"] = True
            if include_index:
                unwind_stage["includeArrayIndex"] = include_index

            current_temp_table = create_temp(unwind_stage, unwind_query)

        return current_temp_table

