"""
Simplified temporary table aggregation pipeline implementation for NeoSQLite.
This focuses on the core concept: using temporary tables to process complex pipelines
that the current implementation can't optimize with a single SQL query.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
import warnings
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Tuple

from .._sqlite import sqlite3
from ..sql_utils import quote_table_name
from .expr_evaluator import AggregationContext, ExprEvaluator, _is_expression
from .json_path_utils import parse_json_path
from .jsonb_support import (
    _contains_text_operator,
    _get_json_each_function,
    _get_json_function_prefix,
    _get_json_group_array_function,
    _get_json_tree_function,
    json_data_column,
    supports_jsonb,
    supports_jsonb_each,
)
from .sql_translator_unified import SQLTranslator

logger = logging.getLogger(__name__)

HASH_JOIN_MEMORY_THRESHOLD = 100 * 1024 * 1024  # 100 MB default threshold


class DeterministicTempTableManager:
    """
    Manager for deterministic temporary table names.

    This class generates unique but deterministic temporary table names based on
    pipeline stages and a pipeline ID. It ensures that the same pipeline stage
    will always generate the same table name within the same pipeline execution,
    which is useful for caching and optimization purposes.
    """

    def __init__(self, pipeline_id: str):
        """
        Initialize the DeterministicTempTableManager with a pipeline ID for generating
        unique table names.

        Args:
            pipeline_id (str): A unique identifier for the pipeline, used to ensure
                               table names are deterministic and unique across
                               different pipeline executions.
        """
        self.pipeline_id = pipeline_id
        self.stage_counter = 0
        self.name_counter: Dict[str, int] = (
            {}
        )  # Track how many times each name has been used

    def make_temp_table_name(
        self, stage: Dict[str, Any], name_suffix: str = ""
    ) -> str:
        """
        Generate a deterministic temporary table name based on the pipeline stage
        and pipeline ID.

        This method creates a unique but deterministic name for a temporary table by:
        1. Creating a canonical representation of the stage
        2. Hashing the stage to create a short, unique suffix
        3. Combining the pipeline ID, stage type, and hash to form a base name
        4. Ensuring uniqueness by tracking name usage within the pipeline

        Args:
            stage (Dict[str, Any]): The pipeline stage dictionary used to generate
                                    the table name
            name_suffix (str, optional): An additional suffix to append to the
                                         table name. Defaults to "".

        Returns:
            str: A deterministic temporary table name unique to this stage and
                 pipeline
        """
        # Create a canonical representation of the stage
        stage_key = str(sorted(stage.items()))
        # Hash the stage to create a short, unique suffix
        hash_suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:6]
        # Get the stage type (e.g., "match", "unwind")
        stage_type = next(iter(stage.keys())).lstrip("$")

        # Create a base name
        base_name = (
            f"temp_{self.pipeline_id}_{stage_type}_{hash_suffix}{name_suffix}"
        )

        # Ensure uniqueness by tracking usage
        if base_name in self.name_counter:
            self.name_counter[base_name] += 1
            unique_name = f"{base_name}_{self.name_counter[base_name]}"
        else:
            self.name_counter[base_name] = 0
            unique_name = base_name

        return unique_name


@contextmanager
def aggregation_pipeline_context(db_connection, pipeline_id: str | None = None):
    """
    Context manager for temporary aggregation tables with automatic cleanup.

    This context manager provides a clean and safe way to work with temporary
    tables during aggregation pipeline processing. It handles:

    1. Creating a savepoint for atomicity of the entire pipeline
    2. Generating deterministic temporary table names
    3. Providing a function to create temporary tables with proper naming
    4. Automatic cleanup of all temporary tables and savepoint on exit

    The context manager supports both new deterministic naming (using stage dictionaries)
    and backward compatibility (using string suffixes) for temporary tables.

    Args:
        db_connection: The database connection object
        pipeline_id (str | None): A unique identifier for the pipeline. If None,
                                  a default ID is generated for backward compatibility.

    Yields:
        Callable: A function to create temporary tables with the signature:
                  create_temp_table(stage_or_suffix, query, params=None, name_suffix="")

                  Where:
                  - stage_or_suffix: Either a stage dict (new approach) or string
                                     (backward compatibility)
                  - query: The SQL query to populate the temporary table
                  - params: Optional parameters for the SQL query
                  - name_suffix: Optional suffix for backward compatibility naming

    Raises:
        Exception: Any exception that occurs during pipeline processing is re-raised
                   after cleanup operations
    """
    temp_tables = []

    # Generate a default pipeline ID if none provided (for backward compatibility)
    if pipeline_id is None:
        pipeline_id = f"default_{uuid.uuid4().hex[:8]}"

    savepoint_name = f"agg_pipeline_{pipeline_id}"

    # Create savepoint for atomicity
    db_connection.execute(f"SAVEPOINT {savepoint_name}")

    # Create a deterministic temp table manager
    temp_manager = DeterministicTempTableManager(pipeline_id)

    def create_temp_table(
        stage_or_suffix: Any,  # Can be Dict[str, Any] for new usage or str for backward compatibility
        query: str,
        params: List[Any] | None = None,
        name_suffix: str = "",  # Used only for backward compatibility
    ) -> str:
        """
        Create a temporary table for pipeline processing with deterministic naming.

        This function supports both the new deterministic naming approach (using
        stage dictionaries) and the old backward-compatible approach (using string
        suffixes) for temporary table names.

        The function creates a temporary table by executing a CREATE TEMP TABLE
        AS SELECT statement with the provided query and optional parameters. The
        table name is generated deterministically based on the pipeline stage or
        provided suffix, ensuring uniqueness within the pipeline context.

        Args:
            stage_or_suffix (Any): Either a stage dictionary (new approach) for
                                   deterministic naming or a string suffix (backward
                                   compatibility). When using the new approach,
                                   this should be the pipeline stage dictionary
                                   that determines the table name. When using the
                                   old approach, this should be a string suffix
                                   for the table name.
            query (str): The SQL query used to populate the temporary table
            params (List[Any] | None, optional): Parameters for the SQL query.
                                                 Defaults to None.
            name_suffix (str, optional): Additional suffix for table name (used
                                         only in backward compatibility mode).
                                         Defaults to "".

        Returns:
            str: The name of the created temporary table

        Raises:
            Exception: Any database execution errors are propagated to the caller
        """
        # Check if we're using the new approach (stage is a dict) or old approach (stage is a string)
        if isinstance(stage_or_suffix, dict):
            # New approach - deterministic naming
            table_name = temp_manager.make_temp_table_name(
                stage_or_suffix, name_suffix
            )
        else:
            # Old approach - backward compatibility
            if isinstance(stage_or_suffix, str):
                suffix = stage_or_suffix
            else:
                suffix = "unknown"

            table_name = f"temp_{suffix}_{uuid.uuid4().hex}"

        if params is not None:
            db_connection.execute(
                f"CREATE TEMP TABLE {table_name} AS {query}", params
            )
        else:
            db_connection.execute(f"CREATE TEMP TABLE {table_name} AS {query}")
        temp_tables.append(table_name)
        return table_name

    try:
        yield create_temp_table
    except Exception as e:
        # Rollback on error
        db_connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        logger.error(f"Temporary table aggregation error: {e}", exc_info=True)
        raise
    finally:
        # Cleanup
        db_connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Explicitly drop temp tables
        for table_name in temp_tables:
            try:
                db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as drop_error:
                logger.debug(
                    f"Failed to drop temp table '{table_name}': {drop_error}"
                )
                pass


class TemporaryTableAggregationProcessor:
    """Processor for aggregation pipelines using temporary tables."""

    def __init__(self, collection, query_engine=None):
        """
        Initialize the TemporaryTableAggregationProcessor with a collection.

        Args:
            collection: The NeoSQLite collection to process aggregation pipelines
                        on. This collection provides the database connection and
                        document loading functionality needed for pipeline processing.
            query_engine: Optional QueryEngine instance for accessing helpers.
                          If not provided, text search in match stages will use
                          simplified processing.
        """
        self.collection = collection
        self.db = collection.db
        self.query_engine = query_engine
        self.sql_translator = SQLTranslator(collection.name, "data", "id")
        # Create ExprEvaluator for expression key support in $group
        self.expr_evaluator = ExprEvaluator(
            data_column="data", db_connection=collection.db
        )
        # Check if JSONB is supported for this connection
        self._jsonb_supported = supports_jsonb(self.db)
        self._jsonb_each_supported = supports_jsonb_each(self.db)
        # Set appropriate JSON function prefixes and names based on support
        self._json_function_prefix = _get_json_function_prefix(
            self._jsonb_supported
        )
        self._json_each_function = _get_json_each_function(
            self._jsonb_supported, self._jsonb_each_supported
        )
        self.json_group_array_function = _get_json_group_array_function(
            self._jsonb_supported
        )
        # Track if pipeline has $sort stage (for $first/$last limitation)
        self._has_sort_stage = False
        # Track if we've warned about $text on temp tables (FTS after $unwind)
        self._text_on_temp_table_warned = False
        # Track if $unwind has been processed in the current pipeline
        self._has_unwind_in_pipeline = False

    def process_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        is_count: bool = False,
        count_field: str | None = None,
        batch_size: int = 101,
    ) -> List[Dict[str, Any]]:
        """
        Process an aggregation pipeline using temporary tables for intermediate results.

        This method implements a temporary table approach for processing complex
        aggregation pipelines that cannot be optimized into a single SQL query by
        the current NeoSQLite implementation. It works by:

        1. Generating a deterministic pipeline ID based on the pipeline content
        2. Using the aggregation_pipeline_context for atomicity and cleanup
        3. Creating temporary tables for each stage or group of compatible stages
        4. Processing pipeline stages in an optimized order (grouping compatible stages)
        5. Returning the final results from the last temporary table

        The method supports these pipeline stages:
        - $match: For filtering documents
        - $unwind: For deconstructing array fields
        - $lookup: For joining documents from different collections
        - $sort, $skip, $limit: For sorting and pagination
        - $addFields: For adding fields to documents
        - $count: For counting documents (optimized to use SQL COUNT)

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages
                                             to process

        Returns:
            List[Dict[str, Any]]: A list of result documents after processing the
                                  pipeline

        Raises:
            NotImplementedError: If the pipeline contains unsupported stages
        """
        # Reset sort stage tracking for this pipeline
        self._has_sort_stage = False
        self._has_unwind_in_pipeline = False
        self._text_on_temp_table_warned = False

        # Check if pipeline ends with $count for optimization
        if (
            pipeline
            and isinstance(pipeline[-1], dict)
            and "$count" in pipeline[-1]
        ):
            count_field = pipeline[-1]["$count"]
            # Process pipeline without the $count stage
            intermediate_pipeline = pipeline[:-1]
            return self.process_pipeline(
                intermediate_pipeline, is_count=True, count_field=count_field
            )

        # Generate a deterministic pipeline ID based on the pipeline content
        pipeline_key = "".join(str(sorted(stage.items())) for stage in pipeline)
        pipeline_id = hashlib.sha256(pipeline_key.encode()).hexdigest()[:8]

        with aggregation_pipeline_context(self.db, pipeline_id) as create_temp:
            # Start with base data - include both id and _id for proper sorting support
            base_stage = {"_base": True}
            current_table = create_temp(
                base_stage,
                f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)}",
            )

            # Process pipeline stages in groups that can be handled together
            i = 0
            while i < len(pipeline):
                stage = pipeline[i]
                stage_name = next(iter(stage.keys()))

                # Handle groups of compatible stages using match-case for better readability
                match stage_name:
                    case "$match":
                        current_table = self._process_match_stage(
                            create_temp, current_table, stage["$match"]
                        )
                        i += 1

                    case "$unwind":
                        # Process consecutive $unwind stages
                        unwind_stages = []
                        j = i
                        while j < len(pipeline) and "$unwind" in pipeline[j]:
                            unwind_stages.append(pipeline[j]["$unwind"])
                            j += 1

                        current_table = self._process_unwind_stages(
                            create_temp, current_table, unwind_stages
                        )
                        self._has_unwind_in_pipeline = True
                        i = j  # Skip processed stages

                    case "$lookup":
                        current_table = self._process_lookup_stage(
                            create_temp, current_table, stage["$lookup"]
                        )
                        i += 1

                    case "$sort" | "$skip" | "$limit":
                        # Process consecutive sort/skip/limit stages
                        sort_spec = None
                        skip_value = 0
                        limit_value = None
                        j = i

                        # Process consecutive sort/skip/limit stages
                        while j < len(pipeline):
                            next_stage = pipeline[j]
                            next_stage_name = next(iter(next_stage.keys()))

                            match next_stage_name:
                                case "$sort":
                                    sort_spec = next_stage["$sort"]
                                case "$skip":
                                    skip_value = next_stage["$skip"]
                                case "$limit":
                                    limit_value = next_stage["$limit"]
                                case _:
                                    break
                            j += 1

                        current_table = self._process_sort_skip_limit_stage(
                            create_temp,
                            current_table,
                            sort_spec,
                            skip_value,
                            limit_value,
                        )
                        i = j  # Skip processed stages

                        # Track that we've seen a $sort stage (needed for $first/$last limitation)
                        if sort_spec is not None:
                            self._has_sort_stage = True

                    case "$addFields":
                        current_table = self._process_add_fields_stage(
                            create_temp, current_table, stage["$addFields"]
                        )
                        i += 1

                    case "$project":
                        current_table = self._process_project_stage(
                            create_temp, current_table, stage["$project"]
                        )
                        i += 1

                    case "$replaceRoot" | "$replaceWith":
                        current_table = self._process_replace_root_stage(
                            create_temp, current_table, stage[stage_name]
                        )
                        i += 1

                    case "$group":
                        current_table = self._process_group_stage(
                            create_temp, current_table, stage["$group"]
                        )
                        i += 1

                    case "$setWindowFields":
                        current_table = self._process_set_window_fields_stage(
                            create_temp,
                            current_table,
                            stage["$setWindowFields"],
                        )
                        i += 1

                    case "$graphLookup":
                        current_table = self._process_graph_lookup_stage(
                            create_temp,
                            current_table,
                            stage["$graphLookup"],
                        )
                        i += 1

                    case "$fill":
                        current_table = self._process_fill_stage(
                            create_temp,
                            current_table,
                            stage["$fill"],
                        )
                        i += 1

                    case "$sample":
                        sample_spec = stage["$sample"]
                        sample_size = sample_spec["size"]
                        sample_stage = {"$sample": sample_spec}
                        new_table = create_temp(
                            sample_stage,
                            f"SELECT * FROM {current_table} ORDER BY RANDOM() LIMIT {sample_size}",
                        )
                        current_table = new_table
                        i += 1

                    case "$unset":
                        unset_spec = stage["$unset"]
                        if isinstance(unset_spec, str):
                            unset_fields = [unset_spec]
                        else:
                            unset_fields = unset_spec
                        # Build json_remove expressions
                        data_expr = "data"
                        for field in unset_fields:
                            json_path = parse_json_path(field)
                            if self._jsonb_supported:
                                data_expr = (
                                    f"jsonb_remove({data_expr}, '{json_path}')"
                                )
                            else:
                                data_expr = (
                                    f"json_remove({data_expr}, '{json_path}')"
                                )
                        unset_stage = {"$unset": unset_spec}
                        new_table = create_temp(
                            unset_stage,
                            f"SELECT id, _id, {data_expr} as data FROM {current_table}",
                        )
                        current_table = new_table
                        i += 1

                    case "$bucket":
                        current_table = self._process_bucket_stage(
                            create_temp, current_table, stage["$bucket"]
                        )
                        i += 1

                    case "$bucketAuto":
                        current_table = self._process_bucket_auto_stage(
                            create_temp, current_table, stage["$bucketAuto"]
                        )
                        i += 1

                    case "$unionWith":
                        current_table = self._process_union_with_stage(
                            create_temp, current_table, stage["$unionWith"]
                        )
                        i += 1

                    case "$merge":
                        # $merge writes to a collection and can continue the pipeline
                        # For now, we'll process it and continue
                        current_table = self._process_merge_stage(
                            create_temp, current_table, stage["$merge"]
                        )
                        i += 1

                    case "$redact":
                        current_table = self._process_redact_stage(
                            create_temp, current_table, stage["$redact"]
                        )
                        i += 1

                    case "$densify":
                        current_table = self._process_densify_stage(
                            create_temp, current_table, stage["$densify"]
                        )
                        i += 1

                    case "$merge":
                        # $merge requires Python fallback for full functionality
                        raise NotImplementedError(
                            "$merge requires Python fallback - use force_fallback or simplify pipeline"
                        )

                    case "$redact":
                        # $redact requires Python fallback for full functionality
                        raise NotImplementedError(
                            "$redact requires Python fallback - use force_fallback or simplify pipeline"
                        )

                    case _:
                        # For unsupported stages, we would need to fall back to Python
                        # But for this demonstration, we'll raise an exception
                        raise NotImplementedError(
                            f"Stage '{stage_name}' not yet supported in temporary table approach"
                        )

            # Return final results
            return self._get_results_from_table(
                current_table, is_count, count_field, batch_size
            )

    def _process_match_stage(
        self,
        create_temp: Callable,
        current_table: str,
        match_spec: Dict[str, Any],
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
            match_spec (Dict[str, Any]): The $match stage specification

        Returns:
            str: Name of the newly created temporary table with matched documents
        """
        # Check if text search is involved
        if _contains_text_search(match_spec):
            return self._process_text_search_stage(
                create_temp, current_table, match_spec
            )

        # Try to use SQLTranslator to build WHERE clause
        # If it returns (None, []), it means text search is involved and we should fall back
        where_clause, params = self.sql_translator.translate_match(match_spec)

        # Check if text search is involved (SQLTranslator returns None for text search)
        if where_clause is None:
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

        # Create filtered temporary table for regular match operations
        match_stage = {"$match": match_spec}
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        # We must explicitly select columns and inject _id into JSON data
        # to ensure it's available for subsequent stages (like $lookup or $graphLookup)
        sql = (
            f"SELECT id, _id, "
            f"json({json_set_func}(data, '$._id', _id)) AS data "
            f"FROM {current_table} {where_clause}"
        )

        new_table = create_temp(match_stage, sql, params)
        return new_table

    def _process_unwind_stages(
        self, create_temp: Callable, current_table: str, unwind_specs: List[Any]
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
            unwind_specs (List[Any]): List of $unwind stage specifications to
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
            unwind_stage: Dict[str, Any] = {"$unwind": field_path}
            if preserve_null:
                unwind_stage["preserveNullAndEmptyArrays"] = True
            if include_index:
                unwind_stage["includeArrayIndex"] = include_index

            current_temp_table = create_temp(unwind_stage, unwind_query)

        return current_temp_table

    def _create_lookup_hash_table(
        self,
        from_collection: str,
        foreign_field: str | None,
        pipeline: List[Dict[str, Any]] | None = None,
    ) -> Tuple[str, str]:
        """
        Create a hash table (temp table with index) from a foreign collection
        for efficient hash join lookup.

        This implements O(n+m) hash join instead of O(n*m) correlated subquery.

        Args:
            from_collection: The collection to build hash table from
            foreign_field: The field to use as join key (None for _id)
            pipeline: Optional pipeline to run on foreign collection first

        Returns:
            Tuple of (hash_table_name, join_key_column)
        """
        if foreign_field is None:
            foreign_field = "_id"
        stage_key = f"{from_collection}:{foreign_field}:{str(pipeline) if pipeline else ''}"
        hash_suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:8]
        hash_table_name = f"_lookup_hash_{hash_suffix}"
        join_key = "_join_key"

        try:
            if pipeline:
                target_coll = self.collection.database.get_collection(
                    from_collection
                )
                processor = TemporaryTableAggregationProcessor(
                    target_coll, None
                )
                pipeline_result = processor.process_pipeline(pipeline)

                if not pipeline_result:
                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT, {join_key} TEXT)"
                    )
                else:
                    from .json_helpers import neosqlite_json_dumps

                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT, {join_key} TEXT)"
                    )

                    if foreign_field == "_id":
                        for doc in pipeline_result:
                            self.db.execute(
                                f"INSERT INTO {hash_table_name} (id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                                (
                                    doc.get("id", 0),
                                    doc.get("_id"),
                                    neosqlite_json_dumps(doc),
                                    str(doc.get("_id")),
                                ),
                            )
                    else:
                        for doc in pipeline_result:
                            key_val = self._extract_field_value(
                                doc, foreign_field
                            )
                            self.db.execute(
                                f"INSERT INTO {hash_table_name} (id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                                (
                                    doc.get("id", 0),
                                    doc.get("_id"),
                                    neosqlite_json_dumps(doc),
                                    (
                                        str(key_val)
                                        if key_val is not None
                                        else None
                                    ),
                                ),
                            )
            else:
                if foreign_field == "_id":
                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} AS "
                        f"SELECT id, _id, data, CAST(_id AS TEXT) as {join_key} "
                        f"FROM {quote_table_name(from_collection)}"
                    )
                else:
                    json_extract = f"{self._json_function_prefix}_extract"
                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} AS "
                        f"SELECT id, _id, data, CAST({json_extract}(data, '$.{foreign_field}') AS TEXT) as {join_key} "
                        f"FROM {quote_table_name(from_collection)}"
                    )

            self.db.execute(
                f"CREATE INDEX {hash_table_name}_idx ON {hash_table_name}({join_key})"
            )

            return hash_table_name, join_key

        except Exception as e:
            logger.debug(
                f"Failed to create hash table '{hash_table_name}': {e}"
            )
            self.db.execute(f"DROP TABLE IF EXISTS {hash_table_name}")
            raise

    def _estimate_collection_size(self, collection_name: str) -> int:
        """
        Estimate the size of a collection in bytes.

        Uses SQLite's table statistics to estimate size.

        Args:
            collection_name: Name of the collection to estimate

        Returns:
            Estimated size in bytes
        """
        try:
            result = self.db.execute(
                f"SELECT COUNT(*), AVG(LENGTH(data)) FROM {quote_table_name(collection_name)}"
            ).fetchone()
            if result and result[0]:
                count, avg_size = result
                avg_size = avg_size or 0
                row_size = (
                    int(avg_size) + 50
                )  # Add overhead for id, _id columns
                return count * row_size
        except Exception as e:
            logger.debug(
                f"Failed to estimate collection size for '{collection_name}': {e}"
            )
            pass
        return 0

    def _get_available_memory(self) -> int:
        """
        Get available memory for hash join operations.

        Returns:
            Available memory in bytes (estimated from SQLite cache or system)
        """
        try:
            page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
            cache_pages = self.db.execute("PRAGMA cache_size").fetchone()[0]
            if cache_pages < 0:
                cache_pages = -cache_pages
            sqlite_memory = page_size * cache_pages
            return int(sqlite_memory * 0.5)
        except Exception as e:
            logger.debug(f"Failed to get SQLite memory info: {e}")
            pass
        try:
            import resource

            soft, hard = resource.getrlimit(resource.RLIMIT_AS)
            if soft != resource.RLIM_INFINITY:
                return int(soft * 0.3)
        except Exception as e:
            logger.debug(f"Failed to get system memory info: {e}")
            pass
        return HASH_JOIN_MEMORY_THRESHOLD

    def _should_use_hash_join(
        self,
        from_collection: str,
        pipeline: List[Dict[str, Any]] | None = None,
    ) -> bool:
        """
        Decide whether to use hash join or correlated subquery for $lookup.

        Uses memory estimation to decide:
        - If foreign collection estimate < 30% of available memory: use hash join (faster)
        - Otherwise: use correlated subquery (lower memory, slower)

        Args:
            from_collection: The foreign collection name
            pipeline: Optional pipeline to run on foreign collection first

        Returns:
            True if should use hash join, False for correlated subquery
        """
        if pipeline:
            return True
        try:
            est_size = self._estimate_collection_size(from_collection)
            available = self._get_available_memory()
            return est_size < (available * 0.3)
        except Exception as e:
            logger.debug(f"Failed during _should_use_hash_join check: {e}")
            return True

    def _extract_field_value(self, doc: Dict[str, Any], field: str) -> Any:
        """Extract field value from document, supporting dot notation."""
        parts = field.split(".")
        val: Any = doc
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return None
        return val

    def _process_lookup_stage(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: Dict[str, Any],
    ) -> str:
        """
        Process a $lookup stage using hash join for O(n+m) complexity.

        This method implements the $lookup aggregation stage which performs a left
        outer join to another collection in the same database. It uses an optimized
        hash join approach:
        1. Creates a temporary table with an index on the foreign field (hash table)
        2. Uses a single JOIN query instead of correlated subquery

        This replaces the previous correlated subquery approach which was O(n*m).

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            lookup_spec (Dict[str, Any]): The $lookup stage specification containing:
                - "from": The name of the collection to join with
                - "localField": The field from the input documents
                - "foreignField": The field from the documents of the "from" collection
                - "as": The name of the new array field to add to the matching documents
                - "pipeline": Optional pipeline to run on foreign collection

        Returns:
            str: Name of the newly created temporary table with lookup results added
        """
        from_collection = lookup_spec["from"]
        pipeline = lookup_spec.get("pipeline", [])

        use_hash_join = self._should_use_hash_join(from_collection, pipeline)

        if use_hash_join:
            return self._process_lookup_hash_join(
                create_temp, current_table, lookup_spec
            )
        else:
            return self._process_lookup_correlated_subquery(
                create_temp, current_table, lookup_spec
            )

    def _process_lookup_correlated_subquery(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: Dict[str, Any],
    ) -> str:
        """
        Process $lookup using correlated subquery (O(n*m) but low memory).

        This is the fallback when the foreign collection is too large for hash join.

        Args:
            create_temp: Function to create temporary tables
            current_table: Current temp table name
            lookup_spec: The $lookup specification

        Returns:
            Name of the new temporary table
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec.get("localField")
        foreign_field = lookup_spec.get("foreignField")
        as_field = lookup_spec["as"]
        pipeline = lookup_spec.get("pipeline", [])

        json_set_func = f"{self._json_function_prefix}_set"

        if pipeline:
            if not local_field or not foreign_field:
                raise NotImplementedError(
                    "$lookup with pipeline requires localField and foreignField"
                )

            from .temporary_table_aggregation import (
                TemporaryTableAggregationProcessor,
            )

            target_coll = self.collection.database.get_collection(
                from_collection
            )
            processor = TemporaryTableAggregationProcessor(target_coll, None)

            pipeline_key = f"{from_collection}:{str(pipeline)}"
            pipeline_hash = hashlib.sha256(pipeline_key.encode()).hexdigest()[
                :8
            ]
            pipeline_result_table = f"_lookup_pipeline_{pipeline_hash}"
            try:
                pipeline_result = processor.process_pipeline(pipeline)
                if not pipeline_result:
                    self.collection.db.execute(
                        f"CREATE TEMP TABLE {pipeline_result_table} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT)"
                    )
                else:
                    from .json_helpers import neosqlite_json_dumps

                    self.collection.db.execute(
                        f"CREATE TEMP TABLE {pipeline_result_table} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT)"
                    )
                    for doc in pipeline_result:
                        self.collection.db.execute(
                            f"INSERT INTO {pipeline_result_table} (id, _id, data) VALUES (?, ?, ?)",
                            (
                                doc.get("id", 0),
                                doc.get("_id"),
                                neosqlite_json_dumps(doc),
                            ),
                        )

                if foreign_field == "_id":
                    foreign_extract = "related._id"
                else:
                    foreign_extract = f"{self._json_function_prefix}_extract(related.data, '{parse_json_path(foreign_field)}')"

                if local_field == "_id":
                    local_extract = f"COALESCE({self._json_function_prefix}_extract(main_table.data, '$._id'), main_table.id)"
                else:
                    local_extract = f"{self._json_function_prefix}_extract(main_table.data, '{parse_json_path(local_field)}')"

                select_clause = (
                    f"SELECT main_table.id, "
                    f"json({json_set_func}({json_set_func}(main_table.data, '$._id', main_table.id), '{parse_json_path(as_field)}', "
                    f"coalesCE(( "
                    f"  SELECT {self.json_group_array_function}(json(related.data)) "
                    f"  FROM {pipeline_result_table} as related "
                    f"  WHERE {foreign_extract} = "
                    f"        {local_extract} "
                    f"), '[]'))) as data"
                )

                from_clause = f"FROM {current_table} as main_table"

                lookup_stage = {"$lookup": lookup_spec}
                new_table = create_temp(
                    lookup_stage, f"{select_clause} {from_clause}"
                )
                return new_table
            finally:
                try:
                    self.collection.db.execute(
                        f"DROP TABLE IF EXISTS {pipeline_result_table}"
                    )
                except Exception as e:
                    logger.debug(
                        f"Failed to drop pipeline result table '{pipeline_result_table}': {e}"
                    )
                    pass

        if not all([from_collection, local_field, foreign_field, as_field]):
            raise ValueError(
                "$lookup requires from, localField, foreignField, and as"
            )

        local_field_str: str = local_field  # type: ignore[assignment]
        foreign_field_str: str = foreign_field  # type: ignore[assignment]

        if foreign_field_str == "_id":
            foreign_extract = "related._id"
        else:
            foreign_extract = f"{self._json_function_prefix}_extract(related.data, '{parse_json_path(foreign_field_str)}')"

        if local_field_str == "_id":
            local_extract = f"COALESCE({self._json_function_prefix}_extract(main_table.data, '$._id'), main_table.id)"
        else:
            local_extract = f"{self._json_function_prefix}_extract(main_table.data, '{parse_json_path(local_field_str)}')"

        select_clause = (
            f"SELECT main_table.id, "
            f"json({json_set_func}({json_set_func}(main_table.data, '$._id', main_table.id), '{parse_json_path(as_field)}', "
            f"coalesCE(( "
            f"  SELECT {self.json_group_array_function}(json(related.data)) "
            f"  FROM {from_collection} as related "
            f"  WHERE {foreign_extract} = "
            f"        {local_extract} "
            f"), '[]'))) as data"
        )

        from_clause = f"FROM {current_table} as main_table"

        lookup_stage = {"$lookup": lookup_spec}
        new_table = create_temp(lookup_stage, f"{select_clause} {from_clause}")
        return new_table

    def _process_lookup_hash_join(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: Dict[str, Any],
    ) -> str:
        """
        Process $lookup using hash join (O(n+m) but uses more memory).

        Args:
            create_temp: Function to create temporary tables
            current_table: Current temp table name
            lookup_spec: The $lookup specification

        Returns:
            Name of the new temporary table
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec.get("localField")
        foreign_field = lookup_spec.get("foreignField")
        as_field = lookup_spec["as"]
        pipeline = lookup_spec.get("pipeline", [])

        json_set_func = f"{self._json_function_prefix}_set"

        if pipeline:
            if not local_field or not foreign_field:
                raise NotImplementedError(
                    "$lookup with pipeline requires localField and foreignField"
                )

            hash_table_name, join_key = self._create_lookup_hash_table(
                from_collection, foreign_field, pipeline
            )
        else:
            if not all([from_collection, local_field, foreign_field, as_field]):
                raise ValueError(
                    "$lookup requires from, localField, foreignField, and as"
                )
            hash_table_name, join_key = self._create_lookup_hash_table(
                from_collection, foreign_field, None
            )

        try:
            if local_field == "_id":
                local_extract = f"CAST(COALESCE({self._json_function_prefix}_extract(main_table.data, '$._id'), main_table.id) AS TEXT)"
            else:
                local_extract = f"CAST({self._json_function_prefix}_extract(main_table.data, '$.{local_field}') AS TEXT)"

            select_clause = (
                f"SELECT main_table.id, "
                f"json({json_set_func}({json_set_func}(main_table.data, '$._id', main_table.id), '$.{as_field}', "
                f"COALESCE(aggregated.results, '[]'))) as data "
            )

            from_clause = (
                f"FROM {current_table} as main_table "
                f"LEFT JOIN ("
                f"  SELECT {join_key}, {self.json_group_array_function}(json(data)) as results "
                f"  FROM {hash_table_name} "
                f"  GROUP BY {join_key}"
                f") aggregated ON {local_extract} = aggregated.{join_key}"
            )

            lookup_stage = {"$lookup": lookup_spec}
            new_table = create_temp(
                lookup_stage, f"{select_clause} {from_clause}"
            )
            return new_table
        finally:
            try:
                self.collection.db.execute(
                    f"DROP TABLE IF EXISTS {hash_table_name}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop hash table '{hash_table_name}': {e}"
                )
                pass

    def _process_sort_skip_limit_stage(
        self,
        create_temp: Callable,
        current_table: str,
        sort_spec: Dict[str, Any] | None,
        skip_value: int = 0,
        limit_value: int | None = None,
    ) -> str:
        """
        Process sort/skip/limit stages using temporary tables.

        This method handles the $sort, $skip, and $limit aggregation stages, which
        can be used individually or in combination. It creates a temporary table
        with the results sorted and/or paginated according to the specifications.

        The method supports sorting on both regular fields (using json_extract)
        and the special _id field (using the id column directly). It handles
        ascending and descending sort orders, as well as skip and limit operations
        with proper OFFSET and LIMIT clauses in the SQL query.

        When multiple sort/skip/limit stages are consecutive in a pipeline, they
        are processed together in a single operation for efficiency.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            sort_spec (Dict[str, Any] | None): The $sort stage specification, mapping
                                              field names to sort directions (1
                                              for ascending, -1 for descending)
            skip_value (int): The number of documents to skip (from $skip stage)
            limit_value (int | None): The maximum number of documents to return
                                      (from $limit stage)

        Returns:
            str: Name of the newly created temporary table with sorted/skipped/limited results
        """
        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

        # Use SQLTranslator to build ORDER BY clause
        order_clause = ""
        if sort_spec:
            order_clause = self.sql_translator.translate_sort(sort_spec)
            # If sorting by _id but table doesn't have _id column, use id instead
            if "_id" in sort_spec and not has_underscore_id and has_id:
                order_clause = order_clause.replace(
                    "ORDER BY _id", "ORDER BY id"
                )

        # Use SQLTranslator to build LIMIT/OFFSET clause
        limit_clause = self.sql_translator.translate_skip_limit(
            limit_value, skip_value
        )

        # Create a stage spec for naming (use the first non-null stage type)
        stage_spec: Dict[str, Any] = {}
        if sort_spec:
            stage_spec["$sort"] = sort_spec
        elif skip_value > 0:
            stage_spec["$skip"] = skip_value
        elif limit_value is not None:
            stage_spec["$limit"] = limit_value
        else:
            # Default case if all are None/default values
            stage_spec["$sort"] = {}

        # Build SELECT clause based on available columns
        # Always preserve id and _id columns if they exist
        if has_id and has_underscore_id:
            select_clause = f"SELECT id, _id, data FROM {current_table}"
        elif has_id:
            select_clause = f"SELECT id, data FROM {current_table}"
        elif has_underscore_id:
            select_clause = f"SELECT _id, data FROM {current_table}"
        else:
            select_clause = f"SELECT data FROM {current_table}"

        # Create sorted/skipped/limited temporary table
        new_table = create_temp(
            stage_spec,
            f"{select_clause} {order_clause} {limit_clause}",
        )
        return new_table

    def _process_add_fields_stage(
        self,
        create_temp: Callable,
        current_table: str,
        add_fields_spec: Dict[str, Any],
    ) -> str:
        """
        Process an $addFields stage using temporary tables.

        This method implements the $addFields aggregation stage which adds new fields
        to documents. It uses SQLite's json_set function to add fields to the JSON data.

        Supports:
        - Simple field copying: {"newField": "$existingField"}
        - $replaceOne: {$replaceOne: {input: "$text", find: "old", replacement: "new"}}
        - Literal values: {"field": "value"}

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            add_fields_spec (Dict[str, Any]): The $addFields stage specification mapping
                                              new field names to source field paths

        Returns:
            str: Name of the newly created temporary table with added fields
        """
        # Build json_set expressions for each field to add
        # We'll construct a nested json_set call for each field
        data_expr = "data"  # Start with the original data
        params: List[Any] = []

        # Process each field to add
        for new_field, source_field in add_fields_spec.items():
            # Handle $replaceOne operator
            if isinstance(source_field, dict) and "$replaceOne" in source_field:
                replace_spec = source_field["$replaceOne"]
                if isinstance(replace_spec, dict):
                    input_expr = replace_spec.get("input", "")
                    find_str = replace_spec.get("find", "")
                    replacement_str = replace_spec.get("replacement", "")

                    # Escape single quotes for SQL
                    find_str_escaped = find_str.replace("'", "''")
                    replacement_str_escaped = replacement_str.replace("'", "''")

                    # Build SQL for $replaceOne using instr() and substr()
                    json_extract = f"{self._json_function_prefix}_extract"
                    json_set_func = f"{self._json_function_prefix}_set"
                    if isinstance(input_expr, str) and input_expr.startswith(
                        "$"
                    ):
                        input_field = input_expr[1:]
                        # SQL: substr(data, 1, instr-1) || replacement || substr(data, instr+len(find))
                        data_expr = (
                            f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', "
                            f"CASE "
                            f"WHEN instr({json_extract}(data, '{parse_json_path(input_field)}'), '{find_str_escaped}') > 0 THEN "
                            f"substr({json_extract}(data, '{parse_json_path(input_field)}'), 1, "
                            f"instr({json_extract}(data, '{parse_json_path(input_field)}'), '{find_str_escaped}') - 1) || "
                            f"'{replacement_str_escaped}' || "
                            f"substr({json_extract}(data, '{parse_json_path(input_field)}'), "
                            f"instr({json_extract}(data, '{parse_json_path(input_field)}'), '{find_str_escaped}') + length('{find_str_escaped}')) "
                            f"ELSE {json_extract}(data, '{parse_json_path(input_field)}') END)"
                        )
                    else:
                        # For non-field input, fall back to Python
                        raise NotImplementedError(
                            "$replaceOne with non-field input requires Python fallback"
                        )

            # Handle simple field copying (e.g., {"newField": "$existingField"})
            elif isinstance(source_field, str) and source_field.startswith("$"):
                source_field_name = source_field[1:]  # Remove leading $
                json_set_func = f"{self._json_function_prefix}_set"
                if source_field_name == "_id":
                    # Special handling for _id field
                    data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', id)"
                else:
                    # Use json_extract/jsonb_extract to get the source field value
                    json_extract = f"{self._json_function_prefix}_extract"
                    data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', {json_extract}(data, '{parse_json_path(source_field_name)}'))"

            # Handle literal values
            elif not isinstance(source_field, dict):
                # For literal values, use json_set with parameterized value
                json_set_func = f"{self._json_function_prefix}_set"
                data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', json(?))"
                params.append(source_field)
            # For other complex expressions, fall back to Python
            # (This is handled by raising NotImplementedError)

        # Create addFields temporary table
        add_fields_stage = {"$addFields": add_fields_spec}

        # When using JSONB, we need to convert final output to text JSON for Python
        jsonb = self._jsonb_supported
        new_table = create_temp(
            add_fields_stage,
            f"SELECT id, _id, {json_data_column(jsonb, data_expr)} as data FROM {current_table}",
            params if params else None,
        )
        return new_table

    def _process_project_stage(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: Dict[str, Any],
    ) -> str:
        """
        Process a $project stage using temporary tables.

        This method implements the $project aggregation stage which reshapes
        each document in the stream, by adding new fields, removing existing
        fields, or renaming fields. It reconstructs a unified ``data`` column
        using ``json_object`` / ``jsonb_object`` so that downstream stages
        (especially FTS5 text search via ``json_tree``) continue to work
        without modification.

        Supports:
        - Simple inclusion: ``{"field": 1}``
        - Exclusion: ``{"field": 0}``
        - Field references: ``{"alias": "$some.path"}``
        - Expression projections: ``{"alias": {$concat: [...]}}``
        - ``_id`` inclusion/exclusion

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table
            project_spec (Dict[str, Any]): The $project stage specification

        Returns:
            str: Name of the newly created temporary table
        """
        # Check kill switch FIRST — force Python fallback
        from .query_helper import get_force_fallback

        if get_force_fallback():
            raise NotImplementedError(
                "Force fallback - use Tier 3 Python evaluation"
            )

        include_id = project_spec.get("_id", 1) == 1

        # Determine mode: inclusion vs exclusion
        # Inclusion mode if any value == 1 or is an expression/field ref
        # Exclusion mode if all non-_id values are 0
        non_id_values = {k: v for k, v in project_spec.items() if k != "_id"}
        is_exclusion_mode = all(v == 0 for v in non_id_values.values())

        if is_exclusion_mode:
            # Exclusion mode: use json_remove to strip fields
            return self._process_project_exclusion(
                create_temp, current_table, project_spec, include_id
            )
        else:
            # Inclusion mode: reconstruct data via json_object
            return self._process_project_inclusion(
                create_temp, current_table, project_spec, include_id
            )

    def _process_project_exclusion(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: Dict[str, Any],
        include_id: bool,
    ) -> str:
        """Handle exclusion-mode projection by removing fields via json_remove."""
        fields_to_remove = [
            k
            for k, v in project_spec.items()
            if v == 0 and k != "_id"  # _id is a separate column, not in data
        ]

        select_cols = ["id"]
        if include_id:
            select_cols.append("_id")

        if fields_to_remove:
            json_remove = f"{self._json_function_prefix}_remove"
            # SQLite's json_remove supports multiple paths in a single call:
            #   json_remove(data, p1, p2, ...)  -- more efficient than nesting
            path_args = ", ".join(
                f"'{parse_json_path(f)}'" for f in fields_to_remove
            )
            data_expr = f"{json_remove}(data, {path_args})"
        else:
            data_expr = "data"

        select_cols.append(
            f"{json_data_column(self._jsonb_supported, data_expr)} AS data"
        )

        sql = f"SELECT {', '.join(select_cols)} FROM {current_table}"
        project_stage = {"$project": project_spec}
        return create_temp(project_stage, sql)

    def _process_project_inclusion(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: Dict[str, Any],
        include_id_default: bool,
    ) -> str:
        """Handle inclusion-mode projection by reconstructing data via json_object.

        Handles:
        - Simple inclusion: ``{"field": 1}``
        - Field references: ``{"alias": "$some.path"}``
        - Expression projections: ``{"alias": {$concat: [...]}}``
        """
        jsonb = self._jsonb_supported
        json_obj_func = "jsonb_object" if jsonb else "json_object"
        json_extract_func = f"{self._json_function_prefix}_extract"

        # Determine if projection uses expressions or field references.
        # When it does, _id is only included if explicitly specified
        # (matches Python _apply_projection behavior).
        # For simple inclusion ({field: 1}), _id is included by default.
        has_expressions_or_refs = any(
            _is_expression(value)
            or (isinstance(value, str) and value.startswith("$"))
            for value in project_spec.values()
        )

        if has_expressions_or_refs:
            # Expression/field reference mode: _id only if explicitly listed
            include_id = "_id" in project_spec and project_spec["_id"] != 0
        else:
            # Simple inclusion mode: _id included by default
            include_id = include_id_default

        # Build key-value pairs for json_object
        json_parts = []
        all_params: List[Any] = []

        for field, value in project_spec.items():
            if field == "_id":
                continue

            if _is_expression(value):
                # Expression projection: use ExprEvaluator
                agg_ctx = AggregationContext()
                expr_sql, expr_params = (
                    self.expr_evaluator.build_select_expression(
                        value, context=agg_ctx
                    )
                )
                all_params.extend(expr_params)
                json_parts.append(f"'{field}'")
                json_parts.append(expr_sql)

            elif isinstance(value, str) and value.startswith("$"):
                # Field reference: "$some.path"
                source_field = value[1:]
                if source_field == "_id":
                    json_parts.append(f"'{field}'")
                    json_parts.append("_id")
                else:
                    json_parts.append(f"'{field}'")
                    json_parts.append(
                        f"{json_extract_func}(data, '{parse_json_path(source_field)}')"
                    )

            elif value == 1:
                # Simple inclusion: copy field from data
                json_parts.append(f"'{field}'")
                json_parts.append(
                    f"{json_extract_func}(data, '{parse_json_path(field)}')"
                )

            # value == 0 is exclusion — skip in inclusion mode

        # Build the reconstructed data column
        if json_parts:
            data_expr = f"{json_obj_func}({', '.join(json_parts)})"
        else:
            # No fields projected — empty object
            data_expr = f"{json_obj_func}()"

        select_cols = ["id"]
        if include_id:
            select_cols.append("_id")
        select_cols.append(f"{json_data_column(jsonb, data_expr)} AS data")

        sql = f"SELECT {', '.join(select_cols)} FROM {current_table}"
        project_stage = {"$project": project_spec}
        return create_temp(
            project_stage, sql, all_params if all_params else None
        )

    def _process_replace_root_stage(
        self,
        create_temp: Callable,
        current_table: str,
        replace_spec: Any,
    ) -> str:
        """
        Process a $replaceRoot or $replaceWith stage using temporary tables.

        This method implements the $replaceRoot/$replaceWith aggregation stage which
        replaces the root document with a specified field or expression.

        MongoDB syntax:
            {$replaceRoot: {newRoot: "$field"}}
            {$replaceWith: "$field"}

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            replace_spec (Any): The replace specification (field path or expression)

        Returns:
            str: Name of the newly created temporary table with replaced root documents
        """
        # Handle both $replaceRoot ({newRoot: ...}) and $replaceWith (direct value)
        if isinstance(replace_spec, dict) and "newRoot" in replace_spec:
            new_root_expr = replace_spec["newRoot"]
        else:
            new_root_expr = replace_spec

        # Handle field reference (e.g., "$field")
        if isinstance(new_root_expr, str) and new_root_expr.startswith("$"):
            field_name = new_root_expr[1:]  # Remove leading $

            # Create replaceRoot temporary table
            replace_stage = {"$replaceRoot": {"newRoot": new_root_expr}}
            json_extract = f"{self._json_function_prefix}_extract"

            # Extract the field and use it as the new root document
            new_table = create_temp(
                replace_stage,
                f"SELECT id, _id, {json_extract}(data, '{parse_json_path(field_name)}') as data FROM {current_table}",
            )
            return new_table
        else:
            # For complex expressions, fall back to Python evaluation
            # This handles cases like {$replaceRoot: {newRoot: {$mergeObjects: [...]}}}
            raise NotImplementedError(
                f"$replaceRoot with expression {new_root_expr} requires Python fallback"
            )

    def _process_group_stage(
        self,
        create_temp: Callable,
        current_table: str,
        group_spec: Dict[str, Any],
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
            group_spec (Dict[str, Any]): The $group stage specification

        Returns:
            str: Name of the newly created temporary table with grouped results
        """
        # Check kill switch FIRST (Bug 010 fix)
        from .query_helper import get_force_fallback

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
                            "$first/$last with preceding $sort requires Python fallback for correctness"
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
                        f"$group with expression key {group_id_expr} requires Python fallback"
                    )
            except NotImplementedError:
                raise
            except Exception as e:
                logger.debug(
                    f"$group with expression key {group_id_expr} requires Python fallback: {e}"
                )
                raise NotImplementedError(
                    f"$group with expression key {group_id_expr} requires Python fallback: {e}"
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
            else:
                # Complex expression - fall back to Python
                raise NotImplementedError(
                    f"$group accumulator {op} with expression {expr} requires Python fallback"
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
                    if expr_field:
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
                    if expr_field:
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
                        f"$group accumulator ${op} requires Python fallback"
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
                "$group with parameterized expression key requires Python fallback"
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

    def _id_to_json_object_args(self, select_parts: List[str]) -> str:
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
    ) -> List[Dict[str, Any]]:
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
            List[Dict[str, Any]]: List of documents retrieved from the temporary table,
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

    def _matches_text_search(
        self, document: Dict[str, Any], search_term: str
    ) -> bool:
        """
        Apply Python-based text search to a document.

        This method uses the unified_text_search function to determine if a document
        matches a given search term. It's used as a fallback when text search cannot
        be efficiently handled with SQL queries, particularly in cases involving
        unwound elements or complex text search operations.

        Args:
            document (Dict[str, Any]): The document to search in
            search_term (str): The term to search for

        Returns:
            bool: True if the document matches the text search, False otherwise
        """

        from neosqlite.collection.text_search import unified_text_search

        return unified_text_search(document, search_term)

    def _batch_insert_documents(
        self, table_name: str, documents: List[tuple]
    ) -> None:
        """
        Insert multiple documents into a temporary table efficiently.

        This method provides an optimized way to insert multiple documents into a
        temporary table by using a single INSERT statement with multiple value sets.
        It's used primarily in the text search processing where documents need to be
        filtered and inserted into a result table.

        Args:
            table_name (str): The name of the table to insert into
            documents (List[tuple]): List of (id, data) tuples to insert
        """
        if not documents:
            return

        placeholders = ",".join(["(?,?)"] * len(documents))
        query = f"INSERT INTO {table_name} (id, data) VALUES {placeholders}"
        flat_params = [item for doc_tuple in documents for item in doc_tuple]
        self.db.execute(query, flat_params)

    def _process_text_search_stage(
        self,
        create_temp: Callable,
        current_table: str,
        match_spec: Dict[str, Any],
    ) -> str:
        """
        Process a $text search stage using FTS5 on temporary table.

        This method creates an FTS5 virtual table on the temporary data and uses
        SQLite's FTS5 for efficient text search. The tokenizer configuration is
        detected from the existing FTS index on the collection to ensure consistent
        behavior.

        Note:
            When $text is used after $unwind (or other stages that create temp tables),
            the search operates on the unwound elements in the temp table, not on the
            original collection documents. This differs from MongoDB's semantics where
            $text always uses the collection-level text index on original documents.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            match_spec (Dict[str, Any]): The $match stage specification containing the
                                        $text operator with a $search term

        Returns:
            str: Name of the newly created temporary table with text search results

        Raises:
            ValueError: If the $text operator specification is invalid or the search
                        term is not a string
        """
        # Warn about NeoSQLite extension (different semantics from MongoDB)
        # Only warn if $text is used AFTER $unwind (i.e., on a temp table with unwound data)
        if self._has_unwind_in_pipeline and not self._text_on_temp_table_warned:
            warnings.warn(
                "$text search after $unwind is a NeoSQLite extension using FTS5 on "
                "temporary tables, which searches unwound elements directly. "
                "This differs from MongoDB where $text can only be the first stage. "
                "For MongoDB compatibility, place the $text stage at the beginning of the pipeline.",
                UserWarning,
                stacklevel=4,
            )
            self._text_on_temp_table_warned = True

        # Extract and validate search term
        if "$text" not in match_spec or "$search" not in match_spec["$text"]:
            raise ValueError("Invalid $text operator specification")

        search_term = match_spec["$text"]["$search"]
        if not isinstance(search_term, str):
            raise ValueError("$text search term must be a string")

        # Detect tokenizer from existing FTS index on the collection
        tokenizer_clause = self._detect_fts_tokenizer()

        # Generate deterministic table names
        fts_table_name = f"temp_text_fts_{hashlib.sha256(str(match_spec).encode()).hexdigest()[:8]}"
        result_table_name = f"temp_text_filtered_{hashlib.sha256(str(match_spec).encode()).hexdigest()[:8]}"

        # Step 1: Create FTS5 virtual table with detected tokenizer
        # We need to extract text content from the JSON data for indexing
        # After $unwind, the unwound field contains the element (e.g., {"text": "...", ...})
        # We try multiple paths to find text content and concatenate them
        self.db.execute(f"""
            CREATE VIRTUAL TABLE {fts_table_name} USING fts5(src_rowid, id, content {tokenizer_clause})
        """)

        # Step 2: Populate FTS5 table with text content from current table
        # Use json_tree/jsonb_tree to recursively extract ALL string values from
        # the JSON object at any depth, then concatenate them for FTS indexing.
        # This handles any unwound object/array structure without hardcoded paths.
        json_tree_func = _get_json_tree_function(
            self._jsonb_supported, self._jsonb_each_supported
        )

        self.db.execute(f"""
            INSERT INTO {fts_table_name}(src_rowid, id, content)
            SELECT c.rowid, c.id,
                   GROUP_CONCAT(t.value, ' ') as content
            FROM {current_table} c, {json_tree_func}(c.data) t
            WHERE t.type = 'text'
            GROUP BY c.rowid
        """)

        # Step 3: Query FTS5 and create result table with matching documents
        # Join on source rowid to get exact matching rows
        # Also preserve _id column for proper sorting support
        self.db.execute(f"DROP TABLE IF EXISTS {result_table_name}")
        self.db.execute(
            f"""
            CREATE TEMP TABLE {result_table_name} AS
            SELECT c.id, c._id, c.data
            FROM {current_table} c
            INNER JOIN {fts_table_name} f ON c.rowid = f.src_rowid
            WHERE {fts_table_name} MATCH ?
        """,
            [search_term],
        )

        # Clean up FTS table
        self.db.execute(f"DROP TABLE IF EXISTS {fts_table_name}")

        return result_table_name

    def _detect_fts_tokenizer(self) -> str:
        """
        Detect the tokenizer configuration from existing FTS indexes on the collection.

        Returns:
            str: The tokenizer clause for FTS5 (e.g., ", tokenize=porter" or "")
        """
        # Query sqlite_master to find FTS tables for this collection
        fts_table_pattern = f"{quote_table_name(self.collection.name)}_%_fts"
        cursor = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (fts_table_pattern,),
        )

        for (sql,) in cursor.fetchall():
            if sql:
                # Parse tokenizer from CREATE VIRTUAL TABLE ... USING FTS5(..., tokenize=xxx)
                # Example: "CREATE VIRTUAL TABLE test USING FTS5(content, tokenize=porter)"
                import re

                match = re.search(r"tokenize\s*=\s*(\w+)", sql, re.IGNORECASE)
                if match:
                    tokenizer = match.group(1)
                    return f", tokenize={tokenizer}"

        # Default: no tokenizer specified
        return ""

    # ========== NEW AGGREGATION STAGE METHODS ==========

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

    def _process_densify_stage(self, create_temp, current_table, densify_spec):
        """
        Process $densify stage - fills in missing values in a sequence.

        MongoDB syntax:
        {
          $densify: {
            field: <field_name>,
            range: {
              step: <number>,
              bounds: [<lower>, <upper>]
            },
            partitionBy: <expression>  // optional
          }
        }
        """
        field = densify_spec.get("field")
        range_spec = densify_spec.get("range")
        partition_by = densify_spec.get("partitionBy") or densify_spec.get(
            "partitionByFields"
        )

        if not field or not range_spec:
            raise NotImplementedError(
                "$densify requires field and range - use force_fallback or simplify pipeline"
            )

        if partition_by:
            raise NotImplementedError(
                "$densify with partitionBy not supported - use force_fallback"
            )

        print(
            f"DEBUG DENSIFY: field={field}, range_spec={range_spec}, partition_by={partition_by}"
        )

        if not field or not range_spec:
            raise NotImplementedError(
                "$densify requires field and range - use force_fallback or simplify pipeline"
            )

        if partition_by:
            print(
                "DEBUG DENSIFY: partition_by is truthy, raising NotImplementedError"
            )
            raise NotImplementedError(
                "$densify with partitionBy not supported - use force_fallback"
            )

        step = range_spec.get("step")
        bounds = range_spec.get("bounds")

        if not step or not bounds:
            raise NotImplementedError(
                "$densify requires step and bounds - use force_fallback or simplify pipeline"
            )

        if not isinstance(bounds, list) or len(bounds) != 2:
            raise NotImplementedError(
                "$densify with non-array bounds not supported - use force_fallback"
            )

        lower_bound = bounds[0]
        upper_bound = bounds[1]

        if not isinstance(step, (int, float)):
            raise NotImplementedError(
                "$densify with non-numeric step not supported - use force_fallback"
            )

        if not isinstance(lower_bound, (int, float)) or not isinstance(
            upper_bound, (int, float)
        ):
            raise NotImplementedError(
                "$densify with non-numeric bounds not supported - use force_fallback"
            )

        json_extract = f"{self._json_function_prefix}_extract"

        densify_key = f"{field}:{step}:{lower_bound}:{upper_bound}"
        densify_hash = hashlib.sha256(densify_key.encode()).hexdigest()[:8]
        series_table = f"_densify_series_{densify_hash}"

        step_series = []
        current = float(lower_bound)
        while current <= upper_bound:
            step_series.append(current)
            current += step
            if len(step_series) > 1000:
                break

        if not step_series:
            return current_table

        try:
            self.collection.db.execute(
                f"CREATE TEMP TABLE {series_table} (val REAL)"
            )
            self.collection.db.execute(
                f"INSERT INTO {series_table} (val) VALUES "
                + "("
                + "),((".join([str(v) for v in step_series])
                + ")"
            )

            json_set_func = f"{self._json_function_prefix}_set"

            select_clause = f"""
                SELECT id, _id,
                json({json_set_func}(data, '{field}', s.val)) as data
                FROM {current_table}, {series_table} s
                WHERE s.val >= {lower_bound} AND s.val <= {upper_bound}
                AND NOT EXISTS (
                    SELECT 1 FROM {current_table} c
                    WHERE {json_extract}(c.data, '{field}') = s.val
                )
            """

            new_table = create_temp({"$densify": densify_spec}, select_clause)

            return new_table
        finally:
            try:
                self.collection.db.execute(
                    f"DROP TABLE IF EXISTS {series_table}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop series table '{series_table}': {e}"
                )
                pass

    def _process_union_with_stage(self, create_temp, current_table, union_spec):
        """
        Process $unionWith stage - combines documents from another collection.

        MongoDB syntax:
        {
          $unionWith: {
            coll: <collection_name>,
            pipeline: [<pipeline stages>]  // optional
          }
        }
        """
        coll_name = union_spec.get("coll")
        pipeline = union_spec.get("pipeline", [])

        if not coll_name:
            return current_table

        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

        # Build SELECT clause for current table based on available columns
        if has_id and has_underscore_id:
            current_select = f"SELECT id, _id, data FROM {current_table}"
        elif has_id:
            current_select = f"SELECT id, data FROM {current_table}"
        elif has_underscore_id:
            current_select = f"SELECT _id, data FROM {current_table}"
        else:
            current_select = f"SELECT data FROM {current_table}"

        # Get documents from the other collection with matching columns
        other_columns = []
        if has_id:
            other_columns.append("id")
        if has_underscore_id:
            other_columns.append("_id")
        other_columns.append("data")

        other_select_cols = (
            ", ".join(other_columns) if other_columns else "data"
        )

        if pipeline:
            # If pipeline specified, process it
            # For simplicity, just get all documents
            other_table = create_temp(
                {"$unionWith": union_spec},
                f"SELECT {other_select_cols} FROM {coll_name}",
            )
        else:
            other_table = create_temp(
                {"$unionWith": union_spec},
                f"SELECT {other_select_cols} FROM {coll_name}",
            )

        # Union the two tables with explicit column lists
        result_table = create_temp(
            {"$unionWith": union_spec},
            f"{current_select} UNION ALL SELECT {other_select_cols} FROM {other_table}",
        )

        return result_table

    def _process_merge_stage(self, create_temp, current_table, merge_spec):
        """
        Process $merge stage - writes results to a collection.

        MongoDB syntax:
        {
          $merge: {
            into: <collection_name>,
            on: <field>,  // optional
            whenMatched: <action>,  // optional
            whenNotMatched: <action>  // optional
          }
        }
        """
        into = merge_spec.get("into")
        if isinstance(into, dict):
            db_name = into.get("db") or ""
            coll_name = into.get("coll") or ""
            into = db_name + "." + coll_name

        if not into:
            return current_table

        # For now, just return current table (actual merge would write to collection)
        # This is a placeholder - full implementation would INSERT/UPDATE the target
        return current_table

    def _process_redact_stage(self, create_temp, current_table, redact_spec):
        """
        Process $redact stage - field-level redaction based on conditions.

        MongoDB syntax:
        {
          $redact: {
            $cond: {
              if: <condition>,
              then: <level>,
              else: <level>
            }
          }
        }

        Levels:
        - $$DESCEND: Include the field and process sub-fields
        - $$PRUNE: Exclude the field
        - $$KEEP: Include the field as-is
        """
        # For now, this is a placeholder - full redaction requires complex expression evaluation
        # Return current table unchanged
        return current_table

    def _process_set_window_fields_stage(
        self,
        create_temp: Callable[[Dict[str, Any], str, List[Any]], str],
        current_table: str,
        spec: Dict[str, Any],
    ) -> str:
        """
        Process $setWindowFields stage.
        """
        partition_by = spec.get("partitionBy")
        sort_by: Dict[str, int] = spec.get("sortBy", {})
        output: Dict[str, Any] = spec.get("output", {})
        all_params: List[Any] = []

        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names
        has_data = "data" in column_names

        # 1. Build PARTITION BY clause
        partition_parts = []
        if partition_by is not None:
            # Handle _id specially - it's a column, not in JSON
            if partition_by == "_id":
                if has_underscore_id:
                    partition_parts.append("_id")
                elif has_id:
                    partition_parts.append("id")
                else:
                    # Can't partition by _id if column doesn't exist
                    partition_by = None
            else:
                sql, params = self.expr_evaluator.build_select_expression(
                    partition_by
                )
                partition_parts.append(sql)
                all_params.extend(params)

        partition_clause = ""
        if partition_parts:
            partition_clause = f"PARTITION BY {', '.join(partition_parts)}"

        # 2. Build ORDER BY clause
        sort_parts = []
        if sort_by:
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                # Handle _id specially
                if field == "_id":
                    if has_underscore_id:
                        sort_parts.append(f"_id {order}")
                    elif has_id:
                        sort_parts.append(f"id {order}")
                    # else skip this sort field
                else:
                    sql, params = self.expr_evaluator.build_select_expression(
                        f"${field}"
                    )
                    sort_parts.append(f"{sql} {order}")
                    all_params.extend(params)

        sort_clause = ""
        if sort_parts:
            sort_clause = f"ORDER BY {', '.join(sort_parts)}"

        # 3. Build output fields with window functions
        json_set_args = []
        for field, op_spec in output.items():
            # Skip _id field - it's a separate column, not in JSON data
            if field == "_id":
                # For _id, we need to handle it separately in the SELECT clause
                continue

            if not isinstance(op_spec, dict) or not op_spec:
                # Skip invalid op_spec
                continue

            op_name = next(iter(op_spec.keys()))
            op_val = op_spec[op_name]
            window_spec = op_spec.get("window")

            sql_func, sql_operand, sql_params = (
                self._map_window_operator_to_sql(op_name, op_val)
            )
            if sql_func is None:
                # Fall back to Python if operator not supported in SQL
                raise NotImplementedError(
                    f"Window operator {op_name} not supported in SQL"
                )

            all_params.extend(sql_params)
            # Only include frame clause if we have ORDER BY (required by SQLite)
            frame_clause = ""
            if sort_parts and window_spec:
                frame_clause = self._build_window_frame_sql(window_spec)

            # Build window SQL - ensure proper spacing
            window_parts = []
            if partition_clause:
                window_parts.append(partition_clause)
            if sort_clause:
                window_parts.append(sort_clause)
            if frame_clause:
                window_parts.append(frame_clause)

            if window_parts:
                window_sql = (
                    f"{sql_func}({sql_operand}) OVER ({' '.join(window_parts)})"
                )
            else:
                window_sql = f"{sql_func}({sql_operand}) OVER ()"

            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(window_sql)

        # 4. Create the temporary table
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        # Build SELECT clause based on available columns
        if has_id and has_underscore_id and has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT id, _id, json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT id, _id, data FROM {current_table}"
        elif has_id and has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT id, json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT id, data FROM {current_table}"
        elif has_underscore_id and has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT _id, json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT _id, data FROM {current_table}"
        elif has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT data FROM {current_table}"
        else:
            # No data column - can't process this stage
            logger.warning(
                f"Table {current_table} has no data column for setWindowFields"
            )
            return current_table

        return create_temp({"$setWindowFields": spec}, sql, all_params)

    def _map_window_operator_to_sql(
        self, op_name: str, op_val: Any
    ) -> Tuple[str | None, str, List[Any]]:
        """Map MongoDB window operator to SQL function and operand."""
        match op_name:
            case "$rank":
                return "RANK", "", []
            case "$denseRank":
                return "DENSE_RANK", "", []
            case "$documentNumber":
                return "ROW_NUMBER", "", []
            case "$first":
                sql, params = self.expr_evaluator.build_select_expression(
                    op_val
                )
                return "FIRST_VALUE", sql, params
            case "$last":
                sql, params = self.expr_evaluator.build_select_expression(
                    op_val
                )
                return "LAST_VALUE", sql, params
            case "$shift":
                output_expr = op_val.get("output")
                by = op_val.get("by", 0)
                default = op_val.get("default")

                if by >= 0:
                    func = "LEAD"
                    offset = by
                else:
                    func = "LAG"
                    offset = -by

                sql, params = self.expr_evaluator.build_select_expression(
                    output_expr
                )
                if default is not None:
                    return f"{func}", f"{sql}, {offset}, ?", params + [default]
                return f"{func}", f"{sql}, {offset}", params

            case "$sum" | "$avg" | "$min" | "$max":
                func = op_name[1:].upper()
                sql, params = self.expr_evaluator.build_select_expression(
                    op_val
                )
                return func, sql, params

            case _:
                return None, "", []

    def _build_window_frame_sql(
        self, window_spec: Dict[str, Any] | None
    ) -> str:
        """Build SQL window frame clause (ROWS BETWEEN ...)."""
        if not window_spec:
            return ""

        if "documents" in window_spec:
            lower, upper = window_spec["documents"]

            def map_bound(val: Any, is_upper: bool = False) -> str:
                if val == "unbounded":
                    return (
                        "UNBOUNDED FOLLOWING"
                        if is_upper
                        else "UNBOUNDED PRECEDING"
                    )
                if val == "current":
                    return "CURRENT ROW"
                if isinstance(val, int):
                    if val < 0:
                        return f"{-val} PRECEDING"
                    if val > 0:
                        return f"{val} FOLLOWING"
                return "CURRENT ROW"

            l_bound = map_bound(lower, is_upper=False)
            u_bound = map_bound(upper, is_upper=True)

            # Validate bounds - if either is empty, return empty string
            if not l_bound or not u_bound:
                return ""

            return f"ROWS BETWEEN {l_bound} AND {u_bound}"

        return ""

    def _process_graph_lookup_stage(
        self,
        create_temp: Callable[[Dict[str, Any], str, List[Any]], str],
        current_table: str,
        spec: Dict[str, Any],
    ) -> str:
        """
        Process $graphLookup stage.
        """
        from_collection = spec.get("from")
        start_with = spec.get("startWith")
        connect_from_field = spec.get("connectFromField")
        connect_to_field = spec.get("connectToField")
        as_field = spec.get("as")
        max_depth = spec.get("maxDepth")
        depth_field = spec.get("depthField")
        restrict_search = spec.get("restrictSearchWithMatch")

        if not all(
            [
                from_collection,
                start_with,
                connect_from_field,
                connect_to_field,
                as_field,
            ]
        ):
            return current_table

        all_params: List[Any] = []

        # 1. Build startWith expression
        start_with_sql, start_with_params = (
            self.expr_evaluator.build_select_expression(start_with)
        )
        # Prefix with p. to avoid ambiguity during JOIN
        start_with_sql = start_with_sql.replace(
            "json_extract(data", "json_extract(p.data"
        )
        start_with_sql = start_with_sql.replace(
            "jsonb_extract(data", "jsonb_extract(p.data"
        )
        all_params.extend(start_with_params)

        # 2. Build restrictSearchWithMatch if present
        restrict_where = ""
        restrict_params = []
        if restrict_search:
            from .query_helper import QueryHelper

            target_coll = self.collection.database.get_collection(
                from_collection
            )
            helper = QueryHelper(target_coll)
            query_result = helper._build_simple_where_clause(restrict_search)
            if query_result:
                r_sql, r_params, _ = query_result
                # Remove leading "WHERE " if present since we're adding it to existing WHERE clause
                r_sql = r_sql.strip()
                if r_sql.upper().startswith("WHERE "):
                    r_sql = r_sql[6:]  # Remove "WHERE " prefix
                r_sql = r_sql.replace(
                    "json_extract(data", "json_extract(t.data"
                )
                r_sql = r_sql.replace(
                    "jsonb_extract(data", "jsonb_extract(t.data"
                )
                restrict_where = f"AND ({r_sql})"
                restrict_params = (
                    r_params * 2
                )  # Used twice: in start_points_sql and recursive_step_sql
                all_params.extend(restrict_params)

        # 3. Build recursive search
        recurse_cte = "graph_recurse_tier2"

        # Build field access SQL
        def get_field_sql(table_alias, field_name, is_recursive_table=False):
            if field_name == "_id":
                return (
                    f"{table_alias}.found_id"
                    if is_recursive_table
                    else f"{table_alias}._id"
                )
            data_col = "found_data" if is_recursive_table else "data"
            return f"json_extract({table_alias}.{data_col}, '$.{field_name}')"

        target_to_sql = get_field_sql("t", connect_to_field)
        recurse_from_sql = get_field_sql(
            "r", connect_from_field, is_recursive_table=True
        )

        start_points_sql = f"""
            SELECT
                p.id as original_id,
                t.id as found_id,
                t.data as found_data,
                0 as depth
            FROM {current_table} p
            JOIN {from_collection} t ON {target_to_sql} = {start_with_sql}
            WHERE 1=1 {restrict_where}
        """

        max_depth_cond = (
            f"AND r.depth < {max_depth}" if max_depth is not None else ""
        )
        recursive_step_sql = f"""
            SELECT
                r.original_id,
                t.id as found_id,
                t.data as found_data,
                r.depth + 1
            FROM {recurse_cte} r
            JOIN {from_collection} t ON {target_to_sql} = {recurse_from_sql}
            WHERE 1=1 {max_depth_cond} {restrict_where}
        """

        # 4. Combine into stage SQL
        # Move WITH clause to top level (SQLite doesn't allow nested WITH)
        depth_json_sql = ""
        if depth_field:
            depth_json_sql = f", '{parse_json_path(str(depth_field))}', depth"

        json_group_func = _get_json_group_array_function(self._jsonb_supported)
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        as_field_str = str(as_field)
        stage_sql = f"""
            WITH RECURSIVE {recurse_cte} AS (
                {start_points_sql}
                UNION ALL
                {recursive_step_sql}
            )
            SELECT
                p.id AS id,
                json({json_set_func}({json_set_func}(p.data, '$._id', p.id), '{parse_json_path(as_field_str)}',
                    COALESCE((
                        SELECT {json_group_func}(
                            json({json_set_func}(sub.found_data, '$._id', sub.found_id {depth_json_sql}))
                        )
                        FROM (
                            SELECT found_id, found_data, depth FROM {recurse_cte}
                            WHERE original_id = p.id
                            GROUP BY found_id
                        ) sub
                    ), json('[]'))
                )) as data
            FROM {current_table} p
        """

        return create_temp({"$graphLookup": spec}, stage_sql, all_params)

    def _process_fill_stage(
        self,
        create_temp: Callable[[Dict[str, Any], str, List[Any]], str],
        current_table: str,
        spec: Dict[str, Any],
    ) -> str:
        """
        Process $fill stage.
        """
        partition_by = spec.get("partitionBy")
        sort_by: Dict[str, int] = spec.get("sortBy", {})
        output: Dict[str, Any] = spec.get("output", {})
        all_params: List[Any] = []

        # Check for 'linear' method
        for fill_spec in output.values():
            if fill_spec.get("method") == "linear":
                raise NotImplementedError(
                    "$fill method 'linear' requires Python fallback"
                )

        # 1. Build PARTITION BY and ORDER BY clauses
        partition_parts = []
        if partition_by is not None:
            sql, params = self.expr_evaluator.build_select_expression(
                partition_by
            )
            partition_parts.append(sql)
            all_params.extend(params)
        partition_clause = (
            f"PARTITION BY {', '.join(partition_parts)}"
            if partition_parts
            else ""
        )

        sort_parts = []
        if sort_by:
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                sql, params = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                sort_parts.append(f"{sql} {order}")
                all_params.extend(params)
        sort_clause = f"ORDER BY {', '.join(sort_parts)}" if sort_parts else ""

        # 2. Process output fields
        has_locf = any(fs.get("method") == "locf" for fs in output.values())
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        if not has_locf:
            # Simple constant value fill
            json_set_args = []
            for field, fill_spec in output.items():
                value = fill_spec.get("value")
                field_sql, field_params = (
                    self.expr_evaluator.build_select_expression(f"${field}")
                )
                all_params.extend(field_params)

                fill_expr = f"COALESCE({field_sql}, ?)"
                all_params.append(value)

                json_set_args.append(f"'{parse_json_path(field)}'")
                json_set_args.append(fill_expr)

            args_str = ", ".join(json_set_args)
            data_expr = f"json({json_set_func}(data, {args_str}))"
            sql = f"SELECT id, _id, {data_expr} AS data FROM {current_table}"
            return create_temp({"$fill": spec}, sql, all_params)

        # Complex locf fill
        block_id_selects = ["id", "_id", "data"]
        for field, fill_spec in output.items():
            if fill_spec.get("method") == "locf":
                field_sql, _ = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                block_id_selects.append(
                    f"COUNT({field_sql}) OVER ({partition_clause} {sort_clause}) AS block_id_{parse_json_path(field).replace('.', '_')}"
                )

        subquery_alias = "fill_blocks_tier2"
        final_json_args = []
        for field, fill_spec in output.items():
            field_path = parse_json_path(field)
            if fill_spec.get("method") == "locf":
                field_sql, _ = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                block_col = f"block_id_{field_path.replace('.', '_')}"
                block_partition = (
                    f"PARTITION BY {', '.join(partition_parts + [block_col])}"
                    if partition_parts
                    else f"PARTITION BY {block_col}"
                )
                locf_expr = f"FIRST_VALUE({field_sql}) OVER ({block_partition} {sort_clause})"
                final_json_args.append(f"'{field_path}'")
                final_json_args.append(locf_expr)
            else:
                value = fill_spec.get("value")
                field_sql, _ = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                final_json_args.append(f"'{field_path}'")
                final_json_args.append(f"COALESCE({field_sql}, ?)")
                all_params.append(value)

        args_str = ", ".join(final_json_args)
        data_expr = f"json({json_set_func}(data, {args_str}))"

        stage_sql = f"""
            SELECT id, _id, {data_expr} AS data
            FROM (
                SELECT {", ".join(block_id_selects)}
                FROM {current_table}
            ) {subquery_alias}
        """
        return create_temp({"$fill": spec}, stage_sql, all_params)


def can_process_with_temporary_tables(pipeline: List[Dict[str, Any]]) -> bool:
    """
    Determine if a pipeline can be processed with temporary tables.

    This function checks if all stages in an aggregation pipeline are supported
    by the temporary table processing approach. It verifies that each stage in
    the pipeline is one of the supported stage types.

    Additionally, it handles special cases for text search operations:
    - Pure text search operations are supported with hybrid processing
    - Text search with simple unwind operations are supported (uses Python text search on temp tables)
    - Complex nested unwinds (multiple unwinds or dotted paths) fall back to Python

    Args:
        pipeline (List[Dict[str, Any]]): List of aggregation pipeline stages to check

    Returns:
        bool: True if all stages in the pipeline are supported and can be processed
              with temporary tables, False otherwise
    """
    # Check if all stages are supported
    # Note: $merge and $redact require Python fallback for full functionality
    supported_stages = {
        "$addFields",
        "$bucket",
        "$bucketAuto",
        "$densify",
        "$facet",
        "$fill",
        "$graphLookup",
        "$group",
        "$limit",
        "$lookup",
        "$match",
        "$project",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$setWindowFields",
        "$skip",
        "$sort",
        "$unionWith",
        "$unset",
        "$unwind",
    }

    # Count unwind stages and check for complex patterns
    unwind_count = 0
    has_nested_unwind = False

    for stage in pipeline:
        stage_name = next(iter(stage.keys()))
        if stage_name not in supported_stages:
            return False

        if stage_name == "$match":
            match_spec = stage["$match"]
            if "$jsonSchema" in match_spec:
                return False

        if stage_name in ("$setWindowFields", "$fill"):
            if sqlite3.sqlite_version_info < (3, 25, 0):
                return False

        if stage_name == "$unwind":
            unwind_count += 1
            unwind_spec = stage["$unwind"]
            # Check for nested/dotted paths which are complex
            if isinstance(unwind_spec, str) and "." in unwind_spec:
                has_nested_unwind = True
            elif isinstance(unwind_spec, dict):
                path = unwind_spec.get("path", "")
                if "." in path:
                    has_nested_unwind = True

    # Multiple unwinds or nested paths are complex - fall back to Python
    if unwind_count > 1 or has_nested_unwind:
        return False

    return True


def _contains_text_search(match_spec: Dict[str, Any]) -> bool:
    """
    Check if a match specification contains text search operations.

    This function delegates to the centralized _contains_text_operator function
    to ensure consistent text search detection across all NeoSQLite components.

    Args:
        match_spec (Dict[str, Any]): The match specification to check for text search operations

    Returns:
        bool: True if the match specification contains text search operations, False otherwise
    """
    return _contains_text_operator(match_spec)


def execute_2nd_tier_aggregation(
    query_engine,
    pipeline: List[Dict[str, Any]],
    batch_size: int = 101,
) -> List[Dict[str, Any]]:
    """
    Execute aggregation pipeline using temporary table approach for complex pipelines.

    This function is designed to be called as the second tier in a three-tier processing system:
    1. First tier (QueryEngine): Try existing SQL optimization for simple pipelines
    2. Second tier (this function): Try temporary table approach for complex pipelines
    3. Third tier (QueryEngine): Fall back to Python implementation for unsupported operations

    This function focuses specifically on processing complex pipelines that the current
    NeoSQLite SQL optimization cannot handle efficiently, using temporary tables for better performance.

    Args:
        query_engine: The NeoSQLite QueryEngine instance to use for processing
        pipeline (List[Dict[str, Any]]): List of aggregation pipeline stages to process
        batch_size (int): Batch size for fetching results from temporary tables

    Returns:
        List[Dict[str, Any]]: List of result documents after processing the pipeline
    """
    # Check if we should force fallback for benchmarking/debugging
    from .query_helper import get_force_fallback

    if get_force_fallback():
        raise NotImplementedError(
            "Temporary table aggregation skipped due to force fallback flag"
        )

    # Process the pipeline with temporary tables if possible
    if can_process_with_temporary_tables(pipeline):
        try:
            processor = TemporaryTableAggregationProcessor(
                query_engine.collection, query_engine
            )
            return processor.process_pipeline(pipeline, batch_size=batch_size)
        except Exception as e:
            logger.debug(
                f"Temporary table aggregation failed, fallback required: {e}"
            )
            raise NotImplementedError(
                f"Temporary table aggregation failed, fallback required: {e}"
            )

    # If we can't process with temporary tables, signal for fallback.
    raise NotImplementedError(
        "Pipeline not supported by temporary table aggregation."
    )
