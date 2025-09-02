"""
Simplified temporary table aggregation pipeline implementation for NeoSQLite.
This focuses on the core concept: using temporary tables to process complex pipelines
that the current implementation can't optimize with a single SQL query.
"""

import hashlib
from contextlib import contextmanager
from typing import Any, Dict, List, Callable
from .cursor import DESCENDING


class DeterministicTempTableManager:
    """Manager for deterministic temporary table names."""

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
        import uuid

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
    except Exception:
        # Rollback on error
        db_connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        raise
    finally:
        # Cleanup
        db_connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Explicitly drop temp tables
        for table_name in temp_tables:
            try:
                db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass


class TemporaryTableAggregationProcessor:
    """Processor for aggregation pipelines using temporary tables."""

    def __init__(self, collection):
        """
        Initialize the TemporaryTableAggregationProcessor with a collection.

        Args:
            collection: The NeoSQLite collection to process aggregation pipelines
                        on. This collection provides the database connection and
                        document loading functionality needed for pipeline processing.
        """
        self.collection = collection
        self.db = collection.db

    def process_pipeline(
        self, pipeline: List[Dict[str, Any]]
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

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages
                                             to process

        Returns:
            List[Dict[str, Any]]: A list of result documents after processing the
                                  pipeline

        Raises:
            NotImplementedError: If the pipeline contains unsupported stages
        """
        # Generate a deterministic pipeline ID based on the pipeline content
        pipeline_key = "".join(str(sorted(stage.items())) for stage in pipeline)
        pipeline_id = hashlib.sha256(pipeline_key.encode()).hexdigest()[:8]

        with aggregation_pipeline_context(self.db, pipeline_id) as create_temp:
            # Start with base data
            base_stage = {"_base": True}
            current_table = create_temp(
                base_stage, f"SELECT id, data FROM {self.collection.name}"
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

                    case "$addFields":
                        current_table = self._process_add_fields_stage(
                            create_temp, current_table, stage["$addFields"]
                        )
                        i += 1

                    case _:
                        # For unsupported stages, we would need to fall back to Python
                        # But for this demonstration, we'll raise an exception
                        raise NotImplementedError(
                            f"Stage '{stage_name}' not yet supported in temporary table approach"
                        )

            # Return final results
            return self._get_results_from_table(current_table)

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
        # Build WHERE clause
        where_conditions = []
        params = []

        for field, value in match_spec.items():
            if field == "_id":
                where_conditions.append("id = ?")
                params.append(value)
            else:
                if isinstance(value, dict):
                    # Handle operators like $gt, $lt, etc.
                    for op, op_val in value.items():
                        match op:
                            case "$eq":
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') = ?"
                                )
                                params.append(op_val)
                            case "$gt":
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') > ?"
                                )
                                params.append(op_val)
                            case "$lt":
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') < ?"
                                )
                                params.append(op_val)
                            case "$gte":
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') >= ?"
                                )
                                params.append(op_val)
                            case "$lte":
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') <= ?"
                                )
                                params.append(op_val)
                            case "$in":
                                placeholders = ", ".join("?" for _ in op_val)
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') IN ({placeholders})"
                                )
                                params.extend(op_val)
                            case "$ne":
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') != ?"
                                )
                                params.append(op_val)
                            case "$nin":
                                placeholders = ", ".join("?" for _ in op_val)
                                where_conditions.append(
                                    f"json_extract(data, '$.{field}') NOT IN ({placeholders})"
                                )
                                params.extend(op_val)
                else:
                    # Simple equality
                    where_conditions.append(
                        f"json_extract(data, '$.{field}') = ?"
                    )
                    params.append(value)

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Create filtered temporary table
        match_stage = {"$match": match_spec}
        new_table = create_temp(
            match_stage, f"SELECT * FROM {current_table} {where_clause}", params
        )
        return new_table

    def _process_unwind_stages(
        self, create_temp: Callable, current_table: str, unwind_specs: List[Any]
    ) -> str:
        """
        Process one or more consecutive $unwind stages using temporary tables.

        This method handles the $unwind stage which deconstructs an array field
        from input documents to output a document for each element. It can process
        either a single unwind stage or multiple consecutive unwind stages
        efficiently in one operation.

        For a single unwind, it uses SQLite's json_each function to expand the
        array into separate rows. For multiple consecutive unwinds, it creates a
        Cartesian product of all the arrays being unwound, using nested json_each
        calls.

        The method properly handles array validation, ensuring that only documents
        with array fields are processed. It also supports the special _id field
        handling if it were to be unwound (though this would be unusual).

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
        if len(unwind_specs) == 1:
            # Simple case - single unwind
            field = unwind_specs[0]
            if isinstance(field, str) and field.startswith("$"):
                field_name = field[1:]  # Remove leading $

                unwind_stage = {"$unwind": field}
                new_table = create_temp(
                    unwind_stage,
                    f"""
                    SELECT {self.collection.name}.id,
                           json_set({self.collection.name}.data,
                                    '$."{field_name}"', je.value) as data
                    FROM {current_table} as {self.collection.name},
                         json_each(json_extract({self.collection.name}.data,
                                                '$.{field_name}')) as je
                    WHERE json_type(json_extract({self.collection.name}.data,
                                                 '$.{field_name}')) = 'array'
                    """,
                )
                return new_table
            else:
                raise ValueError(f"Invalid unwind specification: {field}")
        else:
            # Multiple consecutive unwind stages
            field_names = []
            for field in unwind_specs:
                if isinstance(field, str) and field.startswith("$"):
                    field_names.append(field[1:])
                else:
                    raise ValueError(f"Invalid unwind specification: {field}")

            # Build SELECT clause with nested json_set calls
            select_parts = [f"{self.collection.name}.data"]
            for i, field_name in enumerate(field_names):
                select_parts.insert(0, "json_set(")
                select_parts.append(f", '$.\"{field_name}\"', je{i + 1}.value)")
            select_expr = "".join(select_parts)

            # Build FROM clause with multiple json_each calls
            from_parts = [f"FROM {current_table} as {self.collection.name}"]
            for i, field_name in enumerate(field_names):
                from_parts.append(
                    f""", json_each(json_extract({self.collection.name}.data,
                                                 '$.{field_name}')) as je{i + 1}"""
                )

            # Build WHERE clause to ensure all fields are arrays
            where_conditions = []
            for field_name in field_names:
                where_conditions.append(
                    f"""json_type(json_extract({self.collection.name}.data,
                                               '$.{field_name}')) = 'array'"""
                )
            where_clause = "WHERE " + " AND ".join(where_conditions)

            # Create a representative stage spec for naming
            # Use the first unwind spec for naming purposes
            naming_stage: Dict[str, Any] = {
                "$unwind": unwind_specs[0] if unwind_specs else {}
            }
            new_table = create_temp(
                naming_stage,
                f"SELECT {self.collection.name}.id, {select_expr} as data "
                + " ".join(from_parts)
                + f" {where_clause}",
            )
            return new_table

    def _process_lookup_stage(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: Dict[str, Any],
    ) -> str:
        """
        Process a $lookup stage using temporary tables.

        This method implements the $lookup aggregation stage which performs a left
        outer join to another collection in the same database. It uses an optimized
        SQL query with a subquery to efficiently join the collections.

        The method handles both the special _id field and regular fields for both
        the local and foreign fields. It constructs an SQL query that:
        1. Selects all fields from the current collection
        2. Adds a new array field containing the matched documents from the foreign collection
        3. Uses json_set to add the lookup results to the document data
        4. Uses a correlated subquery with json_group_array to collect all matching documents

        The lookup results are stored as an array field in the document, with an
        empty array used when no matches are found.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            lookup_spec (Dict[str, Any]): The $lookup stage specification containing:
                - "from": The name of the collection to join with
                - "localField": The field from the input documents
                - "foreignField": The field from the documents of the "from" collection
                - "as": The name of the new array field to add to the matching documents

        Returns:
            str: Name of the newly created temporary table with lookup results added
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec["localField"]
        foreign_field = lookup_spec["foreignField"]
        as_field = lookup_spec["as"]

        # Build the optimized SQL query for $lookup
        if foreign_field == "_id":
            foreign_extract = "related.id"
        else:
            foreign_extract = f"json_extract(related.data, '$.{foreign_field}')"

        if local_field == "_id":
            local_extract = f"{self.collection.name}.id"
        else:
            local_extract = (
                f"json_extract({self.collection.name}.data, '$.{local_field}')"
            )

        select_clause = (
            f"SELECT {self.collection.name}.id, "
            f"json_set({self.collection.name}.data, '$.\"{as_field}\"', "
            f"coalesce(( "
            f"  SELECT json_group_array(json(related.data)) "
            f"  FROM {from_collection} as related "
            f"  WHERE {foreign_extract} = "
            f"        {local_extract} "
            f"), '[]')) as data"
        )

        from_clause = f"FROM {current_table} as {self.collection.name}"

        lookup_stage = {"$lookup": lookup_spec}
        # Create lookup temporary table
        new_table = create_temp(lookup_stage, f"{select_clause} {from_clause}")
        return new_table

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
        # Build ORDER BY clause
        order_clause = ""
        if sort_spec:
            order_parts = []
            for field, direction in sort_spec.items():
                if field == "_id":
                    order_parts.append(
                        f"id {'DESC' if direction == -1 else 'ASC'}"
                    )
                else:
                    order_parts.append(
                        f"json_extract(data, '$.{field}') {'DESC' if direction == -1 else 'ASC'}"
                    )
            order_clause = "ORDER BY " + ", ".join(order_parts)

        # Build LIMIT/OFFSET clause
        limit_clause = ""
        if limit_value is not None:
            if skip_value > 0:
                limit_clause = f"LIMIT {limit_value} OFFSET {skip_value}"
            else:
                limit_clause = f"LIMIT {limit_value}"
        elif skip_value > 0:
            limit_clause = f"LIMIT -1 OFFSET {skip_value}"  # SQLite requires LIMIT with OFFSET

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

        # Create sorted/skipped/limited temporary table
        new_table = create_temp(
            stage_spec,
            f"SELECT * FROM {current_table} {order_clause} {limit_clause}",
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

        For this basic implementation, it handles simple field copying using json_extract
        and json_set. A full implementation would handle computed fields and expressions.

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
            # Handle simple field copying (e.g., {"newField": "$existingField"})
            if isinstance(source_field, str) and source_field.startswith("$"):
                source_field_name = source_field[1:]  # Remove leading $
                if source_field_name == "_id":
                    # Special handling for _id field
                    data_expr = f"json_set({data_expr}, '$.{new_field}', id)"
                else:
                    # Use json_extract to get the source field value
                    data_expr = f"json_set({data_expr}, '$.{new_field}', json_extract(data, '$.{source_field_name}'))"
            # For this basic implementation, we won't handle complex expressions

        # Create addFields temporary table
        add_fields_stage = {"$addFields": add_fields_spec}
        new_table = create_temp(
            add_fields_stage,
            f"SELECT id, {data_expr} as data FROM {current_table}",
            params if params else None,
        )
        return new_table

    def _get_results_from_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get results from a temporary table.

        This method retrieves all documents from a temporary table and converts
        them back into their Python dictionary representation using the collection's
        document loading mechanism.

        Args:
            table_name (str): Name of the temporary table to retrieve results from

        Returns:
            List[Dict[str, Any]]: List of documents retrieved from the temporary table,
                                  with each document represented as a dictionary
        """
        cursor = self.db.execute(f"SELECT * FROM {table_name}")
        results = []
        for row in cursor.fetchall():
            doc = self.collection._load(row[0], row[1])
            results.append(doc)
        return results


def can_process_with_temporary_tables(pipeline: List[Dict[str, Any]]) -> bool:
    """
    Determine if a pipeline can be processed with temporary tables.

    This function checks if all stages in an aggregation pipeline are supported
    by the temporary table processing approach. It verifies that each stage in
    the pipeline is one of the supported stage types.

    Args:
        pipeline (List[Dict[str, Any]]): List of aggregation pipeline stages to check

    Returns:
        bool: True if all stages in the pipeline are supported and can be processed
              with temporary tables, False otherwise
    """
    # Check if all stages are supported
    supported_stages = {
        "$match",
        "$unwind",
        "$sort",
        "$skip",
        "$limit",
        "$lookup",
        "$addFields",  # Added support for $addFields
    }

    for stage in pipeline:
        stage_name = next(iter(stage.keys()))
        if stage_name not in supported_stages:
            return False

    return True


def execute_three_tier_aggregation(
    query_engine, pipeline: List[Dict[str, Any]]
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

    Returns:
        List[Dict[str, Any]]: List of result documents after processing the pipeline
    """
    # Process the pipeline with temporary tables if possible
    if can_process_with_temporary_tables(pipeline):
        try:
            processor = TemporaryTableAggregationProcessor(
                query_engine.collection
            )
            return processor.process_pipeline(pipeline)
        except Exception:
            # If temporary table approach fails, let the caller handle fallback
            pass

    # If we can't process with temporary tables, or if processing fails,
    # let the QueryEngine handle the fallback to Python implementation
    # We raise a special exception that the QueryEngine can catch
    raise NotImplementedError("Pipeline requires Python fallback processing")
