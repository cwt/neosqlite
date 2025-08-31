"""
Simplified temporary table aggregation pipeline implementation for NeoSQLite.
This focuses on the core concept: using temporary tables to process complex pipelines
that the current implementation can't optimize with a single SQL query.
"""

import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Callable, Optional
import json
from .cursor import DESCENDING


@contextmanager
def aggregation_pipeline_context(db_connection):
    """Context manager for temporary aggregation tables with automatic cleanup."""
    temp_tables = []
    savepoint_name = f"agg_pipeline_{uuid.uuid4().hex}"

    # Create savepoint for atomicity
    db_connection.execute(f"SAVEPOINT {savepoint_name}")

    def create_temp_table(
        name_suffix: str, query: str, params: Optional[List[Any]] = None
    ) -> str:
        """Create a temporary table for pipeline processing."""
        table_name = f"temp_{name_suffix}_{uuid.uuid4().hex}"
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
        self.collection = collection
        self.db = collection.db

    def process_pipeline(
        self, pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Process an aggregation pipeline using temporary tables for intermediate results.

        This implementation focuses on processing complex pipelines that the current
        NeoSQLite implementation can't optimize with a single SQL query.

        Args:
            pipeline: List of aggregation pipeline stages

        Returns:
            List of result documents
        """
        with aggregation_pipeline_context(self.db) as create_temp:
            # Start with base data
            current_table = create_temp(
                "base", f"SELECT id, data FROM {self.collection.name}"
            )

            # Process pipeline stages in groups that can be handled together
            i = 0
            while i < len(pipeline):
                stage = pipeline[i]
                stage_name = next(iter(stage.keys()))

                # Handle groups of compatible stages
                if stage_name == "$match":
                    current_table = self._process_match_stage(
                        create_temp, current_table, stage["$match"]
                    )
                    i += 1

                elif stage_name == "$unwind":
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

                elif stage_name == "$lookup":
                    current_table = self._process_lookup_stage(
                        create_temp, current_table, stage["$lookup"]
                    )
                    i += 1

                elif stage_name in ["$sort", "$skip", "$limit"]:
                    # Process consecutive sort/skip/limit stages
                    sort_spec = None
                    skip_value = 0
                    limit_value = None
                    j = i

                    # Process consecutive sort/skip/limit stages
                    while j < len(pipeline):
                        next_stage = pipeline[j]
                        next_stage_name = next(iter(next_stage.keys()))

                        if next_stage_name == "$sort":
                            sort_spec = next_stage["$sort"]
                        elif next_stage_name == "$skip":
                            skip_value = next_stage["$skip"]
                        elif next_stage_name == "$limit":
                            limit_value = next_stage["$limit"]
                        else:
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

                else:
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
        """Process a $match stage using temporary tables."""
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
                        if op == "$eq":
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') = ?"
                            )
                            params.append(op_val)
                        elif op == "$gt":
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') > ?"
                            )
                            params.append(op_val)
                        elif op == "$lt":
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') < ?"
                            )
                            params.append(op_val)
                        elif op == "$gte":
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') >= ?"
                            )
                            params.append(op_val)
                        elif op == "$lte":
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') <= ?"
                            )
                            params.append(op_val)
                        elif op == "$in":
                            placeholders = ", ".join("?" for _ in op_val)
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') IN ({placeholders})"
                            )
                            params.extend(op_val)
                        elif op == "$ne":
                            where_conditions.append(
                                f"json_extract(data, '$.{field}') != ?"
                            )
                            params.append(op_val)
                        elif op == "$nin":
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
        new_table = create_temp(
            "matched", f"SELECT * FROM {current_table} {where_clause}", params
        )
        return new_table

    def _process_unwind_stages(
        self, create_temp: Callable, current_table: str, unwind_specs: List[Any]
    ) -> str:
        """Process one or more consecutive $unwind stages."""
        if len(unwind_specs) == 1:
            # Simple case - single unwind
            field = unwind_specs[0]
            if isinstance(field, str) and field.startswith("$"):
                field_name = field[1:]  # Remove leading $

                new_table = create_temp(
                    "unwound",
                    f"""
                    SELECT {self.collection.name}.id,
                           json_set({self.collection.name}.data, '$."{field_name}"', je.value) as data
                    FROM {current_table} as {self.collection.name},
                         json_each(json_extract({self.collection.name}.data, '$.{field_name}')) as je
                    WHERE json_type(json_extract({self.collection.name}.data, '$.{field_name}')) = 'array'
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
                    f", json_each(json_extract({self.collection.name}.data, '$.{field_name}')) as je{i + 1}"
                )

            # Build WHERE clause to ensure all fields are arrays
            where_conditions = []
            for field_name in field_names:
                where_conditions.append(
                    f"json_type(json_extract({self.collection.name}.data, '$.{field_name}')) = 'array'"
                )
            where_clause = "WHERE " + " AND ".join(where_conditions)

            new_table = create_temp(
                "multi_unwound",
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
        """Process a $lookup stage using temporary tables."""
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

        # Create lookup temporary table
        new_table = create_temp("lookup", f"{select_clause} {from_clause}")
        return new_table

    def _process_sort_skip_limit_stage(
        self,
        create_temp: Callable,
        current_table: str,
        sort_spec: Optional[Dict[str, Any]],
        skip_value: int = 0,
        limit_value: Optional[int] = None,
    ) -> str:
        """Process sort/skip/limit stages using temporary tables."""
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

        # Create sorted/skipped/limited temporary table
        new_table = create_temp(
            "sorted",
            f"SELECT * FROM {current_table} {order_clause} {limit_clause}",
        )
        return new_table

    def _get_results_from_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Get results from a temporary table."""
        cursor = self.db.execute(f"SELECT * FROM {table_name}")
        results = []
        for row in cursor.fetchall():
            doc = self.collection._load(row[0], row[1])
            results.append(doc)
        return results


def can_process_with_temporary_tables(pipeline: List[Dict[str, Any]]) -> bool:
    """
    Determine if a pipeline can be processed with temporary tables.

    Args:
        pipeline: List of aggregation pipeline stages

    Returns:
        True if the pipeline can be processed with temporary tables, False otherwise
    """
    # Check if all stages are supported
    supported_stages = {
        "$match",
        "$unwind",
        "$sort",
        "$skip",
        "$limit",
        "$lookup",
    }

    for stage in pipeline:
        stage_name = next(iter(stage.keys()))
        if stage_name not in supported_stages:
            return False

    return True


def integrate_with_neosqlite(
    query_engine, pipeline: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Integration function that tries to use temporary tables for complex pipelines
    that the current NeoSQLite implementation can't optimize.

    This function can be integrated into the QueryEngine.aggregate_with_constraints method.

    Args:
        query_engine: The NeoSQLite QueryEngine instance
        pipeline: List of aggregation pipeline stages

    Returns:
        List of result documents
    """
    # First, try the existing SQL optimization approach
    try:
        query_result = query_engine.helpers._build_aggregation_query(pipeline)
        if query_result is not None:
            cmd, params, output_fields = query_result
            db_cursor = query_engine.collection.db.execute(cmd, params)
            if output_fields:
                # Handle results from a GROUP BY query
                from neosqlite.collection.json_helpers import (
                    neosqlite_json_loads,
                )

                results = []
                for row in db_cursor.fetchall():
                    processed_row = []
                    for i, value in enumerate(row):
                        # If this field contains a JSON array string, parse it
                        if (
                            output_fields[i] != "_id"
                            and isinstance(value, str)
                            and value.startswith("[")
                            and value.endswith("]")
                        ):
                            try:
                                processed_row.append(
                                    neosqlite_json_loads(value)
                                )
                            except:
                                processed_row.append(value)
                        else:
                            processed_row.append(value)
                    results.append(dict(zip(output_fields, processed_row)))
                return results
            else:
                # Handle results from a regular find query
                return [
                    query_engine.collection._load(row[0], row[1])
                    for row in db_cursor.fetchall()
                ]
    except Exception:
        # If SQL optimization fails, continue to next approach
        pass

    # Try the temporary table approach for supported pipelines
    if can_process_with_temporary_tables(pipeline):
        try:
            processor = TemporaryTableAggregationProcessor(
                query_engine.collection
            )
            return processor.process_pipeline(pipeline)
        except Exception:
            # If temporary table approach fails, continue to fallback
            pass

    # Fall back to the existing Python implementation
    # This is the existing code from the aggregate_with_constraints method
    docs: List[Dict[str, Any]] = list(query_engine.collection.find())
    for stage in pipeline:
        stage_name = next(iter(stage.keys()))
        match stage_name:
            case "$match":
                query = stage["$match"]
                docs = [
                    doc
                    for doc in docs
                    if query_engine.helpers._apply_query(query, doc)
                ]
            case "$sort":
                sort_spec = stage["$sort"]
                for key, direction in reversed(list(sort_spec.items())):
                    docs.sort(
                        key=lambda doc: query_engine.collection._get_val(
                            doc, key
                        ),
                        reverse=direction == DESCENDING,
                    )
            case "$skip":
                count = stage["$skip"]
                docs = docs[count:]
            case "$limit":
                count = stage["$limit"]
                docs = docs[:count]
            case "$project":
                projection = stage["$project"]
                docs = [
                    query_engine.helpers._apply_projection(projection, doc)
                    for doc in docs
                ]
            case "$group":
                group_spec = stage["$group"]
                docs = query_engine.helpers._process_group_stage(
                    group_spec, docs
                )
            case "$unwind":
                # Handle both string and object forms of $unwind
                unwind_spec = stage["$unwind"]
                if isinstance(unwind_spec, str):
                    # Legacy string form
                    field_path = unwind_spec.lstrip("$")
                    include_array_index = None
                    preserve_null_and_empty = False
                elif isinstance(unwind_spec, dict):
                    # New object form with advanced options
                    field_path = unwind_spec["path"].lstrip("$")
                    include_array_index = unwind_spec.get("includeArrayIndex")
                    preserve_null_and_empty = unwind_spec.get(
                        "preserveNullAndEmptyArrays", False
                    )
                else:
                    from neosqlite.exceptions import MalformedQueryException

                    raise MalformedQueryException(
                        f"Invalid $unwind specification: {unwind_spec}"
                    )

                unwound_docs = []
                for doc in docs:
                    array_to_unwind = query_engine.collection._get_val(
                        doc, field_path
                    )

                    # For nested fields, check if parent exists
                    field_parts = field_path.split(".")
                    process_document = True
                    if len(field_parts) > 1:
                        # This is a nested field
                        parent_path = ".".join(field_parts[:-1])
                        parent_value = query_engine.collection._get_val(
                            doc, parent_path
                        )
                        if parent_value is None:
                            # Parent is None or missing, don't process this document
                            process_document = False

                    if not process_document:
                        continue

                    if isinstance(array_to_unwind, list):
                        # Handle array values
                        if array_to_unwind:
                            # Non-empty array - unwind normally
                            for idx, item in enumerate(array_to_unwind):
                                from copy import deepcopy

                                new_doc = deepcopy(doc)
                                query_engine.collection._set_val(
                                    new_doc, field_path, item
                                )
                                # Add array index if requested
                                if include_array_index:
                                    new_doc[include_array_index] = idx
                                unwound_docs.append(new_doc)
                        elif preserve_null_and_empty:
                            # Empty array but preserve is requested
                            from copy import deepcopy

                            new_doc = deepcopy(doc)
                            query_engine.collection._set_val(
                                new_doc, field_path, None
                            )
                            # Add array index if requested
                            if include_array_index:
                                new_doc[include_array_index] = None
                            unwound_docs.append(new_doc)
                        # If empty array and preserve is False, don't add any documents
                    elif (
                        not isinstance(array_to_unwind, list)
                        and field_path in doc
                        and preserve_null_and_empty
                    ):
                        # Non-array value that exists in the document and preserve is requested
                        from copy import deepcopy

                        new_doc = deepcopy(doc)
                        # Keep the value as-is
                        # Add array index if requested
                        if include_array_index:
                            new_doc[include_array_index] = None
                        unwound_docs.append(new_doc)
                    # Missing fields are never preserved
                docs = unwound_docs
            case "$lookup":
                # Python fallback implementation for $lookup
                lookup_spec = stage["$lookup"]
                from_collection_name = lookup_spec["from"]
                local_field = lookup_spec["localField"]
                foreign_field = lookup_spec["foreignField"]
                as_field = lookup_spec["as"]

                # Get the from collection from the database
                from_collection = query_engine.collection._database[
                    from_collection_name
                ]

                # Process each document
                for doc in docs:
                    # Get the local field value
                    local_value = query_engine.collection._get_val(
                        doc, local_field
                    )

                    # Find matching documents in the from collection
                    matching_docs = []
                    for match_doc in from_collection.find():
                        foreign_value = from_collection._get_val(
                            match_doc, foreign_field
                        )
                        if local_value == foreign_value:
                            # Add the matching document (without _id)
                            match_doc_copy = match_doc.copy()
                            match_doc_copy.pop("_id", None)
                            matching_docs.append(match_doc_copy)

                    # Add the matching documents as an array field
                    doc[as_field] = matching_docs
            case _:
                from neosqlite.exceptions import MalformedQueryException

                raise MalformedQueryException(
                    f"Aggregation stage '{stage_name}' not supported"
                )
    return docs
