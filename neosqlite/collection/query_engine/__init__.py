from __future__ import annotations

from ...bulk_operations import BulkOperationExecutor
from ...exceptions import MalformedQueryException
from ...requests import DeleteOne, InsertOne, UpdateOne
from ...results import BulkWriteResult
from ..cursor import DESCENDING
from ..query_helper import QueryHelper
from ..raw_batch_cursor import RawBatchCursor
from ..sql_translator_unified import SQLTranslator
from ..expr_evaluator import ExprEvaluator, AggregationContext, _is_expression
from ..sql_tier_aggregator import SQLTierAggregator
from ..type_utils import validate_session
from copy import deepcopy
from neosqlite.collection.jsonb_support import supports_jsonb
from typing import Any, Dict, List, TYPE_CHECKING
import importlib.util

from .crud_operations import CRUDOperationsMixin
from .find_operations import FindOperationsMixin
from .query_methods import QueryMethodsMixin

if TYPE_CHECKING:
    from quez import CompressedQueue
    from ..client_session import ClientSession

# Check if quez is available
_HAS_QUEZ = importlib.util.find_spec("quez") is not None


class QueryEngine(CRUDOperationsMixin, FindOperationsMixin, QueryMethodsMixin):
    """
    A class that provides methods for querying and manipulating documents in a collection.

    The QueryEngine handles all database operations including inserting, updating, deleting,
    and finding documents. It also supports aggregation pipelines, bulk operations, and
    various utility methods for counting and retrieving distinct values.
    """

    def __init__(self, collection):
        """
        Initialize the QueryEngine with a collection.

        Args:
            collection: The collection instance this QueryEngine will operate on.
        """
        self.collection = collection
        self.helpers = QueryHelper(collection)
        # Check if JSONB is supported for this connection
        self._jsonb_supported = supports_jsonb(collection.db)
        self.sql_translator = SQLTranslator(
            collection.name, "data", "id", self._jsonb_supported
        )
        # Get translation cache size from connection (default: 100, 0 to disable)
        # collection._database is the NeoSQLite Connection, collection.db is sqlite3
        neosqlite_conn = collection._database
        cache_size = getattr(neosqlite_conn, "_translation_cache_size", 100)
        # Initialize SQL tier aggregator for optimized aggregation pipelines
        self.sql_tier_aggregator = SQLTierAggregator(
            collection,
            expr_evaluator=ExprEvaluator(
                data_column="data", db_connection=collection.db
            ),
            translation_cache_size=cache_size,
        )

    def cleanup(self) -> None:
        """Clean up resources used by the QueryEngine."""
        if hasattr(self, "helpers"):
            self.helpers.cleanup()

    def aggregate(
        self,
        pipeline: List[Dict[str, Any]],
        batch_size: int = 101,
        session: ClientSession | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Applies a list of aggregation pipeline stages to the collection.

        This method handles both simple and complex queries. For simpler queries,
        it leverages the database's native indexing capabilities to optimize
        performance. For more complex queries, it falls back to a Python-based
        processing mechanism.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The batch size for fetching results from database.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            List[Dict[str, Any]]: The list of documents after applying the aggregation pipeline.
        """
        validate_session(session, self.collection._database)
        return self.aggregate_with_constraints(
            pipeline, batch_size=batch_size, session=session
        )

    def aggregate_with_constraints(
        self,
        pipeline: List[Dict[str, Any]],
        batch_size: int = 101,
        memory_constrained: bool = False,
        session: ClientSession | None = None,
    ) -> List[Dict[str, Any]] | "CompressedQueue":
        """
        Applies a list of aggregation pipeline stages with memory constraints.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The batch size for processing large result sets.
            memory_constrained (bool): Whether to use memory-constrained processing.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            List[Dict[str, Any]] | CompressedQueue: The results as either a list or compressed queue.
        """
        validate_session(session, self.collection._database)
        # If memory_constrained is True and quez is available, use quez for processing
        if memory_constrained and _HAS_QUEZ:
            # Use quez for memory-constrained processing
            return self._aggregate_with_quez(pipeline, batch_size)

        # Try SQL Tier 1 optimization first (new CTE-based approach)
        try:
            if self.sql_tier_aggregator.can_optimize_pipeline(pipeline):
                sql, params = self.sql_tier_aggregator.build_pipeline_sql(
                    pipeline
                )
                if sql is not None:
                    db_cursor = self.collection.db.execute(sql, params)
                    results = []
                    # Use fetchmany to avoid loading all results into memory at once
                    while True:
                        rows = db_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            # Load document from data column
                            # Row structure:
                            # If root_data preserved: (id, _id, root_data, data) - len 4
                            # Normal: (id, _id, data) - len 3
                            # GROUP BY results might have id=NULL and data as a custom object

                            doc_data = row[-1]
                            doc_id = row[0]
                            stored_id = row[1]

                            if doc_data.startswith("{") and doc_data.endswith(
                                "}"
                            ):
                                # It's a JSON object (standard or GROUP BY result)
                                from neosqlite.collection.json_helpers import (
                                    neosqlite_json_loads,
                                )

                                document = neosqlite_json_loads(doc_data)
                                if (
                                    "_id" not in document
                                    and stored_id is not None
                                ):
                                    document["_id"] = (
                                        self.collection._parse_stored_id(
                                            stored_id
                                        )
                                    )
                                results.append(document)
                            else:
                                # Normal loading via _load
                                results.append(
                                    self.collection._load(
                                        doc_id, doc_data, stored_id=stored_id
                                    )
                                )
                    return results
        except Exception:
            # If SQL tier optimization fails, continue to next approach
            pass

        # Try existing SQL optimization (legacy CTE-based approach)
        try:
            query_result = self.helpers._build_aggregation_query(pipeline)
            if query_result is not None:
                cmd, params, output_fields = query_result
                db_cursor = self.collection.db.execute(cmd, params)
                if output_fields:
                    # Handle results from a GROUP BY query
                    from neosqlite.collection.json_helpers import (
                        neosqlite_json_loads,
                    )

                    results = []
                    # Use fetchmany to avoid loading all results into memory at once
                    while True:
                        rows = db_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            processed_row = []
                            for i, value in enumerate(row):
                                # If this field contains a JSON array string, parse it
                                # This handles $push and $addToSet results
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
                                    except Exception:
                                        processed_row.append(value)
                                else:
                                    processed_row.append(value)
                            results.append(
                                dict(zip(output_fields, processed_row))
                            )
                    return results
                else:
                    # Handle results from a regular find query
                    # Use fetchmany to avoid loading all results into memory at once
                    results = []
                    while True:
                        rows = db_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            # Row structure: (id, data) or (id, root_data, data)
                            if len(row) == 3:
                                # root_data is present, data is in row[2]
                                results.append(
                                    self.collection._load(row[0], row[2])
                                )
                            else:
                                # No root_data, data is in row[1]
                                results.append(
                                    self.collection._load(row[0], row[1])
                                )
                    return results
        except Exception:
            # If SQL optimization fails, continue to next approach
            pass

        # Try the temporary table approach for complex pipelines that the
        # current SQL optimization can't handle efficiently
        try:
            from ..temporary_table_aggregation import (
                execute_2nd_tier_aggregation,
            )

            # Use the temporary table aggregation which provides enhanced
            # SQL processing for complex pipelines
            return execute_2nd_tier_aggregation(
                self, pipeline, batch_size=batch_size
            )
        except NotImplementedError:
            # If temporary table approach indicates it needs Python fallback,
            # continue to fallback below
            pass
        except Exception:
            # If temporary table approach fails for other reasons,
            # continue to fallback below
            pass

        # Optimize $count in SQLite when possible
        if (
            pipeline
            and isinstance(pipeline[-1], dict)
            and "$count" in pipeline[-1]
        ):
            count_field = pipeline[-1]["$count"]
            if not pipeline[:-1]:
                # No previous stages, count all documents
                count = self.estimated_document_count()
                return [{count_field: count}]
            elif len(pipeline) == 2 and "$match" in pipeline[0]:
                # Only $match before $count, use count_documents
                filter = pipeline[0]["$match"]
                count = self.count_documents(filter)
                return [{count_field: count}]
            # For more complex pipelines, fall back to Python

        # Fallback to old method for complex queries (Python implementation)
        docs: List[Dict[str, Any]] = list(self.find())

        # Store original documents for $$ROOT variable support
        # Each document is wrapped with metadata for variable scoping
        docs_with_context = [
            {"__doc__": doc, "__root__": deepcopy(doc)} for doc in docs
        ]

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            match stage_name:
                case "$match":
                    query = stage["$match"]
                    docs_with_context = [
                        dc
                        for dc in docs_with_context
                        if self.helpers._apply_query(query, dc["__doc__"])
                    ]
                case "$sort":
                    sort_spec = stage["$sort"]
                    for key, direction in reversed(list(sort_spec.items())):

                        def make_sort_key(key, dir):
                            """
                            Create a sort key function for the given key and direction.
                            """

                            def sort_key(dc):
                                """
                                Extract sort key from document context.
                                """
                                val = self.collection._get_val(
                                    dc["__doc__"], key
                                )
                                # Handle None values - sort them last for ascending, first for descending
                                if val is None:
                                    return (0 if dir == DESCENDING else 1, None)
                                return (0, val)

                            return sort_key

                        sort_key_func = make_sort_key(key, direction)
                        docs_with_context.sort(
                            key=sort_key_func,
                            reverse=direction == DESCENDING,
                        )
                case "$skip":
                    count = stage["$skip"]
                    docs_with_context = docs_with_context[count:]
                case "$limit":
                    count = stage["$limit"]
                    docs_with_context = docs_with_context[:count]
                case "$project":
                    projection = stage["$project"]
                    docs_with_context = [
                        {
                            "__doc__": self.helpers._apply_projection(
                                projection, dc["__doc__"]
                            ),
                            "__root__": dc["__root__"],
                        }
                        for dc in docs_with_context
                    ]
                case "$replaceRoot" | "$replaceWith":
                    # Handle $replaceRoot and $replaceWith
                    if stage_name == "$replaceRoot":
                        new_root_expr = stage["$replaceRoot"].get("newRoot")
                    else:  # $replaceWith
                        new_root_expr = stage["$replaceWith"]

                    new_docs_with_context = []
                    for dc in docs_with_context:
                        # Evaluate the new root expression
                        evaluator = ExprEvaluator()
                        # Use _evaluate_operand_python to get the actual value (not forced to bool)
                        try:
                            new_doc = evaluator._evaluate_operand_python(
                                new_root_expr, dc["__doc__"]
                            )
                        except Exception:
                            # Fallback if evaluation fails
                            new_doc = dc["__doc__"]

                        # MongoDB requirement: result MUST be an object
                        if not isinstance(new_doc, dict):
                            raise MalformedQueryException(
                                f"$replaceRoot requires an object, got {type(new_doc).__name__}"
                            )

                        # Ensure _id is preserved if it was present in the original
                        if (
                            "_id" not in new_doc
                            and "__doc__" in dc
                            and "_id" in dc["__doc__"]
                        ):
                            new_doc["_id"] = dc["__doc__"]["_id"]

                        new_docs_with_context.append(
                            {"__doc__": new_doc, "__root__": dc["__root__"]}
                        )
                    docs_with_context = new_docs_with_context
                case "$unset":
                    # Handle $unset aggregation stage
                    unset_spec = stage["$unset"]
                    if isinstance(unset_spec, str):
                        fields_to_unset = [unset_spec]
                    elif isinstance(unset_spec, list):
                        fields_to_unset = unset_spec
                    else:
                        raise MalformedQueryException(
                            "$unset requires a string or a list of strings"
                        )

                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        for field in fields_to_unset:
                            # Use collection._get_val logic to navigate and pop
                            if "." in field:
                                parts = field.split(".")
                                target: dict[str, Any] | None = doc
                                for part in parts[:-1]:
                                    if (
                                        isinstance(target, dict)
                                        and part in target
                                    ):
                                        target = target[part]
                                    else:
                                        target = None
                                        break
                                if isinstance(target, dict):
                                    target.pop(parts[-1], None)
                            else:
                                doc.pop(field, None)
                case "$group":
                    group_spec = stage["$group"]
                    # For $group, we don't preserve __root__ since grouping creates new documents
                    grouped_docs = self.helpers._process_group_stage(
                        group_spec, [dc["__doc__"] for dc in docs_with_context]
                    )
                    docs_with_context = [
                        {"__doc__": doc, "__root__": doc}
                        for doc in grouped_docs
                    ]
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
                        include_array_index = unwind_spec.get(
                            "includeArrayIndex"
                        )
                        preserve_null_and_empty = unwind_spec.get(
                            "preserveNullAndEmptyArrays", False
                        )
                    else:
                        raise MalformedQueryException(
                            f"Invalid $unwind specification: {unwind_spec}"
                        )

                    unwound_docs_with_context = []
                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        root = dc["__root__"]
                        array_to_unwind = self.collection._get_val(
                            doc, field_path
                        )

                        # For nested fields, check if parent exists
                        # If parent is None or missing and we're trying to unwind a nested field,
                        # don't process this document
                        field_parts = field_path.split(".")
                        process_document = True
                        if len(field_parts) > 1:
                            # This is a nested field
                            parent_path = ".".join(field_parts[:-1])
                            parent_value = self.collection._get_val(
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
                                    new_doc = deepcopy(doc)
                                    self.collection._set_val(
                                        new_doc, field_path, item
                                    )
                                    # Add array index if requested
                                    if include_array_index:
                                        new_doc[include_array_index] = idx
                                    # Preserve __root__ for $$ROOT variable
                                    unwound_docs_with_context.append(
                                        {"__doc__": new_doc, "__root__": root}
                                    )
                            elif preserve_null_and_empty:
                                # Empty array but preserve is requested
                                new_doc = deepcopy(doc)
                                # Remove the field entirely (MongoDB behavior)
                                # For nested fields, we need to navigate to parent
                                if "." in field_path:
                                    # Handle nested field removal
                                    parts = field_path.split(".")
                                    current = new_doc
                                    for part in parts[:-1]:
                                        if part in current:
                                            current = current[part]
                                        else:
                                            break
                                    else:
                                        # Remove the final field
                                        if parts[-1] in current:
                                            del current[parts[-1]]
                                else:
                                    # Simple field removal
                                    if field_path in new_doc:
                                        del new_doc[field_path]
                                # Add array index if requested
                                if include_array_index:
                                    new_doc[include_array_index] = None
                                unwound_docs_with_context.append(
                                    {"__doc__": new_doc, "__root__": root}
                                )
                            # If empty array and preserve is False, don't add any documents
                        elif (
                            not isinstance(array_to_unwind, list)
                            and field_path in doc
                            and preserve_null_and_empty
                        ):
                            # Non-array value (None, string, number, etc.) that exists in the document and preserve is requested
                            new_doc = deepcopy(doc)
                            # Keep the value as-is
                            # Add array index if requested
                            if include_array_index:
                                new_doc[include_array_index] = None
                            unwound_docs_with_context.append(
                                {"__doc__": new_doc, "__root__": root}
                            )
                        # Missing fields (field_path not in doc) are never preserved
                        # Default case: non-array values are ignored unless they exist and preserveNullAndEmptyArrays is True
                    docs_with_context = unwound_docs_with_context
                case "$lookup":
                    # Python fallback implementation for $lookup
                    lookup_spec = stage["$lookup"]
                    from_collection_name = lookup_spec["from"]
                    local_field = lookup_spec["localField"]
                    foreign_field = lookup_spec["foreignField"]
                    as_field = lookup_spec["as"]

                    # Get the from collection from the database
                    from_collection = self.collection._database[
                        from_collection_name
                    ]

                    # Process each document
                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        # Get the local field value
                        local_value = self.collection._get_val(doc, local_field)

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
                case "$addFields":
                    add_fields_spec = stage["$addFields"]
                    # Create expression evaluator for this stage
                    evaluator_add = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )

                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        root = dc["__root__"]

                        # Create context for this document
                        ctx = AggregationContext()
                        ctx.bind_document(
                            root
                        )  # Bind original document as $$ROOT
                        ctx.update_current(doc)  # Set current document state

                        for new_field, expr in add_fields_spec.items():
                            if _is_expression(expr):
                                # Full expression - evaluate in Python with current context
                                value = evaluator_add._evaluate_expr_python(
                                    expr, doc
                                )
                                self.collection._set_val(doc, new_field, value)
                            elif isinstance(expr, str) and expr.startswith("$"):
                                # Field reference
                                if expr.startswith("$$"):
                                    # Aggregation variable
                                    if expr == "$$ROOT":
                                        # $$ROOT always refers to original document
                                        value = root.copy()
                                    elif expr == "$$CURRENT":
                                        # $$CURRENT refers to document as it evolves
                                        value = doc.copy()
                                    else:
                                        value = None
                                    self.collection._set_val(
                                        doc, new_field, value
                                    )
                                else:
                                    # Regular field reference - may reference newly added field
                                    source_field_name = expr[1:]
                                    source_value = self.collection._get_val(
                                        doc, source_field_name
                                    )
                                    self.collection._set_val(
                                        doc, new_field, source_value
                                    )
                            else:
                                # Literal value
                                self.collection._set_val(doc, new_field, expr)

                        # Update $$CURRENT after all fields are added
                        ctx.update_current(doc)
                case "$setWindowFields":
                    from ..query_helper.window_operators import (
                        process_set_window_fields,
                    )

                    window_spec = stage["$setWindowFields"]
                    evaluator_window = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )
                    docs_with_context = process_set_window_fields(
                        docs_with_context,
                        window_spec,
                        self.collection,
                        evaluator_window,
                    )
                case "$graphLookup":
                    from ..query_helper.graph_lookup import process_graph_lookup

                    graph_spec = stage["$graphLookup"]
                    evaluator_graph = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )
                    docs_with_context = process_graph_lookup(
                        docs_with_context,
                        graph_spec,
                        self.collection,
                        evaluator_graph,
                    )
                case "$fill":
                    from ..query_helper.fill_stage import process_fill

                    fill_spec = stage["$fill"]
                    evaluator_fill = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )
                    docs_with_context = process_fill(
                        docs_with_context,
                        fill_spec,
                        self.collection,
                        evaluator_fill,
                    )
                case "$sample":
                    sample_spec = stage["$sample"]
                    sample_size = sample_spec["size"]
                    import random

                    docs_with_context = random.sample(
                        docs_with_context,
                        min(sample_size, len(docs_with_context)),
                    )
                case "$unset":
                    unset_spec = stage["$unset"]
                    if isinstance(unset_spec, str):
                        unset_fields = [unset_spec]
                    else:
                        unset_fields = unset_spec
                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        for field in unset_fields:
                            # Handle nested fields
                            keys = field.split(".")
                            current = doc
                            for key in keys[:-1]:
                                if isinstance(current, dict) and key in current:
                                    current = current[key]
                                else:
                                    break
                            else:
                                if keys[-1] in current:
                                    del current[keys[-1]]
                case "$facet":
                    facet_spec = stage["$facet"]
                    facet_tables: Dict[str, str] = {}

                    # Get input documents
                    sub_docs = [dc["__doc__"] for dc in docs_with_context]

                    # Run each sub-pipeline, streaming results to temp tables
                    for facet_name, sub_pipeline in facet_spec.items():
                        result_table = self.helpers._run_subpipeline(
                            sub_pipeline, sub_docs
                        )
                        facet_tables[facet_name] = result_table

                    # Load all results from temp tables and combine
                    facet_results: Dict[str, Any] = {}
                    for facet_name, table_name in facet_tables.items():
                        cursor = self.collection.db.execute(
                            f"SELECT data FROM {table_name}"
                        )
                        from neosqlite.collection.json_helpers import (
                            neosqlite_json_loads,
                        )

                        facet_results[facet_name] = [
                            neosqlite_json_loads(row[0])
                            for row in cursor.fetchall()
                        ]
                        # Clean up temp table after loading
                        try:
                            self.collection.db.execute(
                                f"DROP TABLE IF EXISTS {table_name}"
                            )
                        except Exception:
                            pass

                    docs_with_context = [
                        {"__doc__": facet_results, "__root__": facet_results}
                    ]
                case "$count":
                    count_field = stage["$count"]
                    docs_with_context = [
                        {
                            "__doc__": {count_field: len(docs_with_context)},
                            "__root__": {count_field: len(docs_with_context)},
                        }
                    ]
                case "$bucket":
                    bucket_spec = stage["$bucket"]
                    group_by = bucket_spec.get("groupBy", "").lstrip("$")
                    boundaries = bucket_spec.get("boundaries", [])
                    default_label = bucket_spec.get("default", "Other")
                    output_spec = bucket_spec.get(
                        "output", {"count": {"$sum": 1}}
                    )

                    if not group_by or not boundaries:
                        docs_with_context = []
                        break

                    sorted_boundaries = sorted(boundaries)

                    # Group documents by bucket
                    # MongoDB uses the lower boundary value as _id, not a string label
                    buckets: Dict[Any, List[Dict[str, Any]]] = {}
                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        val = self.collection._get_val(doc, group_by)

                        # Skip documents with None values
                        if val is None:
                            continue

                        # Determine bucket - use lower boundary as key (MongoDB behavior)
                        bucket_key: Any = default_label
                        try:
                            for i in range(len(sorted_boundaries) - 1):
                                if (
                                    sorted_boundaries[i]
                                    <= val
                                    < sorted_boundaries[i + 1]
                                ):
                                    bucket_key = sorted_boundaries[
                                        i
                                    ]  # Use lower boundary as _id
                                    break
                            else:
                                # Last bucket (inclusive) - use last boundary
                                if val >= sorted_boundaries[-1]:
                                    bucket_key = sorted_boundaries[-1]
                        except TypeError:
                            # Comparison failed (e.g., mixed types), use default
                            bucket_key = default_label

                        if bucket_key not in buckets:
                            buckets[bucket_key] = []
                        buckets[bucket_key].append(doc)

                    # Build output documents
                    new_docs = []
                    for bucket_id, bucket_docs in sorted(buckets.items()):
                        output_doc: Dict[str, Any] = {"_id": bucket_id}
                        for field_name, accumulator in output_spec.items():
                            if "$sum" in accumulator:
                                sum_field = accumulator["$sum"]
                                if sum_field == 1:
                                    output_doc[field_name] = len(bucket_docs)
                                else:
                                    sum_field = sum_field.lstrip("$")
                                    output_doc[field_name] = sum(
                                        self.collection._get_val(d, sum_field)
                                        or 0
                                        for d in bucket_docs
                                    )
                            elif "$avg" in accumulator:
                                avg_field = accumulator["$avg"].lstrip("$")
                                values = [
                                    self.collection._get_val(d, avg_field)
                                    for d in bucket_docs
                                ]
                                output_doc[field_name] = (
                                    sum(values) / len(values) if values else 0
                                )
                            elif "$count" in accumulator:
                                output_doc[field_name] = len(bucket_docs)
                            elif "$min" in accumulator:
                                min_field = accumulator["$min"].lstrip("$")
                                values = [
                                    self.collection._get_val(d, min_field)
                                    for d in bucket_docs
                                ]
                                output_doc[field_name] = (
                                    min(values) if values else None
                                )
                            elif "$max" in accumulator:
                                max_field = accumulator["$max"].lstrip("$")
                                values = [
                                    self.collection._get_val(d, max_field)
                                    for d in bucket_docs
                                ]
                                output_doc[field_name] = (
                                    max(values) if values else None
                                )
                            else:
                                output_doc[field_name] = len(bucket_docs)
                        new_docs.append(output_doc)

                    docs_with_context = [
                        {"__doc__": doc, "__root__": doc} for doc in new_docs
                    ]
                case "$bucketAuto":
                    bucket_auto_spec = stage["$bucketAuto"]
                    group_by = bucket_auto_spec.get("groupBy", "").lstrip("$")
                    num_buckets = bucket_auto_spec.get("buckets", 10)
                    output_spec = bucket_auto_spec.get(
                        "output", {"count": {"$sum": 1}}
                    )

                    if not group_by or num_buckets <= 0:
                        docs_with_context = []
                        break

                    # Sort documents by groupBy field
                    def get_group_val(dc):
                        """
                        Extract the value to group by for a given document.

                        Args:
                            dc: Document with context from the aggregation stage.

                        Returns:
                            The value to group by or 0 if not found.
                        """
                        return (
                            self.collection._get_val(dc["__doc__"], group_by)
                            or 0
                        )

                    sorted_docs = sorted(
                        docs_with_context,
                        key=get_group_val,
                    )

                    # Distribute into buckets
                    bucket_size = max(1, len(sorted_docs) // num_buckets)
                    bucket_list: List[List[Dict[str, Any]]] = []
                    current_bucket = []
                    for dc in sorted_docs:
                        current_bucket.append(dc["__doc__"])
                        if (
                            len(current_bucket) >= bucket_size
                            and len(bucket_list) < num_buckets - 1
                        ):
                            bucket_list.append(current_bucket)
                            current_bucket = []
                    if current_bucket:
                        bucket_list.append(current_bucket)

                    # Build output documents
                    new_docs = []
                    for i, bucket_docs in enumerate(bucket_list):
                        output_doc2: Dict[str, Any] = {"_id": i + 1}
                        for field_name, accumulator in output_spec.items():
                            if "$sum" in accumulator:
                                sum_field = accumulator["$sum"]
                                if sum_field == 1:
                                    output_doc2[field_name] = len(bucket_docs)
                                else:
                                    sum_field = sum_field.lstrip("$")
                                    output_doc2[field_name] = sum(
                                        self.collection._get_val(d, sum_field)
                                        or 0
                                        for d in bucket_docs
                                    )
                            elif "$avg" in accumulator:
                                avg_field = accumulator["$avg"].lstrip("$")
                                values = [
                                    self.collection._get_val(d, avg_field)
                                    for d in bucket_docs
                                ]
                                output_doc2[field_name] = (
                                    sum(values) / len(values) if values else 0
                                )
                            elif "$count" in accumulator:
                                output_doc2[field_name] = len(bucket_docs)
                            else:
                                output_doc2[field_name] = len(bucket_docs)
                        new_docs.append(output_doc2)

                    docs_with_context = [
                        {"__doc__": doc, "__root__": doc} for doc in new_docs
                    ]
                case "$unionWith":
                    union_spec = stage["$unionWith"]
                    coll_name = union_spec.get("coll")
                    pipeline = union_spec.get("pipeline", [])

                    if not coll_name:
                        break

                    # Get documents from other collection
                    other_coll = self.collection._database[coll_name]
                    other_docs = list(other_coll.find())

                    # Apply pipeline if specified
                    if pipeline:
                        other_docs = list(other_coll.aggregate(pipeline))

                    # Combine documents
                    current_docs = [dc["__doc__"] for dc in docs_with_context]
                    combined_docs = current_docs + other_docs

                    docs_with_context = [
                        {"__doc__": doc, "__root__": doc}
                        for doc in combined_docs
                    ]
                case "$merge":
                    # $merge writes results to a collection
                    merge_spec = stage["$merge"]

                    # Handle different merge spec formats
                    if isinstance(merge_spec, str):
                        # Simple format: just collection name
                        target_coll_name = merge_spec
                        merge_options = {}
                    elif isinstance(merge_spec, dict):
                        # Full format with options
                        into = merge_spec.get("into", "")
                        if isinstance(into, dict):
                            target_coll_name = (
                                into.get("db", "") + "." + into.get("coll", "")
                            )
                        else:
                            target_coll_name = into
                        merge_options = {
                            "on": merge_spec.get("on", "_id"),
                            "whenMatched": merge_spec.get(
                                "whenMatched", "replace"
                            ),
                            "whenNotMatched": merge_spec.get(
                                "whenNotMatched", "insert"
                            ),
                        }
                    else:
                        target_coll_name = "merged"
                        merge_options = {}

                    # Get or create target collection
                    if "." in target_coll_name:
                        db_name, coll_name = target_coll_name.split(".", 1)
                        target_coll = self.collection._database.client[db_name][
                            coll_name
                        ]
                    else:
                        target_coll = self.collection._database[
                            target_coll_name
                        ]

                    # Process each document
                    for dc in docs_with_context:
                        doc = dc["__doc__"]

                        # Get the "on" field value for matching
                        on_field = merge_options.get("on", "_id")
                        on_value = self.collection._get_val(doc, on_field)

                        # Try to find existing document
                        existing = None
                        if on_value is not None:
                            existing = target_coll.find_one(
                                {on_field: on_value}
                            )

                        when_matched = merge_options.get(
                            "whenMatched", "replace"
                        )
                        when_not_matched = merge_options.get(
                            "whenNotMatched", "insert"
                        )

                        if existing:
                            # Document exists - handle based on whenMatched
                            if when_matched == "replace":
                                # Replace entire document (keep existing _id)
                                update_doc = {
                                    k: v for k, v in doc.items() if k != "_id"
                                }
                                target_coll.update_one(
                                    {on_field: on_value}, {"$set": update_doc}
                                )
                            elif when_matched == "merge":
                                # Merge fields (update existing with new values, exclude _id)
                                update_doc = {
                                    k: v for k, v in doc.items() if k != "_id"
                                }
                                target_coll.update_one(
                                    {on_field: on_value}, {"$set": update_doc}
                                )
                            elif when_matched == "keepExisting":
                                # Keep existing, don't update
                                pass
                            elif when_matched == "fail":
                                raise Exception(
                                    f"$merge failed: document with {on_field}={on_value} already exists"
                                )
                            # Note: "pipeline" mode not implemented
                        else:
                            # Document doesn't exist - handle based on whenNotMatched
                            if when_not_matched == "insert":
                                target_coll.insert_one(doc)
                            elif when_not_matched == "fail":
                                raise Exception(
                                    f"$merge failed: no document found with {on_field}={on_value}"
                                )

                    # After merge, return empty or pass through based on requirements
                    # MongoDB returns the merged documents for further pipeline processing
                    pass

                case "$redact":
                    # $redact filters document content based on conditions
                    redact_spec = stage["$redact"]

                    # Create evaluator for condition evaluation
                    evaluator_redact = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )

                    def apply_redact(doc, spec):
                        """Recursively apply redaction to a document."""
                        if not isinstance(doc, dict):
                            return doc

                        result = {}
                        for key, value in doc.items():
                            # Evaluate the redact condition for this field
                            redact_action = evaluate_redact_condition(
                                spec, doc, key, value
                            )

                            if redact_action == "$$KEEP":
                                # Keep the field as-is
                                result[key] = value
                            elif redact_action == "$$DESCEND":
                                # Keep and process sub-fields
                                if isinstance(value, dict):
                                    result[key] = apply_redact(value, spec)
                                elif isinstance(value, list):
                                    result[key] = [
                                        (
                                            apply_redact(item, spec)
                                            if isinstance(item, dict)
                                            else item
                                        )
                                        for item in value
                                    ]
                                else:
                                    result[key] = value
                            elif redact_action == "$$PRUNE":
                                # Remove this field (don't add to result)
                                pass

                        return result

                    def evaluate_redact_condition(spec, doc, key, value):
                        """Evaluate the redact condition and return KEEP/DESCEND/PRUNE."""
                        if "$cond" in spec:
                            cond = spec["$cond"]
                            if_expr = cond.get("if", {})
                            then_expr = cond.get("then", "$$DESCEND")
                            else_expr = cond.get("else", "$$DESCEND")

                            # Evaluate the condition
                            try:
                                cond_result = (
                                    evaluator_redact._evaluate_expr_python(
                                        if_expr, doc
                                    )
                                )
                                if cond_result:
                                    return then_expr
                                else:
                                    return else_expr
                            except Exception:
                                return "$$DESCEND"

                        # If spec is a direct expression, evaluate it
                        if (
                            spec.startswith("$")
                            if isinstance(spec, str)
                            else False
                        ):
                            try:
                                result = evaluator_redact._evaluate_expr_python(
                                    spec, doc
                                )
                                if result in ("$$KEEP", "$$DESCEND", "$$PRUNE"):
                                    return result
                            except Exception:
                                pass

                        return "$$DESCEND"

                    # Apply redaction to each document
                    new_docs_with_context = []
                    for dc in docs_with_context:
                        doc = dc["__doc__"]
                        root = dc["__root__"]

                        # Apply redaction
                        redacted_doc = apply_redact(doc, redact_spec)

                        # Check if document should be kept (not fully pruned)
                        if redacted_doc:  # Non-empty document
                            new_docs_with_context.append(
                                {"__doc__": redacted_doc, "__root__": root}
                            )

                    docs_with_context = new_docs_with_context

                case "$densify":
                    # $densify fills gaps in sequential data
                    densify_spec = stage["$densify"]

                    field = densify_spec.get("field")
                    range_spec = densify_spec.get("range", {})
                    partition_by = densify_spec.get("partitionByFields", [])
                    output_spec = densify_spec.get("output", {})

                    if not field:
                        # No field specified, pass through
                        pass
                    else:
                        # Get bounds
                        bounds = range_spec.get("bounds")
                        step = range_spec.get("step", 1)
                        unit = range_spec.get("unit", None)  # For dates

                        # Determine if we're working with dates or numbers
                        is_date = unit is not None

                        # Group documents by partition fields
                        partitions: Dict[tuple, List[Dict[str, Any]]] = {}
                        for dc in docs_with_context:
                            doc = dc["__doc__"]

                            # Get partition key
                            if partition_by:
                                partition_key = tuple(
                                    self.collection._get_val(doc, pf)
                                    for pf in partition_by
                                )
                            else:
                                partition_key = ()

                            if partition_key not in partitions:
                                partitions[partition_key] = []

                            field_val = self.collection._get_val(doc, field)
                            partitions[partition_key].append(
                                {"doc": doc, "field_val": field_val, "dc": dc}
                            )

                        # Generate densified output for each partition
                        new_docs_with_context = []
                        for partition_key, items in partitions.items():
                            # Get all field values in this partition
                            field_values = [
                                item["field_val"]
                                for item in items
                                if item["field_val"] is not None
                            ]

                            if not field_values:
                                continue

                            # Determine range
                            if bounds == "full":
                                min_val = min(field_values)
                                max_val = max(field_values)
                            elif isinstance(bounds, list) and len(bounds) >= 2:
                                min_val, max_val = bounds[0], bounds[1]
                            else:
                                min_val = min(field_values)
                                max_val = max(field_values)

                            # Generate all values in range
                            existing_values = set(field_values)
                            all_values = []

                            if is_date:
                                # Handle date ranges
                                from datetime import timedelta

                                current = min_val
                                while current <= max_val:
                                    all_values.append(current)
                                    if unit == "year":
                                        current = current.replace(
                                            year=current.year + step
                                        )
                                    elif unit == "month":
                                        new_month = current.month + step
                                        new_year = (
                                            current.year + (new_month - 1) // 12
                                        )
                                        new_month = ((new_month - 1) % 12) + 1
                                        try:
                                            current = current.replace(
                                                year=new_year, month=new_month
                                            )
                                        except ValueError:
                                            # Handle month-end edge cases
                                            break
                                    elif unit == "day":
                                        current = current + timedelta(days=step)
                                    elif unit == "hour":
                                        current = current + timedelta(
                                            hours=step
                                        )
                                    elif unit == "minute":
                                        current = current + timedelta(
                                            minutes=step
                                        )
                                    elif unit == "second":
                                        current = current + timedelta(
                                            seconds=step
                                        )
                                    else:
                                        break
                            else:
                                # Handle numeric ranges
                                current = min_val
                                while current <= max_val:
                                    all_values.append(current)
                                    current = current + step

                            # Create documents for all values
                            for val in all_values:
                                if val in existing_values:
                                    # Use existing document
                                    for item in items:
                                        if item["field_val"] == val:
                                            new_docs_with_context.append(
                                                item["dc"]
                                            )
                                            break
                                else:
                                    # Create new document with filled value
                                    for item in items:
                                        base_doc = deepcopy(item["doc"])
                                        base_doc[field] = val

                                        # Apply output spec for additional fields
                                        for (
                                            out_field,
                                            out_expr,
                                        ) in output_spec.items():
                                            if out_field != field:
                                                base_doc[out_field] = (
                                                    out_expr  # Could evaluate expression
                                                )

                                        new_docs_with_context.append(
                                            {
                                                "__doc__": base_doc,
                                                "__root__": base_doc,
                                            }
                                        )
                                        break  # Use first item as template

                        docs_with_context = new_docs_with_context
                case _:
                    raise MalformedQueryException(
                        f"Aggregation stage '{stage_name}' not supported"
                    )
        return [dc["__doc__"] for dc in docs_with_context]

    def explain_aggregation(
        self,
        pipeline: List[Dict[str, Any]],
        session: ClientSession | None = None,
    ) -> Dict[str, Any]:
        """
        Explain the execution plan for an aggregation pipeline.

        Args:
            pipeline (List[Dict[str, Any]]): The aggregation pipeline to explain.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            Dict[str, Any]: The execution plan explanation.
        """
        # 1. Try SQL Tier 1 optimization
        if self.sql_tier_aggregator.can_optimize_pipeline(pipeline):
            sql, params = self.sql_tier_aggregator.build_pipeline_sql(pipeline)
            if sql is not None:
                # Use EXPLAIN QUERY PLAN to get SQLite's plan
                explain_sql = f"EXPLAIN QUERY PLAN {sql}"
                db_cursor = self.collection.db.execute(explain_sql, params)
                plan = db_cursor.fetchall()
                return {
                    "tier": 1,
                    "type": "SQL Tier 1 (CTE-based)",
                    "sql": sql,
                    "params": params,
                    "sqlite_plan": plan,
                }

        # 2. Try legacy SQL optimization
        query_result = self.helpers._build_aggregation_query(pipeline)
        if query_result is not None:
            cmd, params, _ = query_result
            explain_sql = f"EXPLAIN QUERY PLAN {cmd}"
            db_cursor = self.collection.db.execute(explain_sql, params)
            plan = db_cursor.fetchall()
            return {
                "tier": 1,
                "type": "SQL Tier (Legacy)",
                "sql": cmd,
                "params": params,
                "sqlite_plan": plan,
            }

        # 3. Check if Tier 2 (Temp Table) can handle it
        from ..temporary_table_aggregation import (
            can_process_with_temporary_tables,
        )

        if can_process_with_temporary_tables(pipeline):
            return {
                "tier": 2,
                "type": "Temporary Table Aggregation",
                "pipeline": pipeline,
            }

        # 4. Fallback to Python
        return {
            "tier": 3,
            "type": "Python Fallback",
            "pipeline": pipeline,
        }

    def aggregate_raw_batches(
        self,
        pipeline: List[Dict[str, Any]],
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """
        Perform aggregation and retrieve batches of raw JSON.

        Similar to the :meth:`aggregate` method but returns a
        :class:`~neosqlite.raw_batch_cursor.RawBatchCursor`.

        This method returns raw JSON batches which can be more efficient for
        certain use cases where you want to process data in batches rather than
        individual documents.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The number of documents to include in each batch.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            RawBatchCursor instance.
        """
        validate_session(session, self.collection._database)
        return RawBatchCursor(
            self.collection,
            None,
            None,
            None,
            batch_size,
            pipeline=pipeline,
            session=session,
        )

    # --- Bulk Write methods ---
    def bulk_write(
        self,
        requests: List[Any],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> BulkWriteResult:
        """
        Execute bulk write operations on the collection.

        Args:
            requests: List of write operations to execute.
            ordered: If true, operations will be performed in order and will
                     raise an exception if a single operation fails.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            BulkWriteResult: A result object containing the number of matched,
                             modified, and inserted documents.
        """
        validate_session(session, self.collection._database)
        inserted_count = 0
        matched_count = 0
        modified_count = 0
        deleted_count = 0
        upserted_count = 0

        self.collection.db.execute("SAVEPOINT bulk_write")
        try:
            for req in requests:
                match req:
                    case InsertOne(document=doc):
                        self.insert_one(doc)
                        inserted_count += 1
                    case UpdateOne(filter=f, update=u, upsert=up):
                        update_res = self.update_one(f, u, up)
                        matched_count += update_res.matched_count
                        modified_count += update_res.modified_count
                        if update_res.upserted_id:
                            upserted_count += 1
                    case DeleteOne(filter=f):
                        delete_res = self.delete_one(f)
                        deleted_count += delete_res.deleted_count
            self.collection.db.execute("RELEASE SAVEPOINT bulk_write")
        except Exception as e:
            self.collection.db.execute("ROLLBACK TO SAVEPOINT bulk_write")
            raise e

        return BulkWriteResult(
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            upserted_count=upserted_count,
        )

    def _aggregate_with_quez(
        self, pipeline: List[Dict[str, Any]], batch_size: int = 101
    ) -> CompressedQueue:
        """
        Process aggregation pipeline with quez compressed queue for memory efficiency.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The batch size for quez queue processing.

        Returns:
            CompressedQueue: A compressed queue containing the results.
        """
        try:
            if _HAS_QUEZ:
                from quez import CompressedQueue

                # Create a compressed queue for results with a reasonable size
                # Use unbounded queue to avoid blocking during population
                result_queue = CompressedQueue()

            # Get results from normal aggregation
            results = self.aggregate(pipeline)

            # Add all results to the compressed queue
            for result in results:
                result_queue.put(result)

            return result_queue

        except ImportError:
            # If quez is not available, fall back to normal processing
            # This should never happen since we check for quez availability before calling this method
            raise RuntimeError("Quez is not available but was expected to be")

    def initialize_ordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an ordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for ordered bulk operations.
        """
        return BulkOperationExecutor(self.collection, ordered=True)

    def initialize_unordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an unordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for unordered bulk operations.
        """
        return BulkOperationExecutor(self.collection, ordered=False)
