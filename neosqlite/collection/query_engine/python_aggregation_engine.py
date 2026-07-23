"""
Python fallback aggregation engine for NeoSQLite.

This module implements the Tier-3 (Python-based) aggregation pipeline
execution.  It is used only when the SQL-based tiers (Tier 1 and Tier 2)
cannot optimize a pipeline.

The logic was extracted from QueryEngine.aggregate_with_constraints()
to keep that class at a manageable size.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from ...exceptions import MalformedQueryException
from ..cursor import DESCENDING
from ..expr_evaluator import (
    AggregationContext,
    ExprEvaluator,
    _is_expression,
)

if TYPE_CHECKING:
    from . import QueryEngine

logger = logging.getLogger(__name__)


def execute_python_aggregation(
    query_engine: "QueryEngine",
    pipeline: list[dict[str, Any]],
    session: Any = None,
) -> list[dict[str, Any]]:
    """Execute *pipeline* entirely in Python (Tier-3 fallback).

    Args:
    query_engine: The ``QueryEngine`` instance that owns this pipeline.
    pipeline: List of aggregation stage dicts.
    session: Optional client session (currently unused by Tier 3).

    Returns:
    List of result documents after applying all pipeline stages.
    """
    docs: list[dict[str, Any]] = list(query_engine.find(session=session))

    # Store original documents for $$ROOT variable support
    # Each document is wrapped with metadata for variable scoping
    docs_with_context = [
        {"__doc__": doc, "__root__": deepcopy(doc)} for doc in docs
    ]

    for stage in pipeline:
        if not stage:
            raise MalformedQueryException("Empty pipeline stage")
        stage_name = next(iter(stage.keys())).strip()
        match stage_name:
            case "$match":
                query = stage["$match"]
                docs_with_context = [
                    dc
                    for dc in docs_with_context
                    if query_engine.helpers._apply_query(query, dc["__doc__"])
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
                            val = query_engine.collection._get_val(
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
                        "__doc__": query_engine.helpers._apply_projection(
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
                    except Exception as e:
                        # Fallback if evaluation fails
                        logger.debug(f"$replaceRoot evaluation failed: {e}")
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
                                if isinstance(target, dict) and part in target:
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
                grouped_docs = query_engine.helpers._process_group_stage(
                    group_spec, [dc["__doc__"] for dc in docs_with_context]
                )
                docs_with_context = [
                    {"__doc__": doc, "__root__": doc} for doc in grouped_docs
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
                    include_array_index = unwind_spec.get("includeArrayIndex")
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
                    array_to_unwind = query_engine.collection._get_val(
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
                                new_doc = deepcopy(doc)
                                query_engine.collection._set_val(
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
                from_collection = query_engine.collection._database[
                    from_collection_name
                ]

                # Process each document
                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    # Get the local field value
                    local_value = query_engine.collection._get_val(
                        doc, local_field
                    )

                    # Find matching documents in the from collection
                    matching_docs = []
                    for match_doc in from_collection.find(session=session):
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
                    data_column="data", db_connection=query_engine.collection.db
                )

                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    root = dc["__root__"]

                    # Create context for this document
                    ctx = AggregationContext()
                    ctx.bind_document(root)  # Bind original document as $$ROOT
                    ctx.update_current(doc)  # Set current document state

                    for new_field, expr in add_fields_spec.items():
                        if _is_expression(expr):
                            # Full expression - evaluate in Python with current context
                            value = evaluator_add._evaluate_expr_python(
                                expr, doc
                            )
                            query_engine.collection._set_val(
                                doc, new_field, value
                            )
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
                                query_engine.collection._set_val(
                                    doc, new_field, value
                                )
                            else:
                                # Regular field reference - may reference newly added field
                                source_field_name = expr[1:]
                                source_value = query_engine.collection._get_val(
                                    doc, source_field_name
                                )
                                query_engine.collection._set_val(
                                    doc, new_field, source_value
                                )
                        else:
                            # Literal value
                            query_engine.collection._set_val(
                                doc, new_field, expr
                            )

                    # Update $$CURRENT after all fields are added
                    ctx.update_current(doc)
            case "$setWindowFields":
                from ..query_helper.window_operators import (
                    process_set_window_fields,
                )

                window_spec = stage["$setWindowFields"]
                evaluator_window = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                docs_with_context = process_set_window_fields(
                    docs_with_context,
                    window_spec,
                    query_engine.collection,
                    evaluator_window,
                )
            case "$graphLookup":
                from ..query_helper.graph_lookup import process_graph_lookup

                graph_spec = stage["$graphLookup"]
                evaluator_graph = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                docs_with_context = process_graph_lookup(
                    docs_with_context,
                    graph_spec,
                    query_engine.collection,
                    evaluator_graph,
                )
            case "$fill":
                from ..query_helper.fill_stage import process_fill

                fill_spec = stage["$fill"]
                evaluator_fill = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                docs_with_context = process_fill(
                    docs_with_context,
                    fill_spec,
                    query_engine.collection,
                    evaluator_fill,
                )
            case "$sample":
                sample_spec = stage["$sample"]
                sample_size = sample_spec["size"]
                if sample_size < 0:
                    raise MalformedQueryException(
                        "$sample size must be non-negative"
                    )
                import random

                docs_with_context = random.sample(
                    docs_with_context,
                    min(sample_size, len(docs_with_context)),
                )
            case "$facet":
                facet_spec = stage["$facet"]
                facet_tables: dict[str, str] = {}

                # Get input documents
                sub_docs = [dc["__doc__"] for dc in docs_with_context]

                # Run each sub-pipeline, streaming results to temp tables
                for facet_name, sub_pipeline in facet_spec.items():
                    result_table = query_engine.helpers._run_subpipeline(
                        sub_pipeline, sub_docs
                    )
                    facet_tables[facet_name] = result_table

                # Load all results from temp tables and combine
                facet_results: dict[str, Any] = {}
                for facet_name, table_name in facet_tables.items():
                    cursor = query_engine.collection.db.execute(
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
                        query_engine.collection.db.execute(
                            f"DROP TABLE IF EXISTS {table_name}"
                        )
                    except Exception as e:
                        logger.debug(
                            f"Failed to drop facet temporary table '{table_name}': {e}"
                        )
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
                output_spec = bucket_spec.get("output", {"count": {"$sum": 1}})

                if not group_by or not boundaries:
                    docs_with_context = []
                    break

                sorted_boundaries = sorted(boundaries)

                # Group documents by bucket
                # MongoDB uses the lower boundary value as _id, not a string label
                buckets: dict[Any, list[dict[str, Any]]] = {}
                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    val = query_engine.collection._get_val(doc, group_by)

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
                    output_doc: dict[str, Any] = {"_id": bucket_id}
                    for field_name, accumulator in output_spec.items():
                        if "$sum" in accumulator:
                            sum_field = accumulator["$sum"]
                            if sum_field == 1:
                                output_doc[field_name] = len(bucket_docs)
                            else:
                                sum_field = sum_field.lstrip("$")
                                output_doc[field_name] = sum(
                                    query_engine.collection._get_val(
                                        d, sum_field
                                    )
                                    or 0
                                    for d in bucket_docs
                                )
                        elif "$avg" in accumulator:
                            avg_field = accumulator["$avg"].lstrip("$")
                            values = [
                                query_engine.collection._get_val(d, avg_field)
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
                                query_engine.collection._get_val(d, min_field)
                                for d in bucket_docs
                            ]
                            output_doc[field_name] = (
                                min(values) if values else None
                            )
                        elif "$max" in accumulator:
                            max_field = accumulator["$max"].lstrip("$")
                            values = [
                                query_engine.collection._get_val(d, max_field)
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
                        query_engine.collection._get_val(
                            dc["__doc__"], group_by
                        )
                        or 0
                    )

                sorted_docs = sorted(
                    docs_with_context,
                    key=get_group_val,
                )

                # Distribute into buckets
                bucket_size = max(1, len(sorted_docs) // num_buckets)
                bucket_list: list[list[dict[str, Any]]] = []
                bucket_bounds: list[tuple[Any, Any]] = []
                current_bucket = []
                current_min = None
                for dc in sorted_docs:
                    current_bucket.append(dc["__doc__"])
                    if current_min is None:
                        current_min = get_group_val(dc)
                    if (
                        len(current_bucket) >= bucket_size
                        and len(bucket_list) < num_buckets - 1
                    ):
                        bucket_list.append(current_bucket)
                        bucket_bounds.append((current_min, get_group_val(dc)))
                        current_bucket = []
                        current_min = None
                if current_bucket:
                    bucket_list.append(current_bucket)
                    bucket_bounds.append(
                        (
                            current_min,
                            (
                                get_group_val(sorted_docs[-1])
                                if sorted_docs
                                else current_min
                            ),
                        )
                    )

                # Build output documents
                new_docs = []
                for i, bucket_docs in enumerate(bucket_list):
                    output_doc2: dict[str, Any] = {
                        "_id": {
                            "min": bucket_bounds[i][0],
                            "max": bucket_bounds[i][1],
                        }
                    }
                    for field_name, accumulator in output_spec.items():
                        if "$sum" in accumulator:
                            sum_field = accumulator["$sum"]
                            if sum_field == 1:
                                output_doc2[field_name] = len(bucket_docs)
                            else:
                                sum_field = sum_field.lstrip("$")
                                output_doc2[field_name] = sum(
                                    query_engine.collection._get_val(
                                        d, sum_field
                                    )
                                    or 0
                                    for d in bucket_docs
                                )
                        elif "$avg" in accumulator:
                            avg_field = accumulator["$avg"].lstrip("$")
                            values = [
                                query_engine.collection._get_val(d, avg_field)
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
                other_coll = query_engine.collection._database[coll_name]
                other_docs = list(other_coll.find())

                # Apply pipeline if specified
                if pipeline:
                    other_docs = list(other_coll.aggregate(pipeline))

                # Combine documents
                current_docs = [dc["__doc__"] for dc in docs_with_context]
                combined_docs = current_docs + other_docs

                docs_with_context = [
                    {"__doc__": doc, "__root__": doc} for doc in combined_docs
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
                        "whenMatched": merge_spec.get("whenMatched", "replace"),
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
                    target_coll = query_engine.collection._database.client[
                        db_name
                    ][coll_name]
                else:
                    target_coll = query_engine.collection._database[
                        target_coll_name
                    ]

                # Process each document
                for dc in docs_with_context:
                    doc = dc["__doc__"]

                    # Get the "on" field value for matching
                    on_field = merge_options.get("on", "_id")
                    on_value = query_engine.collection._get_val(doc, on_field)

                    # Try to find existing document
                    existing = None
                    if on_value is not None:
                        existing = target_coll.find_one({on_field: on_value})

                    when_matched = merge_options.get("whenMatched", "replace")
                    when_not_matched = merge_options.get(
                        "whenNotMatched", "insert"
                    )

                    if existing:
                        match when_matched:
                            case "replace":
                                existing_id = existing.get("_id")
                                new_doc = {
                                    k: v for k, v in doc.items() if k != "_id"
                                }
                                if existing_id is not None:
                                    new_doc["_id"] = existing_id
                                target_coll.update_one(
                                    {on_field: on_value},
                                    {"$set": new_doc},
                                )
                                fields_to_remove = [
                                    k
                                    for k in existing
                                    if k not in new_doc and k != on_field
                                ]
                                if fields_to_remove:
                                    target_coll.update_one(
                                        {on_field: on_value},
                                        {
                                            "$unset": {
                                                f: "" for f in fields_to_remove
                                            }
                                        },
                                    )
                            case "merge":
                                update_doc = {
                                    k: v for k, v in doc.items() if k != "_id"
                                }
                                target_coll.update_one(
                                    {on_field: on_value},
                                    {"$set": update_doc},
                                )
                            case "keepExisting":
                                # Keep existing, don't update
                                pass
                            case "fail":
                                raise Exception(
                                    f"$merge failed: document with {on_field}={on_value} already exists"
                                )
                            # Note: "pipeline" mode not implemented
                    else:
                        # Document doesn't exist - handle based on whenNotMatched
                        match when_not_matched:
                            case "insert":
                                target_coll.insert_one(doc)
                            case "fail":
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
                    data_column="data", db_connection=query_engine.collection.db
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
                        except Exception as e:
                            logger.debug(f"Redaction evaluation failed: {e}")
                            return "$$DESCEND"

                    # If spec is a direct expression, evaluate it
                    if spec.startswith("$") if isinstance(spec, str) else False:
                        try:
                            result = evaluator_redact._evaluate_expr_python(
                                spec, doc
                            )
                            if result in ("$$KEEP", "$$DESCEND", "$$PRUNE"):
                                return result
                        except Exception as e:
                            logger.debug(
                                f"Redaction expression evaluation failed: {e}"
                            )
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
                    partitions: dict[tuple, list[dict[str, Any]]] = {}
                    for dc in docs_with_context:
                        doc = dc["__doc__"]

                        # Get partition key
                        if partition_by:
                            partition_key = tuple(
                                query_engine.collection._get_val(doc, pf)
                                for pf in partition_by
                            )
                        else:
                            partition_key = ()

                        if partition_key not in partitions:
                            partitions[partition_key] = []

                        field_val = query_engine.collection._get_val(doc, field)
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
                                match unit:
                                    case "year":
                                        current = current.replace(
                                            year=current.year + step
                                        )
                                    case "month":
                                        new_month = current.month + step
                                        new_year = (
                                            current.year + (new_month - 1) // 12
                                        )
                                        new_month = ((new_month - 1) % 12) + 1
                                        try:
                                            current = current.replace(
                                                year=new_year,
                                                month=new_month,
                                            )
                                        except ValueError:
                                            # Handle month-end edge
                                            break
                                    case "day":
                                        current = current + timedelta(days=step)
                                    case "hour":
                                        current = current + timedelta(
                                            hours=step
                                        )
                                    case "minute":
                                        current = current + timedelta(
                                            minutes=step
                                        )
                                    case "second":
                                        current = current + timedelta(
                                            seconds=step
                                        )
                                    case _:
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
                                        new_docs_with_context.append(item["dc"])
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
            case "$collStats":
                from ...sql_utils import quote_table_name

                coll_stats_spec = (
                    stage.get("$collStats") or stage.get(" $collStats") or {}
                )
                table_name = query_engine.collection.name
                quoted_table = quote_table_name(table_name)
                db = query_engine.collection.db

                count_cursor = db.execute(
                    f"SELECT COUNT(*) FROM {quoted_table}"
                )
                count = count_cursor.fetchone()[0] or 0

                size = 0
                try:
                    size_cursor = db.execute(
                        f"SELECT SUM(LENGTH(data)) FROM {quoted_table}"
                    )
                    size = size_cursor.fetchone()[0] or 0
                except Exception as e:
                    logger.debug(
                        f"Failed to calculate collection size for stats: {e}"
                    )
                    pass

                avg_obj_size = size / count if count > 0 else 0

                storage_size = 0
                total_index_size = 0
                index_sizes: dict[str, int] = {}

                try:
                    db.execute(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS temp.dbstat USING dbstat(main)"
                    )

                    storage_cursor = db.execute(
                        "SELECT SUM(pgsize) FROM dbstat WHERE name = ?",
                        (table_name,),
                    )
                    storage_size = storage_cursor.fetchone()[0] or 0

                    index_cursor = db.execute(
                        "SELECT name, SUM(pgsize) as size FROM dbstat "
                        "WHERE tbl_name = ? AND type = 'index' GROUP BY name",
                        (table_name,),
                    )
                    for row in index_cursor.fetchall():
                        idx_name, idx_size = row
                        if idx_name and idx_size:
                            index_sizes[idx_name] = idx_size
                            total_index_size += idx_size
                except Exception as e:
                    logger.debug(
                        f"Failed to calculate storage/index sizes for stats: {e}"
                    )
                    pass

                db_name = (
                    query_engine.collection._database.name
                    if query_engine.collection._database
                    else "unknown"
                )
                stats_result: dict[str, Any] = {
                    "ns": f"{db_name}.{table_name}",
                    "count": count,
                    "size": size,
                    "avgObjSize": avg_obj_size,
                    "storageSize": storage_size,
                    "totalIndexSize": total_index_size,
                    "indexSizes": index_sizes,
                }

                if coll_stats_spec and "count" in coll_stats_spec:
                    stats_result = {"count": count}
                elif coll_stats_spec and "storageStats" in coll_stats_spec:
                    stats_result = {
                        "ns": f"{db_name}.{table_name}",
                        "storageStats": {
                            "count": count,
                            "size": size,
                            "avgObjSize": avg_obj_size,
                            "storageSize": storage_size,
                            "totalIndexSize": total_index_size,
                            "indexSizes": index_sizes,
                        },
                    }

                docs_with_context = [
                    {"__doc__": stats_result, "__root__": stats_result}
                ]
            case _:
                raise MalformedQueryException(
                    f"Aggregation stage '{stage_name}' not supported"
                )
    query_engine._notify_tier_change("tier3", pipeline)
    return [dc["__doc__"] for dc in docs_with_context]
