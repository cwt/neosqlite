"""
Aggregation pipeline methods for NeoSQLite.

This module contains the AggregationMixin class, which provides Python-based
aggregation pipeline processing.  SQL-based aggregation lives in
_sql_aggregation.py (SqlAggregationMixin).
"""

import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from ..expr_evaluator import (
    AggregationContext,
    ExprEvaluator,
    _is_expression,
)

logger = logging.getLogger(__name__)

from ._sql_aggregation import SqlAggregationMixin

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext


class AggregationMixin(SqlAggregationMixin):
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
        self.jsonb.jsonb_supported: Whether JSONB is supported
        self.jsonb.json_function_prefix: "json" or "jsonb"
        self.jsonb.json_each_function: "json_each" or "jsonb_each"
        self._build_simple_where_clause: Method to build WHERE clauses
        self._reorder_pipeline_for_indexes: Method to reorder pipelines
        self._estimate_pipeline_cost: Method to estimate costs
        self._optimize_match_pushdown: Method to optimize match pushdown
        self._is_datetime_indexed_field: Method to check datetime indexes
        self._build_group_query: Method to build group queries
        self._apply_query: Method to apply queries to documents
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _build_simple_where_clause: Any
    _reorder_pipeline_for_indexes: Any
    _estimate_pipeline_cost: Any
    _optimize_match_pushdown: Any
    _is_datetime_indexed_field: Any
    _apply_query: Any

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

            if include_id and "_id" in doc:
                projected_doc["_id"] = doc["_id"]

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
