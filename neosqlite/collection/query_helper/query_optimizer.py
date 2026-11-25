"""Query optimization utilities for NeoSQLite collections.

This module provides a mixin class with methods for query cost estimation,
index analysis, and pipeline optimization.
"""

from typing import TYPE_CHECKING, Any

from ...sql_utils import validate_identifier
from ..index_manager import _load_index_keys

if TYPE_CHECKING:
    from .. import Collection


class QueryOptimizerMixin:
    """Mixin class providing query optimization and cost-estimation methods.

    Expects the host class to define ``self.collection`` (providing ``db``,
    ``name``, and ``estimated_document_count``) as well as ``_jsonb_supported``
    and ``_json_function_prefix`` attributes.
    """

    collection: "Collection"
    _jsonb_supported: bool
    _json_function_prefix: str

    def _get_indexed_fields(self) -> list[str]:
        """Get a list of indexed fields for this collection.

        Reads SQLite index names matching ``idx_<table_name>_%`` and resolves
        stored index key definitions via ``_load_index_keys``, falling back to
        parsing legacy index names while excluding the automatic ``id`` index.

        Returns:
            list[str]: Field names that have indexes on this collection.
        """
        # Get indexes that match our naming convention on this table
        coll_name = validate_identifier(self.collection.name)
        escaped_name = (
            coll_name.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND tbl_name = ? AND name LIKE ? ESCAPE '\\'"
        )
        like_pattern = f"idx_{escaped_name}\\_%"
        indexes = self.collection.db.execute(
            cmd, (coll_name, like_pattern)
        ).fetchall()

        indexed_fields = []
        prefix_len = len(f"idx_{coll_name}_")
        for idx in indexes:
            index_name = idx[0]
            stored = _load_index_keys(self.collection.db, index_name)
            if stored:
                indexed_fields.extend(stored)
                continue
            # Legacy fallback: derive from the (lossy) index name
            key_name = index_name[prefix_len:]
            # Convert underscores back to dots for nested keys
            key_name = key_name.replace("_", ".")
            # Skip the automatically created _id index since it should be hidden
            # like MongoDB's automatic _id index
            if key_name == "id":  # This corresponds to the _id column index
                continue
            indexed_fields.append(key_name)

        return indexed_fields

    def _estimate_result_size(self, pipeline: list[dict[str, Any]]) -> int:
        """Estimate the size of the aggregation result in bytes.

        Starts from ``estimated_document_count()``, applies heuristic row-count
        multipliers for ``$match``, ``$limit``, ``$skip``, ``$unwind``, and
        ``$group`` stages, clamps the count to ``[0, base_count * 10]``, and
        multiplies by an assumed 1 KB average document size.

        Args:
            pipeline: Aggregation pipeline stages to analyze.

        Returns:
            int: Estimated result size in bytes.
        """
        # Get the base collection size
        base_count = self.collection.estimated_document_count()

        # Apply pipeline stages to estimate result size
        estimated_count = base_count
        estimated_avg_doc_size = 1024  # Default estimate of 1KB per document

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            match stage_name:
                case "$match":
                    # Matches typically reduce the result set
                    # For now, we'll use a rough estimate
                    estimated_count = max(1, int(estimated_count * 0.5))
                case "$limit":
                    limit_count = stage["$limit"]
                    estimated_count = min(estimated_count, limit_count)
                case "$skip":
                    skip_count = stage["$skip"]
                    estimated_count = max(0, estimated_count - skip_count)
                case "$unwind":
                    # Unwind operations can multiply the result set
                    # This is a very rough estimate
                    estimated_count = (
                        estimated_count * 3
                    )  # Assume 3 elements per array on average
                case "$group":
                    # Group operations typically reduce the result set
                    # This is a very rough estimate
                    estimated_count = max(1, int(estimated_count * 0.1))
                case _:
                    # For other operations, we'll assume they don't significantly change the size
                    pass

        # Apply some limits to prevent extreme estimates
        estimated_count = min(
            estimated_count, base_count * 10
        )  # Cap at 10x the base count
        estimated_count = max(estimated_count, 0)  # Ensure non-negative

        return estimated_count * estimated_avg_doc_size

    def _estimate_query_cost(self, query: dict[str, Any]) -> float:
        """Estimate the cost of executing a query based on index availability.

        Lower cost values indicate more selective or index-backed queries.
        Weights ``_id`` filters by ``0.1``, indexed fields by ``0.3``, and
        recursively multiplies subquery costs for logical operators.

        Args:
            query: Dictionary representing the query filter criteria.

        Returns:
            float: Estimated cost of the query (lower is better).
        """
        # Get indexed fields
        indexed_fields = self._get_indexed_fields()

        # Base cost
        cost = 1.0

        # Check if we can use indexes for any fields in the query
        for field, value in query.items():
            if field in ("$and", "$or", "$nor", "$not"):
                # Handle logical operators recursively
                if isinstance(value, list):
                    for subquery in value:
                        if isinstance(subquery, dict):
                            cost *= self._estimate_query_cost(subquery)
                elif isinstance(value, dict):
                    cost *= self._estimate_query_cost(value)
            elif field == "_id":
                # _id field is always indexed (it's a column)
                cost *= 0.1  # Very low cost for _id queries
            elif field in indexed_fields:
                # Field is indexed, reduce cost
                cost *= 0.3  # Lower cost when using an index
            else:
                # Field is not indexed, increase cost
                cost *= 1.0  # No change for non-indexed fields

        return cost

    def _estimate_pipeline_cost(self, pipeline: list[dict[str, Any]]) -> float:
        """Estimate the total cost of executing an aggregation pipeline.

        Sums per-stage cost weights scaled by a cumulative data-volume
        multiplier across stages; lower values indicate cheaper pipelines.

        Args:
            pipeline: List of aggregation pipeline stage dictionaries.

        Returns:
            float: Estimated cost of the pipeline (lower is better).
        """
        total_cost = 0.0
        cumulative_multiplier = (
            1.0  # Represents how much data flows through each stage
        )

        for i, stage in enumerate(pipeline):
            stage_name = next(iter(stage.keys()))
            stage_cost = 0.0

            match stage_name:
                case "$match":
                    # Estimate cost of match stage
                    query = stage["$match"]
                    stage_cost = self._estimate_query_cost(query)

                    # Matches early in the pipeline are more beneficial because they reduce
                    # the amount of data flowing to later stages
                    stage_cost *= cumulative_multiplier

                    # Update data flow multiplier based on selectivity
                    # Assume matches reduce data by 50% on average
                    cumulative_multiplier *= 0.5

                case "$sort":
                    # Sort operations have moderate cost, weighted by data volume
                    stage_cost = 1.0 * cumulative_multiplier
                case "$skip":
                    # Skip operations have low cost
                    stage_cost = 0.1 * cumulative_multiplier
                case "$limit":
                    # Limit operations have low cost but dramatically reduce data flow
                    stage_cost = 0.1 * cumulative_multiplier

                    # Limits significantly reduce data flow to subsequent stages
                    cumulative_multiplier *= (
                        0.1  # Assume limits reduce data by 90%
                    )

                case "$group":
                    # Group operations have high cost (require processing all data)
                    stage_cost = 5.0 * cumulative_multiplier

                    # Groups typically reduce data significantly
                    cumulative_multiplier *= (
                        0.2  # Assume groups reduce data by 80%
                    )

                case "$unwind":
                    # Unwind operations multiply the data size, increasing cost and data flow
                    stage_cost = 2.0 * cumulative_multiplier

                    # Unwinds increase data volume (assume 5x increase on average)
                    cumulative_multiplier *= 5.0

                case "$lookup":
                    # Lookup operations have high cost (joins)
                    stage_cost = 3.0 * cumulative_multiplier

                    # Lookups may increase data slightly
                    cumulative_multiplier *= 1.2

                case _:
                    # Unknown operations have moderate cost
                    stage_cost = 1.5 * cumulative_multiplier

                    # Assume unknown operations don't significantly change data volume
                    # cumulative_multiplier stays the same

            total_cost += stage_cost

        return total_cost

    def _optimize_match_pushdown(
        self, pipeline: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Optimize a pipeline by moving eligible ``$match`` stages earlier.

        Moves ``$match`` stages ahead of other stages when the pipeline has
        at least two stages and contains expensive stages (``$unwind``,
        ``$group``, or ``$lookup``). Stages containing ``$text`` or
        ``$jsonSchema`` are kept in place so their semantics stay intact.

        Args:
            pipeline: Aggregation pipeline stages to optimize.

        Returns:
            list[dict[str, Any]]: Optimized pipeline stage list.
        """
        if len(pipeline) < 2:
            return pipeline

        # Look for patterns where we can push matches down
        optimized = pipeline.copy()

        # Find all $match stages
        match_stages = []
        other_stages = []

        for i, stage in enumerate(optimized):
            stage_name = next(iter(stage.keys()))
            if stage_name == "$match":
                # Don't push down $match with $text search or $jsonSchema - they need to operate
                # on the data as transformed by previous stages or required specific handling
                match_spec = stage["$match"]
                if "$text" in match_spec or "$jsonSchema" in match_spec:
                    other_stages.append((i, stage))
                else:
                    match_stages.append((i, stage))
            else:
                other_stages.append((i, stage))

        # If we have matches and expensive operations, consider reordering
        expensive_ops = {"$unwind", "$group", "$lookup"}
        has_expensive_ops = any(
            next(iter(stage.keys())) in expensive_ops
            for _, stage in other_stages
        )

        if match_stages and has_expensive_ops:
            # Move matches to the front to filter early
            match_stage_items = [stage for _, stage in match_stages]
            other_stage_items = [stage for _, stage in other_stages]
            return match_stage_items + other_stage_items

        return optimized

    def _is_datetime_indexed_field(self, field: str) -> bool:
        """Check if a field has a UTC datetime index in ``sqlite_master``.

        Datetime indexes follow the naming pattern
        ``idx_{collection}_{field}_utc`` with dots replaced by underscores.

        Args:
            field: Field name (supports dot notation) to check.

        Returns:
            bool: ``True`` if the datetime index exists, ``False`` otherwise.
        """
        # Construct the expected index name for datetime indexes
        # Convert dots to underscores in field name
        field_name_for_index = field.replace(".", "_")
        coll_name = validate_identifier(self.collection.name)
        expected_datetime_index_name = (
            f"idx_{coll_name}_{field_name_for_index}_utc"
        )

        # Query the SQLite master table to check if this specific index exists
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
            (expected_datetime_index_name,),
        )
        return cursor.fetchone() is not None

    def _reorder_pipeline_for_indexes(
        self, pipeline: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Reorder pipeline stages to place index-backed matches first.

        Moves ``$match`` stages that reference ``_id`` or an indexed field
        (either directly or nested inside ``$and``/``$or`` lists) to the start
        of the pipeline ahead of all other stages.

        Args:
            pipeline: Original list of aggregation pipeline stages.

        Returns:
            list[dict[str, Any]]: Reordered pipeline stage list.
        """
        if not pipeline:
            return pipeline

        # Get indexed fields
        indexed_fields = set(self._get_indexed_fields())

        # Separate match stages with indexed fields from others
        indexed_matches = []
        other_stages = []

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            if stage_name == "$match":
                # Check if this match uses indexed fields
                match_query = stage["$match"]
                has_indexed_field = False

                # Simple check for direct field references
                for field in match_query.keys():
                    if field in indexed_fields or field == "_id":
                        has_indexed_field = True
                        break

                # For logical operators, check nested fields
                if not has_indexed_field:
                    for field, value in match_query.items():
                        if field in ("$and", "$or") and isinstance(value, list):
                            for condition in value:
                                if isinstance(condition, dict):
                                    for subfield in condition.keys():
                                        if (
                                            subfield in indexed_fields
                                            or subfield == "_id"
                                        ):
                                            has_indexed_field = True
                                            break
                                    if has_indexed_field:
                                        break
                        elif field == "_id":
                            has_indexed_field = True

                if has_indexed_field:
                    indexed_matches.append(stage)
                else:
                    other_stages.append(stage)
            else:
                other_stages.append(stage)

        # Return reordered pipeline: indexed matches first, then other stages
        return indexed_matches + other_stages
