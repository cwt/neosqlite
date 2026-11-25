"""Query optimization utilities for NeoSQLite collections.

This module provides a mixin class with methods for query cost estimation,
index analysis, and pipeline optimization.
"""

from typing import TYPE_CHECKING, Any

from ...sql_utils import validate_identifier
from ..index_manager import _load_index_keys

if TYPE_CHECKING:
    from .. import Collection


def _extract_match_fields(match_spec: dict[str, Any]) -> set[str] | None:
    """Collect field paths referenced in a ``$match`` specification.

    Returns ``None`` if the specification contains query-level operators whose
    dependencies cannot be statically determined or safely reordered (such as
    ``$expr``, ``$where``, ``$text``, or ``$jsonSchema``), or non-dict inputs.

    Args:
        match_spec: A ``$match`` filter dictionary.

    Returns:
        set[str] | None: Referenced document field paths, or ``None`` if the
        filter must stay in place.
    """
    if not isinstance(match_spec, dict):
        return None
    fields: set[str] = set()
    for key, value in match_spec.items():
        if key in ("$and", "$or", "$nor"):
            if not isinstance(value, list):
                return None
            for sub in value:
                sub_fields = _extract_match_fields(sub)
                if sub_fields is None:
                    return None
                fields.update(sub_fields)
        elif key.startswith("$"):
            return None
        else:
            fields.add(key)
    return fields


def _fields_overlap(field_a: str, field_b: str) -> bool:
    """Return True if two dotted field paths equal or one nests inside another."""
    return (
        field_a == field_b
        or field_a.startswith(f"{field_b}.")
        or field_b.startswith(f"{field_a}.")
    )


def _can_match_cross_stage(
    match_fields: set[str] | None, prev_stage: dict[str, Any]
) -> bool:
    """Return whether a ``$match`` stage may safely bubble left of ``prev_stage``."""
    if (
        match_fields is None
        or not isinstance(prev_stage, dict)
        or not prev_stage
    ):
        return False
    stage_name = next(iter(prev_stage.keys()))
    if stage_name == "$sort":
        return True
    if stage_name == "$unwind":
        unwind_spec = prev_stage["$unwind"]
        unwound_path: str | None = None
        index_field: str | None = None
        if isinstance(unwind_spec, str) and unwind_spec.startswith("$"):
            unwound_path = unwind_spec[1:]
        elif isinstance(unwind_spec, dict):
            raw_path = unwind_spec.get("path")
            if isinstance(raw_path, str) and raw_path.startswith("$"):
                unwound_path = raw_path[1:]
            idx_val = unwind_spec.get("includeArrayIndex")
            if isinstance(idx_val, str):
                index_field = idx_val
        if not unwound_path:
            return False
        if any(_fields_overlap(f, unwound_path) for f in match_fields):
            return False
        if index_field and any(
            _fields_overlap(f, index_field) for f in match_fields
        ):
            return False
        return True
    if stage_name == "$lookup":
        lookup_spec = prev_stage["$lookup"]
        if not isinstance(lookup_spec, dict):
            return False
        as_field = lookup_spec.get("as")
        if not isinstance(as_field, str) or not as_field:
            return False
        return not any(_fields_overlap(f, as_field) for f in match_fields)
    if stage_name in ("$addFields", "$set"):
        add_spec = prev_stage[stage_name]
        if not isinstance(add_spec, dict):
            return False
        return not any(
            _fields_overlap(f, added)
            for f in match_fields
            for added in add_spec.keys()
        )
    if stage_name == "$unset":
        unset_spec = prev_stage["$unset"]
        if isinstance(unset_spec, str):
            unset_fields = [unset_spec]
        elif isinstance(unset_spec, list):
            unset_fields = [u for u in unset_spec if isinstance(u, str)]
        else:
            return False
        return not any(
            _fields_overlap(f, u) for f in match_fields for u in unset_fields
        )
    return False


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

    def _estimate_query_cost(
        self,
        query: dict[str, Any],
        indexed_fields: set[str] | list[str] | None = None,
    ) -> float:
        """Estimate the cost of executing a query based on index availability.

        Lower cost values indicate more selective or index-backed queries.
        Weights ``_id`` filters by ``0.1``, indexed fields by ``0.3``, and
        recursively multiplies subquery costs for logical operators.

        Args:
            query: Dictionary representing the query filter criteria.
            indexed_fields: Optional pre-fetched collection index field names
                to avoid repeated ``sqlite_master`` lookups on recursive calls.

        Returns:
            float: Estimated cost of the query (lower is better).
        """
        # Get indexed fields once and convert to set for O(1) lookups
        indexed_set = (
            set(self._get_indexed_fields())
            if indexed_fields is None
            else (
                indexed_fields
                if isinstance(indexed_fields, set)
                else set(indexed_fields)
            )
        )

        # Base cost
        cost = 1.0

        # Check if we can use indexes for any fields in the query
        for field, value in query.items():
            if field in ("$and", "$or", "$nor", "$not"):
                # Handle logical operators recursively
                if isinstance(value, list):
                    for subquery in value:
                        if isinstance(subquery, dict):
                            cost *= self._estimate_query_cost(
                                subquery, indexed_fields=indexed_set
                            )
                elif isinstance(value, dict):
                    cost *= self._estimate_query_cost(
                        value, indexed_fields=indexed_set
                    )
            elif field == "_id":
                # _id field is always indexed (it's a column)
                cost *= 0.1  # Very low cost for _id queries
            elif field in indexed_set:
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

    def _bubble_matches_left(
        self,
        pipeline: list[dict[str, Any]],
        *,
        only_indexed: bool,
        indexed_fields: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Bubble eligible ``$match`` stages leftward one barrier-safe step at a time."""
        optimized = list(pipeline)
        for i in range(len(optimized)):
            stage = optimized[i]
            if (
                not isinstance(stage, dict)
                or next(iter(stage.keys()), None) != "$match"
            ):
                continue
            match_fields = _extract_match_fields(stage["$match"])
            if match_fields is None:
                continue
            if only_indexed:
                assert indexed_fields is not None
                if not any(
                    f == "_id" or f in indexed_fields for f in match_fields
                ):
                    continue
            pos = i
            while pos > 0 and _can_match_cross_stage(
                match_fields, optimized[pos - 1]
            ):
                optimized[pos - 1], optimized[pos] = (
                    optimized[pos],
                    optimized[pos - 1],
                )
                pos -= 1
        return optimized

    def _optimize_match_pushdown(
        self, pipeline: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Optimize a pipeline by bubbling eligible ``$match`` stages earlier.

        Moves ``$match`` stages leftward across one adjacent stage at a time
        only when semantically safe: never across ``$limit``, ``$skip``,
        ``$sample``, ``$group``, ``$bucket``, ``$bucketAuto``, ``$facet``,
        ``$count``, ``$project``, window/densify/fill/redact/replace stages,
        ``$out``, ``$merge``, ``$unionWith``, or ``$graphLookup``, and only
        across ``$unwind``/``$lookup``/``$addFields``/``$set``/``$unset`` when
        no referenced match field overlaps modified or generated paths.

        Args:
            pipeline: Aggregation pipeline stages to optimize.

        Returns:
            list[dict[str, Any]]: Optimized pipeline stage list.
        """
        if len(pipeline) < 2:
            return pipeline

        return self._bubble_matches_left(pipeline, only_indexed=False)

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
        """Reorder pipeline stages to place index-backed matches earlier.

        Moves ``$match`` stages that reference ``_id`` or an indexed field
        leftward only across stages that preserve filter semantics.

        Args:
            pipeline: Original list of aggregation pipeline stages.

        Returns:
            list[dict[str, Any]]: Reordered pipeline stage list.
        """
        if not pipeline:
            return pipeline

        indexed_fields = set(self._get_indexed_fields())
        return self._bubble_matches_left(
            pipeline, only_indexed=True, indexed_fields=indexed_fields
        )
