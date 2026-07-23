from __future__ import annotations

import hashlib
import logging
from typing import Any

from ...sql_utils import quote_table_name
from ..expr_evaluator import ExprEvaluator
from ..json_path_utils import parse_json_path
from ..jsonb_support import JSONBContext
from ..sql_translator_unified import SQLTranslator
from .core import (
    can_process_with_temporary_tables,
    execute_2nd_tier_aggregation,
)
from .manager import DeterministicTempTableManager, aggregation_pipeline_context
from .operators import HASH_JOIN_MEMORY_THRESHOLD, OperatorsMixin
from .utils import (
    _contains_text_search,
    _json_extract_field_with_objectid_support,
    _sanitize_params,
)

logger = logging.getLogger(__name__)

__all__ = [
    "TemporaryTableAggregationProcessor",
    "DeterministicTempTableManager",
    "aggregation_pipeline_context",
    "can_process_with_temporary_tables",
    "execute_2nd_tier_aggregation",
    "_sanitize_params",
    "_json_extract_field_with_objectid_support",
    "_contains_text_search",
    "HASH_JOIN_MEMORY_THRESHOLD",
]


class TemporaryTableAggregationProcessor(OperatorsMixin):
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
        # Create ExprEvaluator for expression key support in $group
        self.expr_evaluator = ExprEvaluator(
            data_column="data", db_connection=collection.db
        )
        # Initialize JSONB capabilities
        self.jsonb = JSONBContext.from_db(self.db)
        self.sql_translator = SQLTranslator(
            collection.name,
            "data",
            "id",
            self.jsonb.jsonb_supported,
            self.jsonb.json_each_function,
        )
        # Track if pipeline has $sort stage (for $first/$last limitation)
        self._has_sort_stage = False
        # Track if we've warned about $text on temp tables (FTS after $unwind)
        self._text_on_temp_table_warned = False
        # Track if $unwind has been processed in the current pipeline
        self._has_unwind_in_pipeline = False

    def process_pipeline(
        self,
        pipeline: list[dict[str, Any]],
        is_count: bool = False,
        count_field: str | None = None,
        batch_size: int = 101,
    ) -> list[dict[str, Any]]:
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
            pipeline (list[dict[str, Any]]): A list of aggregation pipeline stages
                                             to process

        Returns:
            list[dict[str, Any]]: A list of result documents after processing the
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
                            if self.jsonb.jsonb_supported:
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

                    case "$facet":
                        current_table = self._process_facet_stage(
                            create_temp, current_table, stage["$facet"]
                        )
                        i += 1

                    case "$merge":
                        # $merge not supported in SQL tier for full functionality
                        raise NotImplementedError(
                            "$merge not supported in SQL tier - use force_fallback or simplify pipeline"
                        )

                    case "$redact":
                        # $redact not supported in SQL tier for full functionality
                        raise NotImplementedError(
                            "$redact not supported in SQL tier - use force_fallback or simplify pipeline"
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
