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
    """Execute multi-stage aggregation pipelines using SQLite temp tables.

    Coordinates deterministic temporary table creation via
    ``aggregation_pipeline_context``, groups compatible consecutive stages
    (such as ``$unwind`` runs and ``$sort``/``$skip``/``$limit`` windows), and
    delegates individual stage compilation to methods inherited from
    ``OperatorsMixin``.
    """

    def __init__(self, collection, query_engine=None):
        """Initialize the processor with a target collection and helpers.

        Sets up the expression evaluator, JSONB capability context, unified
        SQL translator, and per-pipeline execution tracking flags.

        Args:
            collection: NeoSQLite collection on which aggregation pipelines
                are executed.
            query_engine: Optional ``QueryEngine`` instance providing shared
                query helpers; when omitted, ``$match`` text search uses
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
            db_connection=collection.db,
            jsonb_context=self.jsonb,
        )
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
        """Process an aggregation pipeline using SQLite temporary tables.

        Executes stages atomically inside ``aggregation_pipeline_context``
        using a deterministic pipeline hash, grouping consecutive ``$unwind``
        stages and consecutive ``$sort``/``$skip``/``$limit`` windows, and
        optimizing a trailing ``$count`` stage via recursive SQL counting.

        Supported stages include ``$match``, ``$unwind``, ``$lookup``,
        ``$sort``, ``$skip``, ``$limit``, ``$addFields``, ``$project``,
        ``$replaceRoot``, ``$replaceWith``, ``$group``, ``$setWindowFields``,
        ``$graphLookup``, ``$fill``, ``$sample``, ``$unset``, ``$bucket``,
        ``$bucketAuto``, ``$unionWith``, ``$merge``, ``$out``, ``$redact``,
        ``$densify``, ``$facet``, and ``$count``.

        Args:
            pipeline: List of aggregation pipeline stage dictionaries to run.
            is_count: Whether to return a single count document instead of
                materializing stage rows.
            count_field: Output field name for the count document when
                ``is_count`` is ``True``.
            batch_size: Number of rows fetched per batch when reading results
                from the final temporary table.

        Returns:
            list[dict[str, Any]]: Resulting documents produced by the final
            pipeline stage.

        Raises:
            ValueError: If a ``$limit`` stage specifies a non-integer, boolean,
                or negative value.
            NotImplementedError: If a ``$count`` field name is invalid or the
                pipeline contains an unsupported stage.
        """
        # Reset pipeline tracking state
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
                        # Compose consecutive sort/skip/limit stages in order.
                        # A $skip after a $limit shrinks the remaining window;
                        # later limits cap what is left — never overwrite (#170).
                        sort_spec = None
                        skip_value = 0
                        limit_value: int | None = None
                        j = i

                        def _apply_skip(
                            cur_skip: int, cur_lim: int | None, n: int
                        ) -> int:
                            """Accumulate consecutive $skip counts.

                            Args:
                                cur_skip: Current skip offset.
                                cur_lim: Current limit (unused).
                                n: Additional documents to skip.

                            Returns:
                                int: Updated total skip offset.
                            """
                            return cur_skip + n

                        def _apply_limit(
                            cur_lim: int | None, n: int
                        ) -> int | None:
                            """Cap the window limit with a new $limit.

                            Args:
                                cur_lim: Current limit or None.
                                n: New non-negative limit value.

                            Returns:
                                int | None: Smaller of the limits.
                            """
                            if cur_lim is None or n < cur_lim:
                                return n
                            return cur_lim

                        while j < len(pipeline):
                            next_stage = pipeline[j]
                            next_stage_name = next(iter(next_stage.keys()))

                            match next_stage_name:
                                case "$sort":
                                    if (
                                        sort_spec is not None
                                        or skip_value > 0
                                        or limit_value is not None
                                    ):
                                        break
                                    sort_spec = next_stage["$sort"]
                                case "$skip":
                                    n = int(next_stage["$skip"])
                                    skip_value = _apply_skip(
                                        skip_value, limit_value, n
                                    )
                                    # A skip consumes from the current window
                                    if limit_value is not None:
                                        limit_value -= n
                                        if limit_value < 0:
                                            limit_value = 0
                                case "$limit":
                                    lim_val = next_stage["$limit"]
                                    if (
                                        not isinstance(lim_val, int)
                                        or isinstance(lim_val, bool)
                                        or lim_val < 0
                                    ):
                                        raise ValueError(
                                            f"The $limit stage requires a non-negative integer, got {lim_val}"
                                        )
                                    limit_value = _apply_limit(
                                        limit_value,
                                        lim_val,
                                    )
                                case _:
                                    break
                            j += 1

                        if limit_value is not None and limit_value <= 0:
                            # Window fully consumed: synthesize an empty table
                            current_table = create_temp(
                                {"$limit": 0},
                                f"SELECT id, _id, data FROM {current_table} WHERE 0",
                            )
                        else:
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
                        if (
                            not isinstance(sample_size, int)
                            or isinstance(sample_size, bool)
                            or sample_size < 0
                        ):
                            raise ValueError(
                                "The $sample size must be a non-negative integer, "
                                f"got {sample_size}"
                            )
                        sample_stage = {"$sample": sample_spec}
                        new_table = create_temp(
                            sample_stage,
                            f"SELECT * FROM {current_table} ORDER BY RANDOM() LIMIT ?",
                            params=[sample_size],
                        )
                        current_table = new_table
                        i += 1

                    case "$unset":
                        unset_spec = stage["$unset"]
                        if isinstance(unset_spec, str):
                            unset_fields = [unset_spec]
                        else:
                            unset_fields = unset_spec
                        # Build json_remove expressions (preserve_id=False -> '$._id')
                        data_expr = "data"
                        json_remove_func = self.jsonb.function("remove")
                        for field in unset_fields:
                            json_path = parse_json_path(
                                field, preserve_id=False
                            )
                            data_expr = f"{json_remove_func}({data_expr}, '{json_path}')"
                        unset_stage = {"$unset": unset_spec}
                        id_col = (
                            "NULL AS _id" if "_id" in unset_fields else "_id"
                        )
                        new_table = create_temp(
                            unset_stage,
                            f"SELECT id, {id_col}, {self.jsonb.data_column(data_expr)} as data FROM {current_table}",
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

                    case "$out":
                        current_table = self._process_out_stage(
                            create_temp, current_table, stage["$out"]
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

                    case "$count":
                        c_field = stage["$count"]
                        if (
                            not isinstance(c_field, str)
                            or not c_field
                            or c_field.startswith("$")
                            or "." in c_field
                        ):
                            raise NotImplementedError(
                                f"Invalid $count field: {c_field!r}"
                            )
                        escaped_field = c_field.replace("'", "''")
                        json_obj_func = (
                            f"{self.jsonb.json_function_prefix}_object"
                        )
                        data_expr = (
                            f"{json_obj_func}('{escaped_field}', COUNT(*))"
                        )
                        current_table = create_temp(
                            stage,
                            f"SELECT 1 AS id, "
                            f"{self.jsonb.data_column(data_expr)} AS data "
                            f"FROM {quote_table_name(current_table)}",
                        )
                        i += 1

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
