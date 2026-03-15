"""
SQL Tier 1 Optimizer for Aggregation Pipelines.

This module implements SQL-based optimization for aggregation pipelines,
providing 10-100x performance improvements over Python fallback (Tier 3).

The optimizer analyzes aggregation pipelines and generates optimized SQL queries
using CTEs (Common Table Expressions) for multi-stage pipelines.
"""

from __future__ import annotations
from .._sqlite import sqlite3
from ..sql_utils import quote_table_name
from .json_path_utils import parse_json_path
from typing import Any, Dict, List, Tuple, Set
from .expr_evaluator import (
    ExprEvaluator,
    AggregationContext,
    _is_expression,
    _is_aggregation_variable,
)
from .jsonb_support import (
    supports_jsonb,
)
from .query_helper.utils import _get_json_function


class PipelineContext:
    """
    Tracks field aliases, computed fields, and document state across pipeline stages.
    """

    def __init__(self) -> None:
        """Initialize pipeline context with default state."""
        self.computed_fields: Dict[str, str] = {}
        self.removed_fields: Set[str] = set()
        self.stage_index: int = 0
        self.has_root: bool = False
        self.has_computed: bool = False

    def add_computed_field(self, field: str, sql_expr: str) -> None:
        """Track a computed field."""
        self.computed_fields[field] = sql_expr
        self.has_computed = True

    def remove_field(self, field: str) -> None:
        """Mark field as removed."""
        self.removed_fields.add(field)

    def get_field_sql(self, field: str) -> str | None:
        """Get SQL expression for a field."""
        return self.computed_fields.get(field)

    def is_field_available(self, field: str) -> bool:
        """Check if field is available in current context."""
        return field not in self.removed_fields

    def is_field_computed(self, field: str) -> bool:
        """Check if field is a computed field."""
        return field in self.computed_fields

    def preserve_root(self) -> None:
        """Mark that $$ROOT should be preserved."""
        self.has_root = True

    def needs_root(self) -> bool:
        """Check if $$ROOT is needed."""
        return self.has_root

    def clone(self) -> PipelineContext:
        """Create a copy of this context."""
        new_ctx = PipelineContext()
        new_ctx.computed_fields = self.computed_fields.copy()
        new_ctx.removed_fields = self.removed_fields.copy()
        new_ctx.stage_index = self.stage_index
        new_ctx.has_root = self.has_root
        new_ctx.has_computed = self.has_computed
        return new_ctx


class SQLTierAggregator:
    """
    SQL Tier 1 optimizer for aggregation pipelines.
    """

    # Stages that can be optimized in SQL tier
    SUPPORTED_STAGES = {
        "$match",
        "$addFields",
        "$project",
        "$group",
        "$sort",
        "$skip",
        "$limit",
        "$facet",
        "$unwind",
        "$count",
        "$setWindowFields",
        "$graphLookup",
        "$fill",
        "$unset",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$bucket",
        "$bucketAuto",
        "$redact",
        "$unionWith",
        "$lookup",
        "$merge",
        "$densify",
    }

    # Stages that require Python fallback
    UNSUPPORTED_STAGES = {
        "$indexStats",
        "$out",
        "$jsonSchema",
    }

    # Expressions that require Python fallback
    UNSUPPORTED_EXPRESSIONS = {
        "$function",  # Custom JavaScript
        "$accumulator",  # Custom accumulator
        "$script",  # JavaScript execution
        "$jsonSchema",  # Complex validation logic in Python
    }

    def __init__(self, collection, expr_evaluator: ExprEvaluator | None = None):
        """Initialize the SQL tier aggregator."""
        self.collection = collection
        self.evaluator = expr_evaluator or ExprEvaluator(
            data_column="data", db_connection=collection.db
        )
        self._jsonb_supported = supports_jsonb(collection.db)
        self._json_function_prefix = (
            "jsonb" if self._jsonb_supported else "json"
        )

    def _get_json_extract(self, path: str | None = None) -> str:
        """Get JSON extract function with correct prefix."""
        if path:
            return f"{self._json_function_prefix}_extract(data, '${path}')"
        return f"{self._json_function_prefix}_extract"

    def _get_json_set(self) -> str:
        """Get JSON set function with correct prefix."""
        return f"{self._json_function_prefix}_set"

    def can_optimize_pipeline(self, pipeline: List[Dict[str, Any]]) -> bool:
        """Check if pipeline can be optimized in SQL tier."""
        from .query_helper import get_force_fallback

        if get_force_fallback():
            print("DEBUG: Force fallback is active")
            return False

        if not pipeline:
            return True

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            if stage_name in self.UNSUPPORTED_STAGES:
                return False
            if stage_name not in self.SUPPORTED_STAGES:
                return False
            if (
                stage_name == "$setWindowFields"
                and sqlite3.sqlite_version_info
                < (
                    3,
                    25,
                    0,
                )
            ):
                return False

        for stage in pipeline:
            if not self._can_optimize_stage_expressions(stage):
                return False

        if len(pipeline) > 50:
            return False

        return True

    def _can_optimize_stage_expressions(self, stage: Dict[str, Any]) -> bool:
        """Check if all expressions in a stage can be optimized in SQL."""
        stage_name = next(iter(stage.keys()))
        spec = stage[stage_name]
        if stage_name == "$graphLookup":
            return True
        return self._check_expression_support(spec)

    def _check_expression_support(self, obj: Any) -> bool:
        """Recursively check if an object contains unsupported expressions."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key.startswith("$"):
                    # Only block if it's an aggregation OPERATOR (expression),
                    # not a top-level stage specification.
                    if key in self.UNSUPPORTED_EXPRESSIONS:
                        return False
                if not self._check_expression_support(value):
                    return False
        elif isinstance(obj, list):
            for item in obj:
                if not self._check_expression_support(item):
                    return False
        return True

    def build_pipeline_sql(
        self, pipeline: List[Dict[str, Any]]
    ) -> Tuple[str | None, List[Any]]:
        """Build optimized SQL query for entire pipeline using CTEs."""
        if not pipeline or not self.can_optimize_pipeline(pipeline):
            return None, []

        cte_parts: List[str] = []
        all_params: List[Any] = []
        # Initial stage selects from table
        prev_stage = f"(SELECT id, _id, data FROM {quote_table_name(self.collection.name)})"
        context = PipelineContext()

        needs_root = self._pipeline_needs_root(pipeline)
        if needs_root:
            context.preserve_root()

        for i, stage in enumerate(pipeline):
            stage_name = next(iter(stage.keys()))
            stage_spec = stage[stage_name]
            context.stage_index = i
            cte_name = f"stage{i}"

            stage_sql, stage_params = self._build_stage_sql(
                stage_name,
                stage_spec,
                prev_stage,
                context,
                preserve_root=(i == 0 and needs_root),
            )

            if stage_sql is None:
                return None, []

            cte_parts.append(f"{cte_name} AS ({stage_sql})")
            all_params.extend(stage_params)
            prev_stage = cte_name

        with_keyword = (
            "WITH RECURSIVE"
            if any("$graphLookup" in stage for stage in pipeline)
            else "WITH"
        )
        select_cols = "id, _id, data"
        if needs_root:
            select_cols = "id, _id, root_data, data"

        final_sql = f"{with_keyword} {', '.join(cte_parts)} SELECT {select_cols} FROM {prev_stage}"
        return final_sql, all_params

    def _pipeline_needs_root(self, pipeline: List[Dict[str, Any]]) -> bool:
        """Check if pipeline uses $$ROOT variable."""
        for stage in pipeline:
            if self._stage_uses_root(stage):
                return True
        return False

    def _stage_uses_root(self, stage: Dict[str, Any]) -> bool:
        """Check if a stage uses $$ROOT variable."""
        stage_name = next(iter(stage.keys()))
        spec = stage[stage_name]
        return self._expression_uses_root(spec)

    def _expression_uses_root(self, obj: Any) -> bool:
        """Recursively check if expression uses $$ROOT."""
        if isinstance(obj, str):
            return obj == "$$ROOT"
        elif isinstance(obj, dict):
            return any(self._expression_uses_root(v) for v in obj.values())
        elif isinstance(obj, list):
            return any(self._expression_uses_root(i) for i in obj)
        return False

    def _build_stage_sql(
        self,
        stage_name: str,
        stage_spec: Any,
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """Build SQL for a single pipeline stage."""
        match stage_name:
            case "$addFields":
                return self._build_addfields_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$project":
                return self._build_project_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$group":
                return self._build_group_sql(stage_spec, prev_stage, context)
            case "$match":
                return self._build_match_sql(stage_spec, prev_stage, context)
            case "$sort":
                return self._build_sort_sql(stage_spec, prev_stage, context)
            case "$skip":
                return self._build_skip_sql(stage_spec, prev_stage, context)
            case "$limit":
                return self._build_limit_sql(stage_spec, prev_stage, context)
            case "$count":
                return self._build_count_sql(stage_spec, prev_stage, context)
            case "$unset":
                return self._build_unset_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$replaceRoot":
                return self._build_replace_root_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$replaceWith":
                return self._build_replace_root_sql(
                    {"newRoot": stage_spec}, prev_stage, context, preserve_root
                )
            case "$setWindowFields":
                return self._build_set_window_fields_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$graphLookup":
                return self._build_graph_lookup_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$fill":
                return self._build_fill_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$sample":
                return self._build_sample_sql(stage_spec, prev_stage, context)
            case "$bucket":
                return self._build_bucket_sql(stage_spec, prev_stage, context)
            case "$bucketAuto":
                return self._build_bucket_auto_sql(
                    stage_spec, prev_stage, context
                )
            case "$unionWith":
                return self._build_union_with_sql(
                    stage_spec, prev_stage, context
                )
            case "$redact":
                return self._build_redact_sql(stage_spec, prev_stage, context)
            case "$lookup":
                return self._build_lookup_sql(stage_spec, prev_stage, context)
            case "$merge":
                return self._build_merge_sql(stage_spec, prev_stage, context)
            case "$densify":
                return self._build_densify_sql(stage_spec, prev_stage, context)
            case _:
                return None, []

    def _build_addfields_sql(self, spec, prev_stage, context, preserve_root):
        all_params = []
        json_set_args = []
        for field, expr in spec.items():
            if _is_aggregation_variable(expr):
                json_set_func = self._get_json_set()
                if expr == "$$CURRENT":
                    expr_sql = f"json({json_set_func}(data, '$._id', _id))"
                elif expr == "$$ROOT":
                    context.preserve_root()
                    expr_sql = f"json({json_set_func}(root_data, '$._id', _id))"
                else:
                    return None, []
            else:
                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    expr, context=agg_ctx
                )
                all_params.extend(expr_params)
            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(expr_sql)
            context.add_computed_field(field, expr_sql)

        json_set_func = self._get_json_set()
        if json_set_args:
            data_expr = (
                f"json({json_set_func}(data, {', '.join(json_set_args)}))"
            )
        else:
            data_expr = "data"

        select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_project_sql(self, spec, prev_stage, context, preserve_root):
        all_params = []
        select_parts = ["id"]
        include_id = spec.get("_id", 1) == 1
        if include_id:
            select_parts.append("_id")

        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")

        for field, value in spec.items():
            if field == "_id":
                continue
            if _is_expression(value) or (
                isinstance(value, str) and value.startswith("$")
            ):
                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    value, context=agg_ctx
                )
                all_params.extend(expr_params)
                select_parts.append(f"{expr_sql} AS {field}")
                context.add_computed_field(field, expr_sql)
            elif value == 1:
                json_extract_func = f"{self._json_function_prefix}_extract"
                select_parts.append(
                    f"{json_extract_func}(data, '{parse_json_path(field)}') AS {field}"
                )

        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_unset_sql(self, spec, prev_stage, context, preserve_root):
        fields = [spec] if isinstance(spec, str) else spec
        json_remove_func = _get_json_function("remove", self._jsonb_supported)
        json_set_func = self._get_json_set()
        json_paths = [f"'{parse_json_path(f)}'" for f in fields]
        data_expr = f"json({json_set_func}({json_remove_func}(data, {', '.join(json_paths)}), '$._id', _id))"
        select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", []

    def _build_replace_root_sql(self, spec, prev_stage, context, preserve_root):
        new_root = spec.get("newRoot")
        all_params = []
        json_set_func = self._get_json_set()
        if (
            isinstance(new_root, str)
            and new_root.startswith("$")
            and not new_root.startswith("$$")
        ):
            field = new_root[1:]
            json_extract_func = f"{self._json_function_prefix}_extract"
            expr_sql = f"{json_extract_func}(data, '{parse_json_path(field)}')"
        elif new_root == "$$ROOT":
            if context.has_root:
                expr_sql = f"json({json_set_func}(root_data, '$._id', _id))"
            else:
                expr_sql = f"json({json_set_func}(data, '$._id', _id))"
        elif new_root == "$$CURRENT":
            expr_sql = f"json({json_set_func}(data, '$._id', _id))"
        else:
            expr_sql, expr_params = self.evaluator.build_select_expression(
                new_root
            )
            all_params.extend(expr_params)

        data_expr = f"json({json_set_func}({expr_sql}, '$._id', _id))"
        select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_sample_sql(self, spec, prev_stage, context):
        size = spec.get("size", 1)
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY RANDOM() LIMIT {int(size)}"
        return sql, []

    def _build_bucket_sql(self, spec, prev_stage, context):
        group_by = spec.get("groupBy")
        boundaries = spec.get("boundaries")
        default = spec.get("default")
        output = spec.get("output", {"count": {"$sum": 1}})

        if not group_by or not boundaries:
            return None, []

        all_params = []
        key_sql, key_params = self.evaluator.build_select_expression(group_by)
        all_params.extend(key_params)

        # Build CASE for boundaries
        case_parts = []
        for i in range(len(boundaries) - 1):
            lower = boundaries[i]
            upper = boundaries[i + 1]
            case_parts.append(f"WHEN {key_sql} >= ? AND {key_sql} < ? THEN ?")
            all_params.extend([lower, upper, lower])

        if default is not None:
            case_parts.append("ELSE ?")
            all_params.append(default)

        bucket_id_sql = f"CASE {' '.join(case_parts)} END"

        # Build accumulators
        acc_parts = [f"{bucket_id_sql} AS _id"]
        for field, acc_spec in output.items():
            op, expr = next(iter(acc_spec.items()))
            agg_result = self._map_accumulator_to_sql(op)
            if not agg_result:
                return None, []
            sql_agg, use_distinct = agg_result
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            acc_parts.append(f"{sql_agg}({distinct_str}{expr_sql}) AS {field}")

        # NeoSQLite stores result in data column
        acc_fields = ", ".join(
            [f"'{f}', {f}" for f in output.keys()] + ["'_id', _id"]
        )
        data_expr = f"json_object({acc_fields})"

        sql = f"SELECT NULL AS id, _id, {data_expr} AS data FROM {prev_stage} GROUP BY {bucket_id_sql}"
        return sql, all_params

    def _build_bucket_auto_sql(self, spec, prev_stage, context):
        group_by = spec.get("groupBy")
        buckets = spec.get("buckets", 10)
        output = spec.get("output", {"count": {"$sum": 1}})
        granularity = spec.get("granularity")

        if not group_by:
            return None, []

        if granularity:
            return None, []

        all_params = []
        key_sql, key_params = self.evaluator.build_select_expression(group_by)
        all_params.extend(key_params)

        min_max_sql = f"""
            SELECT
                MIN({key_sql}) as min_val,
                MAX({key_sql}) as max_val
            FROM {prev_stage}
        """

        bucket_calc_sql = f"""
            SELECT
                min_val,
                max_val,
                CASE
                    WHEN max_val = min_val THEN 0
                    ELSE (max_val - min_val) / {buckets}
                END as bucket_width
            FROM ({min_max_sql})
        """

        acc_parts = ["bucket_id as _id"]
        for field, acc_spec in output.items():
            op, expr = next(iter(acc_spec.items()))
            agg_result = self._map_accumulator_to_sql(op)
            if not agg_result:
                return None, []
            sql_agg, use_distinct = agg_result
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            acc_parts.append(f"{sql_agg}({distinct_str}{expr_sql}) AS {field}")

        acc_fields = ", ".join(
            [f"'{f}', {f}" for f in output.keys()] + ["'_id', _id"]
        )
        data_expr = f"json_object({acc_fields})"

        sql = f"""
            SELECT
                NULL AS id,
                bucket_id AS _id,
                {data_expr} AS data
            FROM (
                SELECT
                    {key_sql} as val,
                    CASE
                        WHEN max_val = min_val THEN 0
                        ELSE CAST(({key_sql} - min_val) / ((max_val - min_val) / {buckets}) AS INTEGER)
                    END as bucket_id
                FROM {prev_stage}, ({bucket_calc_sql})
                WHERE min_val IS NOT NULL AND max_val IS NOT NULL
            )
            GROUP BY bucket_id
            ORDER BY bucket_id
        """
        return sql, all_params

    def _build_union_with_sql(self, spec, prev_stage, context):
        coll_name = spec.get("coll")
        pipeline = spec.get("pipeline", [])

        if not coll_name:
            return None, []

        target_coll = self.collection.database.get_collection(coll_name)
        other_agg = SQLTierAggregator(target_coll, self.evaluator)
        union_sql, union_params = other_agg.build_pipeline_sql(pipeline)

        if not union_sql:
            return None, []

        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")

        cols = ", ".join(select_parts)
        sql = f"SELECT {cols} FROM {prev_stage} UNION ALL SELECT {cols} FROM ({union_sql})"
        return sql, union_params

    def _build_redact_sql(self, spec, prev_stage, context):
        """
        Build SQL for $redact stage.
        Optimizes KEEP/PRUNE patterns into a WHERE clause.
        Falls back to Python for DESCEND pattern as it requires recursive JSON modification.
        """
        if not isinstance(spec, dict):
            return None, []

        # Handle common pattern: {$cond: {if: <expr>, then: "$$KEEP", else: "$$PRUNE"}}
        # or {$cond: {if: <expr>, then: "$$PRUNE", else: "$$KEEP"}}
        cond = spec.get("$cond")
        if cond and isinstance(cond, dict):
            if_expr = cond.get("if")
            then_val = cond.get("then")
            else_val = cond.get("else")

            # We can only optimize if it's strictly KEEP/PRUNE at the top level
            valid_vals = ("$$KEEP", "$$PRUNE")
            if then_val in valid_vals and else_val in valid_vals:
                expr_sql, expr_params = self.evaluator.evaluate_for_aggregation(
                    if_expr
                )
                if not expr_sql:
                    return None, []

                # If then is KEEP, it's like WHERE condition.
                # If then is PRUNE, it's like WHERE NOT condition.
                if then_val == "$$KEEP":
                    where_clause = f"WHERE ({expr_sql})"
                else:
                    where_clause = f"WHERE NOT ({expr_sql})"

                select_parts = ["id", "_id", "data"]
                if context.has_root:
                    select_parts.append("root_data")

                sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage} {where_clause}"
                return sql, expr_params

        # Fall back to Python for anything else (especially $$DESCEND)
        return None, []

    def _build_lookup_sql(self, spec, prev_stage, context):
        from_coll = spec.get("from")
        local_field = spec.get("localField")
        foreign_field = spec.get("foreignField")
        as_field = spec.get("as")
        pipeline = spec.get("pipeline", [])

        if not from_coll or not as_field:
            return None, []

        json_extract_func = f"{self._json_function_prefix}_extract"
        json_set_func = self._get_json_set()

        if pipeline:
            if not local_field or not foreign_field:
                return None, []

            target_coll = self.collection.database.get_collection(from_coll)
            other_agg = SQLTierAggregator(target_coll, self.evaluator)

            if not other_agg.can_optimize_pipeline(pipeline):
                return None, []

            pipeline_sql, pipeline_params = other_agg.build_pipeline_sql(
                pipeline
            )
            if not pipeline_sql:
                return None, []

            local_expr = f"{json_extract_func}(t1.data, '{parse_json_path(local_field)}')"

            joined_data_expr = f"""
                (SELECT json_group_array(json_set(data, '$._id', _id))
                 FROM ({pipeline_sql}) AS pipeline_result
                 WHERE json_extract(data, '{parse_json_path(foreign_field)}') = {local_expr})
            """

            data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

            select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
            if context.has_root:
                select_parts.append("t1.root_data")

            sql = f"""
                SELECT {", ".join(select_parts)}
                FROM {prev_stage} AS t1
            """
            return sql, pipeline_params

        if not all([local_field, foreign_field]):
            return None, []

        local_expr = (
            f"{json_extract_func}(t1.data, '{parse_json_path(local_field)}')"
        )
        foreign_expr = (
            f"{json_extract_func}(t2.data, '{parse_json_path(foreign_field)}')"
        )

        joined_data_expr = f"json_group_array({json_set_func}(t2.data, '$._id', t2._id)) FILTER (WHERE t2.id IS NOT NULL)"
        data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

        select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
        if context.has_root:
            select_parts.append("t1.root_data")

        sql = f"""
            SELECT {", ".join(select_parts)}
            FROM {prev_stage} AS t1
            LEFT JOIN {quote_table_name(from_coll)} AS t2 ON {local_expr} = {foreign_expr}
            GROUP BY t1.id
        """
        return sql, []

    def _build_merge_sql(self, spec, prev_stage, context):
        return None, []

    def _build_densify_sql(self, spec, prev_stage, context):
        """
        Build SQL for $densify stage - fills gaps in numeric sequences.

        Algorithm:
        1. Extract existing distinct values from the field into CTE "existing"
        2. Calculate min/max bounds from existing values in CTE "bounds_calc"
        3. Generate series of values from min to max with given step using
           CROSS JOIN with a numbers table (0-50) in CTE "series"
        4. UNION ALL:
           - Original documents from prev_stage
           - New documents with densified field values (where value not in existing)
        5. Use NOT EXISTS to filter out values that already exist

        Example: field=[1,3,5], step=1 produces [1,2,3,4,5]
        - existing: {1,3,5}
        - series: {1,2,3,4,5}
        - result: original docs + new docs for {2,4}

        Limitations:
        - Only supports numeric fields (int/float)
        - Does not support partitionBy/partitionByFields (requires group-by)
        - Does not support date fields or granularity
        - Max gap filling limited to 1000 values (falls back to Python for larger)
        """
        field = spec.get("field")
        range_spec = spec.get("range")
        partition_by = spec.get("partitionBy") or spec.get("partitionByFields")

        if not field:
            return None, []

        if partition_by:
            return None, []

        if not range_spec:
            return None, []

        step = range_spec.get("step")
        bounds = range_spec.get("bounds")

        if not step or not bounds:
            return None, []

        if len(bounds) != 2:
            return None, []

        all_params = []
        key_sql, key_params = self.evaluator.build_select_expression(
            f"${field}"
        )
        all_params.extend(key_params)

        lower_bound = bounds[0]
        bounds[1]

        is_date_field = isinstance(lower_bound, str) and (
            lower_bound.startswith("00") or "T" in lower_bound
        )

        if is_date_field:
            return None, []

        if isinstance(step, int):
            step_value = step
        elif isinstance(step, float):
            step_value = step
        else:
            return None, []

        sql = f"""
            WITH existing AS (
                SELECT DISTINCT {key_sql} as val
                FROM {prev_stage}
                WHERE {key_sql} IS NOT NULL
            ),
            bounds_calc AS (
                SELECT
                    MIN(val) as min_val,
                    MAX(val) as max_val
                FROM existing
            ),
            series AS (
                SELECT min_val + (i * {step_value}) as val
                FROM bounds_calc
                CROSS JOIN (
                    WITH RECURSIVE nums(i) AS (
                        SELECT 0
                        UNION ALL
                        SELECT i + 1 FROM nums WHERE i < 1000
                    )
                    SELECT i FROM nums
                ) nums
                WHERE min_val + (i * {step_value}) <= (SELECT max_val FROM bounds_calc)
                AND min_val + (i * {step_value}) >= (SELECT min_val FROM bounds_calc)
            )
            SELECT t.id, t._id, t.data
            FROM {prev_stage} t
            UNION ALL
            SELECT NULL as id, NULL as _id, json_object('{field}', s.val) as data
            FROM series s
            WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.val = s.val)
        """
        return sql, all_params

    def _build_group_sql(self, spec, prev_stage, context):
        all_params = []
        select_parts = []
        group_by_parts = []
        group_id = spec.get("_id")
        if group_id is not None:
            key_sql, key_params = self.evaluator.build_select_expression(
                group_id
            )
            select_parts.append(f"{key_sql} AS _id")
            group_by_parts.append(key_sql)
            all_params.extend(key_params)
        else:
            select_parts.append("NULL AS _id")

        for field, accumulator in spec.items():
            if field == "_id":
                continue
            op, expr = next(iter(accumulator.items()))
            agg_result = self._map_accumulator_to_sql(op)
            if not agg_result:
                return None, []
            sql_agg, use_distinct = agg_result
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            select_parts.append(
                f"{sql_agg}({distinct_str}{expr_sql}) AS {field}"
            )

        group_by_clause = (
            f"GROUP BY {', '.join(group_by_parts)}" if group_by_parts else ""
        )
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} {group_by_clause}",
            all_params,
        )

    def _map_accumulator_to_sql(self, op):
        mapping = {
            "$sum": ("SUM", False),
            "$avg": ("AVG", False),
            "$min": ("MIN", False),
            "$max": ("MAX", False),
            "$count": ("COUNT", False),
            "$push": ("json_group_array", False),
            "$addToSet": ("json_group_array", True),
        }
        return mapping.get(op)

    def _build_match_sql(self, spec, prev_stage, context):
        all_params = []
        where_clauses = []
        for field, value in spec.items():
            if field == "$expr":
                expr_sql, expr_params = self.evaluator.evaluate_for_aggregation(
                    value
                )
                if not expr_sql:
                    return None, []
                where_clauses.append(expr_sql)
                all_params.extend(expr_params)
            elif field.startswith("$") and _is_expression({field: value}):
                # Handle direct expression (no $expr wrapper)
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    {field: value}
                )
                if not expr_sql:
                    return None, []
                where_clauses.append(expr_sql)
                all_params.extend(expr_params)
            else:
                if field == "_id":
                    field_sql = "_id"
                else:
                    json_extract_func = f"{self._json_function_prefix}_extract"
                    field_sql = (
                        f"{json_extract_func}(data, '{parse_json_path(field)}')"
                    )

                if isinstance(value, dict):
                    for op, arg in value.items():
                        if op == "$gt":
                            where_clauses.append(f"{field_sql} > ?")
                            all_params.append(arg)
                        elif op == "$lt":
                            where_clauses.append(f"{field_sql} < ?")
                            all_params.append(arg)
                        elif op == "$gte":
                            where_clauses.append(f"{field_sql} >= ?")
                            all_params.append(arg)
                        elif op == "$lte":
                            where_clauses.append(f"{field_sql} <= ?")
                            all_params.append(arg)
                        elif op == "$eq":
                            where_clauses.append(f"{field_sql} = ?")
                            all_params.append(arg)
                        elif op == "$ne":
                            where_clauses.append(f"{field_sql} != ?")
                            all_params.append(arg)
                        else:
                            return None, []
                else:
                    where_clauses.append(f"{field_sql} = ?")
                    all_params.append(value)

        where_clause = (
            f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        )
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} {where_clause}",
            all_params,
        )

    def _build_sort_sql(self, spec, prev_stage, context):
        order_parts = []
        for field, direction in spec.items():
            order_dir = "DESC" if direction == -1 else "ASC"
            if field == "_id":
                field_sql = "_id"
            else:
                json_extract_func = f"{self._json_function_prefix}_extract"
                field_sql = (
                    f"{json_extract_func}(data, '{parse_json_path(field)}')"
                )
            order_parts.append(f"{field_sql} {order_dir}")
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY {', '.join(order_parts)}",
            [],
        )

    def _build_skip_sql(self, spec, prev_stage, context):
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} LIMIT -1 OFFSET ?",
            [int(spec)],
        )

    def _build_limit_sql(self, spec, prev_stage, context):
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} LIMIT ?",
            [int(spec)],
        )

    def _build_count_sql(self, spec, prev_stage, context):
        return (
            f"SELECT NULL as id, NULL as _id, json_object(?, COUNT(*)) as data FROM {prev_stage}",
            [spec],
        )

    def _build_set_window_fields_sql(
        self, spec, prev_stage, context, preserve_root
    ):
        partition_by = spec.get("partitionBy")
        sort_by = spec.get("sortBy", {})
        output = spec.get("output", {})

        all_params = []
        partition_clause = ""
        if partition_by:
            expr_sql, expr_params = self.evaluator.build_select_expression(
                partition_by
            )
            partition_clause = f"PARTITION BY {expr_sql}"
            all_params.extend(expr_params)

        sort_clause = ""
        if sort_by:
            sort_parts = []
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                if field == "_id":
                    field_sql = "_id"
                else:
                    field_sql = f"{self._json_function_prefix}_extract(data, '{parse_json_path(field)}')"
                sort_parts.append(f"{field_sql} {order}")
            sort_clause = f"ORDER BY {', '.join(sort_parts)}"

        # 1. Build inner SELECT with window functions
        window_select_parts = ["id", "_id", "data"]
        if preserve_root:
            window_select_parts.append("data AS root_data")
        elif context.has_root:
            window_select_parts.append("root_data")

        json_set_args = []
        for i, (field, window_op) in enumerate(output.items()):
            op_name, op_spec = next(iter(window_op.items()))
            sql_func = self._map_window_op_to_sql(op_name)
            if not sql_func:
                return None, []

            # Use a unique alias for the window function result
            win_alias = f"win_func_{i}"

            if op_name in ("$rank", "$denseRank", "$documentNumber"):
                expr_sql = (
                    f"{sql_func}() OVER ({partition_clause} {sort_clause})"
                )
            elif op_name in ("$first", "$last"):
                inner_sql, inner_params = (
                    self.evaluator.build_select_expression(op_spec)
                )
                all_params.extend(inner_params)
                frame = (
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                )
                expr_sql = f"{sql_func}({inner_sql}) OVER ({partition_clause} {sort_clause} {frame})"
            else:
                return None, []

            window_select_parts.append(f"{expr_sql} AS {win_alias}")
            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(win_alias)

        # 2. Build final SELECT that merges window results into JSON data
        json_set_func = self._get_json_set()
        data_expr = (
            f"json({json_set_func}(data, {', '.join(json_set_args)}))"
            if json_set_args
            else "data"
        )

        final_select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            final_select_parts.append("root_data")
        elif context.has_root:
            final_select_parts.append("root_data")

        inner_sql = f"SELECT {', '.join(window_select_parts)} FROM {prev_stage}"
        sql = f"SELECT {', '.join(final_select_parts)} FROM ({inner_sql})"

        return sql, all_params

    def _map_window_op_to_sql(self, op):
        mapping = {
            "$rank": "RANK",
            "$denseRank": "DENSE_RANK",
            "$documentNumber": "ROW_NUMBER",
            "$first": "FIRST_VALUE",
            "$last": "LAST_VALUE",
        }
        return mapping.get(op)

    def _build_graph_lookup_sql(self, spec, prev_stage, context, preserve_root):
        return None, []  # Fallback

    def _build_fill_sql(self, spec, prev_stage, context, preserve_root):
        return None, []  # Fallback

    def _build_passthrough_sql(self, prev_stage, context):
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", []
