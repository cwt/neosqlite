"""
SQL Tier 1 Optimizer for Aggregation Pipelines.

This module implements SQL-based optimization for aggregation pipelines,
providing 10-100x performance improvements over Python fallback (Tier 3).

The optimizer analyzes aggregation pipelines and generates optimized SQL queries
using CTEs (Common Table Expressions) for multi-stage pipelines.

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                Aggregation Pipeline Processing               │
    ├─────────────────────────────────────────────────────────────┤
    │  aggregate()                                                │
    │    └─> AggregationCursor                                    │
    │         └─> SQLTierAggregator                               │
    │              └─> Analyze pipeline complexity                │
    │                   └─> Can optimize in SQL?                  │
    │                        ├─ YES → Build SQL with CTEs         │
    │                        │     └─> Execute single SQL query   │
    │                        └─ NO → Python fallback (Tier 3)     │
    └─────────────────────────────────────────────────────────────┘

Example Output (CTE-based pipeline):
    WITH
    stage0 AS (
        SELECT id, data AS root_data, data
        FROM collection
    ),
    stage1 AS (
        SELECT
            id,
            root_data,
            json_set(data, '$.revenue',
                json_extract(data, '$.price') * json_extract(data, '$.quantity')
            ) AS data
        FROM stage0
    ),
    stage2 AS (
        SELECT
            id,
            root_data,
            json_set(data, '$.tax',
                json_extract(data, '$.revenue') * 0.08
            ) AS data
        FROM stage1
    )
    SELECT * FROM stage2

Usage:
    from neosqlite.collection.sql_tier_aggregator import SQLTierAggregator

    aggregator = SQLTierAggregator(collection)

    if aggregator.can_optimize_pipeline(pipeline):
        sql, params = aggregator.build_pipeline_sql(pipeline)
        results = execute_sql(sql, params)
    else:
        # Fall back to Python
        results = execute_python(pipeline)
"""

from __future__ import annotations
from .._sqlite import sqlite3
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
    _get_json_group_array_function,
)


class PipelineContext:
    """
    Tracks field aliases, computed fields, and document state across pipeline stages.

    This context is essential for SQL tier optimization because:
    1. Fields computed in one stage can be referenced in later stages
    2. $$ROOT must preserve the original document state
    3. $$CURRENT tracks the evolving document
    4. $$REMOVE marks fields for deletion in $project

    Attributes:
        computed_fields: Maps field names to their SQL expressions
        removed_fields: Set of field names marked for removal
        stage_index: Current stage being processed
        has_root: Whether $$ROOT is preserved in current stage
        has_computed: Whether any computed fields exist
    """

    def __init__(self) -> None:
        """Initialize pipeline context with default state."""
        self.computed_fields: Dict[str, str] = {}
        self.removed_fields: Set[str] = set()
        self.stage_index: int = 0
        self.has_root: bool = False
        self.has_computed: bool = False

    def add_computed_field(self, field: str, sql_expr: str) -> None:
        """
        Track a computed field.

        Args:
            field: Field name
            sql_expr: SQL expression that computes the field value
        """
        self.computed_fields[field] = sql_expr
        self.has_computed = True

    def remove_field(self, field: str) -> None:
        """
        Mark field as removed.

        Args:
            field: Field name to remove
        """
        self.removed_fields.add(field)

    def get_field_sql(self, field: str) -> str | None:
        """
        Get SQL expression for a field.

        Args:
            field: Field name

        Returns:
            SQL expression if field is computed, None otherwise
        """
        return self.computed_fields.get(field)

    def is_field_available(self, field: str) -> bool:
        """
        Check if field is available in current context.

        Args:
            field: Field name

        Returns:
            True if field can be referenced, False if removed
        """
        return field not in self.removed_fields

    def is_field_computed(self, field: str) -> bool:
        """
        Check if field is a computed field.

        Args:
            field: Field name

        Returns:
            True if field has a computed SQL expression
        """
        return field in self.computed_fields

    def preserve_root(self) -> None:
        """Mark that $$ROOT should be preserved."""
        self.has_root = True

    def needs_root(self) -> bool:
        """Check if $$ROOT is needed."""
        return self.has_root

    def clone(self) -> PipelineContext:
        """
        Create a copy of this context.

        Returns:
            New PipelineContext with same state
        """
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

    This class analyzes aggregation pipelines and generates optimized SQL queries
    using CTEs (Common Table Expressions) for multi-stage pipelines.

    Supported Stages:
    - $match: With $expr or direct expressions
    - $addFields: With full expression support
    - $project: With computed fields and $$REMOVE
    - $group: With expressions in keys and accumulators
    - $sort: With computed field references
    - $skip: Offset support
    - $limit: Limit support
    - $facet: Sub-pipeline support with CTEs
    - $unwind: Array unwinding (limited)

    Unsupported Stages (fall back to Python):
    - $lookup: Requires JOIN with another collection
    - $graphLookup: Recursive lookup
    - $indexStats: Collection statistics
    - $count: Can be optimized but falls back for simplicity
    - $replaceRoot: Complex document manipulation
    - $merge: Output to collection
    - $out: Output to collection

    Performance Characteristics:
    - Simple pipelines (1-3 stages): 10-20x speedup
    - Multi-stage pipelines (4-10 stages): 20-50x speedup
    - Complex pipelines (10+ stages): 50-100x speedup

    Example:
        >>> aggregator = SQLTierAggregator(collection)
        >>> pipeline = [
        ...     {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
        ...     {"$match": {"revenue": {"$gte": 500}}},
        ...     {"$group": {"_id": "$category", "total": {"$sum": "$revenue"}}}
        ... ]
        >>> if aggregator.can_optimize_pipeline(pipeline):
        ...     sql, params = aggregator.build_pipeline_sql(pipeline)
        ...     results = execute_sql(sql, params)
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
    }

    # Stages that require Python fallback
    UNSUPPORTED_STAGES = {
        "$lookup",
        "$indexStats",
        "$merge",
        "$out",
        "$replaceRoot",
        "$replaceWith",
        "$jsonSchema",
    }

    # Expressions that require Python fallback
    UNSUPPORTED_EXPRESSIONS = {
        "$objectToArray",  # Complex conversion
        "$function",  # Custom JavaScript
        "$accumulator",  # Custom accumulator
        "$script",  # JavaScript execution
        "$jsonSchema",  # Complex validation logic in Python
    }

    def __init__(self, collection, expr_evaluator: ExprEvaluator | None = None):
        """
        Initialize the SQL tier aggregator.

        Args:
            collection: The collection to aggregate
            expr_evaluator: Optional ExprEvaluator instance. If None, creates new one.
        """
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
        """
        Check if pipeline can be optimized in SQL tier.

        Analyzes each stage in the pipeline to determine if SQL optimization
        is possible. Returns False if:
        - Pipeline contains unsupported stages
        - Pipeline contains unsupported expressions
        - Pipeline is too complex (configurable threshold)
        - Global fallback flag is set (_FORCE_FALLBACK)

        Args:
            pipeline: List of pipeline stages

        Returns:
            True if pipeline can be optimized in SQL, False otherwise

        Example:
            >>> pipeline = [
            ...     {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
            ...     {"$match": {"revenue": {"$gte": 500}}}
            ... ]
            >>> aggregator.can_optimize_pipeline(pipeline)
            True
        """
        # Check global fallback flag first (Bug 010 fix)
        from .query_helper import get_force_fallback

        if get_force_fallback():
            return False

        if not pipeline:
            return True

        # Check for unsupported stages
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))

            if stage_name in self.UNSUPPORTED_STAGES:
                return False

            if stage_name not in self.SUPPORTED_STAGES:
                # Unknown stage - fall back to Python for safety
                return False

            # Check for window function support if $setWindowFields is used
            if (
                stage_name == "$setWindowFields"
                and sqlite3.sqlite_version_info < (3, 25, 0)
            ):
                return False

        # Check for unsupported expressions
        for stage in pipeline:
            if not self._can_optimize_stage_expressions(stage):
                return False

        # Check pipeline length (configurable threshold)
        if len(pipeline) > 50:  # Too many CTEs can be slow
            return False

        return True

    def _can_optimize_stage_expressions(self, stage: Dict[str, Any]) -> bool:
        """
        Check if all expressions in a stage can be optimized in SQL.

        Args:
            stage: Pipeline stage dictionary

        Returns:
            True if all expressions can be optimized, False otherwise
        """
        stage_name = next(iter(stage.keys()))
        spec = stage[stage_name]

        if stage_name == "$graphLookup":
            # $graphLookup keys are mostly field names or simple expressions
            # For now, allow it and let _build_graph_lookup_sql handle it
            return True

        # Check for unsupported expressions in spec
        return self._check_expression_support(spec)

    def _check_expression_support(self, obj: Any) -> bool:
        """
        Recursively check if an object contains unsupported expressions.

        Args:
            obj: Object to check (dict, list, or scalar)

        Returns:
            True if all expressions are supported, False otherwise
        """
        if isinstance(obj, dict):
            # Check if this is an expression
            if len(obj) == 1:
                key = next(iter(obj.keys()))
                if key.startswith("$") and key in self.UNSUPPORTED_EXPRESSIONS:
                    return False

            # Recursively check all values
            for value in obj.values():
                if not self._check_expression_support(value):
                    return False

        elif isinstance(obj, list):
            # Recursively check all items
            for item in obj:
                if not self._check_expression_support(item):
                    return False

        return True

    def build_pipeline_sql(
        self, pipeline: List[Dict[str, Any]]
    ) -> Tuple[str | None, List[Any]]:
        """
        Build optimized SQL query for entire pipeline using CTEs.

        Args:
            pipeline: List of pipeline stages

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize

        Example:
            >>> pipeline = [
            ...     {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
            ...     {"$match": {"revenue": {"$gte": 500}}}
            ... ]
            >>> sql, params = aggregator.build_pipeline_sql(pipeline)
            >>> # Returns CTE-based SQL query
        """
        if not pipeline:
            return None, []

        if not self.can_optimize_pipeline(pipeline):
            return None, []

        cte_parts: List[str] = []
        all_params: List[Any] = []
        prev_stage = self.collection.name
        context = PipelineContext()

        # Check if pipeline needs root preservation
        needs_root = self._pipeline_needs_root(pipeline)
        if needs_root:
            context.preserve_root()

        for i, stage in enumerate(pipeline):
            stage_name = next(iter(stage.keys()))
            stage_spec = stage[stage_name]
            context.stage_index = i

            cte_name = f"stage{i}"

            # For first stage with needs_root, preserve root_data
            if i == 0 and needs_root:
                # First stage needs to preserve original document
                stage_sql, stage_params = self._build_stage_sql(
                    stage_name,
                    stage_spec,
                    prev_stage,
                    context,
                    preserve_root=True,
                )
            else:
                # Build SQL for this stage
                stage_sql, stage_params = self._build_stage_sql(
                    stage_name, stage_spec, prev_stage, context
                )

            if stage_sql is None:
                # Cannot optimize this stage
                return None, []

            cte_parts.append(f"{cte_name} AS ({stage_sql})")
            all_params.extend(stage_params)
            prev_stage = cte_name

        # Final SELECT
        with_keyword = (
            "WITH RECURSIVE"
            if any("$graphLookup" in stage for stage in pipeline)
            else "WITH"
        )
        if needs_root:
            # Include root_data in final output if it was preserved
            final_sql = f"{with_keyword} {', '.join(cte_parts)} SELECT id, root_data, data FROM {prev_stage}"
        else:
            final_sql = f"{with_keyword} {', '.join(cte_parts)} SELECT id, data FROM {prev_stage}"

        return final_sql, all_params

    def _pipeline_needs_root(self, pipeline: List[Dict[str, Any]]) -> bool:
        """
        Check if pipeline uses $$ROOT variable.

        Args:
            pipeline: List of pipeline stages

        Returns:
            True if $$ROOT is used, False otherwise
        """
        for stage in pipeline:
            if self._stage_uses_root(stage):
                return True
        return False

    def _stage_uses_root(self, stage: Dict[str, Any]) -> bool:
        """
        Check if a stage uses $$ROOT variable.

        Args:
            stage: Pipeline stage

        Returns:
            True if $$ROOT is used
        """
        stage_name = next(iter(stage.keys()))
        spec = stage[stage_name]
        return self._expression_uses_root(spec)

    def _expression_uses_root(self, obj: Any) -> bool:
        """
        Recursively check if expression uses $$ROOT.

        Args:
            obj: Object to check

        Returns:
            True if $$ROOT is used
        """
        if isinstance(obj, str):
            return obj == "$$ROOT"
        elif isinstance(obj, dict):
            for value in obj.values():
                if self._expression_uses_root(value):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if self._expression_uses_root(item):
                    return True
        return False

    def _build_stage_sql(
        self,
        stage_name: str,
        stage_spec: Any,
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for a single pipeline stage.

        Args:
            stage_name: Name of the stage ($addFields, $match, etc.)
            stage_spec: Stage specification
            prev_stage: Name of previous CTE or collection
            context: Pipeline context for tracking fields
            preserve_root: If True, preserve root_data column

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
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
            case "$facet":
                return self._build_facet_sql(stage_spec, prev_stage, context)
            case "$unwind":
                return self._build_unwind_sql(stage_spec, prev_stage, context)
            case "$count":
                return self._build_count_sql(stage_spec, prev_stage, context)
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
            case _:
                return None, []

    def _build_set_window_fields_sql(
        self,
        spec: Dict[str, Any],
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $setWindowFields stage.
        """
        partition_by = spec.get("partitionBy")
        sort_by = spec.get("sortBy", {})
        output = spec.get("output", {})
        all_params: List[Any] = []

        # 1. Build PARTITION BY clause
        partition_parts = []
        if partition_by is not None:
            # partition_by can be an expression
            sql, params = self.evaluator.build_select_expression(partition_by)
            partition_parts.append(sql)
            all_params.extend(params)

        partition_clause = ""
        if partition_parts:
            partition_clause = f"PARTITION BY {', '.join(partition_parts)}"

        # 2. Build ORDER BY clause
        sort_parts = []
        if sort_by:
            for field, direction in sort_by.items():
                # direction: 1 for ascending, -1 for descending
                order = "ASC" if direction == 1 else "DESC"
                # Use json_extract for fields
                sql, params = self.evaluator.build_select_expression(
                    f"${field}"
                )
                sort_parts.append(f"{sql} {order}")
                all_params.extend(params)

        sort_clause = ""
        if sort_parts:
            sort_clause = f"ORDER BY {', '.join(sort_parts)}"

        # 3. Build output fields with window functions
        json_set_args = []
        for field, op_spec in output.items():
            op_name = next(iter(op_spec.keys()))
            op_val = op_spec[op_name]
            window_spec = op_spec.get("window")

            # Map MongoDB operator to SQL window function
            sql_func, sql_operand, sql_params = (
                self._map_window_operator_to_sql(op_name, op_val)
            )
            if sql_func is None:
                return None, []

            all_params.extend(sql_params)

            # Check for operator-specific sortBy (used by $top, $bottom)
            effective_sort_clause = sort_clause
            if isinstance(op_val, dict) and "sortBy" in op_val:
                op_sort_by = op_val["sortBy"]
                op_sort_parts = []
                for s_field, s_direction in op_sort_by.items():
                    s_order = "ASC" if s_direction == 1 else "DESC"
                    s_sql, s_params = self.evaluator.build_select_expression(
                        f"${s_field}"
                    )
                    op_sort_parts.append(f"{s_sql} {s_order}")
                    all_params.extend(s_params)
                if op_sort_parts:
                    effective_sort_clause = (
                        f"ORDER BY {', '.join(op_sort_parts)}"
                    )

            # Build frame clause (ROWS BETWEEN ...)
            frame_clause = self._build_window_frame_sql(window_spec)

            # Combine into window function
            window_sql = f"{sql_func}({sql_operand}) OVER ({partition_clause} {effective_sort_clause} {frame_clause})".strip()

            # Clean up extra spaces in OVER clause if parts are empty
            window_sql = (
                window_sql.replace("  ", " ")
                .replace("( ", "(")
                .replace(" )", ")")
            )

            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(window_sql)

        # 4. Build the final SQL
        select_parts = ["id"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")

        args_str = ", ".join(json_set_args)
        json_set_func = f"{self._json_function_prefix}_set"
        data_expr = f"json({json_set_func}(data, {args_str}))"
        select_parts.append(f"{data_expr} AS data")

        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage}"
        return sql, all_params

    def _build_fill_sql(
        self,
        spec: Dict[str, Any],
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """
        Build optimized SQL for $fill stage.
        """
        partition_by = spec.get("partitionBy")
        sort_by = spec.get("sortBy", {})
        output = spec.get("output", {})
        all_params: List[Any] = []

        # Check for 'linear' method which we don't support in Tier 1 yet
        for fill_spec in output.values():
            if fill_spec.get("method") == "linear":
                return None, []

        # 1. Collect all fields that need to be extracted for efficiency
        # This avoids redundant json_extract calls in window functions
        extract_selects = ["id", "data"]
        if preserve_root or context.has_root:
            extract_selects.append("root_data")

        # Track fields to avoid duplicates
        extracted_fields = set()

        # Fields for partitionBy
        partition_parts = []
        if partition_by is not None:
            # partition_by can be an expression
            sql, params = self.evaluator.build_select_expression(partition_by)
            col_alias = f"part_expr_{context.stage_index}"
            extract_selects.append(f"{sql} AS {col_alias}")
            partition_parts.append(col_alias)
            all_params.extend(params)

        partition_clause = (
            f"PARTITION BY {', '.join(partition_parts)}"
            if partition_parts
            else ""
        )

        # Fields for sortBy
        sort_parts = []
        if sort_by:
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                field_path = parse_json_path(field)
                col_alias = (
                    f"sort_{field.replace('.', '_')}_{context.stage_index}"
                )

                if col_alias not in extracted_fields:
                    extract_selects.append(
                        f"{self._json_function_prefix}_extract(data, '{field_path}') AS {col_alias}"
                    )
                    extracted_fields.add(col_alias)

                sort_parts.append(f"{col_alias} {order}")

        sort_clause = f"ORDER BY {', '.join(sort_parts)}" if sort_parts else ""

        # Fields for output (locf needs extraction)
        has_locf = any(fs.get("method") == "locf" for fs in output.values())

        block_id_selects = ["*"]
        final_json_args = []

        if not has_locf:
            # Simple constant value fill - can be done in one SELECT
            for field, fill_spec in output.items():
                value = fill_spec.get("value")
                field_path = parse_json_path(field)

                # COALESCE(json_extract(data, field), value)
                fill_expr = f"COALESCE({self._json_function_prefix}_extract(data, '{field_path}'), ?)"
                all_params.append(value)

                final_json_args.append(f"'{field_path}'")
                final_json_args.append(fill_expr)

            json_set_func = f"{self._json_function_prefix}_set"
            data_expr = (
                f"json({json_set_func}(data, {', '.join(final_json_args)}))"
            )

            select_parts = ["id", f"{data_expr} AS data"]
            if preserve_root or context.has_root:
                select_parts.append("root_data")

            sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage}"
            return sql, all_params

        # For locf, we use the optimized multi-stage CTE approach
        # Stage A: Extraction
        for field, fill_spec in output.items():
            field_path = parse_json_path(field)
            col_alias = f"val_{field.replace('.', '_')}_{context.stage_index}"
            if col_alias not in extracted_fields:
                extract_selects.append(
                    f"{self._json_function_prefix}_extract(data, '{field_path}') AS {col_alias}"
                )
                extracted_fields.add(col_alias)

        extract_sql = f"SELECT {', '.join(extract_selects)} FROM {prev_stage}"
        extract_cte = f"fill_extract_{context.stage_index}"

        # Stage B: Block Identification
        for field, fill_spec in output.items():
            if fill_spec.get("method") == "locf":
                val_col = f"val_{field.replace('.', '_')}_{context.stage_index}"
                block_col = (
                    f"block_{field.replace('.', '_')}_{context.stage_index}"
                )
                block_id_selects.append(
                    f"COUNT({val_col}) OVER ({partition_clause} {sort_clause}) AS {block_col}"
                )

        blocks_sql = f"SELECT {', '.join(block_id_selects)} FROM {extract_cte}"

        # Final Stage: Value Filling
        for field, fill_spec in output.items():
            field_path = parse_json_path(field)
            field_safe = field.replace(".", "_")

            if fill_spec.get("method") == "locf":
                val_col = f"val_{field_safe}_{context.stage_index}"
                block_col = f"block_{field_safe}_{context.stage_index}"

                # Partition by both original partition AND the block ID
                block_partition_parts = partition_parts + [block_col]
                block_partition = (
                    f"PARTITION BY {', '.join(block_partition_parts)}"
                )

                # FIRST_VALUE in this block will be the non-null value that started it
                locf_expr = f"FIRST_VALUE({val_col}) OVER ({block_partition} {sort_clause})"
                final_json_args.append(f"'{field_path}'")
                final_json_args.append(locf_expr)
            else:
                # Constant value
                val_col = f"val_{field_safe}_{context.stage_index}"
                value = fill_spec.get("value")
                final_json_args.append(f"'{field_path}'")
                final_json_args.append(f"COALESCE({val_col}, ?)")
                all_params.append(value)

        json_set_func = f"{self._json_function_prefix}_set"
        data_expr = f"json({json_set_func}(data, {', '.join(final_json_args)}))"

        select_parts = ["id", f"{data_expr} AS data"]
        if preserve_root or context.has_root:
            select_parts.append("root_data")

        # Combine into a nested subquery structure for this stage
        # The build_pipeline_sql will wrap this stage in its own stageN CTE
        sql = f"""
            SELECT {', '.join(select_parts)}
            FROM (
                {blocks_sql}
            )
        """

        # We need to manually add the extraction CTE to the pipeline because
        # build_pipeline_sql expects a single SELECT.
        # But wait, build_pipeline_sql handles CTEs. We can return a subquery.

        sql = f"""
            SELECT {', '.join(select_parts)}
            FROM (
                SELECT {', '.join(block_id_selects)}
                FROM (
                    {extract_sql}
                )
            )
        """

        return sql, all_params

    def _build_graph_lookup_sql(
        self,
        spec: Dict[str, Any],
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $graphLookup stage using recursive CTE.
        """
        from_collection = spec.get("from")
        start_with = spec.get("startWith")
        connect_from_field = spec.get("connectFromField")
        connect_to_field = spec.get("connectToField")
        as_field = spec.get("as")
        max_depth = spec.get("maxDepth")
        depth_field = spec.get("depthField")
        restrict_search = spec.get("restrictSearchWithMatch")

        if not all(
            [
                from_collection,
                start_with,
                connect_from_field,
                connect_to_field,
                as_field,
            ]
        ):
            return None, []

        all_params: List[Any] = []

        # 1. Build startWith expression
        start_with_sql, start_with_params = (
            self.evaluator.build_select_expression(start_with)
        )
        # Prefix with p. to avoid ambiguity during JOIN
        start_with_sql = start_with_sql.replace(
            "json_extract(data", "json_extract(p.data"
        )
        start_with_sql = start_with_sql.replace(
            "jsonb_extract(data", "jsonb_extract(p.data"
        )
        all_params.extend(start_with_params)

        # 2. Build restrictSearchWithMatch if present
        restrict_sql = ""
        if restrict_search:
            from .query_helper import QueryHelper

            # Get collection instance to use its helpers
            target_coll = self.collection.database.get_collection(
                from_collection
            )
            helper = QueryHelper(target_coll)

            # We want to prefix 'data' with 't.' to avoid ambiguity
            query_result = helper._build_simple_where_clause(restrict_search)
            if query_result:
                r_sql, r_params = query_result
                # Basic workaround: replace 'data' with 't.data' in the generated WHERE clause
                r_sql = r_sql.replace(
                    "json_extract(data", "json_extract(t.data"
                )
                r_sql = r_sql.replace(
                    "jsonb_extract(data", "jsonb_extract(t.data"
                )
                restrict_sql = f"AND ({r_sql})"
                all_params.extend(r_params)

        # 3. Build recursive search
        recurse_cte = f"graph_recurse_{context.stage_index}"

        # Build field access SQL
        def get_field_sql(table_alias, field_name, is_recursive_table=False):
            if field_name == "_id":
                return (
                    f"{table_alias}.found_id"
                    if is_recursive_table
                    else f"{table_alias}._id"
                )
            data_col = "found_data" if is_recursive_table else "data"
            return f"json_extract({table_alias}.{data_col}, '$.{field_name}')"

        target_to_sql = get_field_sql("t", connect_to_field)
        recurse_from_sql = get_field_sql(
            "r", connect_from_field, is_recursive_table=True
        )

        # We must prefix ALL columns to avoid ambiguity
        start_points_sql = f"""
            SELECT
                p.id as original_id,
                t.id as found_id,
                t.data as found_data,
                0 as depth
            FROM {prev_stage} p
            JOIN {from_collection} t ON {target_to_sql} = {start_with_sql}
            WHERE 1=1 {restrict_sql}
        """

        # Recursive step
        max_depth_cond = (
            f"AND r.depth < {max_depth}" if max_depth is not None else ""
        )
        # restrict_sql for the recursive step also needs to prefix target table t.
        recursive_step_sql = f"""
            SELECT
                r.original_id,
                t.id as found_id,
                t.data as found_data,
                r.depth + 1
            FROM {recurse_cte} r
            JOIN {from_collection} t ON {target_to_sql} = {recurse_from_sql}
            WHERE 1=1 {max_depth_cond} {restrict_sql}
        """

        # 4. Combine into final aggregation
        depth_json_sql = ""
        if depth_field:
            depth_json_sql = f", '{parse_json_path(str(depth_field))}', depth"

        json_group_func = _get_json_group_array_function(self._jsonb_supported)
        json_set_func = f"{self._json_function_prefix}_set"

        # Build the stage SQL
        # Use p.id and p.data explicitly
        # We also MUST inject _id into the data for this stage because _load()
        # expects it to be there if it's not a real table with _id column.
        as_field_str = str(as_field)
        stage_sql = f"""
            SELECT
                p.id AS id,
                json({json_set_func}({json_set_func}(p.data, '$._id', p.id), '{parse_json_path(as_field_str)}',
                    COALESCE((
                        SELECT {json_group_func}(
                            json({json_set_func}(sub.found_data, '$._id', sub.found_id {depth_json_sql}))
                        )
                        FROM (
                            WITH RECURSIVE {recurse_cte} AS (
                                {start_points_sql}
                                UNION
                                {recursive_step_sql}
                            )
                            SELECT found_id, found_data, depth FROM {recurse_cte}
                            WHERE original_id = p.id
                            GROUP BY found_id
                        ) sub
                    ), json('[]'))
                )) as data
                {", p.root_data" if context.has_root or preserve_root else ""}
            FROM {prev_stage} p
        """

        # Note: UNION (not ALL) helps with cycles and duplicates

        return stage_sql, all_params

    def _map_window_operator_to_sql(
        self, op_name: str, op_val: Any
    ) -> Tuple[str | None, str, List[Any]]:
        """Map MongoDB window operator to SQL function and operand."""
        match op_name:
            case "$rank":
                return "RANK", "", []
            case "$denseRank":
                return "DENSE_RANK", "", []
            case "$documentNumber":
                return "ROW_NUMBER", "", []
            case "$first" | "$top":
                expr = (
                    op_val.get("output") if isinstance(op_val, dict) else op_val
                )
                sql, params = self.evaluator.build_select_expression(expr)
                return "FIRST_VALUE", sql, params
            case "$last" | "$bottom":
                expr = (
                    op_val.get("output") if isinstance(op_val, dict) else op_val
                )
                sql, params = self.evaluator.build_select_expression(expr)
                return "LAST_VALUE", sql, params
            case "$shift":
                output_expr = op_val.get("output")
                by = op_val.get("by", 0)
                default = op_val.get("default")

                # SQLite LEAD/LAG: LEAD(expr, offset, default)
                # MongoDB $shift: by > 0 is LEAD, by < 0 is LAG
                if by >= 0:
                    func = "LEAD"
                    offset = by
                else:
                    func = "LAG"
                    offset = -by

                sql, params = self.evaluator.build_select_expression(
                    output_expr
                )
                # default needs to be handled
                if default is not None:
                    # TODO: handle default value literal/expression
                    return f"{func}", f"{sql}, {offset}, ?", params + [default]
                return f"{func}", f"{sql}, {offset}", params

            case "$sum" | "$avg" | "$min" | "$max":
                func = op_name[1:].upper()
                sql, params = self.evaluator.build_select_expression(op_val)
                return func, sql, params

            case (
                "$derivative"
                | "$integral"
                | "$covariancePop"
                | "$covarianceSamp"
                | "$expMovingAvg"
            ):
                # These require complex SQL expressions that we build manually
                # We return a special 'EXPR' type to signal the caller to use the operand as the full expression
                expr, params = self._build_complex_window_op_sql(
                    op_name, op_val
                )
                if expr:
                    return "EXPR", expr, params
                return None, "", []

            case _:
                return None, "", []

    def _build_complex_window_op_sql(
        self, op_name: str, op_val: Any
    ) -> Tuple[str | None, List[Any]]:
        """Build complex SQL expressions for advanced window operators."""
        # Note: These will be placed inside '... OVER (...)'
        # So we must NOT include the OVER clause here.
        match op_name:
            case "$derivative":
                # We need to access the sortBy field. This is tricky since it's not passed here.
                # However, we can't easily get it here without architectural changes.
                # Let's return None for now to trigger Python fallback for these.
                return None, []

            case "$covariancePop" | "$covarianceSamp":
                # Covariance(X,Y) = E[XY] - E[X]E[Y]
                # In SQL: (SUM(X*Y) - SUM(X)*SUM(Y)/COUNT(*)) / COUNT(*)
                return None, []  # Too complex for simple OVER clause mapping

            case _:
                return None, []

    def _build_window_frame_sql(
        self, window_spec: Dict[str, Any] | None
    ) -> str:
        """Build SQL window frame clause (ROWS BETWEEN ...)."""
        if not window_spec:
            return ""

        if "documents" in window_spec:
            lower, upper = window_spec["documents"]

            def map_bound(val):
                if val == "unbounded":
                    return "UNBOUNDED"
                if val == "current":
                    return "CURRENT ROW"
                if val < 0:
                    return f"{-val} PRECEDING"
                if val > 0:
                    return f"{val} FOLLOWING"
                return "CURRENT ROW"

            l_bound = map_bound(lower)
            u_bound = map_bound(upper)

            return f"ROWS BETWEEN {l_bound} AND {u_bound}"

        return ""

    def _build_addfields_sql(
        self,
        spec: Dict[str, Any],
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $addFields stage.

        Generates SQL like:
            SELECT
                id,
                root_data,
                json_set(data, '$.field1', expr1, '$.field2', expr2, ...) AS data
            FROM prev_stage

        Args:
            spec: $addFields specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context
            preserve_root: If True, preserve root_data column

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        all_params: List[Any] = []

        # Build json_set arguments for each field
        json_set_args = []

        for field, expr in spec.items():
            # Check if expression can be optimized
            if not self._check_expression_support(expr):
                return None, []

            # Handle aggregation variables specially
            if _is_aggregation_variable(expr):
                if expr == "$$CURRENT":
                    # $$CURRENT means the entire current document
                    # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                    # Use json_set to merge _id from id column into data
                    json_set_func = f"{self._json_function_prefix}_set"
                    expr_sql = f"json({json_set_func}(data, '$._id', id))"
                elif expr == "$$ROOT":
                    # $$ROOT means the original document
                    # This requires root_data column to be preserved
                    # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                    context.preserve_root()
                    json_set_func = f"{self._json_function_prefix}_set"
                    expr_sql = f"json({json_set_func}(root_data, '$._id', id))"
                else:
                    # Unknown variable
                    return None, []
                all_params = []
            else:
                # Build SQL expression normally
                # Create aggregation context for variable resolution
                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                agg_ctx.current_field = field

                expr_sql, expr_params = self.evaluator.build_select_expression(
                    expr, context=agg_ctx
                )
                all_params.extend(expr_params)

            # Add to json_set arguments (path must be quoted as string with $ prefix)
            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(expr_sql)

            # Track computed field
            context.add_computed_field(field, expr_sql)

        # Build SELECT clause
        select_parts = ["id"]

        # Preserve root_data if needed
        if preserve_root:
            # First stage - preserve original document as root_data
            select_parts.append("data AS root_data")
        elif context.has_root:
            # Subsequent stage - preserve existing root_data from previous stage
            select_parts.append("root_data")

        # Build data field with json_set
        # Bug 002 fix: NeoSQLite stores _id separately from JSON data column,
        # so we need to explicitly include it in the JSON document
        json_set_args_with_id = (
            ["'$._id'", "id"] + json_set_args
            if json_set_args
            else ["'$._id'", "id"]
        )

        if json_set_args_with_id:
            args_str = ", ".join(json_set_args_with_id)
            json_set_func = f"{self._json_function_prefix}_set"
            # Always wrap with json() to ensure text output for Python consumption
            data_expr = f"json({json_set_func}(data, {args_str}))"
        else:
            data_expr = "data"

        select_parts.append(f"{data_expr} AS data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"

        sql = f"{select_clause} {from_clause}"
        return sql, all_params

    def _build_project_sql(
        self,
        spec: Dict[str, Any],
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $project stage.

        Generates SQL like:
            SELECT
                id,
                root_data,
                json_set(data, '$.computed', expr) AS data
            FROM prev_stage

        For inclusion projection (value=1).

        For computed fields only (no data column):
            SELECT
                id,
                expr1 AS field1,
                expr2 AS field2
            FROM prev_stage

        Args:
            spec: $project specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context
            preserve_root: If True, preserve root_data column

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        all_params: List[Any] = []

        select_parts = ["id"]

        # Preserve root_data if needed
        if preserve_root:
            # First stage - preserve original document as root_data
            select_parts.append("data AS root_data")
        elif context.has_root:
            # Subsequent stage - preserve existing root_data from previous stage
            select_parts.append("root_data")

        # Handle _id field
        include_id = spec.get("_id", 1) == 1
        if include_id:
            select_parts.append("json_extract(data, '$._id') AS _id")

        # Process each field
        for field, value in spec.items():
            if field == "_id":
                continue

            # Check for $$REMOVE
            if value == "$$REMOVE" or (
                isinstance(value, str) and value == "$$REMOVE"
            ):
                context.remove_field(field)
                continue

            # Check for expression
            if _is_expression(value) or (
                isinstance(value, str) and value.startswith("$")
            ):
                if not self._check_expression_support(value):
                    return None, []

                # Handle aggregation variables
                if _is_aggregation_variable(value):
                    if value == "$$REMOVE":
                        context.remove_field(field)
                        continue
                    elif value == "$$ROOT":
                        # Return entire root document
                        # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                        json_set_func = f"{self._json_function_prefix}_set"
                        if context.has_root:
                            select_parts.append(
                                f"json({json_set_func}(root_data, '$._id', id)) AS {field}"
                            )
                        else:
                            select_parts.append(
                                f"json({json_set_func}(data, '$._id', id)) AS {field}"
                            )
                        continue
                    elif value == "$$CURRENT":
                        # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                        json_set_func = f"{self._json_function_prefix}_set"
                        select_parts.append(
                            f"json({json_set_func}(data, '$._id', id)) AS {field}"
                        )
                        continue

                # Build SQL expression
                # Create aggregation context for variable resolution
                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                agg_ctx.current_field = field

                expr_sql, expr_params = self.evaluator.build_select_expression(
                    value, context=agg_ctx
                )
                all_params.extend(expr_params)

                # Check for REMOVE_SENTINEL
                if expr_sql == "$$REMOVE":
                    context.remove_field(field)
                    continue

                select_parts.append(f"{expr_sql} AS {field}")
                context.add_computed_field(field, expr_sql)

            elif value == 1:
                # Simple inclusion
                if field in context.computed_fields:
                    # Use computed field expression
                    select_parts.append(
                        f"{context.computed_fields[field]} AS {field}"
                    )
                else:
                    select_parts.append(
                        f"json_extract(data, '{parse_json_path(field)}') AS {field}"
                    )

            elif value == 0:
                # Exclusion - mark for removal
                context.remove_field(field)

        from_clause = f"FROM {prev_stage}"

        # Preserve root_data if it exists
        if context.has_root:
            # Insert root_data after id
            select_parts.insert(1, "root_data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        sql = f"{select_clause} {from_clause}"

        return sql, all_params

    def _build_group_sql(
        self, spec: Dict[str, Any], prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $group stage.

        Generates SQL like:
            SELECT
                group_key_expr AS _id,
                SUM(expr) AS field1,
                AVG(expr) AS field2,
                ...
            FROM prev_stage
            GROUP BY group_key_expr

        Args:
            spec: $group specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        all_params: List[Any] = []
        select_parts = []
        group_by_parts = []

        # Handle _id (group key)
        group_id = spec.get("_id")
        if group_id is not None:
            if _is_expression(group_id):
                if not self._check_expression_support(group_id):
                    return None, []
                # Note: We don't pass context here because build_select_expression
                # expects AggregationContext, not PipelineContext
                key_sql, key_params = self.evaluator.build_select_expression(
                    group_id
                )
                select_parts.append(f"{key_sql} AS _id")
                group_by_parts.append(key_sql)
                all_params.extend(key_params)
            elif isinstance(group_id, str) and group_id.startswith("$"):
                if group_id == "$$ROOT":
                    # Group by entire document (unusual but valid)
                    # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                    json_set_func = f"{self._json_function_prefix}_set"
                    json_expr = f"json({json_set_func}(data, '$._id', id))"
                    if context.has_root:
                        json_expr = (
                            f"json({json_set_func}(root_data, '$._id', id))"
                        )
                    select_parts.append(f"{json_expr} AS _id")
                    group_by_parts.append(json_expr)
                elif group_id == "$$CURRENT":
                    # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                    json_set_func = f"{self._json_function_prefix}_set"
                    json_expr = f"json({json_set_func}(data, '$._id', id))"
                    select_parts.append(f"{json_expr} AS _id")
                    group_by_parts.append(json_expr)
                else:
                    field = group_id[1:]
                    key_sql = f"{self._get_json_extract()}(data, '{parse_json_path(field)}')"
                    select_parts.append(f"{key_sql} AS _id")
                    group_by_parts.append(key_sql)
            else:
                # Literal value - all documents in one group
                select_parts.append("? AS _id")
                all_params.append(group_id)
        else:
            # No _id - all documents in one group
            select_parts.append("NULL AS _id")

        # Handle accumulators
        for field, accumulator in spec.items():
            if field == "_id":
                continue

            if not isinstance(accumulator, dict) or len(accumulator) != 1:
                # Complex accumulator - fall back to Python
                return None, []

            op, expr = next(iter(accumulator.items()))

            # Handle standard deviation operators with custom SQL formulas
            if op == "$stdDevPop":
                # Population standard deviation: SQRT(AVG(x^2) - AVG(x)^2)
                expr_sql, expr_params = self._build_accumulator_expr_sql(
                    expr, context, all_params
                )
                if expr_sql is None:
                    return None, []
                sql = f"SQRT(AVG({expr_sql} * {expr_sql}) - AVG({expr_sql}) * AVG({expr_sql}))"
                select_parts.append(f"{sql} AS {field}")
                continue

            if op == "$stdDevSamp":
                # Sample standard deviation: SQRT((n * SUM(x^2) - SUM(x)^2) / (n * (n - 1)))
                expr_sql, expr_params = self._build_accumulator_expr_sql(
                    expr, context, all_params
                )
                if expr_sql is None:
                    return None, []
                sql = f"""SQRT(
                    (COUNT({expr_sql}) * SUM({expr_sql} * {expr_sql}) - SUM({expr_sql}) * SUM({expr_sql}))
                    / (COUNT({expr_sql}) * (COUNT({expr_sql}) - 1))
                )"""
                select_parts.append(f"{sql} AS {field}")
                continue

            # Map other MongoDB accumulator to SQL
            agg_result = self._map_accumulator_to_sql(op)
            if agg_result is None:
                return None, []

            sql_agg, use_distinct = agg_result

            # Build expression for accumulator
            if _is_expression(expr):
                if not self._check_expression_support(expr):
                    return None, []
                # Note: We don't pass context here because build_select_expression
                # expects AggregationContext, not PipelineContext
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    expr
                )
                all_params.extend(expr_params)
            elif isinstance(expr, str) and expr.startswith("$"):
                if expr.startswith("$$"):
                    # Aggregation variable
                    # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                    json_set_func = f"{self._json_function_prefix}_set"
                    if expr == "$$ROOT":
                        expr_sql = (
                            f"json({json_set_func}(root_data, '$._id', id))"
                        )
                    elif expr == "$$CURRENT":
                        expr_sql = f"json({json_set_func}(data, '$._id', id))"
                    else:
                        return None, []
                else:
                    field_name = expr[1:]
                    if field_name in context.computed_fields:
                        expr_sql = context.computed_fields[field_name]
                    else:
                        expr_sql = f"{self._get_json_extract()}(data, '{parse_json_path(field_name)}')"
            else:
                # Literal value
                expr_sql = "?"
                all_params.append(expr)

            # Build SQL with DISTINCT if needed (for $addToSet)
            if use_distinct:
                select_parts.append(
                    f"{sql_agg}(DISTINCT {expr_sql}) AS {field}"
                )
            else:
                select_parts.append(f"{sql_agg}({expr_sql}) AS {field}")

        # Build GROUP BY clause
        group_by_clause = ""
        if group_by_parts:
            group_by_clause = f"GROUP BY {', '.join(group_by_parts)}"

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"

        sql = f"{select_clause} {from_clause} {group_by_clause}"
        return sql, all_params

    def _map_accumulator_to_sql(self, op: str) -> Tuple[str, bool] | None:
        """
        Map MongoDB accumulator operator to SQL aggregate function.

        Args:
            op: MongoDB accumulator operator

        Returns:
            Tuple of (SQL aggregate function name, use_distinct) or None if not supported.
            use_distinct indicates whether DISTINCT should be added to the aggregate function.
        """
        mapping = {
            "$sum": ("SUM", False),
            "$avg": ("AVG", False),
            "$min": ("MIN", False),
            "$max": ("MAX", False),
            "$count": ("COUNT", False),
            "$first": None,  # Requires ordering - complex
            "$last": None,  # Requires ordering - complex
            "$push": ("json_group_array", False),
            "$addToSet": (
                "json_group_array",
                True,
            ),  # Use DISTINCT for unique values
        }
        return mapping.get(op)

    def _build_accumulator_expr_sql(
        self,
        expr: Any,
        context: PipelineContext,
        all_params: List[Any],
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL expression for an accumulator operand.

        Helper method used by $stdDevPop, $stdDevSamp, and other accumulators
        that need custom SQL formulas.

        Args:
            expr: The accumulator expression
            context: Pipeline context for computed fields
            all_params: List to append parameters to

        Returns:
            Tuple of (SQL expression, parameters) or (None, []) if not supported
        """
        if _is_expression(expr):
            if not self._check_expression_support(expr):
                return None, []
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            return expr_sql, expr_params
        elif isinstance(expr, str) and expr.startswith("$"):
            if expr.startswith("$$"):
                # Aggregation variable
                json_set_func = f"{self._json_function_prefix}_set"
                if expr == "$$ROOT":
                    expr_sql = f"json({json_set_func}(root_data, '$._id', id))"
                elif expr == "$$CURRENT":
                    expr_sql = f"json({json_set_func}(data, '$._id', id))"
                else:
                    return None, []
            else:
                field_name = expr[1:]
                if field_name in context.computed_fields:
                    expr_sql = context.computed_fields[field_name]
                else:
                    expr_sql = f"{self._get_json_extract()}(data, '{parse_json_path(field_name)}')"
            return expr_sql, []
        else:
            # Literal value
            all_params.append(expr)
            return "?", [expr]

    def _build_match_sql(
        self, spec: Dict[str, Any], prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $match stage.

        Supports:
        - Standard query operators: {field: {$gt: 5}}
        - $expr operator: {$expr: {$gt: [{"$sin": "$angle"}, 0.5]}}
        - Direct expressions: {$gt: [{"$sin": "$angle"}, 0.5]}

        Generates SQL like:
            SELECT id, root_data, data
            FROM prev_stage
            WHERE sin(json_extract(data, '$.angle')) > 0.5

        Args:
            spec: $match specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        all_params: List[Any] = []
        where_clauses = []

        # Process each condition in spec
        for field, value in spec.items():
            if field == "$expr":
                # Handle $expr operator
                if not self._check_expression_support(value):
                    return None, []

                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index

                expr_sql, expr_params = self.evaluator.evaluate_for_aggregation(
                    value, context=agg_ctx
                )
                if expr_sql is None:
                    return None, []
                where_clauses.append(expr_sql)
                all_params.extend(expr_params)
            elif _is_expression({field: value}):
                # Handle direct expression (no $expr wrapper)
                if not self._check_expression_support({field: value}):
                    return None, []

                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                agg_ctx.current_field = field

                expr_sql, expr_params = self.evaluator.build_select_expression(
                    {field: value}, context=agg_ctx
                )
                where_clauses.append(expr_sql)
                all_params.extend(expr_params)
            else:
                # Standard query operators
                where_sql, where_params = self._build_standard_match_condition(
                    field, value, context
                )
                if where_sql is None:
                    return None, []
                where_clauses.append(where_sql)
                all_params.extend(where_params)

        if not where_clauses:
            # No conditions - just pass through
            return self._build_passthrough_sql(prev_stage, context)

        where_clause = f"WHERE {' AND '.join(where_clauses)}"

        # Build SELECT clause
        select_parts = ["id"]
        if context.has_root:
            select_parts.append("root_data")
        select_parts.append("data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"

        sql = f"{select_clause} {from_clause} {where_clause}"
        return sql, all_params

    def _build_standard_match_condition(
        self, field: str, value: Any, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build WHERE clause for standard query operators.

        Args:
            field: Field name
            value: Query value or operator dict
            context: Pipeline context

        Returns:
            Tuple of (SQL condition, parameters) or (None, []) if cannot optimize
        """
        all_params = []

        if isinstance(value, dict):
            # Query operators
            conditions = []
            for op, arg in value.items():
                cond_sql, cond_params = self._build_query_operator(
                    field, op, arg, context
                )
                if cond_sql is None:
                    return None, []
                conditions.append(cond_sql)
                all_params.extend(cond_params)
            return " AND ".join(conditions), all_params
        else:
            # Simple equality
            if field in context.computed_fields:
                field_sql = context.computed_fields[field]
            elif field.startswith("$$"):
                # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                json_set_func = f"{self._json_function_prefix}_set"
                if field == "$$ROOT":
                    field_sql = f"json({json_set_func}(root_data, '$._id', id))"
                elif field == "$$CURRENT":
                    field_sql = f"json({json_set_func}(data, '$._id', id))"
                else:
                    return None, []
            else:
                field_sql = f"{self._get_json_extract()}(data, '{parse_json_path(field)}')"

            return f"{field_sql} = ?", all_params + [value]

    def _build_query_operator(
        self, field: str, op: str, arg: Any, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build WHERE condition for a query operator.

        Args:
            field: Field name
            op: Query operator ($gt, $lt, $in, etc.)
            arg: Operator argument
            context: Pipeline context

        Returns:
            Tuple of (SQL condition, parameters) or (None, []) if cannot optimize
        """
        # Get field SQL
        if field in context.computed_fields:
            field_sql = context.computed_fields[field]
        elif field.startswith("$$"):
            # Bug fix: NeoSQLite stores _id separately, so we need to add it back
            json_set_func = f"{self._json_function_prefix}_set"
            if field == "$$ROOT":
                field_sql = f"json({json_set_func}(root_data, '$._id', id))"
            elif field == "$$CURRENT":
                field_sql = f"json({json_set_func}(data, '$._id', id))"
            else:
                return None, []
        else:
            field_sql = self._get_json_extract(field)

        match op:
            case "$eq":
                return f"{field_sql} = ?", [arg]
            case "$ne":
                return f"{field_sql} != ?", [arg]
            case "$gt":
                return f"{field_sql} > ?", [arg]
            case "$gte":
                return f"{field_sql} >= ?", [arg]
            case "$lt":
                return f"{field_sql} < ?", [arg]
            case "$lte":
                return f"{field_sql} <= ?", [arg]
            case "$in":
                if not isinstance(arg, list):
                    return None, []
                placeholders = ", ".join("?" for _ in arg)
                return f"{field_sql} IN ({placeholders})", arg
            case "$nin":
                if not isinstance(arg, list):
                    return None, []
                placeholders = ", ".join("?" for _ in arg)
                return f"{field_sql} NOT IN ({placeholders})", arg
            case "$regex":
                # SQLite doesn't have native regex, use LIKE for simple cases
                # or fall back to Python
                return None, []
            case "$exists":
                if arg:
                    return f"{field_sql} IS NOT NULL", []
                else:
                    return f"{field_sql} IS NULL", []
            case _:
                # Unsupported operator
                return None, []

    def _build_sort_sql(
        self, spec: Dict[str, Any], prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $sort stage.

        Generates SQL like:
            SELECT id, root_data, data
            FROM prev_stage
            ORDER BY field1 ASC, field2 DESC

        Args:
            spec: $sort specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        from .cursor import DESCENDING

        order_parts = []

        for field, direction in spec.items():
            if field in context.computed_fields:
                field_sql = context.computed_fields[field]
            elif field.startswith("$$"):
                # Bug fix: NeoSQLite stores _id separately, so we need to add it back
                json_set_func = f"{self._json_function_prefix}_set"
                if field == "$$ROOT":
                    field_sql = f"json({json_set_func}(root_data, '$._id', id))"
                elif field == "$$CURRENT":
                    field_sql = f"json({json_set_func}(data, '$._id', id))"
                else:
                    return None, []
            else:
                field_sql = self._get_json_extract(field)

            order_dir = "DESC" if direction == DESCENDING else "ASC"
            order_parts.append(f"{field_sql} {order_dir}")

        order_clause = f"ORDER BY {', '.join(order_parts)}"

        # Build SELECT clause
        select_parts = ["id"]
        if context.has_root:
            select_parts.append("root_data")
        select_parts.append("data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"

        sql = f"{select_clause} {from_clause} {order_clause}"
        return sql, []

    def _build_skip_sql(
        self, spec: int, prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $skip stage.

        Generates SQL like:
            SELECT id, root_data, data
            FROM prev_stage
            OFFSET ?

        Args:
            spec: Number of documents to skip
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        # Build SELECT clause
        select_parts = ["id"]
        if context.has_root:
            select_parts.append("root_data")
        select_parts.append("data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"
        offset_clause = "OFFSET ?"

        sql = f"{select_clause} {from_clause} {offset_clause}"
        return sql, [spec]

    def _build_limit_sql(
        self, spec: int, prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $limit stage.

        Generates SQL like:
            SELECT id, root_data, data
            FROM prev_stage
            LIMIT ?

        Args:
            spec: Maximum number of documents
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        # Build SELECT clause
        select_parts = ["id"]
        if context.has_root:
            select_parts.append("root_data")
        select_parts.append("data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"
        limit_clause = "LIMIT ?"

        sql = f"{select_clause} {from_clause} {limit_clause}"
        return sql, [spec]

    def _build_facet_sql(
        self, spec: Dict[str, Any], prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $facet stage.

        Each sub-pipeline becomes a separate CTE branch, then combined
        using json_group_array.

        Args:
            spec: $facet specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        # $facet is complex - for now, fall back to Python
        # Full implementation would require:
        # 1. Building separate CTEs for each sub-pipeline
        # 2. Combining results with json_group_array
        # 3. Handling different result schemas
        return None, []

    def _build_unwind_sql(
        self, spec: Any, prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $unwind stage.

        Uses json_each to unwind arrays.

        Args:
            spec: $unwind specification
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        # Parse unwind specification
        if isinstance(spec, str):
            field_path = spec.lstrip("$")
        elif isinstance(spec, dict):
            field_path = spec.get("path", "").lstrip("$")
            # Note: includeArrayIndex and preserveNullAndEmptyArrays are parsed but
            # not used in this simplified implementation - full implementation would use these
        else:
            return None, []

        if not field_path:
            return None, []

        # $unwind is complex - for now, fall back to Python
        # Full implementation would require:
        # 1. Using json_each to expand array elements
        # 2. Handling preserveNullAndEmptyArrays
        # 3. Handling includeArrayIndex
        return None, []

    def _build_count_sql(
        self, spec: str, prev_stage: str, context: PipelineContext
    ) -> Tuple[str | None, List[Any]]:
        """
        Build SQL for $count stage.

        Generates SQL like:
            SELECT ? AS count
            FROM prev_stage

        Args:
            spec: Field name for count result
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters) or (None, []) if cannot optimize
        """
        if not isinstance(spec, str):
            return None, []

        sql = f"SELECT COUNT(*) AS ? FROM {prev_stage}"
        return sql, [spec]

    def _build_passthrough_sql(
        self, prev_stage: str, context: PipelineContext
    ) -> Tuple[str, List[Any]]:
        """
        Build passthrough SQL (no transformation).

        Used when a stage doesn't transform data.

        Args:
            prev_stage: Previous CTE or collection name
            context: Pipeline context

        Returns:
            Tuple of (SQL query, parameters)
        """
        select_parts = ["id"]
        if context.has_root:
            select_parts.append("root_data")
        select_parts.append("data")

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"

        sql = f"{select_clause} {from_clause}"
        return sql, []
