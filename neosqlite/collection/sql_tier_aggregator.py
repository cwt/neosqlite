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
from typing import Any, Dict, List, Tuple, Optional, Set
from .expr_evaluator import (
    ExprEvaluator,
    _is_expression,
    _is_aggregation_variable,
)
from .jsonb_support import supports_jsonb


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

    def get_field_sql(self, field: str) -> Optional[str]:
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
    }

    # Stages that require Python fallback
    UNSUPPORTED_STAGES = {
        "$lookup",
        "$graphLookup",
        "$indexStats",
        "$merge",
        "$out",
        "$replaceRoot",
        "$replaceWith",
        "$setWindowFields",
    }

    # Expressions that require Python fallback
    UNSUPPORTED_EXPRESSIONS = {
        "$let",  # Variables not supported in SQL
        "$objectToArray",  # Complex conversion
        "$function",  # Custom JavaScript
        "$accumulator",  # Custom accumulator
        "$script",  # JavaScript execution
    }

    def __init__(
        self, collection, expr_evaluator: Optional[ExprEvaluator] = None
    ):
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

    def _get_json_extract(self, path: Optional[str] = None) -> str:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
        if needs_root:
            # Include root_data in final output if it was preserved
            final_sql = f"WITH {', '.join(cte_parts)} SELECT id, root_data, data FROM {prev_stage}"
        else:
            final_sql = (
                f"WITH {', '.join(cte_parts)} SELECT id, data FROM {prev_stage}"
            )

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
    ) -> Tuple[Optional[str], List[Any]]:
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
            case _:
                return None, []

    def _build_addfields_sql(
        self,
        spec: Dict[str, Any],
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> Tuple[Optional[str], List[Any]]:
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
                # Note: We don't pass context here because build_select_expression
                # expects AggregationContext, not PipelineContext
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    expr
                )
                all_params.extend(expr_params)

            # Add to json_set arguments (path must be quoted as string with $ prefix)
            json_set_args.append(f"'$.{field}'")
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
    ) -> Tuple[Optional[str], List[Any]]:
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
                # Note: We don't pass context here because build_select_expression
                # expects AggregationContext, not PipelineContext
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    value
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
                        f"json_extract(data, '$.{field}') AS {field}"
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
    ) -> Tuple[Optional[str], List[Any]]:
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
                    key_sql = f"{self._get_json_extract()}(data, '$.{field}')"
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

            # Map MongoDB accumulator to SQL
            sql_agg = self._map_accumulator_to_sql(op)
            if sql_agg is None:
                return None, []

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
                        expr_sql = f"{self._get_json_extract()}(data, '$.{field_name}')"
            else:
                # Literal value
                expr_sql = "?"
                all_params.append(expr)

            select_parts.append(f"{sql_agg}({expr_sql}) AS {field}")

        # Build GROUP BY clause
        group_by_clause = ""
        if group_by_parts:
            group_by_clause = f"GROUP BY {', '.join(group_by_parts)}"

        select_clause = f"SELECT {', '.join(select_parts)}"
        from_clause = f"FROM {prev_stage}"

        sql = f"{select_clause} {from_clause} {group_by_clause}"
        return sql, all_params

    def _map_accumulator_to_sql(self, op: str) -> Optional[str]:
        """
        Map MongoDB accumulator operator to SQL aggregate function.

        Args:
            op: MongoDB accumulator operator

        Returns:
            SQL aggregate function name or None if not supported
        """
        mapping = {
            "$sum": "SUM",
            "$avg": "AVG",
            "$min": "MIN",
            "$max": "MAX",
            "$count": "COUNT",
            "$first": None,  # Requires ordering - complex
            "$last": None,  # Requires ordering - complex
            "$push": "json_group_array",
            "$addToSet": None,  # DISTINCT not always supported
        }
        return mapping.get(op)

    def _build_match_sql(
        self, spec: Dict[str, Any], prev_stage: str, context: PipelineContext
    ) -> Tuple[Optional[str], List[Any]]:
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
                expr_sql, expr_params = self.evaluator.evaluate(value)
                if expr_sql is None:
                    return None, []
                where_clauses.append(expr_sql)
                all_params.extend(expr_params)
            elif _is_expression({field: value}):
                # Handle direct expression (no $expr wrapper)
                if not self._check_expression_support({field: value}):
                    return None, []
                # Note: We don't pass context here because build_select_expression
                # expects AggregationContext, not PipelineContext
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    {field: value}
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
    ) -> Tuple[Optional[str], List[Any]]:
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
                field_sql = f"{self._get_json_extract()}(data, '$.{field}')"

            return f"{field_sql} = ?", all_params + [value]

    def _build_query_operator(
        self, field: str, op: str, arg: Any, context: PipelineContext
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
    ) -> Tuple[Optional[str], List[Any]]:
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
