"""
SQL Tier 1 Optimizer for Aggregation Pipelines.

This module implements SQL-based optimization for aggregation pipelines,
providing 10-100x performance improvements over Python fallback (Tier 3).

The optimizer analyzes aggregation pipelines and generates optimized SQL queries
using CTEs (Common Table Expressions) for multi-stage pipelines.
"""

from __future__ import annotations

from typing import Any, cast

from .._sqlite import sqlite3
from ..sql_utils import quote_table_name
from .expr_evaluator import (
    AggregationContext,
    ExprEvaluator,
    _is_aggregation_variable,
    _is_expression,
)
from .json_path_utils import parse_json_path
from .jsonb_support import (
    _get_json_each_function,
    supports_jsonb,
    supports_jsonb_each,
)
from .query_helper.translation_cache import TranslationCache
from .query_helper.utils import _get_json_function


class PipelineContext:
    """
    Tracks field aliases, computed fields, and document state across pipeline stages.
    """

    def __init__(self) -> None:
        """Initialize pipeline context with default state."""
        self.computed_fields: dict[str, str] = {}
        self.removed_fields: set[str] = set()
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

    def __init__(
        self,
        collection,
        expr_evaluator: ExprEvaluator | None = None,
        translation_cache_size: int | None = 100,
    ):
        """Initialize the SQL tier aggregator."""
        self.collection = collection
        self.evaluator = expr_evaluator or ExprEvaluator(
            data_column="data", db_connection=collection.db
        )
        self._jsonb_supported = supports_jsonb(collection.db)
        self._jsonb_each_supported = supports_jsonb_each(collection.db)
        self._json_function_prefix = (
            "jsonb" if self._jsonb_supported else "json"
        )
        self._json_each_function = _get_json_each_function(
            self._jsonb_supported, self._jsonb_each_supported
        )
        self._jsonb_supported = supports_jsonb(collection.db)
        self._json_function_prefix = (
            "jsonb" if self._jsonb_supported else "json"
        )
        # translation_cache_size: None = use default, 0 = disable, positive = custom size
        if translation_cache_size is None:
            translation_cache_size = 100
        self._translation_cache = TranslationCache(
            max_size=translation_cache_size
        )

    def _get_json_extract(self, path: str | None = None) -> str:
        """Get JSON extract function with correct prefix."""
        if path:
            return f"{self._json_function_prefix}_extract(data, '${path}')"
        return f"{self._json_function_prefix}_extract"

    def _get_json_set(self) -> str:
        """Get JSON set function with correct prefix."""
        return f"{self._json_function_prefix}_set"

    def can_optimize_pipeline(self, pipeline: list[dict[str, Any]]) -> bool:
        """Check if pipeline can be optimized in SQL tier."""
        from .query_helper import get_force_fallback

        if get_force_fallback():
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

    def _can_optimize_stage_expressions(self, stage: dict[str, Any]) -> bool:
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
        self, pipeline: list[dict[str, Any]]
    ) -> tuple[str | None, list[Any]]:
        """Build optimized SQL query for entire pipeline using CTEs."""
        if not pipeline or not self.can_optimize_pipeline(pipeline):
            return None, []

        # Try to get from cache
        cache_key = self._translation_cache.make_key(pipeline)
        cached = self._translation_cache.get(cache_key)

        if cached is not None:
            sql_template, param_names = cached
            # Extract actual parameter values from pipeline based on cached names
            params = self._extract_param_values(pipeline, param_names)
            return sql_template, params

        # Build SQL (cache miss)
        sql_result = self._build_sql_template(pipeline)
        if sql_result is None or sql_result[0] is None:
            return None, []

        # Use cast to help type checker
        sql_template, all_params = cast(tuple[str, list[Any]], sql_result)

        # Cache the template - extract param names from pipeline for robustness
        param_names = tuple(self._extract_param_names_from_pipeline(pipeline))
        self._translation_cache.put(cache_key, sql_template, param_names)

        return sql_template, all_params

    def _build_sql_template(
        self, pipeline: list[dict[str, Any]]
    ) -> tuple[str | None, list[Any]]:
        """Build SQL template and return (template, params)."""
        cte_parts: list[str] = []
        all_params: list[Any] = []
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

    def _extract_param_values(
        self, pipeline: list[dict[str, Any]], param_names: tuple[str, ...]
    ) -> list[Any]:
        """Extract actual parameter values from pipeline for given field paths."""
        # Map placeholder names to their values in pipeline
        placeholder_values = self._get_placeholder_values(pipeline)

        params = []
        for field_path in param_names:
            if field_path.startswith("__placeholder_"):
                # It's a placeholder - get value from pipeline
                value = placeholder_values.get(field_path)
                if value is not None:
                    params.append(value)
            else:
                # Original field path extraction
                value = self._get_value_at_path(pipeline, field_path)
                if value is not None:
                    params.append(value)
        return params

    def _get_placeholder_values(
        self, pipeline: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Extract values for placeholder parameters from pipeline."""
        values = {}
        placeholder_idx = 0

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            spec = stage[stage_name]

            match stage_name, spec:
                case "$sample", {"size": size}:
                    values[f"__placeholder_{placeholder_idx}__"] = size
                    placeholder_idx += 1

                case "$limit", int() as limit_val if isinstance(spec, int):
                    values[f"__placeholder_{placeholder_idx}__"] = limit_val
                    placeholder_idx += 1

                case "$skip", int() as skip_val if isinstance(spec, int):
                    values[f"__placeholder_{placeholder_idx}__"] = skip_val
                    placeholder_idx += 1

                case (
                    (
                        "$addFields"
                        | "$project"
                        | "$set"
                        | "$replaceRoot"
                        | "$replaceWith"
                    ),
                    dict() as expr_spec,
                ) if isinstance(spec, dict):
                    for field, expr in expr_spec.items():
                        if isinstance(expr, dict):
                            placeholder_idx = (
                                self._extract_literal_values_from_expression(
                                    expr, values, placeholder_idx
                                )
                            )

                case "$count", str() as field_name if isinstance(spec, str):
                    values[f"__placeholder_{placeholder_idx}__"] = field_name
                    placeholder_idx += 1

                case "$bucket", dict() as bucket_spec if isinstance(spec, dict):
                    # Extract from groupBy
                    if "groupBy" in bucket_spec:
                        placeholder_idx = (
                            self._extract_literal_values_from_expression(
                                bucket_spec["groupBy"], values, placeholder_idx
                            )
                        )
                    # Extract from boundaries - 3 params per range
                    if "boundaries" in bucket_spec:
                        boundaries = bucket_spec["boundaries"]
                        for i in range(len(boundaries) - 1):
                            lower = boundaries[i]
                            upper = boundaries[i + 1]
                            # _build_bucket_sql uses: [lower, upper, lower]
                            values[f"__placeholder_{placeholder_idx}__"] = lower
                            placeholder_idx += 1
                            values[f"__placeholder_{placeholder_idx}__"] = upper
                            placeholder_idx += 1
                            values[f"__placeholder_{placeholder_idx}__"] = lower
                            placeholder_idx += 1
                    # Extract from default
                    if (default := bucket_spec.get("default")) is not None:
                        values[f"__placeholder_{placeholder_idx}__"] = default
                        placeholder_idx += 1
                    # Extract from output accumulators
                    if "output" in bucket_spec:
                        for field, accumulator in bucket_spec["output"].items():
                            if isinstance(accumulator, dict):
                                for op, expr in accumulator.items():
                                    if op.startswith("$") and op != "$meta":
                                        placeholder_idx = self._extract_literal_values_from_expression(
                                            expr, values, placeholder_idx
                                        )

                case "$bucketAuto", dict() as bucket_auto_spec if isinstance(
                    spec, dict
                ):
                    if (
                        granularity := bucket_auto_spec.get("granularity")
                    ) is not None:
                        values[f"__placeholder_{placeholder_idx}__"] = (
                            granularity
                        )
                        placeholder_idx += 1

                case "$fill", dict() as fill_spec if isinstance(spec, dict):
                    for field, fill_expr in fill_spec.items():
                        if isinstance(fill_expr, dict):
                            for op, value in fill_expr.items():
                                if op.startswith("$") and op != "$meta":
                                    placeholder_idx = self._extract_literal_values_from_expression(
                                        {op: value}, values, placeholder_idx
                                    )

        return values

    def _extract_literal_values_from_expression(
        self,
        expr: Any,
        values: dict[str, Any],
        placeholder_idx: int,
    ) -> int:
        """Extract literal values from an expression and store them in values dict."""
        match expr:
            case dict() as d:
                for op, value in d.items():
                    if op.startswith("$"):
                        match value:
                            case list() as lst:
                                for item in lst:
                                    placeholder_idx = self._extract_literal_values_from_expression(
                                        item, values, placeholder_idx
                                    )
                            case dict() as nested:
                                placeholder_idx = self._extract_literal_values_from_expression(
                                    nested, values, placeholder_idx
                                )
                            case str() as s if not s.startswith("$"):
                                values[f"__placeholder_{placeholder_idx}__"] = s
                                placeholder_idx += 1
                            case _ if value is not None:
                                values[f"__placeholder_{placeholder_idx}__"] = (
                                    value
                                )
                                placeholder_idx += 1

            case list() as lst:
                for item in lst:
                    placeholder_idx = (
                        self._extract_literal_values_from_expression(
                            item, values, placeholder_idx
                        )
                    )

            case _ if expr is not None and not isinstance(
                expr, (dict, list, str)
            ):
                values[f"__placeholder_{placeholder_idx}__"] = expr
                placeholder_idx += 1

        return placeholder_idx

    def _extract_param_names_from_pipeline(
        self, pipeline: list[dict[str, Any]]
    ) -> list[str]:
        """Extract parameter names from pipeline structure directly.

        This is more robust than parsing SQL template since it works directly
        with the MongoDB pipeline specification.
        """
        params: list[str] = []
        placeholder_idx = 0

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            spec = stage[stage_name]

            match stage_name, spec:
                case "$match", dict() as match_spec:
                    field_paths = self._extract_field_paths_from_dict(
                        match_spec
                    )
                    params.extend(field_paths)

                case "$sample", {"size": _}:
                    params.append(f"__placeholder_{placeholder_idx}__")
                    placeholder_idx += 1

                case "$limit", int():
                    params.append(f"__placeholder_{placeholder_idx}__")
                    placeholder_idx += 1

                case "$skip", int():
                    params.append(f"__placeholder_{placeholder_idx}__")
                    placeholder_idx += 1

                case "$count", str():
                    params.append(f"__placeholder_{placeholder_idx}__")
                    placeholder_idx += 1

                case "$group", dict() as group_spec:
                    if (
                        "_id" in group_spec
                        and isinstance(group_spec["_id"], str)
                        and group_spec["_id"].startswith("$")
                    ):
                        params.append(group_spec["_id"])
                    for key, value in group_spec.items():
                        if key != "_id" and isinstance(value, dict):
                            for op, field in value.items():
                                if (
                                    op.startswith("$")
                                    and isinstance(field, str)
                                    and field.startswith("$")
                                ):
                                    params.append(field)

                case (
                    (
                        "$addFields"
                        | "$project"
                        | "$set"
                        | "$replaceRoot"
                        | "$replaceWith"
                    ),
                    dict() as expr_spec,
                ):
                    for field, expr in expr_spec.items():
                        if isinstance(expr, dict):
                            placeholder_idx = (
                                self._extract_params_from_expression(
                                    expr, params, placeholder_idx
                                )
                            )

                case "$unset", list() as unset_list:
                    for item in unset_list:
                        if isinstance(item, str) and item.startswith("$"):
                            params.append(item)
                        elif isinstance(item, dict):
                            for k in item.keys():
                                if k.startswith("$"):
                                    params.append(k)

                case "$unset", str() as unset_str if isinstance(spec, str):
                    if unset_str.startswith("$"):
                        params.append(unset_str)

                case "$bucket", dict() as bucket_spec:
                    # Extract from groupBy
                    if "groupBy" in bucket_spec:
                        placeholder_idx = self._extract_params_from_expression(
                            bucket_spec["groupBy"], params, placeholder_idx
                        )
                    # Extract from boundaries - 3 params per range
                    if "boundaries" in bucket_spec:
                        boundaries = bucket_spec["boundaries"]
                        for i in range(len(boundaries) - 1):
                            # _build_bucket_sql uses: [lower, upper, lower]
                            for _ in range(3):
                                params.append(
                                    f"__placeholder_{placeholder_idx}__"
                                )
                                placeholder_idx += 1
                    # Extract from default
                    if bucket_spec.get("default") is not None:
                        params.append(f"__placeholder_{placeholder_idx}__")
                        placeholder_idx += 1
                    # Extract from output accumulators
                    if "output" in bucket_spec:
                        for field, accumulator in bucket_spec["output"].items():
                            if isinstance(accumulator, dict):
                                for op, expr in accumulator.items():
                                    if op.startswith("$") and op != "$meta":
                                        placeholder_idx = self._extract_params_from_expression(
                                            expr, params, placeholder_idx
                                        )

                case "$bucketAuto", dict() as bucket_auto_spec:
                    if "granularity" in bucket_auto_spec:
                        params.append(f"__placeholder_{placeholder_idx}__")
                        placeholder_idx += 1

                case "$fill", dict() as fill_spec:
                    for field, fill_expr in fill_spec.items():
                        if isinstance(fill_expr, dict):
                            for op, value in fill_expr.items():
                                if op.startswith("$") and op != "$meta":
                                    placeholder_idx = (
                                        self._extract_params_from_expression(
                                            value, params, placeholder_idx
                                        )
                                    )

        return params

    def _extract_params_from_expression(
        self,
        expr: Any,
        params: list[str],
        placeholder_idx: int,
    ) -> int:
        """Extract field references and literal values from an expression.

        For expressions like {"$multiply": ["$salary", 0.1]}:
        - Field references ($salary) are extracted directly
        - Literal values (0.1) are stored as placeholders

        Returns the updated placeholder_idx.
        """
        match expr:
            case dict() as d:
                for op, value in d.items():
                    if op.startswith("$"):
                        match value:
                            case list() as lst:
                                for item in lst:
                                    placeholder_idx = (
                                        self._extract_params_from_expression(
                                            item, params, placeholder_idx
                                        )
                                    )
                            case dict() as nested:
                                placeholder_idx = (
                                    self._extract_params_from_expression(
                                        nested, params, placeholder_idx
                                    )
                                )
                            case str() as s if s.startswith("$"):
                                params.append(s)
                            case _ if value is not None:
                                params.append(
                                    f"__placeholder_{placeholder_idx}__"
                                )
                                placeholder_idx += 1

            case list() as lst:
                for item in lst:
                    placeholder_idx = self._extract_params_from_expression(
                        item, params, placeholder_idx
                    )

            case str() as s if s.startswith("$"):
                params.append(s)

            case _ if expr is not None and not isinstance(expr, (dict, list)):
                params.append(f"__placeholder_{placeholder_idx}__")
                placeholder_idx += 1

        return placeholder_idx

    def _extract_field_paths_from_dict(
        self, d: dict, prefix: str = "$"
    ) -> list[str]:
        """Recursively extract all field paths from a dict."""
        paths = []
        for key, value in d.items():
            if key.startswith("$"):
                if isinstance(value, dict):
                    continue
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            paths.extend(
                                self._extract_field_paths_from_dict(
                                    item, prefix
                                )
                            )
            else:
                if isinstance(value, dict):
                    has_operator = any(k.startswith("$") for k in value.keys())
                    if has_operator:
                        # This field has operators like $gt, $eq, etc.
                        # Add the field path
                        paths.append(f"{prefix}{key}")
                        # Also recurse into nested dicts to find deeply nested fields
                        for op_key, op_value in value.items():
                            if isinstance(op_value, dict):
                                paths.extend(
                                    self._extract_field_paths_from_dict(
                                        op_value, f"{prefix}{key}."
                                    )
                                )
                    else:
                        # No operators, this is a nested structure
                        paths.append(f"{prefix}{key}")
                        paths.extend(
                            self._extract_field_paths_from_dict(
                                value, f"{prefix}{key}."
                            )
                        )
                else:
                    # Direct value
                    paths.append(f"{prefix}{key}")
        return paths

    def _get_value_at_path(
        self, pipeline: list[dict[str, Any]], field_path: str
    ) -> Any:
        """Get value from pipeline at given field path."""
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            spec = stage[stage_name]
            if isinstance(spec, dict):
                value = self._find_value_in_dict(spec, field_path)
                if value is not None:
                    return value
        return None

    def _find_value_in_dict(self, d: dict, field_path: str) -> Any:
        """Recursively find value in dict matching field path.

        Also handles comparison operators by extracting the actual operand.
        """
        target_field = field_path.lstrip("$")
        for key, value in d.items():
            if key == target_field:
                if isinstance(value, dict):
                    return self._extract_comparison_value(value)
                return value
            if isinstance(value, dict):
                result = self._find_value_in_dict(value, field_path)
                if result is not None:
                    return result
        return None

    def _extract_comparison_value(self, d: dict) -> Any:
        """Extract actual value from comparison operator dict.

        Handles operators like {"$gt": 25}, {"$gte": 10}, {"$in": [1,2,3]}, etc.
        Returns the actual operand value, not the operator dict.
        """
        if not d or len(d) != 1:
            return d

        op, value = next(iter(d.items()))
        if not op.startswith("$"):
            return d

        COMPARISON_OPS = {
            "$eq",
            "$ne",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "$in",
            "$nin",
            "$exists",
            "$type",
            "$all",
            "$elemMatch",
            "$not",
            "$regex",
            "$options",
            "$mod",
            "$text",
        }

        if op in COMPARISON_OPS:
            return value
        return d

    def get_cache_stats(self) -> dict[str, Any]:
        """Get pipeline cache statistics."""
        stats = self._translation_cache.get_stats()
        stats["enabled"] = self._translation_cache.is_enabled()
        return stats

    def clear_cache(self) -> None:
        """Clear the pipeline cache."""
        self._translation_cache.clear()

    def dump_cache(self) -> list[dict]:
        """Dump all cache entries for debugging."""
        return self._translation_cache.dump()

    def cache_contains(self, pipeline: list[dict]) -> bool:
        """Check if pipeline is in cache."""
        key = self._translation_cache.make_key(pipeline)
        return self._translation_cache.contains(key)

    def evict_from_cache(self, pipeline: list[dict]) -> bool:
        """Evict a specific pipeline from cache."""
        key = self._translation_cache.make_key(pipeline)
        return self._translation_cache.evict(key)

    def cache_size(self) -> int:
        """Get current cache size."""
        return len(self._translation_cache)

    def is_cache_enabled(self) -> bool:
        """Check if cache is enabled."""
        return self._translation_cache.is_enabled()

    def resize_cache(self, new_size: int) -> None:
        """Resize the cache."""
        self._translation_cache.resize(new_size)

    def _pipeline_needs_root(self, pipeline: list[dict[str, Any]]) -> bool:
        """Check if pipeline uses $$ROOT variable."""
        for stage in pipeline:
            if self._stage_uses_root(stage):
                return True
        return False

    def _stage_uses_root(self, stage: dict[str, Any]) -> bool:
        """Check if a stage uses $$ROOT variable."""
        stage_name = next(iter(stage.keys()))
        spec = stage[stage_name]
        return self._expression_uses_root(spec)

    def _expression_uses_root(self, obj: Any) -> bool:
        """Recursively check if expression uses $$ROOT."""
        match obj:
            case str():
                return obj == "$$ROOT"
            case dict():
                return any(self._expression_uses_root(v) for v in obj.values())
            case list():
                return any(self._expression_uses_root(i) for i in obj)
            case _:
                return False

    def _build_stage_sql(
        self,
        stage_name: str,
        stage_spec: Any,
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> tuple[str | None, list[Any]]:
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
            case "$facet":
                return None, []  # $facet not supported in SQL tier
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

        has_expressions_or_refs = any(
            _is_expression(value)
            or (isinstance(value, str) and value.startswith("$"))
            for value in spec.values()
        )

        include_id = spec.get("_id", 1) == 1
        if include_id:
            select_parts.append("_id")

        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")

        if has_expressions_or_refs:
            include_id = "_id" in spec and spec["_id"] != 0
        else:
            include_id = spec.get("_id", 1) == 1

        json_extract_func = f"{self._json_function_prefix}_extract"
        json_obj_func = f"{self._json_function_prefix}_object"

        json_parts = []

        for field, value in spec.items():
            if field == "_id":
                continue

            if _is_expression(value):
                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    value, context=agg_ctx
                )
                if expr_sql is None:
                    return None, []
                all_params.extend(expr_params)
                json_parts.append(f"'{field}'")
                json_parts.append(expr_sql)
                context.add_computed_field(field, expr_sql)
            elif isinstance(value, str) and value.startswith("$"):
                source_field = value[1:]
                if source_field == "_id":
                    json_parts.append(f"'{field}'")
                    json_parts.append("_id")
                else:
                    json_parts.append(f"'{field}'")
                    json_parts.append(
                        f"{json_extract_func}(data, '{parse_json_path(source_field)}')"
                    )
            elif value == 1:
                json_parts.append(f"'{field}'")
                json_parts.append(
                    f"{json_extract_func}(data, '{parse_json_path(field)}')"
                )

        if json_parts:
            data_expr = f"json({json_obj_func}({', '.join(json_parts)}))"
        else:
            data_expr = "json({})"

        select_parts.append(f"{data_expr} AS data")

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
        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY RANDOM() LIMIT ?"
        return sql, [size]

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

        def _wrap_for_where(expr_sql):
            """Wrap expression to work in WHERE clause if it returns JSON booleans."""
            if "json('true')" in expr_sql or 'json("true")' in expr_sql:
                return f"({expr_sql} = json('true'))"
            return expr_sql

        for field, value in spec.items():
            if field == "$expr":
                expr_sql, expr_params = self.evaluator.evaluate(value)
                if not expr_sql:
                    return None, []
                expr_sql = _wrap_for_where(expr_sql)
                where_clauses.append(expr_sql)
                all_params.extend(expr_params)
            elif field.startswith("$") and _is_expression({field: value}):
                # Handle direct expression (no $expr wrapper)
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    {field: value}
                )
                if not expr_sql:
                    return None, []
                expr_sql = _wrap_for_where(expr_sql)
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
                        match op:
                            case "$gt":
                                where_clauses.append(f"{field_sql} > ?")
                                all_params.append(arg)
                            case "$lt":
                                where_clauses.append(f"{field_sql} < ?")
                                all_params.append(arg)
                            case "$gte":
                                where_clauses.append(f"{field_sql} >= ?")
                                all_params.append(arg)
                            case "$lte":
                                where_clauses.append(f"{field_sql} <= ?")
                                all_params.append(arg)
                            case "$eq":
                                where_clauses.append(f"{field_sql} = ?")
                                all_params.append(arg)
                            case "$ne":
                                where_clauses.append(f"{field_sql} != ?")
                                all_params.append(arg)
                            case "$in":
                                if isinstance(arg, (list, tuple)):
                                    placeholders = ", ".join("?" for _ in arg)
                                    where_clauses.append(
                                        f"EXISTS (SELECT 1 FROM {self._json_each_function}({field_sql}) WHERE json_each.value IN ({placeholders}))"
                                    )
                                    all_params.extend(arg)
                                else:
                                    return None, []
                            case "$nin":
                                if isinstance(arg, (list, tuple)):
                                    placeholders = ", ".join("?" for _ in arg)
                                    where_clauses.append(
                                        f"NOT EXISTS (SELECT 1 FROM {self._json_each_function}({field_sql}) WHERE json_each.value IN ({placeholders}))"
                                    )
                                    all_params.extend(arg)
                                else:
                                    return None, []
                            case "$all":
                                if isinstance(arg, (list, tuple)):
                                    if len(arg) == 0:
                                        return None, []
                                    for v in arg:
                                        where_clauses.append(
                                            f"EXISTS (SELECT 1 FROM {self._json_each_function}({field_sql}) WHERE json_each.value = ?)"
                                        )
                                        all_params.append(v)
                                else:
                                    return None, []
                            case _:
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
