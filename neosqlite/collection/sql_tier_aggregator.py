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
from ._stage_builders import StageBuildersMixin
from .expr_evaluator import (
    ExprEvaluator,
)
from .jsonb_support import JSONBContext
from .pipeline_context import PipelineContext
from .query_helper.translation_cache import TranslationCache


class SQLTierAggregator(StageBuildersMixin):
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
        self.jsonb = JSONBContext.from_db(collection.db)
        # translation_cache_size: None = use default, 0 = disable, positive = custom size
        if translation_cache_size is None:
            translation_cache_size = 100
        self._translation_cache = TranslationCache(
            max_size=translation_cache_size
        )

    def _get_json_extract(self, path: str | None = None) -> str:
        """Get JSON extract function with correct prefix."""
        if path:
            return f"{self.jsonb.json_function_prefix}_extract(data, '${path}')"
        return f"{self.jsonb.json_function_prefix}_extract"

    def _get_json_set(self) -> str:
        """Get JSON set function with correct prefix."""
        return f"{self.jsonb.json_function_prefix}_set"

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
