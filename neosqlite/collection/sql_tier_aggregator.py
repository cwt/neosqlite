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

        # Try to get from cache. Keys canonicalize literal values, so a hit
        # implies an identical pipeline and the stored parameters are exact.
        cache_key = self._translation_cache.make_key(pipeline)
        cached = self._translation_cache.get(cache_key)

        if cached is not None:
            sql_template, params = cached
            return sql_template, list(params)

        # Build SQL (cache miss)
        sql_result = self._build_sql_template(pipeline)
        if sql_result is None or sql_result[0] is None:
            return None, []

        # Use cast to help type checker
        sql_template, all_params = cast(tuple[str, list[Any]], sql_result)

        # Cache the template together with its exact parameter values
        self._translation_cache.put(cache_key, sql_template, tuple(all_params))

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
