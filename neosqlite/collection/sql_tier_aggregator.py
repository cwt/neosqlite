"""SQL Tier 1 Optimizer for Aggregation Pipelines.

This module implements SQL-based optimization for aggregation pipelines,
providing 10-100x performance improvements over Python fallback (Tier 3).

The optimizer analyzes aggregation pipelines and generates optimized SQL
queries using CTEs (Common Table Expressions) for multi-stage pipelines.
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
    """SQL Tier 1 optimizer for MongoDB aggregation pipelines.

    Validates whether an aggregation pipeline can be compiled into a single
    parameterized SQLite query using Common Table Expressions (CTEs) and
    caches compiled SQL templates via ``TranslationCache``. Stage-specific
    SQL compilation methods are supplied by ``StageBuildersMixin``.
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
        "$count",
        "$setWindowFields",
        "$unset",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$bucket",
        "$bucketAuto",
        "$redact",
        "$unionWith",
        "$lookup",
        "$densify",
        "$graphLookup",
        "$facet",
    }

    # Stages that require Python fallback
    UNSUPPORTED_STAGES = {
        "$indexStats",
        "$out",
    }

    # Expressions that require Python fallback
    UNSUPPORTED_EXPRESSIONS = {
        "$function",  # Custom JavaScript
        "$accumulator",  # Custom accumulator
        "$script",  # JavaScript execution
    }

    def __init__(
        self,
        collection,
        expr_evaluator: ExprEvaluator | None = None,
        translation_cache_size: int | None = 100,
    ):
        """Initialize the SQL tier aggregator for a collection.

        Args:
            collection: Target collection providing the database connection
                and table metadata.
            expr_evaluator: Optional ``ExprEvaluator`` instance; when ``None``,
                a default evaluator targeting column ``"data"`` is created.
            translation_cache_size: Maximum number of cached SQL templates.
                ``None`` defaults to ``100`` and ``0`` disables caching.
        """
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
        """Return a JSON extract function name or call expression.

        Args:
            path: Optional JSON path suffix (excluding leading ``"$"``). When
                provided, returns an extract call on ``data``; otherwise
                returns the bare function name.

        Returns:
            str: ``json_extract``/``jsonb_extract`` call or function name.
        """
        if path:
            return f"{self.jsonb.json_function_prefix}_extract(data, '${path}')"
        return f"{self.jsonb.json_function_prefix}_extract"

    def _get_json_set(self) -> str:
        """Return the SQLite JSON set function name for this connection.

        Returns:
            str: ``"jsonb_set"`` when JSONB is supported, else ``"json_set"``.
        """
        return f"{self.jsonb.json_function_prefix}_set"

    def can_optimize_pipeline(
        self, pipeline: list[dict[str, Any]], _depth: int = 0
    ) -> bool:
        """Check whether a pipeline can be compiled into SQL Tier 1.

        Rejects pipelines when fallback is forced, recursion depth exceeds 8,
        pipeline length exceeds 50 stages, any stage or expression is
        unsupported, ``$match`` has an uncompilable ``$jsonSchema``, SQLite is
        older than 3.25.0 for ``$setWindowFields``, or any ``$facet``
        sub-pipeline cannot itself be optimized. An empty pipeline returns
        ``True``.

        Args:
            pipeline: Ordered list of MongoDB aggregation stage dictionaries.
            _depth: Internal recursion depth for nested ``$facet`` pipelines.
                Defaults to ``0``.

        Returns:
            bool: ``True`` if the pipeline is eligible for SQL Tier 1, else
                ``False``.
        """
        from .query_helper import get_force_fallback

        if get_force_fallback() or _depth > 8:
            return False

        if not pipeline:
            return True

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            if stage_name in self.UNSUPPORTED_STAGES:
                return False
            if stage_name not in self.SUPPORTED_STAGES:
                return False
            if stage_name == "$match":
                from .query_helper.schema_compiler import (
                    match_has_uncompilable_schema,
                )

                if match_has_uncompilable_schema(stage.get("$match")):
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
            if stage_name == "$facet":
                facet_spec = stage.get("$facet")
                if not isinstance(facet_spec, dict) or not all(
                    isinstance(k, str)
                    and isinstance(sub_p, list)
                    and self.can_optimize_pipeline(sub_p, _depth + 1)
                    for k, sub_p in facet_spec.items()
                ):
                    return False

        for stage in pipeline:
            if not self._can_optimize_stage_expressions(stage):
                return False

        if len(pipeline) > 50:
            return False

        return True

    def _can_optimize_stage_expressions(self, stage: dict[str, Any]) -> bool:
        """Check whether all expressions in a pipeline stage are SQL-safe.

        Skips ``$facet`` because its sub-pipelines are validated recursively
        in ``can_optimize_pipeline``.

        Args:
            stage: Single aggregation stage dictionary.

        Returns:
            bool: ``True`` if no unsupported expressions are found, else
                ``False``.
        """
        stage_name = next(iter(stage.keys()))
        if stage_name == "$facet":
            return True  # Sub-pipelines validated separately in can_optimize_pipeline
        spec = stage[stage_name]
        return self._check_expression_support(spec)

    def _check_expression_support(self, obj: Any) -> bool:
        """Recursively verify that an object uses no unsupported operators.

        Args:
            obj: Stage specification, expression dict, list, or scalar value.

        Returns:
            bool: ``False`` if any dict key belongs to
                ``UNSUPPORTED_EXPRESSIONS``, else ``True``.
        """
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
        """Build or fetch from cache an optimized CTE SQL query for a pipeline.

        Checks ``can_optimize_pipeline`` first, consults the translation cache
        using a canonicalized pipeline key, and caches freshly compiled SQL
        templates alongside their bound parameter tuples.

        Args:
            pipeline: Ordered list of MongoDB aggregation stage dictionaries.

        Returns:
            tuple[str | None, list[Any]]: Pair of ``(sql_query, params)``, or
                ``(None, [])`` when the pipeline is empty, ineligible for SQL
                Tier 1, or fails compilation.
        """
        if not pipeline or not self.can_optimize_pipeline(pipeline):
            return None, []

        # Try to get from cache. Keys include collection name and canonicalized
        # literal values, so a hit implies an identical table and pipeline.
        cache_key = self._make_cache_key(pipeline)
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
        """Compile a pipeline into a ``WITH`` / ``WITH RECURSIVE`` SQL query.

        Builds sequential stage CTEs via ``_build_stage_sequence``, preserving
        ``root_data`` whenever ``_pipeline_needs_root`` returns ``True``.

        Args:
            pipeline: Non-empty list of MongoDB aggregation stage dicts.

        Returns:
            tuple[str | None, list[Any]]: Pair of ``(sql_query, params)``, or
                ``(None, [])`` if any stage falls back.
        """
        prev_stage = f"(SELECT id, _id, data FROM {quote_table_name(self.collection.name)})"
        context = PipelineContext()

        needs_root = self._pipeline_needs_root(pipeline)
        if needs_root:
            context.preserve_root()

        final_stage, ctes = self._build_stage_sequence(
            pipeline, prev_stage, "stage", context, needs_root=needs_root
        )
        if final_stage is None:
            return None, []

        cte_parts = [f"{name} AS ({sql})" for name, sql, _ in ctes]
        all_params: list[Any] = []
        for _, _, params in ctes:
            all_params.extend(params)

        with_keyword = "WITH RECURSIVE" if context.needs_recursive else "WITH"
        select_cols = "id, _id, data"
        if needs_root:
            select_cols = "id, _id, root_data, data"

        final_sql = f"{with_keyword} {', '.join(cte_parts)} SELECT {select_cols} FROM {final_stage}"
        return final_sql, all_params

    def get_cache_stats(self) -> dict[str, Any]:
        """Return translation cache metrics and entry summaries.

        Returns:
            dict[str, Any]: Statistics from ``TranslationCache.get_stats()``
                augmented with an ``"enabled"`` boolean flag.
        """
        stats = self._translation_cache.get_stats()
        stats["enabled"] = self._translation_cache.is_enabled()
        return stats

    def clear_cache(self) -> None:
        """Clear all entries and hit/miss counters in the pipeline cache."""
        self._translation_cache.clear()

    def dump_cache(self) -> list[dict]:
        """Return all cached translation entries sorted by hit count.

        Returns:
            list[dict]: Entry summaries with key, SQL preview, payload, and
                hit count for debugging.
        """
        return self._translation_cache.dump()

    def _make_cache_key(self, pipeline: list[dict]) -> str:
        """Build a collection-scoped cache key for ``pipeline``."""
        return f"{self.collection.name}|{self._translation_cache.make_key(pipeline)}"

    def cache_contains(self, pipeline: list[dict]) -> bool:
        """Check whether a compiled SQL template for ``pipeline`` is cached.

        Args:
            pipeline: Aggregation pipeline list to look up by canonical key.

        Returns:
            bool: ``True`` if an entry exists in the translation cache.
        """
        key = self._make_cache_key(pipeline)
        return self._translation_cache.contains(key)

    def evict_from_cache(self, pipeline: list[dict]) -> bool:
        """Evict the cached entry for ``pipeline``, if present.

        Args:
            pipeline: Aggregation pipeline list whose entry should be evicted.

        Returns:
            bool: ``True`` if an entry was removed, else ``False``.
        """
        key = self._make_cache_key(pipeline)
        return self._translation_cache.evict(key)

    def cache_size(self) -> int:
        """Return the current number of entries in the translation cache.

        Returns:
            int: Number of cached pipeline templates.
        """
        return len(self._translation_cache)

    def is_cache_enabled(self) -> bool:
        """Return whether the translation cache is currently enabled.

        Returns:
            bool: ``True`` if ``max_size > 0``, else ``False``.
        """
        return self._translation_cache.is_enabled()

    def resize_cache(self, new_size: int) -> None:
        """Update the maximum capacity of the translation cache.

        Args:
            new_size: New maximum entry count; ``0`` disables caching.
        """
        self._translation_cache.resize(new_size)

    def _pipeline_needs_root(self, pipeline: list[dict[str, Any]]) -> bool:
        """Check whether any stage in ``pipeline`` references ``$$ROOT``.

        Args:
            pipeline: Ordered list of aggregation stage dictionaries.

        Returns:
            bool: ``True`` if ``$$ROOT`` is referenced, else ``False``.
        """
        for stage in pipeline:
            if self._stage_uses_root(stage):
                return True
        return False

    def _stage_uses_root(self, stage: dict[str, Any]) -> bool:
        """Check whether a single stage references ``$$ROOT``.

        Returns ``False`` for ``$facet`` because facet sub-pipelines track
        their own inner ``$$ROOT``.

        Args:
            stage: Single aggregation stage dictionary.

        Returns:
            bool: ``True`` if the stage specification references ``$$ROOT``.
        """
        stage_name = next(iter(stage.keys()))
        if stage_name == "$facet":
            return False  # $facet sub-pipelines preserve their own inner $$ROOT
        spec = stage[stage_name]
        return self._expression_uses_root(spec)

    def _expression_uses_root(self, obj: Any) -> bool:
        """Recursively check whether an expression value equals ``$$ROOT``.

        Args:
            obj: Arbitrary expression node (string, dict, list, or scalar).

        Returns:
            bool: ``True`` if ``"$$ROOT"`` appears anywhere in ``obj``.
        """
        match obj:
            case str():
                return obj == "$$ROOT"
            case dict():
                return any(self._expression_uses_root(v) for v in obj.values())
            case list():
                return any(self._expression_uses_root(i) for i in obj)
            case _:
                return False
