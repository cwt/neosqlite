from __future__ import annotations

import logging
from typing import Any

from ..._sqlite import sqlite3

logger = logging.getLogger(__name__)


def can_process_with_temporary_tables(pipeline: list[dict[str, Any]]) -> bool:
    """Determine whether a pipeline can run using temporary SQLite tables.

    Verifies that every stage name is supported by Tier-2 temporary-table
    processing, rejects ``$match`` stages containing uncompilable
    ``$jsonSchema`` predicates, requires SQLite 3.25.0+ for window stages
    (``$setWindowFields`` and ``$fill``), and rejects dotted nested
    ``$unwind`` paths.

    Args:
        pipeline: List of aggregation pipeline stage dictionaries to inspect.

    Returns:
        bool: True if the pipeline can be executed with temporary tables,
            False otherwise.
    """
    # Check if all stages are supported in Tier-2 SQL temp-table execution
    supported_stages = {
        "$addFields",
        "$count",
        "$bucket",
        "$bucketAuto",
        "$densify",
        "$facet",
        "$fill",
        "$graphLookup",
        "$group",
        "$limit",
        "$lookup",
        "$match",
        "$merge",
        "$out",
        "$project",
        "$redact",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$setWindowFields",
        "$skip",
        "$sort",
        "$unionWith",
        "$unset",
        "$unwind",
    }

    # Count unwind stages and check for complex patterns
    has_nested_unwind = False

    for stage in pipeline:
        stage_name = next(iter(stage.keys()))
        if stage_name not in supported_stages:
            return False

        if stage_name == "$match":
            from ..query_helper.schema_compiler import (
                match_has_uncompilable_schema,
            )

            match_spec = stage["$match"]
            if match_has_uncompilable_schema(match_spec):
                return False

        if stage_name in ("$setWindowFields", "$fill"):
            if sqlite3.sqlite_version_info < (3, 25, 0):
                return False

        if stage_name == "$unwind":
            unwind_spec = stage["$unwind"]
            # Check for nested/dotted paths which are complex
            if isinstance(unwind_spec, str) and "." in unwind_spec:
                has_nested_unwind = True
            elif isinstance(unwind_spec, dict):
                path = unwind_spec.get("path", "")
                if "." in path:
                    has_nested_unwind = True

    # Nested dotted unwind paths require Python fallback
    if has_nested_unwind:
        return False

    return True


def execute_2nd_tier_aggregation(
    query_engine,
    pipeline: list[dict[str, Any]],
    batch_size: int = 101,
) -> list[dict[str, Any]]:
    """Execute an aggregation pipeline using Tier-2 temporary tables.

    Acts as the second tier in NeoSQLite's three-tier aggregation strategy
    between single-query SQL (Tier 1) and Python fallback (Tier 3). Checks
    ``get_force_fallback()`` and ``can_process_with_temporary_tables()``, then
    runs ``TemporaryTableAggregationProcessor.process_pipeline``.

    Args:
        query_engine: ``QueryEngine`` instance owning the target collection.
        pipeline: List of aggregation pipeline stage dictionaries to execute.
        batch_size: Number of documents per batch when reading results.

    Returns:
        list[dict[str, Any]]: Aggregated result documents produced by the
            pipeline.

    Raises:
        NotImplementedError: If forced fallback is enabled, the pipeline is
            unsupported by temporary tables, or processing fails.
    """
    # Check if we should force fallback for benchmarking/debugging
    from ..query_helper import get_force_fallback
    from . import TemporaryTableAggregationProcessor

    if get_force_fallback():
        raise NotImplementedError(
            "Temporary table aggregation skipped due to force fallback flag"
        )

    # Process the pipeline with temporary tables if possible
    if can_process_with_temporary_tables(pipeline):
        try:
            processor = TemporaryTableAggregationProcessor(
                query_engine.collection, query_engine
            )
            return processor.process_pipeline(pipeline, batch_size=batch_size)
        except Exception as e:
            logger.debug(
                f"Temporary table aggregation failed, fallback required: {e}"
            )
            raise NotImplementedError(
                f"Temporary table aggregation failed, fallback required: {e}"
            )

    # If we can't process with temporary tables, signal for fallback.
    raise NotImplementedError(
        "Pipeline not supported by temporary table aggregation."
    )
