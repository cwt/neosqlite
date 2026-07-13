from __future__ import annotations

import logging
from typing import Any

from ..._sqlite import sqlite3

logger = logging.getLogger(__name__)

def can_process_with_temporary_tables(pipeline: list[dict[str, Any]]) -> bool:
    """
    Determine if a pipeline can be processed with temporary tables.

    This function checks if all stages in an aggregation pipeline are supported
    by the temporary table processing approach. It verifies that each stage in
    the pipeline is one of the supported stage types.

    Additionally, it handles special cases for text search operations:
    - Pure text search operations are supported with hybrid processing
    - Text search with simple unwind operations are supported (uses Python text search on temp tables)
    - Complex nested unwinds (multiple unwinds or dotted paths) fall back to Python

    Args:
        pipeline (list[dict[str, Any]]): List of aggregation pipeline stages to check

    Returns:
        bool: True if all stages in the pipeline are supported and can be processed
              with temporary tables, False otherwise
    """
    # Check if all stages are supported
    # Note: $merge and $redact require Python fallback for full functionality
    supported_stages = {
        "$addFields",
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
        "$project",
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
    unwind_count = 0
    has_nested_unwind = False

    for stage in pipeline:
        stage_name = next(iter(stage.keys()))
        if stage_name not in supported_stages:
            return False

        if stage_name == "$match":
            match_spec = stage["$match"]
            if "$jsonSchema" in match_spec:
                return False

        if stage_name in ("$setWindowFields", "$fill"):
            if sqlite3.sqlite_version_info < (3, 25, 0):
                return False

        if stage_name == "$unwind":
            unwind_count += 1
            unwind_spec = stage["$unwind"]
            # Check for nested/dotted paths which are complex
            if isinstance(unwind_spec, str) and "." in unwind_spec:
                has_nested_unwind = True
            elif isinstance(unwind_spec, dict):
                path = unwind_spec.get("path", "")
                if "." in path:
                    has_nested_unwind = True

    # Multiple unwinds or nested paths are complex - fall back to Python
    if unwind_count > 1 or has_nested_unwind:
        return False

    return True
def execute_2nd_tier_aggregation(
    query_engine,
    pipeline: list[dict[str, Any]],
    batch_size: int = 101,
) -> list[dict[str, Any]]:
    """
    Execute aggregation pipeline using temporary table approach for complex pipelines.

    This function is designed to be called as the second tier in a three-tier processing system:
    1. First tier (QueryEngine): Try existing SQL optimization for simple pipelines
    2. Second tier (this function): Try temporary table approach for complex pipelines
    3. Third tier (QueryEngine): Fall back to Python implementation for unsupported operations

    This function focuses specifically on processing complex pipelines that the current
    NeoSQLite SQL optimization cannot handle efficiently, using temporary tables for better performance.

    Args:
        query_engine: The NeoSQLite QueryEngine instance to use for processing
        pipeline (list[dict[str, Any]]): List of aggregation pipeline stages to process
        batch_size (int): Batch size for fetching results from temporary tables

    Returns:
        list[dict[str, Any]]: List of result documents after processing the pipeline
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
