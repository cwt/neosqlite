"""Compare ``$indexStats`` output against PyMongo.

``host`` and ``accesses`` are runtime-specific (container host, access
counters, creation time), so comparisons use the deterministic parts of each
document: index name, key pattern, and spec. The "first stage only" validation
is compared as a normalized error category.
"""

from __future__ import annotations

import warnings

import neosqlite
from neosqlite import ASCENDING

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def _create_indexes(collection) -> None:
    collection.create_index("a")
    collection.create_index([("b", ASCENDING), ("a", ASCENDING)])
    collection.create_index("b", unique=True)


def _summarize(docs: list[dict]) -> list[dict]:
    """Keep only deterministic index metadata, sorted by index name."""
    summary = [
        {
            "name": doc.get("name"),
            "key": doc.get("key"),
            "spec": doc.get("spec"),
        }
        for doc in docs
    ]
    return sorted(summary, key=lambda item: str(item["name"]))


def _error_category(operation) -> str | None:
    try:
        operation()
    except (
        Exception
    ) as exc:  # noqa: BLE001 - both engines raise their own types
        if "first stage" in str(exc):
            return "indexStats must be first stage"
        return f"unexpected error: {exc}"
    return None


def compare_index_stats():
    """Compare ``$indexStats`` metadata and validation against PyMongo."""
    print("\n=== $indexStats Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    with neosqlite.Connection(":memory:") as neo_conn:
        collection = neo_conn.docs
        collection.insert_many([{"_id": 1, "a": 1, "b": "x"}])
        _create_indexes(collection)
        set_accumulation_mode(True)
        start_neo_timing()
        try:
            neo_stats = _summarize(
                list(collection.aggregate([{"$indexStats": {}}]))
            )
            neo_error = _error_category(
                lambda: list(
                    collection.aggregate(
                        [{"$match": {"a": 1}}, {"$indexStats": {}}]
                    )
                )
            )
        finally:
            end_neo_timing()

    mongo_stats = None
    mongo_error = None
    if mongo_available:
        collection = client.test_database.indexstats_docs
        collection.delete_many({})
        collection.insert_many([{"_id": 1, "a": 1, "b": "x"}])
        _create_indexes(collection)
        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_stats = _summarize(
                list(collection.aggregate([{"$indexStats": {}}]))
            )
            mongo_error = _error_category(
                lambda: list(
                    collection.aggregate(
                        [{"$match": {"a": 1}}, {"$indexStats": {}}]
                    )
                )
            )
        finally:
            end_mongo_timing()

    skip = "MongoDB not available" if not mongo_available else None
    reporter.record_comparison(
        "$indexStats",
        "index_metadata",
        neo_stats,
        mongo_stats,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "$indexStats",
        "first_stage_validation",
        neo_error,
        mongo_error,
        skip_reason=skip,
    )
