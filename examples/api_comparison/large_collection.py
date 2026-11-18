"""Benchmark-only: realistic operations over a large collection.

This module is registered as a *benchmark-only* category in runner.py and is
never invoked by ``run-api-comparison.sh``. It seeds both backends with a
configurable number of documents (untimed) before timing selective reads,
counts, aggregation, and updates — which the tiny per-module datasets in the
regular comparison suite cannot measure meaningfully.
"""

from __future__ import annotations

import os
import warnings

import neosqlite

from .timing import (
    end_mongo_timing,
    end_neo_timing,
    reset_timings,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

_COLLECTION = "benchmark_large_docs"
_DOCS_ENV = "NEOSQLITE_BENCH_DOCS"
_DEFAULT_DOCS = 10000


def _doc_count() -> int:
    raw = os.environ.get(_DOCS_ENV, str(_DEFAULT_DOCS))
    try:
        count = int(raw)
    except ValueError:
        count = _DEFAULT_DOCS
    return max(1, count)


def _documents(count: int) -> list[dict]:
    return [
        {
            "_id": i,
            "group": f"g{i % 10}",
            "value": i,
            "active": i % 2 == 0,
            "tags": [f"t{i % 5}", f"u{i % 7}"],
        }
        for i in range(count)
    ]


def _time_operations(collection, label: str, count: int) -> dict[str, object]:
    """Time a fixed set of operations against a seeded collection."""
    results: dict[str, object] = {}

    if label == "neo":
        start, end = start_neo_timing, end_neo_timing
    else:
        start, end = start_mongo_timing, end_mongo_timing

    start()
    results["selective_find"] = len(list(collection.find({"group": "g3"})))
    lookup_id = min(4321, count - 1)
    results["id_lookup"] = collection.find_one({"_id": lookup_id})["value"]
    results["filtered_count"] = collection.count_documents({"active": True})
    results["aggregation"] = list(
        collection.aggregate(
            [
                {"$match": {"group": "g7"}},
                {
                    "$group": {
                        "_id": "$group",
                        "avg": {"$avg": "$value"},
                        "total": {"$sum": "$value"},
                    }
                },
            ]
        )
    )
    results["selective_update"] = collection.update_many(
        {"group": "g1", "value": {"$lt": 200}},
        {"$inc": {"value": 1}},
    ).modified_count
    end()

    return results


def compare_large_collection():
    """Seed both backends and time operations on the large collection."""
    count = _doc_count()
    print(f"\n=== Large Collection Benchmark ({count} documents) ===")

    client = get_mongo_client()
    reset_timings()
    set_accumulation_mode(True)

    neo_results: dict[str, object] = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        collection = neo_conn[_COLLECTION]
        collection.insert_many(_documents(count))
        collection.create_index("group")
        neo_results = _time_operations(collection, "neo", count)

    mongo_results: dict[str, object] = {}
    if client is not None:
        collection = client.test_database[_COLLECTION]
        collection.delete_many({})
        collection.insert_many(_documents(count))
        collection.create_index("group")
        mongo_results = _time_operations(collection, "mongo", count)
    else:
        print("MongoDB unavailable: NeoSQLite large-collection timing only")

    # Verify the two backends did equivalent work — a failed assertion makes
    # the benchmark runner skip this iteration rather than report bad numbers.
    if client is not None:
        for key in (
            "selective_find",
            "id_lookup",
            "filtered_count",
            "aggregation",
            "selective_update",
        ):
            assert neo_results[key] == mongo_results[key], (
                f"Large-collection {key} differs: "
                f"{neo_results[key]!r} != {mongo_results[key]!r}"
            )
