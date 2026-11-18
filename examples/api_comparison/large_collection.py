"""Benchmark-only: realistic operations over a large collection.

This module is registered as *benchmark-only* categories in runner.py and is
never invoked by ``run-api-comparison.sh``. Each category seeds both backends
with a configurable number of documents (untimed) before timing a distinct
group of operations — which the tiny per-module datasets in the regular
comparison suite cannot measure meaningfully.
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

_READ_KEYS = (
    "selective_find",
    "id_lookup",
    "filtered_count",
    "aggregation",
    "selective_update",
)
_INDEX_KEYS = ("create_index", "compound_index", "reindex")


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


def _time_operations(
    collection, label: str, count: int, *, index_ops: bool
) -> dict[str, object]:
    """Time either read/update or index operations on a seeded collection."""
    results: dict[str, object] = {}
    start, end = (
        (start_neo_timing, end_neo_timing)
        if label == "neo"
        else (start_mongo_timing, end_mongo_timing)
    )

    start()
    if index_ops:
        results["create_index"] = bool(collection.create_index("value"))
        results["compound_index"] = bool(
            collection.create_index([("group", 1), ("value", -1)])
        )
        command = "reindex" if label == "neo" else "reIndex"
        results["reindex"] = bool(
            collection.database.command(command, collection.name).get("ok")
        )
    else:
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


def _run_benchmark(*, index_ops: bool) -> None:
    count = _doc_count()
    kind = "Index Operations" if index_ops else "Reads & Updates"
    print(f"\n=== Large Collection {kind} ({count} documents) ===")

    client = get_mongo_client()
    reset_timings()
    set_accumulation_mode(True)

    neo_results: dict[str, object] = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        collection = neo_conn[_COLLECTION]
        collection.insert_many(_documents(count))
        if not index_ops:
            # Give the read workload an index; index benchmarks create their
            # own indexes inside the timed section.
            collection.create_index("group")
        neo_results = _time_operations(
            collection, "neo", count, index_ops=index_ops
        )

    mongo_results: dict[str, object] = {}
    if client is not None:
        collection = client.test_database[_COLLECTION]
        collection.delete_many({})
        collection.insert_many(_documents(count))
        if not index_ops:
            collection.create_index("group")
        mongo_results = _time_operations(
            collection, "mongo", count, index_ops=index_ops
        )
    else:
        print("MongoDB unavailable: NeoSQLite timing only")

    if client is not None:
        for key in _INDEX_KEYS if index_ops else _READ_KEYS:
            assert neo_results[key] == mongo_results[key], (
                f"Large-collection {key} differs: "
                f"{neo_results[key]!r} != {mongo_results[key]!r}"
            )


def compare_large_collection():
    """Benchmark selective reads, counts, aggregation and updates at scale."""
    _run_benchmark(index_ops=False)


def compare_large_collection_indexes():
    """Benchmark index creation and reindex at scale."""
    _run_benchmark(index_ops=True)
