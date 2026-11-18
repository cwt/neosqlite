"""Compare MongoDB stage aliases against PyMongo."""

from __future__ import annotations

import warnings

import neosqlite

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

_DOCUMENTS = [
    {"_id": 1, "cat": "a", "v": 3},
    {"_id": 2, "cat": "b", "v": 1},
    {"_id": 3, "cat": "a", "v": 2},
    {"_id": 4, "cat": "b", "v": 5},
    {"_id": 5, "cat": "c", "v": 4},
]

_PIPELINES = [
    (
        "$set stage",
        [
            {"$set": {"doubled": {"$multiply": ["$v", 2]}}},
            {"$sort": {"_id": 1}},
        ],
    ),
    ("$sortByCount stage", [{"$sortByCount": "$cat"}]),
    (
        "$sortByCount expression",
        [{"$sortByCount": {"$mod": ["$v", 2]}}],
    ),
    (
        "aliases in $facet",
        [{"$facet": {"x": [{"$set": {"z": 1}}, {"$sortByCount": "$cat"}]}}],
    ),
]


def _run(aggregate) -> dict[str, list[dict]]:
    return {name: list(aggregate(pipeline)) for name, pipeline in _PIPELINES}


def compare_stage_aliases():
    """Compare $set and $sortByCount results against PyMongo."""
    print("\n=== Stage Aliases Comparison ===")

    neo_results: dict[str, list[dict]] = {}
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.alias_docs
        collection.insert_many([dict(doc) for doc in _DOCUMENTS])
        set_accumulation_mode(True)
        start_neo_timing()
        try:
            neo_results = _run(lambda p: collection.aggregate(p))
        finally:
            end_neo_timing()

    client = get_mongo_client()
    mongo_results: dict[str, list[dict]] = {}
    if client is not None:
        collection = client.test_database.alias_docs
        collection.delete_many({})
        collection.insert_many([dict(doc) for doc in _DOCUMENTS])
        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_results = _run(lambda p: collection.aggregate(p))
        finally:
            end_mongo_timing()

    skip = "MongoDB not available" if client is None else None
    for name, _ in _PIPELINES:
        reporter.record_comparison(
            "Stage Aliases",
            name,
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason=skip,
        )
