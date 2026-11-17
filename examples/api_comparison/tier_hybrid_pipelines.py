"""Compare complex pipelines that rely on Python expression evaluation.

These pipelines combine stages whose SQL translation may decline complex
options or expressions. NeoSQLite can evaluate a single declining stage in
Python while keeping the rest of the pipeline in the temporary-table tier;
the results must still match MongoDB exactly.
"""

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
    {
        "_id": 1,
        "name": "alpha,beta",
        "delimiter": ",",
        "values": [1, 2, 3],
        "nested": {"a": 1},
        "score": 10,
    },
    {
        "_id": 2,
        "name": "gamma;delta",
        "delimiter": ";",
        "values": [4, 5],
        "nested": {"a": 2},
        "score": 20,
    },
    {
        "_id": 3,
        "name": "epsilon",
        "delimiter": ",",
        "values": [],
        "nested": {"a": 3},
        "score": 30,
    },
]

_PIPELINES = [
    (
        "split_eval",
        [
            {
                "$project": {
                    "first": {"$arrayElemAt": [{"$split": ["$name", ","]}, 0]}
                }
            },
            {"$sort": {"_id": 1}},
        ],
    ),
    (
        "merge_objects_replace_root",
        [
            {
                "$replaceRoot": {
                    "newRoot": {"$mergeObjects": ["$nested", {"total": 7}]}
                }
            },
            {"$sort": {"_id": 1}},
        ],
    ),
    (
        "reduce_and_filter",
        [
            {
                "$addFields": {
                    "sum": {
                        "$reduce": {
                            "input": "$values",
                            "initialValue": 0,
                            "in": {"$add": ["$$value", "$$this"]},
                        }
                    },
                    "evens": {
                        "$filter": {
                            "input": "$values",
                            "as": "v",
                            "cond": {"$eq": [{"$mod": ["$$v", 2]}, 0]},
                        }
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
    ),
    (
        "group_expression_key",
        [
            {
                "$group": {
                    "_id": {"$mod": ["$score", 20]},
                    "total": {"$sum": "$score"},
                }
            },
            {"$sort": {"_id": 1}},
        ],
    ),
    (
        "conditional_switch",
        [
            {
                "$project": {
                    "bucket": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$lt": ["$score", 15]},
                                    "then": "low",
                                },
                                {
                                    "case": {"$lt": ["$score", 25]},
                                    "then": "mid",
                                },
                            ],
                            "default": "high",
                        }
                    }
                }
            },
            {"$sort": {"_id": 1}},
        ],
    ),
]


def _run_pipelines(aggregate) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {}
    for name, pipeline in _PIPELINES:
        results[name] = [
            {k: v for k, v in doc.items() if k != "_id"}
            for doc in aggregate(pipeline)
        ]
    return results


def compare_tier_hybrid_pipelines() -> None:
    """Compare Python-heavy aggregation pipelines against PyMongo."""
    print("\n=== Tier-2 Hybrid Pipelines Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    neo_results: dict[str, list[dict]] = {}
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.hybrid_docs
        collection.insert_many([dict(doc) for doc in _DOCUMENTS])
        set_accumulation_mode(True)
        start_neo_timing()
        try:
            neo_results = _run_pipelines(
                lambda pipeline: list(collection.aggregate(pipeline))
            )
        finally:
            end_neo_timing()

    mongo_results: dict[str, list[dict]] = {}
    if mongo_available:
        collection = client.test_database.hybrid_docs
        collection.delete_many({})
        collection.insert_many([dict(doc) for doc in _DOCUMENTS])
        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_results = _run_pipelines(
                lambda pipeline: list(collection.aggregate(pipeline))
            )
        finally:
            end_mongo_timing()

    skip = "MongoDB not available" if not mongo_available else None
    for name, _ in _PIPELINES:
        reporter.record_comparison(
            "Tier-2 Hybrid Pipelines",
            name,
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason=skip,
        )
