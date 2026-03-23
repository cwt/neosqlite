"""Module for comparing $elemMatch operator between NeoSQLite and PyMongo"""

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
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_elemmatch_operator():
    """Compare $elemMatch query operator"""
    print("\n=== $elemMatch Query Operator Comparison ===")

    # Initialize results
    neo_results = {}
    mongo_results = {}

    # Test cases
    test_cases = [
        (
            "Array of Primitives",
            {"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}},
        ),
        (
            "Array of Objects",
            {
                "results": {
                    "$elemMatch": {"product": "abc", "score": {"$gte": 10}}
                }
            },
        ),
    ]

    # 1. NeoSQLite Comparison
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_elemmatch

        # Seed data
        neo_collection.insert_many(
            [
                {"name": "A", "scores": [80, 90, 100]},
                {"name": "B", "scores": [70, 75, 80]},
                {"name": "C", "scores": [85, 95]},
                {"name": "D", "scores": [60, 70]},
                {
                    "name": "E",
                    "results": [
                        {"product": "abc", "score": 10},
                        {"product": "xyz", "score": 5},
                    ],
                },
                {
                    "name": "F",
                    "results": [
                        {"product": "abc", "score": 8},
                        {"product": "xyz", "score": 7},
                    ],
                },
            ]
        )

        set_accumulation_mode(True)

        for label, query in test_cases:
            start_neo_timing()
            try:
                try:
                    res = list(neo_collection.find(query))
                    neo_results[label] = res
                    print(f"Neo $elemMatch ({label}): {len(res)} matches")
                except Exception as e:
                    neo_results[label] = f"Error: {e}"
                    print(f"Neo $elemMatch ({label}): Error - {e}")
            finally:
                end_neo_timing()

    # 2. MongoDB Comparison
    client = test_pymongo_connection()
    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_elemmatch
            mongo_collection.delete_many({})

            # Seed data
            mongo_collection.insert_many(
                [
                    {"name": "A", "scores": [80, 90, 100]},
                    {"name": "B", "scores": [70, 75, 80]},
                    {"name": "C", "scores": [85, 95]},
                    {"name": "D", "scores": [60, 70]},
                    {
                        "name": "E",
                        "results": [
                            {"product": "abc", "score": 10},
                            {"product": "xyz", "score": 5},
                        ],
                    },
                    {
                        "name": "F",
                        "results": [
                            {"product": "abc", "score": 8},
                            {"product": "xyz", "score": 7},
                        ],
                    },
                ]
            )

            set_accumulation_mode(True)

            for label, query in test_cases:
                start_mongo_timing()
                try:
                    try:
                        res = list(mongo_collection.find(query))
                        mongo_results[label] = res
                        print(f"Mongo $elemMatch ({label}): {len(res)} matches")
                    except Exception as e:
                        mongo_results[label] = f"Error: {e}"
                        print(f"Mongo $elemMatch ({label}): Error - {e}")
                finally:
                    end_mongo_timing()
        finally:
            client.close()
    else:
        print("MongoDB not available, skipping MongoDB $elemMatch comparison")

    # 3. Record results
    for label, _ in test_cases:
        reporter.record_comparison(
            "$elemMatch Operator",
            f"$elemMatch ({label})",
            neo_results.get(label),
            mongo_results.get(label),
            skip_reason="MongoDB not available" if not client else None,
        )
