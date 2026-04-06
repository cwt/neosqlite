"""Module for comparing $fill stage between NeoSQLite and PyMongo"""

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


def compare_fill_stage():
    """Compare $fill aggregation stage"""
    print("\n=== $fill Stage Comparison ===")

    test_data = [
        {"_id": 1, "time": 1, "val": 10, "cat": "A"},
        {"_id": 2, "time": 2, "val": None, "cat": "A"},
        {"_id": 3, "time": 3, "val": None, "cat": "A"},
        {"_id": 4, "time": 4, "val": 40, "cat": "A"},
        {"_id": 5, "time": 1, "val": 100, "cat": "B"},
        {"_id": 6, "time": 2, "val": None, "cat": "B"},
    ]

    pipelines = {
        "locf": [
            {
                "$fill": {
                    "partitionBy": "$cat",
                    "sortBy": {"time": 1},
                    "output": {"val": {"method": "locf"}},
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "value": [
            {"$fill": {"output": {"val": {"value": -1}}}},
            {"$sort": {"_id": 1}},
        ],
        "linear": [
            {
                "$fill": {
                    "partitionBy": "$cat",
                    "sortBy": {"time": 1},
                    "output": {"val": {"method": "linear"}},
                }
            },
            {"$sort": {"_id": 1}},
        ],
    }

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_fill
        neo_collection.insert_many(test_data)

        set_accumulation_mode(True)
        start_neo_timing()
        try:
            for name, pipeline in pipelines.items():
                try:
                    result = list(neo_collection.aggregate(pipeline))
                    neo_results[name] = result
                    print(f"Neo $fill ({name}): OK")
                except Exception as e:
                    neo_results[name] = f"Error: {e}"
                    print(f"Neo $fill ({name}): Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_fill
            mongo_collection.delete_many({})
            mongo_collection.insert_many(test_data)

            set_accumulation_mode(True)
            start_mongo_timing()
            try:
                for name, pipeline in pipelines.items():
                    try:
                        # MongoDB 5.3+ supports $fill
                        result = list(mongo_collection.aggregate(pipeline))
                        mongo_results[name] = result
                        print(f"Mongo $fill ({name}): OK")
                    except Exception as e:
                        mongo_results[name] = f"Error: {e}"
                        print(f"Mongo $fill ({name}): Error - {e}")
            finally:
                end_mongo_timing()
        finally:
            client.close()

    # Record comparisons
    for name in pipelines:
        reporter.record_comparison(
            "$fill Stage",
            f"$fill ({name})",
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason=(
                "MongoDB not available or doesn't support $fill"
                if not client
                else None
            ),
        )
