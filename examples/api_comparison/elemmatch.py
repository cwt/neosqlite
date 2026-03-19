"""Module for comparing $elemMatch operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
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

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_elemmatch
        neo_collection.insert_many(
            [
                {"name": "A", "scores": [80, 90, 100]},
                {"name": "B", "scores": [70, 75, 80]},
                {"name": "C", "scores": [85, 95]},
                {"name": "D", "scores": [60, 70]},
            ]
        )

        start_neo_timing()
        # Test $elemMatch with multiple conditions
        try:
            neo_elemmatch_result = list(
                neo_collection.find(
                    {"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}}
                )
            )
            print(f"Neo $elemMatch: {len(neo_elemmatch_result)} matches")
        except Exception as e:
            neo_elemmatch_result = f"Error: {e}"
            print(f"Neo $elemMatch: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_elemmatch_result = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_elemmatch
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "A", "scores": [80, 90, 100]},
                {"name": "B", "scores": [70, 75, 80]},
                {"name": "C", "scores": [85, 95]},
                {"name": "D", "scores": [60, 70]},
            ]
        )

        start_mongo_timing()
        # Test $elemMatch with multiple conditions
        try:
            mongo_elemmatch_result = list(
                mongo_collection.find(
                    {"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}}
                )
            )
            print(f"Mongo $elemMatch: {len(mongo_elemmatch_result)} matches")
        except Exception as e:
            mongo_elemmatch_result = f"Error: {e}"
            print(f"Mongo $elemMatch: Error - {e}")

        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "$elemMatch Operator",
        "$elemMatch",
        neo_elemmatch_result,
        mongo_elemmatch_result,
        skip_reason="MongoDB not available" if not client else None,
    )
