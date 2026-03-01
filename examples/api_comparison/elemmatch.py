"""Module for comparing $elemMatch operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
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

        # Test $elemMatch with multiple conditions
        try:
            result = list(
                neo_collection.find(
                    {"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}}
                )
            )
            neo_elemmatch_count = len(result)
            print(f"Neo $elemMatch: {neo_elemmatch_count} matches")
        except Exception as e:
            neo_elemmatch_count = 0
            print(f"Neo $elemMatch: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_elemmatch_count = None

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

        # Test $elemMatch with multiple conditions
        try:
            result = list(
                mongo_collection.find(
                    {"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}}
                )
            )
            mongo_elemmatch_count = len(result)
            print(f"Mongo $elemMatch: {mongo_elemmatch_count} matches")
        except Exception as e:
            mongo_elemmatch_count = 0
            print(f"Mongo $elemMatch: Error - {e}")

        client.close()

        reporter.record_result(
            "$elemMatch Operator",
            "$elemMatch",
            (
                neo_elemmatch_count == mongo_elemmatch_count
                if mongo_elemmatch_count is not None
                else False
            ),
            neo_elemmatch_count,
            mongo_elemmatch_count,
        )
