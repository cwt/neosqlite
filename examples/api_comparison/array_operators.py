"""Module for comparing array operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_array_operators():
    """Compare array operators in aggregation"""
    print("\n=== Array Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "A", "scores": [10, 20, 30]},
                {"name": "B", "scores": [40, 50]},
            ]
        )

        # Test $first
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first": {"$first": "$scores"}}}]
                )
            )
            neo_first = len(result) == 2
            print(f"Neo $first: {'OK' if neo_first else 'FAIL'}")
        except Exception as e:
            neo_first = False
            print(f"Neo $first: Error - {e}")

        # Test $last
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"last": {"$last": "$scores"}}}]
                )
            )
            neo_last = len(result) == 2
            print(f"Neo $last: {'OK' if neo_last else 'FAIL'}")
        except Exception as e:
            neo_last = False
            print(f"Neo $last: Error - {e}")

        # Test $filter (basic)
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "high_scores": {
                                    "$filter": {
                                        "input": "$scores",
                                        "as": "score",
                                        "cond": {"$gte": ["$$score", 25]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_filter = len(result) == 2
            print(f"Neo $filter: {'OK' if neo_filter else 'FAIL'}")
        except Exception as e:
            neo_filter = False
            print(f"Neo $filter: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_filter = None

    mongo_first = None

    mongo_last = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "A", "scores": [10, 20, 30]},
                {"name": "B", "scores": [40, 50]},
            ]
        )

        # Test $first
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first": {"$first": "$scores"}}}]
                )
            )
            mongo_first = len(result) == 2
            print(f"Mongo $first: {'OK' if mongo_first else 'FAIL'}")
        except Exception as e:
            mongo_first = False
            print(f"Mongo $first: Error - {e}")

        # Test $last
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"last": {"$last": "$scores"}}}]
                )
            )
            mongo_last = len(result) == 2
            print(f"Mongo $last: {'OK' if mongo_last else 'FAIL'}")
        except Exception as e:
            mongo_last = False
            print(f"Mongo $last: Error - {e}")

        # Test $filter
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "high_scores": {
                                    "$filter": {
                                        "input": "$scores",
                                        "as": "score",
                                        "cond": {"$gte": ["$$score", 25]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_filter = len(result) == 2
            print(f"Mongo $filter: {'OK' if mongo_filter else 'FAIL'}")
        except Exception as e:
            mongo_filter = False
            print(f"Mongo $filter: Error - {e}")

        client.close()

        reporter.record_result(
            "Array Operators", "$first", neo_first, neo_first, mongo_first
        )
        reporter.record_result(
            "Array Operators", "$last", neo_last, neo_last, mongo_last
        )
        reporter.record_result(
            "Array Operators", "$filter", neo_filter, neo_filter, mongo_filter
        )
