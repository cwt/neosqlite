"""Module for comparing additional aggregation stages extended between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_aggregation_stages_extended():
    """Compare additional aggregation stages"""
    print("\n=== Additional Aggregation Stages (Extended) Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_agg_stages
        neo_collection.insert_many(
            [
                {
                    "name": {"first": "John", "last": "Doe"},
                    "age": 30,
                    "extra": "remove_me",
                },
                {
                    "name": {"first": "Jane", "last": "Smith"},
                    "age": 25,
                    "extra": "remove_me",
                },
            ]
        )

        # Test $replaceRoot
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$replaceRoot": {"newRoot": "$name"}}]
                )
            )
            neo_replaceroot = len(result) == 2 and "first" in result[0]
            print(f"Neo $replaceRoot: {'OK' if neo_replaceroot else 'FAIL'}")
        except Exception as e:
            neo_replaceroot = False
            print(f"Neo $replaceRoot: Error - {e}")

        # Test $replaceWith (alias for $replaceRoot in MongoDB 5.0+)
        neo_collection.insert_one(
            {
                "name": {"first": "Bob", "last": "Jones"},
                "age": 35,
                "extra": "remove_me",
            }
        )
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"name.last": "Jones"}},
                        {"$replaceWith": "$name"},
                    ]
                )
            )
            neo_replacewith = len(result) == 1 and "first" in result[0]
            print(f"Neo $replaceWith: {'OK' if neo_replacewith else 'FAIL'}")
        except Exception as e:
            neo_replacewith = False
            print(f"Neo $replaceWith: Error - {e}")

        # Test $unset (aggregation stage)
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"name": "John", "age": 30, "secret": "hidden1"},
                {"name": "Jane", "age": 25, "secret": "hidden2"},
            ]
        )
        try:
            result = list(neo_collection.aggregate([{"$unset": ["secret"]}]))
            neo_unset = len(result) == 2 and "secret" not in result[0]
            print(f"Neo $unset: {'OK' if neo_unset else 'FAIL'}")
        except Exception as e:
            neo_unset = False
            print(f"Neo $unset: Error - {e}")

        # Test $count
        try:
            result = list(neo_collection.aggregate([{"$count": "total"}]))
            neo_count = len(result) == 1 and result[0].get("total") == 2
            print(f"Neo $count: {'OK' if neo_count else 'FAIL'}")
        except Exception as e:
            neo_count = False
            print(f"Neo $count: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_count = None

    mongo_db = None

    mongo_replaceroot = None

    mongo_replacewith = None

    mongo_unset = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_agg_stages
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": {"first": "John", "last": "Doe"},
                    "age": 30,
                    "extra": "remove_me",
                },
                {
                    "name": {"first": "Jane", "last": "Smith"},
                    "age": 25,
                    "extra": "remove_me",
                },
            ]
        )

        # Test $replaceRoot
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$replaceRoot": {"newRoot": "$name"}}]
                )
            )
            mongo_replaceroot = len(result) == 2 and "first" in result[0]
            print(
                f"Mongo $replaceRoot: {'OK' if mongo_replaceroot else 'FAIL'}"
            )
        except Exception as e:
            mongo_replaceroot = False
            print(f"Mongo $replaceRoot: Error - {e}")

        # Test $replaceWith (MongoDB 5.0+)
        mongo_collection.insert_one(
            {
                "name": {"first": "Bob", "last": "Jones"},
                "age": 35,
                "extra": "remove_me",
            }
        )
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"name.last": "Jones"}},
                        {"$replaceWith": "$name"},
                    ]
                )
            )
            mongo_replacewith = len(result) == 1 and "first" in result[0]
            print(
                f"Mongo $replaceWith: {'OK' if mongo_replacewith else 'FAIL'}"
            )
        except Exception as e:
            mongo_replacewith = False
            print(f"Mongo $replaceWith: Error - {e}")

        # Test $unset (aggregation stage)
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "John", "age": 30, "secret": "hidden1"},
                {"name": "Jane", "age": 25, "secret": "hidden2"},
            ]
        )
        try:
            result = list(mongo_collection.aggregate([{"$unset": ["secret"]}]))
            mongo_unset = len(result) == 2 and "secret" not in result[0]
            print(f"Mongo $unset: {'OK' if mongo_unset else 'FAIL'}")
        except Exception as e:
            mongo_unset = False
            print(f"Mongo $unset: Error - {e}")

        # Test $count
        try:
            result = list(mongo_collection.aggregate([{"$count": "total"}]))
            mongo_count = len(result) == 1 and result[0].get("total") == 2
            print(f"Mongo $count: {'OK' if mongo_count else 'FAIL'}")
        except Exception as e:
            mongo_count = False
            print(f"Mongo $count: Error - {e}")

        client.close()

        reporter.record_result(
            "Aggregation Stages Extended",
            "$replaceRoot",
            neo_replaceroot,
            neo_replaceroot,
            mongo_replaceroot,
        )
        reporter.record_result(
            "Aggregation Stages Extended",
            "$replaceWith",
            neo_replacewith,
            neo_replacewith,
            mongo_replacewith,
        )
        reporter.record_result(
            "Aggregation Stages Extended",
            "$unset",
            neo_unset,
            neo_unset,
            mongo_unset,
        )
        reporter.record_result(
            "Aggregation Stages Extended",
            "$count",
            neo_count,
            neo_count,
            mongo_count,
        )
