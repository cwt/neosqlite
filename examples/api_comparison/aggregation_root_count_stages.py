"""Module for comparing additional aggregation stages extended between NeoSQLite and PyMongo"""

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


def compare_additional_aggregation_stages_extended():
    """Compare additional aggregation stages"""
    print("\n=== Additional Aggregation Stages (Extended) Comparison ===")

    neo_replaceroot = False
    neo_replacewith = False
    neo_unset = False
    neo_count = False

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

        set_accumulation_mode(True)

        # Test $replaceRoot
        start_neo_timing()
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$replaceRoot": {"newRoot": "$name"}}]
                )
            )
            neo_replaceroot = len(result) == 2 and "first" in result[0]
        except Exception as e:
            print(f"Neo $replaceRoot: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo $replaceRoot: {'OK' if neo_replaceroot else 'FAIL'}")

        # Test $replaceWith (alias for $replaceRoot in MongoDB 5.0+)
        neo_collection.insert_one(
            {
                "name": {"first": "Bob", "last": "Jones"},
                "age": 35,
                "extra": "remove_me",
            }
        )

        start_neo_timing()
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
        except Exception as e:
            print(f"Neo $replaceWith: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo $replaceWith: {'OK' if neo_replacewith else 'FAIL'}")

        # Test $unset (aggregation stage)
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"name": "John", "age": 30, "secret": "hidden1"},
                {"name": "Jane", "age": 25, "secret": "hidden2"},
            ]
        )
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate([{"$unset": ["secret"]}]))
            neo_unset = len(result) == 2 and "secret" not in result[0]
        except Exception as e:
            print(f"Neo $unset: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo $unset: {'OK' if neo_unset else 'FAIL'}")

        # Test $count
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate([{"$count": "total"}]))
            neo_count = len(result) == 1 and result[0].get("total") == 2
        except Exception as e:
            print(f"Neo $count: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo $count: {'OK' if neo_count else 'FAIL'}")

    client = test_pymongo_connection()
    mongo_replaceroot = None
    mongo_replacewith = None
    mongo_unset = None
    mongo_count = None

    if client:
        try:
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

            set_accumulation_mode(True)

            # Test $replaceRoot
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate(
                        [{"$replaceRoot": {"newRoot": "$name"}}]
                    )
                )
                mongo_replaceroot = len(result) == 2 and "first" in result[0]
            except Exception as e:
                print(f"Mongo $replaceRoot: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo $replaceRoot: {'OK' if mongo_replaceroot else 'FAIL'}"
            )

            # Test $replaceWith (MongoDB 5.0+)
            mongo_collection.insert_one(
                {
                    "name": {"first": "Bob", "last": "Jones"},
                    "age": 35,
                    "extra": "remove_me",
                }
            )

            start_mongo_timing()
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
            except Exception as e:
                print(f"Mongo $replaceWith: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo $replaceWith: {'OK' if mongo_replacewith else 'FAIL'}"
            )

            # Test $unset (aggregation stage)
            mongo_collection.delete_many({})
            mongo_collection.insert_many(
                [
                    {"name": "John", "age": 30, "secret": "hidden1"},
                    {"name": "Jane", "age": 25, "secret": "hidden2"},
                ]
            )
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate([{"$unset": ["secret"]}])
                )
                mongo_unset = len(result) == 2 and "secret" not in result[0]
            except Exception as e:
                print(f"Mongo $unset: Error - {e}")
            finally:
                end_mongo_timing()
            print(f"Mongo $unset: {'OK' if mongo_unset else 'FAIL'}")

            # Test $count
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate([{"$count": "total"}]))
                mongo_count = len(result) == 1 and result[0].get("total") == 2
            except Exception as e:
                print(f"Mongo $count: Error - {e}")
            finally:
                end_mongo_timing()
            print(f"Mongo $count: {'OK' if mongo_count else 'FAIL'}")

        finally:
            client.close()

    reporter.record_comparison(
        "Aggregation (Root & Count Stages)",
        "$replaceRoot",
        neo_replaceroot if neo_replaceroot else "FAIL",
        mongo_replaceroot if mongo_replaceroot else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation (Root & Count Stages)",
        "$replaceWith",
        neo_replacewith if neo_replacewith else "FAIL",
        mongo_replacewith if mongo_replacewith else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation (Root & Count Stages)",
        "$unset",
        neo_unset if neo_unset else "FAIL",
        mongo_unset if mongo_unset else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation (Root & Count Stages)",
        "$count",
        neo_count if neo_count else "FAIL",
        mongo_count if mongo_count else None,
        skip_reason="MongoDB not available" if not client else None,
    )
