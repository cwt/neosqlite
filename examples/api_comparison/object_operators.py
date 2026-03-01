"""Module for comparing object operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_object_operators():
    """Compare object operators in aggregation"""
    print("\n=== Object Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "A",
                    "meta": {"city": "NYC", "zip": 10001},
                    "extra": {"country": "USA"},
                },
                {
                    "name": "B",
                    "meta": {"city": "LA", "zip": 90001},
                    "extra": {"country": "USA"},
                },
            ]
        )

        # Test $mergeObjects
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$meta", "$extra"]}
                            }
                        }
                    ]
                )
            )
            neo_mergeobjects = len(result) == 2
            print(f"Neo $mergeObjects: {'OK' if neo_mergeobjects else 'FAIL'}")
        except Exception as e:
            neo_mergeobjects = False
            print(f"Neo $mergeObjects: Error - {e}")

        # Test $getField
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "city": {
                                    "$getField": {
                                        "field": "city",
                                        "input": "$meta",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_getfield = len(result) == 2
            print(f"Neo $getField: {'OK' if neo_getfield else 'FAIL'}")
        except Exception as e:
            neo_getfield = False
            print(f"Neo $getField: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_getfield = None

    mongo_mergeobjects = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "A",
                    "meta": {"city": "NYC", "zip": 10001},
                    "extra": {"country": "USA"},
                },
                {
                    "name": "B",
                    "meta": {"city": "LA", "zip": 90001},
                    "extra": {"country": "USA"},
                },
            ]
        )

        # Test $mergeObjects
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$meta", "$extra"]}
                            }
                        }
                    ]
                )
            )
            mongo_mergeobjects = len(result) == 2
            print(
                f"Mongo $mergeObjects: {'OK' if mongo_mergeobjects else 'FAIL'}"
            )
        except Exception as e:
            mongo_mergeobjects = False
            print(f"Mongo $mergeObjects: Error - {e}")

        # Test $getField
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "city": {
                                    "$getField": {
                                        "field": "city",
                                        "input": "$meta",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_getfield = len(result) == 2
            print(f"Mongo $getField: {'OK' if mongo_getfield else 'FAIL'}")
        except Exception as e:
            mongo_getfield = False
            print(f"Mongo $getField: Error - {e}")

        client.close()

        reporter.record_result(
            "Object Operators",
            "$mergeObjects",
            neo_mergeobjects,
            neo_mergeobjects,
            mongo_mergeobjects,
        )
        reporter.record_result(
            "Object Operators",
            "$getField",
            neo_getfield,
            neo_getfield,
            mongo_getfield,
        )
