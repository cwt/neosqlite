"""Module for comparing additional aggregation features between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_aggregation():
    """Compare additional aggregation features"""
    print("\n=== Additional Aggregation Features Comparison ===")

    neo_switch = None
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "sizes": ["S", "M"]},
                {"item": "B", "price": 20, "quantity": 1, "sizes": ["L"]},
                {"item": "A", "price": 30, "quantity": 5, "sizes": ["M", "L"]},
            ]
        )

        # Test $unwind
        unwind_pipeline = [{"$unwind": "$sizes"}]
        try:
            neo_unwind = len(list(neo_collection.aggregate(unwind_pipeline)))
            print(f"Neo $unwind: {neo_unwind}")
        except Exception as e:
            neo_unwind = f"Error: {e}"
            print(f"Neo $unwind: Error - {e}")

        # Test $group with $push
        push_pipeline = [
            {"$group": {"_id": "$item", "prices": {"$push": "$price"}}},
            {"$sort": {"_id": 1}},
        ]
        try:
            neo_push_result = list(neo_collection.aggregate(push_pipeline))
            neo_push = len(neo_push_result)
            print(f"Neo $group $push: {neo_push} groups")
        except Exception as e:
            neo_push = f"Error: {e}"
            print(f"Neo $group $push: Error - {e}")

        # Test $switch
        try:
            neo_switch_result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "category": {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {
                                                    "$gte": ["$price", 20]
                                                },
                                                "then": "expensive",
                                            },
                                            {
                                                "case": {
                                                    "$gte": ["$price", 10]
                                                },
                                                "then": "moderate",
                                            },
                                        ],
                                        "default": "cheap",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_switch = len(neo_switch_result) > 0
            print(f"Neo $switch: {'OK' if neo_switch else 'FAIL'}")
        except Exception as e:
            neo_switch = False
            print(f"Neo $switch: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_db = None
    mongo_push = None
    mongo_push_result = None
    mongo_unwind = None
    mongo_switch = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "sizes": ["S", "M"]},
                {"item": "B", "price": 20, "quantity": 1, "sizes": ["L"]},
                {"item": "A", "price": 30, "quantity": 5, "sizes": ["M", "L"]},
            ]
        )

        # Test $unwind
        try:
            mongo_unwind = len(
                list(mongo_collection.aggregate(unwind_pipeline))
            )
            print(f"Mongo $unwind: {mongo_unwind}")
        except Exception as e:
            mongo_unwind = f"Error: {e}"
            print(f"Mongo $unwind: Error - {e}")

        # Test $group with $push
        try:
            mongo_push_result = list(mongo_collection.aggregate(push_pipeline))
            mongo_push = len(mongo_push_result)
            print(f"Mongo $group $push: {mongo_push} groups")
        except Exception as e:
            mongo_push = f"Error: {e}"
            print(f"Mongo $group $push: Error - {e}")

        # Test $switch
        try:
            mongo_switch_result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "category": {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {
                                                    "$gte": ["$price", 20]
                                                },
                                                "then": "expensive",
                                            },
                                            {
                                                "case": {
                                                    "$gte": ["$price", 10]
                                                },
                                                "then": "moderate",
                                            },
                                        ],
                                        "default": "cheap",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_switch = len(mongo_switch_result) > 0
            print(f"Mongo $switch: {'OK' if mongo_switch else 'FAIL'}")
        except Exception as e:
            mongo_switch = False
            print(f"Mongo $switch: Error - {e}")

        client.close()

    if not isinstance(neo_unwind, str) and not isinstance(mongo_unwind, str):
        reporter.record_result(
            "Additional Aggregation",
            "$unwind",
            neo_unwind == mongo_unwind if mongo_unwind is not None else False,
            neo_unwind,
            mongo_unwind,
        )
    else:
        reporter.record_result(
            "Additional Aggregation", "$unwind", False, neo_unwind, mongo_unwind
        )

    if not isinstance(neo_push, str) and not isinstance(mongo_push, str):
        reporter.record_result(
            "Additional Aggregation", "$group $push", True, neo_push, mongo_push
        )
    else:
        reporter.record_result(
            "Additional Aggregation",
            "$group $push",
            False,
            neo_push,
            mongo_push,
        )

    reporter.record_result(
        "Additional Aggregation",
        "$switch",
        neo_switch if neo_switch is not None else False,
        neo_switch,
        mongo_switch,
    )
