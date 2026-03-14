"""Module for comparing additional aggregation features between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
    set_accumulation_mode,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_aggregation():
    """Compare additional aggregation features"""
    print("\n=== Additional Aggregation Features Comparison ===")

    neo_unwind = None
    neo_unwind_advanced = None
    neo_unwind_advanced_result = None
    neo_push = None
    neo_push_result = None
    neo_switch = None
    neo_switch_result = None
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "sizes": ["S", "M"]},
                {"item": "B", "price": 20, "quantity": 1, "sizes": ["L"]},
                {"item": "A", "price": 30, "quantity": 5, "sizes": ["M", "L"]},
            ]
        )

        set_accumulation_mode(True)
        # Test $unwind
        unwind_pipeline = [{"$unwind": "$sizes"}]
        try:
            start_neo_timing()
            neo_unwind = len(list(neo_collection.aggregate(unwind_pipeline)))
            end_neo_timing()
            print(f"Neo $unwind: {neo_unwind}")
        except Exception as e:
            neo_unwind = f"Error: {e}"
            print(f"Neo $unwind: Error - {e}")

        # Test $unwind with advanced options
        neo_collection.insert_many(
            [
                {"item": "C", "price": 15, "sizes": []},  # Empty array
                {"item": "D", "price": 25, "sizes": None},  # Null
                {"item": "E", "price": 35},  # Missing field
            ]
        )

        unwind_advanced_pipeline = [
            {
                "$unwind": {
                    "path": "$sizes",
                    "preserveNullAndEmptyArrays": True,
                    "includeArrayIndex": "idx",
                }
            }
        ]
        try:
            start_neo_timing()
            neo_unwind_advanced_result = list(
                neo_collection.aggregate(unwind_advanced_pipeline)
            )
            neo_unwind_advanced = len(neo_unwind_advanced_result)
            end_neo_timing()
            print(f"Neo $unwind (advanced): {neo_unwind_advanced} docs")
        except Exception as e:
            neo_unwind_advanced = f"Error: {e}"
            print(f"Neo $unwind (advanced): Error - {e}")

        # Test $group with $push
        push_pipeline = [
            {"$group": {"_id": "$item", "prices": {"$push": "$price"}}},
            {"$sort": {"_id": 1}},
        ]
        try:
            start_neo_timing()
            neo_push_result = list(neo_collection.aggregate(push_pipeline))
            neo_push = len(neo_push_result)
            end_neo_timing()
            print(f"Neo $group $push: {neo_push} groups")
        except Exception as e:
            neo_push = f"Error: {e}"
            print(f"Neo $group $push: Error - {e}")

        # Test $switch
        try:
            start_neo_timing()
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
            end_neo_timing()
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
    mongo_unwind_advanced = None
    mongo_unwind_advanced_result = None
    mongo_switch = None
    mongo_switch_result = None

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

        set_accumulation_mode(True)
        # Test $unwind
        try:
            start_mongo_timing()
            mongo_unwind = len(
                list(mongo_collection.aggregate(unwind_pipeline))
            )
            end_mongo_timing()
            print(f"Mongo $unwind: {mongo_unwind}")
        except Exception as e:
            mongo_unwind = f"Error: {e}"
            print(f"Mongo $unwind: Error - {e}")

        # Test $unwind with advanced options
        mongo_collection.insert_many(
            [
                {"item": "C", "price": 15, "sizes": []},  # Empty array
                {"item": "D", "price": 25, "sizes": None},  # Null
                {"item": "E", "price": 35},  # Missing field
            ]
        )

        try:
            start_mongo_timing()
            mongo_unwind_advanced_result = list(
                mongo_collection.aggregate(unwind_advanced_pipeline)
            )
            mongo_unwind_advanced = len(mongo_unwind_advanced_result)
            end_mongo_timing()
            print(f"Mongo $unwind (advanced): {mongo_unwind_advanced} docs")
        except Exception as e:
            mongo_unwind_advanced = f"Error: {e}"
            print(f"Mongo $unwind (advanced): Error - {e}")

        # Test $group with $push
        try:
            start_mongo_timing()
            mongo_push_result = list(mongo_collection.aggregate(push_pipeline))
            mongo_push = len(mongo_push_result)
            end_mongo_timing()
            print(f"Mongo $group $push: {mongo_push} groups")
        except Exception as e:
            mongo_push = f"Error: {e}"
            print(f"Mongo $group $push: Error - {e}")

        # Test $switch
        try:
            start_mongo_timing()
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
            end_mongo_timing()
            print(f"Mongo $switch: {'OK' if mongo_switch else 'FAIL'}")
        except Exception as e:
            mongo_switch = False
            print(f"Mongo $switch: Error - {e}")

        client.close()

    reporter.record_comparison(
        "Additional Aggregation",
        "$unwind",
        neo_unwind if not isinstance(neo_unwind, str) else neo_unwind,
        mongo_unwind if mongo_unwind is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )

    # Report $unwind advanced options
    reporter.record_comparison(
        "Additional Aggregation",
        "$unwind (advanced)",
        (
            neo_unwind_advanced_result
            if neo_unwind_advanced_result
            else neo_unwind_advanced
        ),
        (
            mongo_unwind_advanced_result
            if mongo_unwind_advanced_result
            else mongo_unwind_advanced
        ),
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_comparison(
        "Additional Aggregation",
        "$group $push",
        neo_push_result if neo_push_result else neo_push,
        mongo_push_result if mongo_push_result else mongo_push,
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_comparison(
        "Additional Aggregation",
        "$switch",
        neo_switch_result if neo_switch_result else neo_switch,
        mongo_switch_result if mongo_switch_result else mongo_switch,
        skip_reason="MongoDB not available" if not client else None,
    )
