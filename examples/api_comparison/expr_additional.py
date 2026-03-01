"""Module for comparing additional aggregation expression operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_expr_operators():
    """Compare additional aggregation expression operators"""
    print("\n=== Additional Aggregation Expression Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "scores": [80, 90, 100],
                    "meta": {"city": "NYC", "zip": 10001},
                },
                {
                    "name": "Bob",
                    "scores": [70, 80],
                    "meta": {"city": "LA", "zip": 90001},
                },
            ]
        )

        # Test $arrayElemAt
        try:
            neo_result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "first_score": {"$arrayElemAt": ["$scores", 0]}
                            }
                        }
                    ]
                )
            )
            neo_arrayelemat = len(neo_result) == 2 and all(
                "first_score" in doc for doc in neo_result
            )
            print(f"Neo $arrayElemAt: {'OK' if neo_arrayelemat else 'FAIL'}")
        except Exception as e:
            neo_arrayelemat = False
            print(f"Neo $arrayElemAt: Error - {e}")

        # Test $concat
        try:
            neo_result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "full_info": {
                                    "$concat": ["$name", " - ", "$meta.city"]
                                }
                            }
                        }
                    ]
                )
            )
            neo_concat = len(neo_result) == 2
            print(f"Neo $concat: {'OK' if neo_concat else 'FAIL'}")
        except Exception as e:
            neo_concat = False
            print(f"Neo $concat: Error - {e}")

        # Test $objectToArray
        try:
            neo_result = list(
                neo_collection.aggregate(
                    [{"$project": {"meta_array": {"$objectToArray": "$meta"}}}]
                )
            )
            neo_objecttoarray = len(neo_result) == 2 and all(
                "meta_array" in doc for doc in neo_result
            )
            print(
                f"Neo $objectToArray: {'OK' if neo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            neo_objecttoarray = False
            print(f"Neo $objectToArray: Error - {e}")

        # Test $switch
        try:
            neo_result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "grade": {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {
                                                    "$gte": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$scores",
                                                                0,
                                                            ]
                                                        },
                                                        90,
                                                    ]
                                                },
                                                "then": "A",
                                            },
                                            {
                                                "case": {
                                                    "$gte": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$scores",
                                                                0,
                                                            ]
                                                        },
                                                        80,
                                                    ]
                                                },
                                                "then": "B",
                                            },
                                        ],
                                        "default": "C",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_switch = len(neo_result) == 2
            print(f"Neo $switch: {'OK' if neo_switch else 'FAIL'}")
        except Exception as e:
            neo_switch = False
            print(f"Neo $switch: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_arrayelemat = None

    mongo_collection = None

    mongo_concat = None

    mongo_db = None

    mongo_objecttoarray = None

    mongo_result = None

    mongo_switch = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "scores": [80, 90, 100],
                    "meta": {"city": "NYC", "zip": 10001},
                },
                {
                    "name": "Bob",
                    "scores": [70, 80],
                    "meta": {"city": "LA", "zip": 90001},
                },
            ]
        )

        # Test $arrayElemAt
        try:
            mongo_result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "first_score": {"$arrayElemAt": ["$scores", 0]}
                            }
                        }
                    ]
                )
            )
            mongo_arrayelemat = len(mongo_result) == 2 and all(
                "first_score" in doc for doc in mongo_result
            )
            print(
                f"Mongo $arrayElemAt: {'OK' if mongo_arrayelemat else 'FAIL'}"
            )
        except Exception as e:
            mongo_arrayelemat = False
            print(f"Mongo $arrayElemAt: Error - {e}")

        # Test $concat
        try:
            mongo_result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "full_info": {
                                    "$concat": ["$name", " - ", "$meta.city"]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_concat = len(mongo_result) == 2
            print(f"Mongo $concat: {'OK' if mongo_concat else 'FAIL'}")
        except Exception as e:
            mongo_concat = False
            print(f"Mongo $concat: Error - {e}")

        # Test $objectToArray
        try:
            mongo_result = list(
                mongo_collection.aggregate(
                    [{"$project": {"meta_array": {"$objectToArray": "$meta"}}}]
                )
            )
            mongo_objecttoarray = len(mongo_result) == 2 and all(
                "meta_array" in doc for doc in mongo_result
            )
            print(
                f"Mongo $objectToArray: {'OK' if mongo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_objecttoarray = False
            print(f"Mongo $objectToArray: Error - {e}")

        # Test $switch
        try:
            mongo_result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "grade": {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {
                                                    "$gte": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$scores",
                                                                0,
                                                            ]
                                                        },
                                                        90,
                                                    ]
                                                },
                                                "then": "A",
                                            },
                                            {
                                                "case": {
                                                    "$gte": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$scores",
                                                                0,
                                                            ]
                                                        },
                                                        80,
                                                    ]
                                                },
                                                "then": "B",
                                            },
                                        ],
                                        "default": "C",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_switch = len(mongo_result) == 2
            print(f"Mongo $switch: {'OK' if mongo_switch else 'FAIL'}")
        except Exception as e:
            mongo_switch = False
            print(f"Mongo $switch: Error - {e}")

        client.close()

    reporter.record_result(
        "Aggregation Expressions",
        "$arrayElemAt",
        neo_arrayelemat,
        neo_arrayelemat,
        mongo_arrayelemat,
    )
    reporter.record_result(
        "Aggregation Expressions",
        "$concat",
        neo_concat,
        neo_concat,
        mongo_concat,
    )
    reporter.record_result(
        "Aggregation Expressions",
        "$objectToArray",
        neo_objecttoarray,
        neo_objecttoarray,
        mongo_objecttoarray,
    )
    reporter.record_result(
        "Aggregation Expressions",
        "$switch",
        neo_switch,
        neo_switch,
        mongo_switch,
    )
