"""Module for comparing additional aggregation expression operators between NeoSQLite and PyMongo"""

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
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_expr_array_object():
    """Compare additional aggregation expression operators"""
    print("\n=== Additional Aggregation Expression Operators Comparison ===")

    # Initialize result variables
    neo_results = {}
    mongo_results = {}

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

        set_accumulation_mode(True)

        # Test $arrayElemAt
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [{"$project": {"val": {"$arrayElemAt": ["$scores", 0]}}}]
                )
            )
            neo_results["$arrayElemAt"] = res
            print("Neo $arrayElemAt: OK")
        except Exception as e:
            neo_results["$arrayElemAt"] = f"Error: {e}"
            print(f"Neo $arrayElemAt: Error - {e}")
        finally:
            end_neo_timing()

        # Test $concat
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "val": {
                                    "$concat": ["$name", " - ", "$meta.city"]
                                }
                            }
                        }
                    ]
                )
            )
            neo_results["$concat"] = res
            print("Neo $concat: OK")
        except Exception as e:
            neo_results["$concat"] = f"Error: {e}"
            print(f"Neo $concat: Error - {e}")
        finally:
            end_neo_timing()

        # Test $objectToArray
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [{"$project": {"val": {"$objectToArray": "$meta"}}}]
                )
            )
            neo_results["$objectToArray"] = res
            print("Neo $objectToArray: OK")
        except Exception as e:
            neo_results["$objectToArray"] = f"Error: {e}"
            print(f"Neo $objectToArray: Error - {e}")
        finally:
            end_neo_timing()

        # Test $switch
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "val": {
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
            neo_results["$switch"] = res
            print("Neo $switch: OK")
        except Exception as e:
            neo_results["$switch"] = f"Error: {e}"
            print(f"Neo $switch: Error - {e}")
        finally:
            end_neo_timing()

    client = get_mongo_client()
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

        set_accumulation_mode(True)

        # Test $arrayElemAt
        start_mongo_timing()
        try:
            res = list(
                mongo_collection.aggregate(
                    [{"$project": {"val": {"$arrayElemAt": ["$scores", 0]}}}]
                )
            )
            mongo_results["$arrayElemAt"] = res
            print("Mongo $arrayElemAt: OK")
        except Exception as e:
            mongo_results["$arrayElemAt"] = f"Error: {e}"
            print(f"Mongo $arrayElemAt: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $concat
        start_mongo_timing()
        try:
            res = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "val": {
                                    "$concat": [
                                        "$name",
                                        " - ",
                                        "$meta.city",
                                    ]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_results["$concat"] = res
            print("Mongo $concat: OK")
        except Exception as e:
            mongo_results["$concat"] = f"Error: {e}"
            print(f"Mongo $concat: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $objectToArray
        start_mongo_timing()
        try:
            res = list(
                mongo_collection.aggregate(
                    [{"$project": {"val": {"$objectToArray": "$meta"}}}]
                )
            )
            mongo_results["$objectToArray"] = res
            print("Mongo $objectToArray: OK")
        except Exception as e:
            mongo_results["$objectToArray"] = f"Error: {e}"
            print(f"Mongo $objectToArray: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $switch
        start_mongo_timing()
        try:
            res = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "val": {
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
            mongo_results["$switch"] = res
            print("Mongo $switch: OK")
        except Exception as e:
            mongo_results["$switch"] = f"Error: {e}"
            print(f"Mongo $switch: Error - {e}")
        finally:
            end_mongo_timing()

    for op in ["$arrayElemAt", "$concat", "$objectToArray", "$switch"]:
        reporter.record_comparison(
            "$expr (Array & Object Operators)",
            op,
            neo_results.get(op),
            mongo_results.get(op),
            skip_reason="MongoDB not available" if not client else None,
        )
