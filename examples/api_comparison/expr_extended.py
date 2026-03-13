"""Module for comparing additional expression operators extended between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_expr_operators_extended():
    """Compare additional expression operators"""
    print("\n=== Additional Expression Operators (Extended) Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_expr_ext
        neo_collection.insert_many(
            [
                {
                    "name": "A",
                    "value": 16,
                    "arr": [1, 2, 3],
                    "str": "hello",
                    "dt": {"$date": "2024-01-15T10:30:00Z"},
                    "meta": {"city": "NYC"},
                },
                {
                    "name": "B",
                    "value": 25,
                    "arr": [4, 5],
                    "str": "world",
                    "dt": {"$date": "2023-06-20T15:45:00Z"},
                    "meta": {"city": "LA"},
                },
            ]
        )

        # Test $cmp
        try:
            result = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$cmp": ["$value", 20]}, -1]}}
                )
            )
            neo_cmp = len(result) == 1
            print(f"Neo $cmp: {neo_cmp} matches")
        except Exception as e:
            neo_cmp = False
            print(f"Neo $cmp: Error - {e}")

        # Test $pow
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            neo_pow = len(result) == 2
            print(f"Neo $pow: {'OK' if neo_pow else 'FAIL'}")
        except Exception as e:
            neo_pow = False
            print(f"Neo $pow: Error - {e}")

        # Test $sqrt
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            neo_sqrt = len(result) == 2
            print(f"Neo $sqrt: {'OK' if neo_sqrt else 'FAIL'}")
        except Exception as e:
            neo_sqrt = False
            print(f"Neo $sqrt: Error - {e}")

        # Test $arrayElemAt
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first": {"$arrayElemAt": ["$arr", 0]}}}]
                )
            )
            neo_arrayelemat = len(result) == 2
            print(f"Neo $arrayElemAt: {'OK' if neo_arrayelemat else 'FAIL'}")
        except Exception as e:
            neo_arrayelemat = False
            print(f"Neo $arrayElemAt: Error - {e}")

        # Test $concat
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "greeting": {"$concat": ["$str", "!", "$str"]}
                            }
                        }
                    ]
                )
            )
            neo_concat = len(result) == 2
            print(f"Neo $concat: {'OK' if neo_concat else 'FAIL'}")
        except Exception as e:
            neo_concat = False
            print(f"Neo $concat: Error - {e}")

        # Test $objectToArray
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"meta_arr": {"$objectToArray": "$meta"}}}]
                )
            )
            neo_objecttoarray = len(result) == 2
            print(
                f"Neo $objectToArray: {'OK' if neo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            neo_objecttoarray = False
            print(f"Neo $objectToArray: Error - {e}")

        # Test $switch
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "category": {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {"$lt": ["$value", 20]},
                                                "then": "small",
                                            },
                                            {
                                                "case": {"$lt": ["$value", 30]},
                                                "then": "medium",
                                            },
                                        ],
                                        "default": "large",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_switch = len(result) == 2
            print(f"Neo $switch: {'OK' if neo_switch else 'FAIL'}")
        except Exception as e:
            neo_switch = False
            print(f"Neo $switch: Error - {e}")

        # Test $ifNull
        try:
            neo_collection.insert_one({"name": "C", "value": None})
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"val": {"$ifNull": ["$value", "default"]}}}]
                )
            )
            neo_ifnull = len(result) >= 2
            print(f"Neo $ifNull: {'OK' if neo_ifnull else 'FAIL'}")
        except Exception as e:
            neo_ifnull = False
            print(f"Neo $ifNull: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_arrayelemat = None

    mongo_cmp = None

    mongo_collection = None

    mongo_concat = None

    mongo_db = None

    mongo_ifnull = None

    mongo_objecttoarray = None

    mongo_pow = None

    mongo_sqrt = None

    mongo_switch = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_expr_ext
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "A",
                    "value": 16,
                    "arr": [1, 2, 3],
                    "str": "hello",
                    "dt": {"$date": "2024-01-15T10:30:00Z"},
                    "meta": {"city": "NYC"},
                },
                {
                    "name": "B",
                    "value": 25,
                    "arr": [4, 5],
                    "str": "world",
                    "dt": {"$date": "2023-06-20T15:45:00Z"},
                    "meta": {"city": "LA"},
                },
            ]
        )

        # Test $cmp
        try:
            result = list(
                mongo_collection.find(
                    {"$expr": {"$eq": [{"$cmp": ["$value", 20]}, -1]}}
                )
            )
            mongo_cmp = len(result) == 1
            print(f"Mongo $cmp: {mongo_cmp} matches")
        except Exception as e:
            mongo_cmp = False
            print(f"Mongo $cmp: Error - {e}")

        # Test $pow
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            mongo_pow = len(result) == 2
            print(f"Mongo $pow: {'OK' if mongo_pow else 'FAIL'}")
        except Exception as e:
            mongo_pow = False
            print(f"Mongo $pow: Error - {e}")

        # Test $sqrt
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            mongo_sqrt = len(result) == 2
            print(f"Mongo $sqrt: {'OK' if mongo_sqrt else 'FAIL'}")
        except Exception as e:
            mongo_sqrt = False
            print(f"Mongo $sqrt: Error - {e}")

        # Test $arrayElemAt
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first": {"$arrayElemAt": ["$arr", 0]}}}]
                )
            )
            mongo_arrayelemat = len(result) == 2
            print(
                f"Mongo $arrayElemAt: {'OK' if mongo_arrayelemat else 'FAIL'}"
            )
        except Exception as e:
            mongo_arrayelemat = False
            print(f"Mongo $arrayElemAt: Error - {e}")

        # Test $concat
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "greeting": {"$concat": ["$str", "!", "$str"]}
                            }
                        }
                    ]
                )
            )
            mongo_concat = len(result) == 2
            print(f"Mongo $concat: {'OK' if mongo_concat else 'FAIL'}")
        except Exception as e:
            mongo_concat = False
            print(f"Mongo $concat: Error - {e}")

        # Test $objectToArray
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"meta_arr": {"$objectToArray": "$meta"}}}]
                )
            )
            mongo_objecttoarray = len(result) == 2
            print(
                f"Mongo $objectToArray: {'OK' if mongo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_objecttoarray = False
            print(f"Mongo $objectToArray: Error - {e}")

        # Test $switch
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "category": {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {"$lt": ["$value", 20]},
                                                "then": "small",
                                            },
                                            {
                                                "case": {"$lt": ["$value", 30]},
                                                "then": "medium",
                                            },
                                        ],
                                        "default": "large",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_switch = len(result) == 2
            print(f"Mongo $switch: {'OK' if mongo_switch else 'FAIL'}")
        except Exception as e:
            mongo_switch = False
            print(f"Mongo $switch: Error - {e}")

        # Test $ifNull
        try:
            mongo_collection.insert_one({"name": "C", "value": None})
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"val": {"$ifNull": ["$value", "default"]}}}]
                )
            )
            mongo_ifnull = len(result) >= 2
            print(f"Mongo $ifNull: {'OK' if mongo_ifnull else 'FAIL'}")
        except Exception as e:
            mongo_ifnull = False
            print(f"Mongo $ifNull: Error - {e}")

        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Expression Operators Extended",
        "$cmp",
        neo_cmp if neo_cmp else "FAIL",
        mongo_cmp if mongo_cmp else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$pow",
        neo_pow if neo_pow else "FAIL",
        mongo_pow if mongo_pow else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$sqrt",
        neo_sqrt if neo_sqrt else "FAIL",
        mongo_sqrt if mongo_sqrt else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$arrayElemAt",
        neo_arrayelemat if neo_arrayelemat else "FAIL",
        mongo_arrayelemat if mongo_arrayelemat else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$concat",
        neo_concat if neo_concat else "FAIL",
        mongo_concat if mongo_concat else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$objectToArray",
        neo_objecttoarray if neo_objecttoarray else "FAIL",
        mongo_objecttoarray if mongo_objecttoarray else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$switch",
        neo_switch if neo_switch else "FAIL",
        mongo_switch if mongo_switch else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Expression Operators Extended",
        "$ifNull",
        neo_ifnull if neo_ifnull else "FAIL",
        mongo_ifnull if mongo_ifnull else None,
        skip_reason="MongoDB not available" if not client else None,
    )
