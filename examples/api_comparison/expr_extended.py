"""Module for comparing additional expression operators extended between NeoSQLite and PyMongo"""

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


def compare_additional_expr_operators_extended():
    """Compare additional expression operators"""
    print("\n=== Additional Expression Operators (Extended) Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)
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
            start_neo_timing()
            result = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$cmp": ["$value", 20]}, -1]}}
                )
            )
            end_neo_timing()
            neo_cmp = len(result) == 1
            print(f"Neo $cmp: {neo_cmp} matches")
        except Exception as e:
            neo_cmp = False
            print(f"Neo $cmp: Error - {e}")

        # Test $pow
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            end_neo_timing()
            neo_pow = len(result) == 2
            print(f"Neo $pow: {'OK' if neo_pow else 'FAIL'}")
        except Exception as e:
            neo_pow = False
            print(f"Neo $pow: Error - {e}")

        # Test $sqrt
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            end_neo_timing()
            neo_sqrt = len(result) == 2
            print(f"Neo $sqrt: {'OK' if neo_sqrt else 'FAIL'}")
        except Exception as e:
            neo_sqrt = False
            print(f"Neo $sqrt: Error - {e}")

        # Test $arrayElemAt
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first": {"$arrayElemAt": ["$arr", 0]}}}]
                )
            )
            end_neo_timing()
            neo_arrayelemat = len(result) == 2
            print(f"Neo $arrayElemAt: {'OK' if neo_arrayelemat else 'FAIL'}")
        except Exception as e:
            neo_arrayelemat = False
            print(f"Neo $arrayElemAt: Error - {e}")

        # Test $concat
        try:
            start_neo_timing()
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
            end_neo_timing()
            neo_concat = len(result) == 2
            print(f"Neo $concat: {'OK' if neo_concat else 'FAIL'}")
        except Exception as e:
            neo_concat = False
            print(f"Neo $concat: Error - {e}")

        # Test $objectToArray
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"meta_arr": {"$objectToArray": "$meta"}}}]
                )
            )
            end_neo_timing()
            neo_objecttoarray = len(result) == 2
            print(
                f"Neo $objectToArray: {'OK' if neo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            neo_objecttoarray = False
            print(f"Neo $objectToArray: Error - {e}")

        # Test $switch
        try:
            start_neo_timing()
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
            end_neo_timing()
            neo_switch = len(result) == 2
            print(f"Neo $switch: {'OK' if neo_switch else 'FAIL'}")
        except Exception as e:
            neo_switch = False
            print(f"Neo $switch: Error - {e}")

        # Test $ifNull
        try:
            neo_collection.insert_one({"name": "C", "value": None})

            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"val": {"$ifNull": ["$value", "default"]}}}]
                )
            )
            end_neo_timing()

            neo_ifnull = len(result) >= 2
            print(f"Neo $ifNull: {'OK' if neo_ifnull else 'FAIL'}")
        except Exception as e:
            neo_ifnull = False
            print(f"Neo $ifNull: Error - {e}")

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
        set_accumulation_mode(True)
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
            start_mongo_timing()
            result = list(
                mongo_collection.find(
                    {"$expr": {"$eq": [{"$cmp": ["$value", 20]}, -1]}}
                )
            )
            end_mongo_timing()
            mongo_cmp = len(result) == 1
            print(f"Mongo $cmp: {mongo_cmp} matches")
        except Exception as e:
            mongo_cmp = False
            print(f"Mongo $cmp: Error - {e}")

        # Test $pow
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            end_mongo_timing()
            mongo_pow = len(result) == 2
            print(f"Mongo $pow: {'OK' if mongo_pow else 'FAIL'}")
        except Exception as e:
            mongo_pow = False
            print(f"Mongo $pow: Error - {e}")

        # Test $sqrt
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            end_mongo_timing()
            mongo_sqrt = len(result) == 2
            print(f"Mongo $sqrt: {'OK' if mongo_sqrt else 'FAIL'}")
        except Exception as e:
            mongo_sqrt = False
            print(f"Mongo $sqrt: Error - {e}")

        # Test $arrayElemAt
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first": {"$arrayElemAt": ["$arr", 0]}}}]
                )
            )
            end_mongo_timing()
            mongo_arrayelemat = len(result) == 2
            print(
                f"Mongo $arrayElemAt: {'OK' if mongo_arrayelemat else 'FAIL'}"
            )
        except Exception as e:
            mongo_arrayelemat = False
            print(f"Mongo $arrayElemAt: Error - {e}")

        # Test $concat
        try:
            start_mongo_timing()
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
            end_mongo_timing()
            mongo_concat = len(result) == 2
            print(f"Mongo $concat: {'OK' if mongo_concat else 'FAIL'}")
        except Exception as e:
            mongo_concat = False
            print(f"Mongo $concat: Error - {e}")

        # Test $objectToArray
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"meta_arr": {"$objectToArray": "$meta"}}}]
                )
            )
            end_mongo_timing()
            mongo_objecttoarray = len(result) == 2
            print(
                f"Mongo $objectToArray: {'OK' if mongo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_objecttoarray = False
            print(f"Mongo $objectToArray: Error - {e}")

        # Test $switch
        try:
            start_mongo_timing()
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
            end_mongo_timing()
            mongo_switch = len(result) == 2
            print(f"Mongo $switch: {'OK' if mongo_switch else 'FAIL'}")
        except Exception as e:
            mongo_switch = False
            print(f"Mongo $switch: Error - {e}")

        # Test $ifNull
        try:
            mongo_collection.insert_one({"name": "C", "value": None})

            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"val": {"$ifNull": ["$value", "default"]}}}]
                )
            )
            end_mongo_timing()

            mongo_ifnull = len(result) >= 2
            print(f"Mongo $ifNull: {'OK' if mongo_ifnull else 'FAIL'}")
        except Exception as e:
            mongo_ifnull = False
            print(f"Mongo $ifNull: Error - {e}")

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
