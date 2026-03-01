"""Module for comparing additional expression operators extended between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_expr_operators_extended():
    """Compare additional expression operators"""
    print("\n=== Additional Expression Operators (Extended) Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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

        client.close()

        reporter.record_result(
            "Expression Operators Extended", "$cmp", neo_cmp, neo_cmp, mongo_cmp
        )
        reporter.record_result(
            "Expression Operators Extended", "$pow", neo_pow, neo_pow, mongo_pow
        )
        reporter.record_result(
            "Expression Operators Extended",
            "$sqrt",
            neo_sqrt,
            neo_sqrt,
            mongo_sqrt,
        )
        reporter.record_result(
            "Expression Operators Extended",
            "$arrayElemAt",
            neo_arrayelemat,
            neo_arrayelemat,
            mongo_arrayelemat,
        )
        reporter.record_result(
            "Expression Operators Extended",
            "$concat",
            neo_concat,
            neo_concat,
            mongo_concat,
        )
        reporter.record_result(
            "Expression Operators Extended",
            "$objectToArray",
            neo_objecttoarray,
            neo_objecttoarray,
            mongo_objecttoarray,
        )
        reporter.record_result(
            "Expression Operators Extended",
            "$switch",
            neo_switch,
            neo_switch,
            mongo_switch,
        )
        reporter.record_result(
            "Expression Operators Extended",
            "$ifNull",
            neo_ifnull,
            neo_ifnull,
            mongo_ifnull,
        )
