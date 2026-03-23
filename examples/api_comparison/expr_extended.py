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

    neo_results = {}
    mongo_results = {}

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
        start_neo_timing()
        try:
            res = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$cmp": ["$value", 20]}, -1]}}
                )
            )
            neo_results["$cmp"] = res
            print(f"Neo $cmp: {len(res)} matches")
        except Exception as e:
            neo_results["$cmp"] = f"Error: {e}"
            print(f"Neo $cmp: Error - {e}")
        finally:
            end_neo_timing()

        # Test $pow
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            neo_results["$pow"] = res
            print("Neo $pow: OK")
        except Exception as e:
            neo_results["$pow"] = f"Error: {e}"
            print(f"Neo $pow: Error - {e}")
        finally:
            end_neo_timing()

        # Test $sqrt
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            neo_results["$sqrt"] = res
            print("Neo $sqrt: OK")
        except Exception as e:
            neo_results["$sqrt"] = f"Error: {e}"
            print(f"Neo $sqrt: Error - {e}")
        finally:
            end_neo_timing()

        # Test $arrayElemAt
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [{"$project": {"first": {"$arrayElemAt": ["$arr", 0]}}}]
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
                                "greeting": {"$concat": ["$str", "!", "$str"]}
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
                    [{"$project": {"meta_arr": {"$objectToArray": "$meta"}}}]
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
            neo_results["$switch"] = res
            print("Neo $switch: OK")
        except Exception as e:
            neo_results["$switch"] = f"Error: {e}"
            print(f"Neo $switch: Error - {e}")
        finally:
            end_neo_timing()

        # Test $ifNull
        neo_collection.insert_one({"name": "C", "value": None})
        start_neo_timing()
        try:
            res = list(
                neo_collection.aggregate(
                    [{"$project": {"val": {"$ifNull": ["$value", "default"]}}}]
                )
            )
            neo_results["$ifNull"] = res
            print("Neo $ifNull: OK")
        except Exception as e:
            neo_results["$ifNull"] = f"Error: {e}"
            print(f"Neo $ifNull: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    if client:
        try:
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
            start_mongo_timing()
            try:
                res = list(
                    mongo_collection.find(
                        {"$expr": {"$eq": [{"$cmp": ["$value", 20]}, -1]}}
                    )
                )
                mongo_results["$cmp"] = res
                print(f"Mongo $cmp: {len(res)} matches")
            except Exception as e:
                mongo_results["$cmp"] = f"Error: {e}"
                print(f"Mongo $cmp: Error - {e}")
            finally:
                end_mongo_timing()

            # Test $pow
            start_mongo_timing()
            try:
                res = list(
                    mongo_collection.aggregate(
                        [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                    )
                )
                mongo_results["$pow"] = res
                print("Mongo $pow: OK")
            except Exception as e:
                mongo_results["$pow"] = f"Error: {e}"
                print(f"Mongo $pow: Error - {e}")
            finally:
                end_mongo_timing()

            # Test $sqrt
            start_mongo_timing()
            try:
                res = list(
                    mongo_collection.aggregate(
                        [{"$project": {"root": {"$sqrt": "$value"}}}]
                    )
                )
                mongo_results["$sqrt"] = res
                print("Mongo $sqrt: OK")
            except Exception as e:
                mongo_results["$sqrt"] = f"Error: {e}"
                print(f"Mongo $sqrt: Error - {e}")
            finally:
                end_mongo_timing()

            # Test $arrayElemAt
            start_mongo_timing()
            try:
                res = list(
                    mongo_collection.aggregate(
                        [{"$project": {"first": {"$arrayElemAt": ["$arr", 0]}}}]
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
                                    "greeting": {
                                        "$concat": ["$str", "!", "$str"]
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
                        [
                            {
                                "$project": {
                                    "meta_arr": {"$objectToArray": "$meta"}
                                }
                            }
                        ]
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
                                    "category": {
                                        "$switch": {
                                            "branches": [
                                                {
                                                    "case": {
                                                        "$lt": ["$value", 20]
                                                    },
                                                    "then": "small",
                                                },
                                                {
                                                    "case": {
                                                        "$lt": ["$value", 30]
                                                    },
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
                mongo_results["$switch"] = res
                print("Mongo $switch: OK")
            except Exception as e:
                mongo_results["$switch"] = f"Error: {e}"
                print(f"Mongo $switch: Error - {e}")
            finally:
                end_mongo_timing()

            # Test $ifNull
            mongo_collection.insert_one({"name": "C", "value": None})
            start_mongo_timing()
            try:
                res = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "val": {"$ifNull": ["$value", "default"]}
                                }
                            }
                        ]
                    )
                )
                mongo_results["$ifNull"] = res
                print("Mongo $ifNull: OK")
            except Exception as e:
                mongo_results["$ifNull"] = f"Error: {e}"
                print(f"Mongo $ifNull: Error - {e}")
            finally:
                end_mongo_timing()

        finally:
            client.close()

    # Record results
    for op in [
        "$cmp",
        "$pow",
        "$sqrt",
        "$arrayElemAt",
        "$concat",
        "$objectToArray",
        "$switch",
        "$ifNull",
    ]:
        reporter.record_comparison(
            "Expression Operators Extended",
            op,
            neo_results.get(op),
            mongo_results.get(op),
            skip_reason="MongoDB not available" if not client else None,
        )
