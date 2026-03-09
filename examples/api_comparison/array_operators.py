"""Module for comparing array operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_array_operators():
    """Compare array operators in aggregation"""
    print("\n=== Array Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "A", "scores": [10, 20, 30]},
                {"name": "B", "scores": [40, 50]},
            ]
        )

        # Test $first
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first": {"$first": "$scores"}}}]
                )
            )
            neo_first = len(result) == 2
            print(f"Neo $first: {'OK' if neo_first else 'FAIL'}")
        except Exception as e:
            neo_first = False
            print(f"Neo $first: Error - {e}")

        # Test $last
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"last": {"$last": "$scores"}}}]
                )
            )
            neo_last = len(result) == 2
            print(f"Neo $last: {'OK' if neo_last else 'FAIL'}")
        except Exception as e:
            neo_last = False
            print(f"Neo $last: Error - {e}")

        # Test $filter (basic)
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "high_scores": {
                                    "$filter": {
                                        "input": "$scores",
                                        "as": "score",
                                        "cond": {"$gte": ["$$score", 25]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_filter = len(result) == 2
            print(f"Neo $filter: {'OK' if neo_filter else 'FAIL'}")
        except Exception as e:
            neo_filter = False
            print(f"Neo $filter: Error - {e}")

        # Test $map
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "doubled": {
                                    "$map": {
                                        "input": "$scores",
                                        "as": "score",
                                        "in": {"$multiply": ["$$score", 2]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_map = len(result) == 2
            print(f"Neo $map: {'OK' if neo_map else 'FAIL'}")
        except Exception as e:
            neo_map = False
            print(f"Neo $map: Error - {e}")

        # Test $reduce
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "total": {
                                    "$reduce": {
                                        "input": "$scores",
                                        "initialValue": 0,
                                        "in": {"$add": ["$$value", "$$this"]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_reduce = len(result) == 2
            print(f"Neo $reduce: {'OK' if neo_reduce else 'FAIL'}")
        except Exception as e:
            neo_reduce = False
            print(f"Neo $reduce: Error - {e}")

        # Test $slice
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first_two": {"$slice": ["$scores", 2]}}}]
                )
            )
            neo_slice = len(result) == 2
            print(f"Neo $slice: {'OK' if neo_slice else 'FAIL'}")
        except Exception as e:
            neo_slice = False
            print(f"Neo $slice: Error - {e}")

        # Test $indexOfArray
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"pos": {"$indexOfArray": ["$scores", 20]}}}]
                )
            )
            neo_indexofarray = len(result) == 2
            print(f"Neo $indexOfArray: {'OK' if neo_indexofarray else 'FAIL'}")
        except Exception as e:
            neo_indexofarray = False
            print(f"Neo $indexOfArray: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_db = None
    mongo_filter = None
    mongo_first = None
    mongo_last = None
    mongo_map = None
    mongo_reduce = None
    mongo_slice = None
    mongo_indexofarray = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "A", "scores": [10, 20, 30]},
                {"name": "B", "scores": [40, 50]},
            ]
        )

        # Test $first
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first": {"$first": "$scores"}}}]
                )
            )
            mongo_first = len(result) == 2
            print(f"Mongo $first: {'OK' if mongo_first else 'FAIL'}")
        except Exception as e:
            mongo_first = False
            print(f"Mongo $first: Error - {e}")

        # Test $last
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"last": {"$last": "$scores"}}}]
                )
            )
            mongo_last = len(result) == 2
            print(f"Mongo $last: {'OK' if mongo_last else 'FAIL'}")
        except Exception as e:
            mongo_last = False
            print(f"Mongo $last: Error - {e}")

        # Test $filter
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "high_scores": {
                                    "$filter": {
                                        "input": "$scores",
                                        "as": "score",
                                        "cond": {"$gte": ["$$score", 25]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_filter = len(result) == 2
            print(f"Mongo $filter: {'OK' if mongo_filter else 'FAIL'}")
        except Exception as e:
            mongo_filter = False
            print(f"Mongo $filter: Error - {e}")

        # Test $map
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "doubled": {
                                    "$map": {
                                        "input": "$scores",
                                        "as": "score",
                                        "in": {"$multiply": ["$$score", 2]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_map = len(result) == 2
            print(f"Mongo $map: {'OK' if mongo_map else 'FAIL'}")
        except Exception as e:
            mongo_map = False
            print(f"Mongo $map: Error - {e}")

        # Test $reduce
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "total": {
                                    "$reduce": {
                                        "input": "$scores",
                                        "initialValue": 0,
                                        "in": {"$add": ["$$value", "$$this"]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_reduce = len(result) == 2
            print(f"Mongo $reduce: {'OK' if mongo_reduce else 'FAIL'}")
        except Exception as e:
            mongo_reduce = False
            print(f"Mongo $reduce: Error - {e}")

        # Test $slice
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first_two": {"$slice": ["$scores", 2]}}}]
                )
            )
            mongo_slice = len(result) == 2
            print(f"Mongo $slice: {'OK' if mongo_slice else 'FAIL'}")
        except Exception as e:
            mongo_slice = False
            print(f"Mongo $slice: Error - {e}")

        # Test $indexOfArray
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"pos": {"$indexOfArray": ["$scores", 20]}}}]
                )
            )
            mongo_indexofarray = len(result) == 2
            print(
                f"Mongo $indexOfArray: {'OK' if mongo_indexofarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_indexofarray = False
            print(f"Mongo $indexOfArray: Error - {e}")

        client.close()

        reporter.record_comparison(
            "Array Operators",
            "$first",
            neo_first if neo_first else "FAIL",
            mongo_first if mongo_first else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Array Operators",
            "$last",
            neo_last if neo_last else "FAIL",
            mongo_last if mongo_last else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Array Operators",
            "$filter",
            neo_filter if neo_filter else "FAIL",
            mongo_filter if mongo_filter else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Array Operators",
            "$map",
            neo_map if neo_map else "FAIL",
            mongo_map if mongo_map else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Array Operators",
            "$reduce",
            neo_reduce if neo_reduce else "FAIL",
            mongo_reduce if mongo_reduce else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Array Operators",
            "$slice",
            neo_slice if neo_slice else "FAIL",
            mongo_slice if mongo_slice else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Array Operators",
            "$indexOfArray",
            neo_indexofarray if neo_indexofarray else "FAIL",
            mongo_indexofarray if mongo_indexofarray else None,
            skip_reason="MongoDB not available" if not client else None,
        )
