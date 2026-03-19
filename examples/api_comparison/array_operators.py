"""Module for comparing array operators between NeoSQLite and PyMongo"""

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

        set_accumulation_mode(True)
        # Test $first
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first": {"$first": "$scores"}}}]
                )
            )
            end_neo_timing()
            neo_first = len(result) == 2
            print(f"Neo $first: {'OK' if neo_first else 'FAIL'}")
        except Exception as e:
            neo_first = False
            print(f"Neo $first: Error - {e}")

        # Test $last
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"last": {"$last": "$scores"}}}]
                )
            )
            end_neo_timing()
            neo_last = len(result) == 2
            print(f"Neo $last: {'OK' if neo_last else 'FAIL'}")
        except Exception as e:
            neo_last = False
            print(f"Neo $last: Error - {e}")

        # Test $filter (basic)
        try:
            start_neo_timing()
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
            end_neo_timing()
            neo_filter = len(result) == 2
            print(f"Neo $filter: {'OK' if neo_filter else 'FAIL'}")
        except Exception as e:
            neo_filter = False
            print(f"Neo $filter: Error - {e}")

        # Test $map
        try:
            start_neo_timing()
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
            end_neo_timing()
            neo_map = len(result) == 2
            print(f"Neo $map: {'OK' if neo_map else 'FAIL'}")
        except Exception as e:
            neo_map = False
            print(f"Neo $map: Error - {e}")

        # Test $reduce
        try:
            start_neo_timing()
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
            end_neo_timing()
            neo_reduce = len(result) == 2
            print(f"Neo $reduce: {'OK' if neo_reduce else 'FAIL'}")
        except Exception as e:
            neo_reduce = False
            print(f"Neo $reduce: Error - {e}")

        # Test $slice
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"first_two": {"$slice": ["$scores", 2]}}}]
                )
            )
            end_neo_timing()
            neo_slice = len(result) == 2
            print(f"Neo $slice: {'OK' if neo_slice else 'FAIL'}")
        except Exception as e:
            neo_slice = False
            print(f"Neo $slice: Error - {e}")

        # Test $indexOfArray
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"pos": {"$indexOfArray": ["$scores", 20]}}}]
                )
            )
            end_neo_timing()
            neo_indexofarray = len(result) == 2
            print(f"Neo $indexOfArray: {'OK' if neo_indexofarray else 'FAIL'}")
        except Exception as e:
            neo_indexofarray = False
            print(f"Neo $indexOfArray: Error - {e}")

        # Test $sortArray
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "sorted": {
                                    "$sortArray": {
                                        "input": "$scores",
                                        "sortBy": 1,
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_neo_timing()
            neo_sortarray = len(result) == 2 and result[0]["sorted"] == [
                10,
                20,
                30,
            ]
            print(f"Neo $sortArray: {'OK' if neo_sortarray else 'FAIL'}")
        except Exception as e:
            neo_sortarray = False
            print(f"Neo $sortArray: Error - {e}")

        # Test $minN / $maxN
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "min2": {"$minN": {"input": "$scores", "n": 2}},
                                "max2": {"$maxN": {"input": "$scores", "n": 2}},
                            }
                        }
                    ]
                )
            )
            end_neo_timing()
            neo_minmax_n = len(result) == 2
            print(f"Neo $minN / $maxN: {'OK' if neo_minmax_n else 'FAIL'}")
        except Exception as e:
            neo_minmax_n = False
            print(f"Neo $minN / $maxN: Error - {e}")

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
    mongo_sortarray = None
    mongo_minmax_n = None

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

        set_accumulation_mode(True)
        # Test $first
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first": {"$first": "$scores"}}}]
                )
            )
            end_mongo_timing()
            mongo_first = len(result) == 2
            print(f"Mongo $first: {'OK' if mongo_first else 'FAIL'}")
        except Exception as e:
            mongo_first = False
            print(f"Mongo $first: Error - {e}")

        # Test $last
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"last": {"$last": "$scores"}}}]
                )
            )
            end_mongo_timing()
            mongo_last = len(result) == 2
            print(f"Mongo $last: {'OK' if mongo_last else 'FAIL'}")
        except Exception as e:
            mongo_last = False
            print(f"Mongo $last: Error - {e}")

        # Test $filter
        try:
            start_mongo_timing()
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
            end_mongo_timing()
            mongo_filter = len(result) == 2
            print(f"Mongo $filter: {'OK' if mongo_filter else 'FAIL'}")
        except Exception as e:
            mongo_filter = False
            print(f"Mongo $filter: Error - {e}")

        # Test $map
        try:
            start_mongo_timing()
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
            end_mongo_timing()
            mongo_map = len(result) == 2
            print(f"Mongo $map: {'OK' if mongo_map else 'FAIL'}")
        except Exception as e:
            mongo_map = False
            print(f"Mongo $map: Error - {e}")

        # Test $reduce
        try:
            start_mongo_timing()
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
            end_mongo_timing()
            mongo_reduce = len(result) == 2
            print(f"Mongo $reduce: {'OK' if mongo_reduce else 'FAIL'}")
        except Exception as e:
            mongo_reduce = False
            print(f"Mongo $reduce: Error - {e}")

        # Test $slice
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"first_two": {"$slice": ["$scores", 2]}}}]
                )
            )
            end_mongo_timing()
            mongo_slice = len(result) == 2
            print(f"Mongo $slice: {'OK' if mongo_slice else 'FAIL'}")
        except Exception as e:
            mongo_slice = False
            print(f"Mongo $slice: Error - {e}")

        # Test $indexOfArray
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"pos": {"$indexOfArray": ["$scores", 20]}}}]
                )
            )
            end_mongo_timing()
            mongo_indexofarray = len(result) == 2
            print(
                f"Mongo $indexOfArray: {'OK' if mongo_indexofarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_indexofarray = False
            print(f"Mongo $indexOfArray: Error - {e}")

        # Test $sortArray
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "sorted": {
                                    "$sortArray": {
                                        "input": "$scores",
                                        "sortBy": 1,
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_mongo_timing()
            mongo_sortarray = len(result) == 2 and result[0]["sorted"] == [
                10,
                20,
                30,
            ]
            print(f"Mongo $sortArray: {'OK' if mongo_sortarray else 'FAIL'}")
        except Exception as e:
            mongo_sortarray = False
            print(f"Mongo $sortArray: Error - {e}")

        # Test $minN / $maxN
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "min2": {"$minN": {"input": "$scores", "n": 2}},
                                "max2": {"$maxN": {"input": "$scores", "n": 2}},
                            }
                        }
                    ]
                )
            )
            end_mongo_timing()
            mongo_minmax_n = len(result) == 2
            print(f"Mongo $minN / $maxN: {'OK' if mongo_minmax_n else 'FAIL'}")
        except Exception as e:
            mongo_minmax_n = False
            print(f"Mongo $minN / $maxN: Error - {e}")

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
    reporter.record_comparison(
        "Array Operators",
        "$sortArray",
        neo_sortarray if neo_sortarray else "FAIL",
        mongo_sortarray if mongo_sortarray else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array Operators",
        "$minN / $maxN",
        neo_minmax_n if neo_minmax_n else "FAIL",
        mongo_minmax_n if mongo_minmax_n else None,
        skip_reason="MongoDB not available" if not client else None,
    )
