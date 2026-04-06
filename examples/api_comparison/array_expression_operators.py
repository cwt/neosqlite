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

    # Initialize variables for NeoSQLite results
    neo_first = None
    neo_last = None
    neo_filter = None
    neo_map = None
    neo_reduce = None
    neo_slice = None
    neo_indexofarray = None
    neo_sortarray = None
    neo_minmax_n = None

    # Initialize variables for MongoDB results
    mongo_first = None
    mongo_last = None
    mongo_filter = None
    mongo_map = None
    mongo_reduce = None
    mongo_slice = None
    mongo_indexofarray = None
    mongo_sortarray = None
    mongo_minmax_n = None

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
            try:
                result = list(
                    neo_collection.aggregate(
                        [{"$project": {"first": {"$first": "$scores"}}}]
                    )
                )
                neo_first = result
            except Exception as e:
                neo_first = f"Error: {e}"
                print(f"Neo $first: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $first: {'OK' if not isinstance(neo_first, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_first = f"Error: {e}"

        # Test $last
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [{"$project": {"last": {"$last": "$scores"}}}]
                    )
                )
                neo_last = result
            except Exception as e:
                neo_last = f"Error: {e}"
                print(f"Neo $last: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $last: {'OK' if not isinstance(neo_last, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_last = f"Error: {e}"

        # Test $filter
        try:
            start_neo_timing()
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
                neo_filter = result
            except Exception as e:
                neo_filter = f"Error: {e}"
                print(f"Neo $filter: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $filter: {'OK' if not isinstance(neo_filter, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_filter = f"Error: {e}"

        # Test $map
        try:
            start_neo_timing()
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
                neo_map = result
            except Exception as e:
                neo_map = f"Error: {e}"
                print(f"Neo $map: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $map: {'OK' if not isinstance(neo_map, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_map = f"Error: {e}"

        # Test $reduce
        try:
            start_neo_timing()
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
                                            "in": {
                                                "$add": ["$$value", "$$this"]
                                            },
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )
                neo_reduce = result
            except Exception as e:
                neo_reduce = f"Error: {e}"
                print(f"Neo $reduce: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $reduce: {'OK' if not isinstance(neo_reduce, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_reduce = f"Error: {e}"

        # Test $slice
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "first_two": {"$slice": ["$scores", 2]}
                                }
                            }
                        ]
                    )
                )
                neo_slice = result
            except Exception as e:
                neo_slice = f"Error: {e}"
                print(f"Neo $slice: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $slice: {'OK' if not isinstance(neo_slice, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_slice = f"Error: {e}"

        # Test $indexOfArray
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "pos": {"$indexOfArray": ["$scores", 20]}
                                }
                            }
                        ]
                    )
                )
                neo_indexofarray = result
            except Exception as e:
                neo_indexofarray = f"Error: {e}"
                print(f"Neo $indexOfArray: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $indexOfArray: {'OK' if not isinstance(neo_indexofarray, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_indexofarray = f"Error: {e}"

        # Test $sortArray
        try:
            start_neo_timing()
            try:
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
                neo_sortarray = result
            except Exception as e:
                neo_sortarray = f"Error: {e}"
                print(f"Neo $sortArray: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $sortArray: {'OK' if not isinstance(neo_sortarray, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_sortarray = f"Error: {e}"

        # Test $minN / $maxN
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "min2": {
                                        "$minN": {"input": "$scores", "n": 2}
                                    },
                                    "max2": {
                                        "$maxN": {"input": "$scores", "n": 2}
                                    },
                                }
                            }
                        ]
                    )
                )
                neo_minmax_n = result
            except Exception as e:
                neo_minmax_n = f"Error: {e}"
                print(f"Neo $minN / $maxN: Error - {e}")
            finally:
                end_neo_timing()
            print(
                f"Neo $minN / $maxN: {'OK' if not isinstance(neo_minmax_n, str) else 'FAIL'}"
            )
        except Exception as e:
            neo_minmax_n = f"Error: {e}"

    client = test_pymongo_connection()
    if client:
        try:
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
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [{"$project": {"first": {"$first": "$scores"}}}]
                        )
                    )
                    mongo_first = result
                except Exception as e:
                    mongo_first = f"Error: {e}"
                    print(f"Mongo $first: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $first: {'OK' if not isinstance(mongo_first, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_first = f"Error: {e}"

            # Test $last
            try:
                start_mongo_timing()
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [{"$project": {"last": {"$last": "$scores"}}}]
                        )
                    )
                    mongo_last = result
                except Exception as e:
                    mongo_last = f"Error: {e}"
                    print(f"Mongo $last: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $last: {'OK' if not isinstance(mongo_last, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_last = f"Error: {e}"

            # Test $filter
            try:
                start_mongo_timing()
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
                                                "cond": {
                                                    "$gte": ["$$score", 25]
                                                },
                                            }
                                        }
                                    }
                                }
                            ]
                        )
                    )
                    mongo_filter = result
                except Exception as e:
                    mongo_filter = f"Error: {e}"
                    print(f"Mongo $filter: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $filter: {'OK' if not isinstance(mongo_filter, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_filter = f"Error: {e}"

            # Test $map
            try:
                start_mongo_timing()
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
                                                "in": {
                                                    "$multiply": ["$$score", 2]
                                                },
                                            }
                                        }
                                    }
                                }
                            ]
                        )
                    )
                    mongo_map = result
                except Exception as e:
                    mongo_map = f"Error: {e}"
                    print(f"Mongo $map: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $map: {'OK' if not isinstance(mongo_map, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_map = f"Error: {e}"

            # Test $reduce
            try:
                start_mongo_timing()
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
                                                "in": {
                                                    "$add": [
                                                        "$$value",
                                                        "$$this",
                                                    ]
                                                },
                                            }
                                        }
                                    }
                                }
                            ]
                        )
                    )
                    mongo_reduce = result
                except Exception as e:
                    mongo_reduce = f"Error: {e}"
                    print(f"Mongo $reduce: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $reduce: {'OK' if not isinstance(mongo_reduce, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_reduce = f"Error: {e}"

            # Test $slice
            try:
                start_mongo_timing()
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [
                                {
                                    "$project": {
                                        "first_two": {"$slice": ["$scores", 2]}
                                    }
                                }
                            ]
                        )
                    )
                    mongo_slice = result
                except Exception as e:
                    mongo_slice = f"Error: {e}"
                    print(f"Mongo $slice: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $slice: {'OK' if not isinstance(mongo_slice, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_slice = f"Error: {e}"

            # Test $indexOfArray
            try:
                start_mongo_timing()
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [
                                {
                                    "$project": {
                                        "pos": {
                                            "$indexOfArray": ["$scores", 20]
                                        }
                                    }
                                }
                            ]
                        )
                    )
                    mongo_indexofarray = result
                except Exception as e:
                    mongo_indexofarray = f"Error: {e}"
                    print(f"Mongo $indexOfArray: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $indexOfArray: {'OK' if not isinstance(mongo_indexofarray, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_indexofarray = f"Error: {e}"

            # Test $sortArray
            try:
                start_mongo_timing()
                try:
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
                    mongo_sortarray = result
                except Exception as e:
                    mongo_sortarray = f"Error: {e}"
                    print(f"Mongo $sortArray: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $sortArray: {'OK' if not isinstance(mongo_sortarray, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_sortarray = f"Error: {e}"

            # Test $minN / $maxN
            try:
                start_mongo_timing()
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [
                                {
                                    "$project": {
                                        "min2": {
                                            "$minN": {
                                                "input": "$scores",
                                                "n": 2,
                                            }
                                        },
                                        "max2": {
                                            "$maxN": {
                                                "input": "$scores",
                                                "n": 2,
                                            }
                                        },
                                    }
                                }
                            ]
                        )
                    )
                    mongo_minmax_n = result
                except Exception as e:
                    mongo_minmax_n = f"Error: {e}"
                    print(f"Mongo $minN / $maxN: Error - {e}")
                finally:
                    end_mongo_timing()
                print(
                    f"Mongo $minN / $maxN: {'OK' if not isinstance(mongo_minmax_n, str) else 'FAIL'}"
                )
            except Exception as e:
                mongo_minmax_n = f"Error: {e}"

        finally:
            client.close()

    # Record comparisons
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$first",
        neo_first,
        mongo_first,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$last",
        neo_last,
        mongo_last,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$filter",
        neo_filter,
        mongo_filter,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$map",
        neo_map,
        mongo_map,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$reduce",
        neo_reduce,
        mongo_reduce,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$slice",
        neo_slice,
        mongo_slice,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$indexOfArray",
        neo_indexofarray,
        mongo_indexofarray,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$sortArray",
        neo_sortarray,
        mongo_sortarray,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Array (Expression Operators)",
        "$minN / $maxN",
        neo_minmax_n,
        mongo_minmax_n,
        skip_reason="MongoDB not available" if not client else None,
    )
