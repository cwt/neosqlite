"""Module for comparing window functions between NeoSQLite and PyMongo"""

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


def compare_window_functions():
    """Compare $setWindowFields and window operators"""
    print("\n=== Window Functions Comparison ===")

    test_data = [
        {"_id": 1, "name": "A", "score": 100, "dept": "Sales"},
        {"_id": 2, "name": "B", "score": 90, "dept": "Sales"},
        {"_id": 3, "name": "C", "score": 90, "dept": "Sales"},
        {"_id": 4, "name": "D", "score": 80, "dept": "Eng"},
        {"_id": 5, "name": "E", "score": 110, "dept": "Eng"},
        {"_id": 6, "name": "F", "score": 110, "dept": "Eng"},
    ]

    pipelines = {
        "rank": [
            {
                "$setWindowFields": {
                    "partitionBy": "$dept",
                    "sortBy": {"score": -1},
                    "output": {
                        "rank": {"$rank": {}},
                        "denseRank": {"$denseRank": {}},
                        "docNum": {"$documentNumber": {}},
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "shift": [
            {
                "$setWindowFields": {
                    "sortBy": {"_id": 1},
                    "output": {
                        "prev": {
                            "$shift": {
                                "output": "$score",
                                "by": -1,
                                "default": -1,
                            }
                        },
                        "next": {
                            "$shift": {
                                "output": "$score",
                                "by": 1,
                                "default": -1,
                            }
                        },
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "accumulators": [
            {
                "$setWindowFields": {
                    "partitionBy": "$dept",
                    "sortBy": {"_id": 1},
                    "output": {
                        "runningSum": {
                            "$sum": "$score",
                            "window": {"documents": ["unbounded", "current"]},
                        },
                        "movingAvg": {
                            "$avg": "$score",
                            "window": {"documents": [-1, 1]},
                        },
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "top_bottom": [
            {
                "$setWindowFields": {
                    "partitionBy": "$dept",
                    "sortBy": {"score": -1},
                    "output": {
                        "topScore": {
                            "$top": {
                                "output": "$score",
                                "sortBy": {"score": -1},
                            }
                        },
                        "bottomScore": {
                            "$bottom": {
                                "output": "$score",
                                "sortBy": {"score": -1},
                            }
                        },
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "topN_bottomN": [
            {
                "$setWindowFields": {
                    "partitionBy": "$dept",
                    "sortBy": {"score": -1},
                    "output": {
                        "top2": {
                            "$topN": {
                                "n": 2,
                                "sortBy": {"score": -1},
                                "output": "$name",
                            }
                        },
                        "bottom2": {
                            "$bottomN": {
                                "n": 2,
                                "sortBy": {"score": -1},
                                "output": "$name",
                            }
                        },
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "addToSet": [
            {
                "$setWindowFields": {
                    "partitionBy": "$dept",
                    "output": {"allScores": {"$addToSet": "$score"}},
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "n_operators": [
            {
                "$setWindowFields": {
                    "partitionBy": "$dept",
                    "sortBy": {"score": -1},
                    "output": {
                        "first2": {"$firstN": {"input": "$name", "n": 2}},
                        "last2": {"$lastN": {"input": "$name", "n": 2}},
                        "min2": {"$minN": {"input": "$score", "n": 2}},
                        "max2": {"$maxN": {"input": "$score", "n": 2}},
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
    }

    neo_results = {}
    neo_explain_ok = False
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_window
        neo_collection.insert_many(test_data)

        set_accumulation_mode(True)
        for name, pipeline in pipelines.items():
            try:
                start_neo_timing()
                result = list(neo_collection.aggregate(pipeline))
                end_neo_timing()

                neo_results[name] = result
                print(f"Neo $setWindowFields ({name}): OK")
            except Exception as e:
                neo_results[name] = f"Error: {e}"
                print(f"Neo $setWindowFields ({name}): Error - {e}")

        # Test explain() - NeoSQLite uses db.command() like MongoDB
        try:
            start_neo_timing()
            explanation = neo_conn.command(
                "aggregate",
                "test_window",
                pipeline=pipelines["rank"],
                explain=True,
            )
            end_neo_timing()

            neo_explain_ok = explanation.get("tier") == 1
            print(
                f"Neo AggregationCursor.explain(): {'OK' if neo_explain_ok else 'FAIL'}"
            )
        except Exception as e:
            neo_explain_ok = False
            print(f"Neo AggregationCursor.explain(): Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}
    mongo_explain_ok = False

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_window
        mongo_collection.delete_many({})
        mongo_collection.insert_many(test_data)

        set_accumulation_mode(True)
        for name, pipeline in pipelines.items():
            try:
                start_mongo_timing()
                result = list(mongo_collection.aggregate(pipeline))
                end_mongo_timing()

                mongo_results[name] = result
                print(f"Mongo $setWindowFields ({name}): OK")
            except Exception as e:
                mongo_results[name] = f"Error: {e}"
                print(f"Mongo $setWindowFields ({name}): Error - {e}")

        # Test explain() on MongoDB
        # Note: MongoDB aggregation explain requires using db.command() approach
        # This is a NeoSQLite extension beyond MongoDB's standard cursor API
        try:
            start_mongo_timing()
            explanation = mongo_db.command(
                "aggregate",
                "test_window",
                pipeline=pipelines["rank"],
                explain=True,
            )
            end_mongo_timing()

            mongo_explain_ok = isinstance(explanation, dict)
            print(
                f"Mongo AggregationCursor.explain(): {'OK' if mongo_explain_ok else 'FAIL'}"
            )
        except Exception as e:
            mongo_explain_ok = False
            print(f"Mongo AggregationCursor.explain(): Error - {e}")

        client.close()

    # Record comparisons
    for name in pipelines:
        neo_res = neo_results.get(name)
        mongo_res = mongo_results.get(name)

        # $addToSet results are unordered sets, so we sort them for comparison
        if (
            name == "addToSet"
            and isinstance(neo_res, list)
            and isinstance(mongo_res, list)
        ):
            for i in range(min(len(neo_res), len(mongo_res))):
                if "allScores" in neo_res[i] and isinstance(
                    neo_res[i]["allScores"], list
                ):
                    neo_res[i]["allScores"].sort()
                if "allScores" in mongo_res[i] and isinstance(
                    mongo_res[i]["allScores"], list
                ):
                    mongo_res[i]["allScores"].sort()

        reporter.record_comparison(
            "Window Functions",
            f"$setWindowFields ({name})",
            neo_res,
            mongo_res,
            skip_reason="MongoDB not available" if not client else None,
        )

    reporter.record_result(
        "Window Functions",
        "cursor_explain",
        passed=neo_explain_ok,
        neo_result="Tier 1 SQL" if neo_explain_ok else "Fail",
        mongo_result="Dict output" if mongo_explain_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
