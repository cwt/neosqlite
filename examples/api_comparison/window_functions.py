"""Module for comparing window functions between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from .reporter import reporter
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
    }

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_window
        neo_collection.insert_many(test_data)

        for name, pipeline in pipelines.items():
            try:
                neo_results[name] = list(neo_collection.aggregate(pipeline))
                print(f"Neo $setWindowFields ({name}): OK")
            except Exception as e:
                neo_results[name] = f"Error: {e}"
                print(f"Neo $setWindowFields ({name}): Error - {e}")

        # Test explain()
        try:
            explanation = neo_collection.aggregate(pipelines["rank"]).explain()
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

        for name, pipeline in pipelines.items():
            try:
                mongo_results[name] = list(mongo_collection.aggregate(pipeline))
                print(f"Mongo $setWindowFields ({name}): OK")
            except Exception as e:
                mongo_results[name] = f"Error: {e}"
                print(f"Mongo $setWindowFields ({name}): Error - {e}")

        # Test explain() on MongoDB (returns a different format but should work)
        try:
            # PyMongo aggregate().explain() returns a dict
            explanation = mongo_collection.aggregate(
                pipelines["rank"]
            ).explain()
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
        reporter.record_comparison(
            "Window Functions",
            f"$setWindowFields ({name})",
            neo_results.get(name),
            mongo_results.get(name),
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
