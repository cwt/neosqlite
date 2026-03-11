"""Module for comparing advanced window math operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_window_math():
    """Compare advanced math window operators"""
    print("\n=== Window Math Comparison ===")

    test_data = [
        {"_id": 1, "time": 1, "x": 10, "y": 100, "cat": "A"},
        {"_id": 2, "time": 2, "x": 20, "y": 200, "cat": "A"},
        {"_id": 3, "time": 3, "x": 40, "y": 300, "cat": "A"},
    ]

    pipelines = {
        "derivative": [
            {
                "$setWindowFields": {
                    "sortBy": {"time": 1},
                    "output": {
                        "deriv": {
                            "$derivative": {"input": "$x"},
                            "window": {"documents": [-1, 0]},
                        }
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "integral": [
            {
                "$setWindowFields": {
                    "sortBy": {"time": 1},
                    "output": {
                        "integ": {
                            "$integral": {"input": "$x"},
                            "window": {"documents": ["unbounded", "current"]},
                        }
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "covariance": [
            {
                "$setWindowFields": {
                    "sortBy": {"time": 1},
                    "output": {
                        "covPop": {"$covariancePop": ["$x", "$y"]},
                        "covSamp": {"$covarianceSamp": ["$x", "$y"]},
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "ema": [
            {
                "$setWindowFields": {
                    "sortBy": {"time": 1},
                    "output": {
                        "ema": {"$expMovingAvg": {"input": "$x", "alpha": 0.5}}
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ],
    }

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.series
        neo_collection.insert_many(test_data)

        for name, pipeline in pipelines.items():
            try:
                neo_results[name] = list(neo_collection.aggregate(pipeline))
                print(f"Neo window math ({name}): OK")
            except Exception as e:
                neo_results[name] = f"Error: {e}"
                print(f"Neo window math ({name}): Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.series
        mongo_collection.delete_many({})
        mongo_collection.insert_many(test_data)

        for name, pipeline in pipelines.items():
            try:
                mongo_results[name] = list(mongo_collection.aggregate(pipeline))
                print(f"Mongo window math ({name}): OK")
            except Exception as e:
                mongo_results[name] = f"Error: {e}"
                print(f"Mongo window math ({name}): Error - {e}")

        client.close()

    # Record comparisons
    for name in pipelines:
        reporter.record_comparison(
            "Window Math",
            f"window_{name}",
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason="MongoDB not available" if not client else None,
        )
