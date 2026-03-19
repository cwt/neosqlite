"""Module for comparing expression operators ($rand, $let) between NeoSQLite and PyMongo"""

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


def compare_expression_operators():
    """Compare expression operators ($rand, $let)"""
    print("\n=== Expression Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_expr_ops
        neo_collection.insert_one({"_id": 1, "value": 5})

        set_accumulation_mode(True)
        neo_results = {}

        # Test $rand
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"random": {"$rand": {}}}},
                    ]
                )
            )
            end_neo_timing()

            val = result[0].get("random") if result else None
            neo_results["$rand"] = (
                "generated" if val is not None and 0 <= val <= 1 else "invalid"
            )
            print(f"Neo $rand: {neo_results['$rand']}")
        except Exception as e:
            neo_results["$rand"] = f"Error: {e}"
            print(f"Neo $rand: Error - {e}")

        # Test $let
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "let_val": {
                                    "$let": {
                                        "vars": {"x": 5},
                                        "in": {"$add": ["$$x", 10]},
                                    }
                                }
                            }
                        },
                    ]
                )
            )
            end_neo_timing()

            neo_results["$let"] = result[0].get("let_val") if result else None
            print(f"Neo $let: {neo_results['$let']}")
        except Exception as e:
            neo_results["$let"] = f"Error: {e}"
            print(f"Neo $let: Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_expr_ops
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"_id": 1, "value": 5})

        set_accumulation_mode(True)
        # Test $rand
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"random": {"$rand": {}}}},
                    ]
                )
            )
            end_mongo_timing()

            val = result[0].get("random") if result else None
            mongo_results["$rand"] = (
                "generated" if val is not None and 0 <= val <= 1 else "invalid"
            )
            print(f"Mongo $rand: {mongo_results['$rand']}")
        except Exception as e:
            mongo_results["$rand"] = f"Error: {e}"
            print(f"Mongo $rand: Error - {e}")

        # Test $let
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "let_val": {
                                    "$let": {
                                        "vars": {"x": 5},
                                        "in": {"$add": ["$$x", 10]},
                                    }
                                }
                            }
                        },
                    ]
                )
            )
            end_mongo_timing()

            mongo_results["$let"] = result[0].get("let_val") if result else None
            print(f"Mongo $let: {mongo_results['$let']}")
        except Exception as e:
            mongo_results["$let"] = f"Error: {e}"
            print(f"Mongo $let: Error - {e}")

        client.close()

    for op_name in neo_results:
        reporter.record_comparison(
            "Expression Operators",
            op_name,
            neo_results[op_name],
            mongo_results.get(op_name) if mongo_results else None,
            skip_reason="MongoDB not available" if not client else None,
        )
