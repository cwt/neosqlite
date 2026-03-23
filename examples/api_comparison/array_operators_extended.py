"""Module for comparing array/set operators between NeoSQLite and PyMongo"""

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


def compare_array_operators_extended():
    """Compare array/set operators ($setIntersection, $firstN)"""
    print("\n=== Array Operators Comparison ===")

    # Initialize results
    neo_results = {}
    mongo_results = {}

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_array_ops
        neo_collection.insert_one(
            {
                "_id": 1,
                "arr": [1, 2, 3, 4, 5],
                "set1": [1, 2, 3],
                "set2": [3, 4, 5],
            }
        )

        set_accumulation_mode(True)

        # Test $firstN
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [
                            {"$match": {"_id": 1}},
                            {
                                "$project": {
                                    "first2": {
                                        "$firstN": {"input": "$arr", "n": 2}
                                    }
                                }
                            },
                        ]
                    )
                )
            except Exception as e:
                result = []
                neo_results["$firstN"] = f"Error: {e}"
                print(f"Neo $firstN: Error - {e}")
            finally:
                end_neo_timing()

            if "$firstN" not in neo_results:
                neo_results["$firstN"] = (
                    result[0].get("first2") if result else None
                )
            print(f"Neo $firstN: {neo_results['$firstN']}")
        except Exception:
            pass

        # Test $setIntersection
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [
                            {"$match": {"_id": 1}},
                            {
                                "$project": {
                                    "intersection": {
                                        "$setIntersection": ["$set1", "$set2"]
                                    }
                                }
                            },
                        ]
                    )
                )
            except Exception as e:
                result = []
                neo_results["$setIntersection"] = f"Error: {e}"
                print(f"Neo $setIntersection: Error - {e}")
            finally:
                end_neo_timing()

            if "$setIntersection" not in neo_results:
                neo_results["$setIntersection"] = (
                    result[0].get("intersection") if result else None
                )
            print(f"Neo $setIntersection: {neo_results['$setIntersection']}")
        except Exception:
            pass

    client = test_pymongo_connection()

    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_array_ops
            mongo_collection.delete_many({})
            mongo_collection.insert_one(
                {
                    "_id": 1,
                    "arr": [1, 2, 3, 4, 5],
                    "set1": [1, 2, 3],
                    "set2": [3, 4, 5],
                }
            )

            # Test $firstN
            try:
                start_mongo_timing()
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [
                                {"$match": {"_id": 1}},
                                {
                                    "$project": {
                                        "first2": {
                                            "$firstN": {"input": "$arr", "n": 2}
                                        }
                                    }
                                },
                            ]
                        )
                    )
                except Exception as e:
                    result = []
                    mongo_results["$firstN"] = f"Error: {e}"
                    print(f"Mongo $firstN: Error - {e}")
                finally:
                    end_mongo_timing()

                if "$firstN" not in mongo_results:
                    mongo_results["$firstN"] = (
                        result[0].get("first2") if result else None
                    )
                print(f"Mongo $firstN: {mongo_results['$firstN']}")
            except Exception:
                pass

            # Test $setIntersection
            try:
                start_mongo_timing()
                try:
                    result = list(
                        mongo_collection.aggregate(
                            [
                                {"$match": {"_id": 1}},
                                {
                                    "$project": {
                                        "intersection": {
                                            "$setIntersection": [
                                                "$set1",
                                                "$set2",
                                            ]
                                        }
                                    }
                                },
                            ]
                        )
                    )
                except Exception as e:
                    result = []
                    mongo_results["$setIntersection"] = f"Error: {e}"
                    print(f"Mongo $setIntersection: Error - {e}")
                finally:
                    end_mongo_timing()

                if "$setIntersection" not in mongo_results:
                    mongo_results["$setIntersection"] = (
                        result[0].get("intersection") if result else None
                    )
                print(
                    f"Mongo $setIntersection: {mongo_results['$setIntersection']}"
                )
            except Exception:
                pass
        finally:
            client.close()

    for op_name in ["$firstN", "$setIntersection"]:
        if op_name in neo_results:
            reporter.record_comparison(
                "Array Operators",
                op_name,
                neo_results[op_name],
                mongo_results.get(op_name) if mongo_results else None,
                skip_reason="MongoDB not available" if not client else None,
            )
