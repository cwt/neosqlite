"""Module for comparing object/document operators between NeoSQLite and PyMongo"""

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


def compare_object_operators_extended():
    """Compare object/document operators ($mergeObjects, $bsonSize)"""
    print("\n=== Object Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_object_ops
        neo_collection.insert_one(
            {
                "_id": 1,
                "obj1": {"a": 1, "b": 2},
                "obj2": {"b": 3, "c": 4},
            }
        )

        set_accumulation_mode(True)
        neo_results = {}

        # Test $mergeObjects
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$obj1", "$obj2"]}
                            }
                        },
                    ]
                )
            )
            end_neo_timing()

            neo_results["$mergeObjects"] = (
                result[0].get("merged") if result else None
            )
            print(f"Neo $mergeObjects: {neo_results['$mergeObjects']}")
        except Exception as e:
            neo_results["$mergeObjects"] = f"Error: {e}"
            print(f"Neo $mergeObjects: Error - {e}")

        # Test $bsonSize
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"size": {"$bsonSize": "$$ROOT"}}},
                    ]
                )
            )
            end_neo_timing()

            val = result[0].get("size") if result else None
            neo_results["$bsonSize"] = (
                "valid" if isinstance(val, int) and val > 0 else "invalid"
            )
            print(f"Neo $bsonSize: {val} bytes")
        except Exception as e:
            neo_results["$bsonSize"] = f"Error: {e}"
            print(f"Neo $bsonSize: Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_object_ops
        mongo_collection.delete_many({})
        mongo_collection.insert_one(
            {
                "_id": 1,
                "obj1": {"a": 1, "b": 2},
                "obj2": {"b": 3, "c": 4},
            }
        )

        set_accumulation_mode(True)
        # Test $mergeObjects
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$obj1", "$obj2"]}
                            }
                        },
                    ]
                )
            )
            end_mongo_timing()

            mongo_results["$mergeObjects"] = (
                result[0].get("merged") if result else None
            )
            print(f"Mongo $mergeObjects: {mongo_results['$mergeObjects']}")
        except Exception as e:
            mongo_results["$mergeObjects"] = f"Error: {e}"
            print(f"Mongo $mergeObjects: Error - {e}")

        # Test $bsonSize
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"size": {"$bsonSize": "$$ROOT"}}},
                    ]
                )
            )
            end_mongo_timing()

            val = result[0].get("size") if result else None
            mongo_results["$bsonSize"] = (
                "valid" if isinstance(val, int) and val > 0 else "invalid"
            )
            print(f"Mongo $bsonSize: {val} bytes")
        except Exception as e:
            mongo_results["$bsonSize"] = f"Error: {e}"
            print(f"Mongo $bsonSize: Error - {e}")

        client.close()

    for op_name in neo_results:
        reporter.record_comparison(
            "Object Operators",
            op_name,
            neo_results[op_name],
            mongo_results.get(op_name) if mongo_results else None,
            skip_reason="MongoDB not available" if not client else None,
        )
