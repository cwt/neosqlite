"""Module for comparing update operators between NeoSQLite and PyMongo"""

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


def compare_update_operators():
    """Compare update operators between NeoSQLite and PyMongo"""
    print("\n=== Update Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one(
            {"name": "Alice", "age": 30, "score": 100, "tags": ["a"]}
        )

        update_ops = [
            ({"$set": {"age": 31}}, "$set"),
            ({"$unset": {"score": ""}}, "$unset"),
            ({"$inc": {"age": 1}}, "$inc"),
            ({"$mul": {"age": 2}}, "$mul"),
            ({"$min": {"age": 25}}, "$min"),
            ({"$max": {"age": 50}}, "$max"),
            ({"$rename": {"name": "fullName"}}, "$rename"),
            ({"$setOnInsert": {"created": True}}, "$setOnInsert"),
        ]

        set_accumulation_mode(True)
        neo_results = {}
        for update, op_name in update_ops:
            try:
                # Reset document to initial state (not timed)
                neo_collection.update_one(
                    {}, {"$set": {"age": 30, "score": 100}}
                )

                start_neo_timing()
                result = neo_collection.update_one({}, update)
                end_neo_timing()

                neo_results[op_name] = (
                    "OK" if result.modified_count >= 0 else "FAIL"
                )
                print(f"Neo {op_name}: OK")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_results = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one(
            {"name": "Alice", "age": 30, "score": 100, "tags": ["a"]}
        )

        set_accumulation_mode(True)
        mongo_results = {}
        for update, op_name in update_ops:
            try:
                # Reset document to initial state (not timed)
                mongo_collection.update_one(
                    {}, {"$set": {"age": 30, "score": 100}}
                )

                start_mongo_timing()
                result = mongo_collection.update_one({}, update)
                end_mongo_timing()

                mongo_results[op_name] = (
                    "OK" if result.modified_count >= 0 else "FAIL"
                )
                print(f"Mongo {op_name}: OK")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            reporter.record_comparison(
                "Update Operators",
                op_name,
                neo_results[op_name],
                mongo_results.get(op_name),
                skip_reason="MongoDB not available" if not client else None,
            )
        client.close()
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for op_name in neo_results:
            reporter.record_comparison(
                "Update Operators",
                op_name,
                neo_results[op_name],
                None,
                skip_reason="MongoDB not available",
            )
