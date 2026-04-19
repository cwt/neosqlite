"""Module for comparing update operators between NeoSQLite and PyMongo"""

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
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_update_operators():
    """Compare update operators between NeoSQLite and PyMongo"""
    print("\n=== Update Operators Comparison ===")

    # Check MongoDB availability FIRST to determine if we should time operations
    client = get_mongo_client()
    mongo_available = client is not None

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

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one(
            {"name": "Alice", "age": 30, "score": 100, "tags": ["a"]}
        )

        set_accumulation_mode(True)
        for update, op_name in update_ops:
            # Reset document to initial state (not timed)
            neo_collection.update_one(
                {},
                {
                    "$set": {"name": "Alice", "age": 30, "score": 100},
                    "$unset": {"fullName": "", "created": ""},
                },
            )

            # ONLY time if MongoDB is available for comparison
            if mongo_available:
                start_neo_timing()
            try:
                try:
                    result = neo_collection.update_one({}, update)
                    neo_results[op_name] = (
                        "OK" if result.modified_count >= 0 else "FAIL"
                    )
                    print(f"Neo {op_name}: {neo_results[op_name]}")
                except Exception as e:
                    neo_results[op_name] = f"Error: {e}"
                    print(f"Neo {op_name}: {neo_results[op_name]}")
            finally:
                if mongo_available:
                    end_neo_timing()

    mongo_results = {}

    if mongo_available:
        client = get_mongo_client()
        if client:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            mongo_collection.insert_one(
                {"name": "Alice", "age": 30, "score": 100, "tags": ["a"]}
            )

            set_accumulation_mode(True)
            for update, op_name in update_ops:
                # Reset document to initial state (not timed)
                mongo_collection.update_one(
                    {},
                    {
                        "$set": {"name": "Alice", "age": 30, "score": 100},
                        "$unset": {"fullName": "", "created": ""},
                    },
                )

                start_mongo_timing()
                try:
                    try:
                        result = mongo_collection.update_one({}, update)
                        mongo_results[op_name] = (
                            "OK" if result.modified_count >= 0 else "FAIL"
                        )
                        print(f"Mongo {op_name}: {mongo_results[op_name]}")
                    except Exception as e:
                        mongo_results[op_name] = f"Error: {e}"
                        print(f"Mongo {op_name}: {mongo_results[op_name]}")
                finally:
                    end_mongo_timing()

            for op_name in neo_results:
                reporter.record_comparison(
                    "Update (Field Operators)",
                    op_name,
                    neo_results[op_name],
                    mongo_results.get(op_name),
                    skip_reason=None,
                )
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for op_name in neo_results:
            reporter.record_comparison(
                "Update (Field Operators)",
                op_name,
                neo_results[op_name],
                None,
                skip_reason="MongoDB not available",
            )
