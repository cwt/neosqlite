"""Module for comparing type checking operators between NeoSQLite and PyMongo"""

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


def compare_type_operators():
    """Compare type checking operators ($isNumber, etc.)"""
    print("\n=== Type Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_type_ops
        neo_collection.insert_one(
            {
                "_id": 1,
                "num": 42,
                "str": "hello",
                "arr": [1, 2, 3],
            }
        )

        set_accumulation_mode(True)
        # Test $isNumber
        neo_is_number = None
        try:
            start_neo_timing()
            try:
                result = list(
                    neo_collection.aggregate(
                        [
                            {"$match": {"_id": 1}},
                            {"$project": {"is_num": {"$isNumber": "$num"}}},
                        ]
                    )
                )
                neo_is_number = result[0].get("is_num") if result else None
                print(f"Neo $isNumber: {neo_is_number}")
            finally:
                end_neo_timing()
        except Exception as e:
            neo_is_number = f"Error: {e}"
            print(f"Neo $isNumber: Error - {e}")

    client = get_mongo_client()
    mongo_is_number = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_type_ops
        mongo_collection.delete_many({})
        mongo_collection.insert_one(
            {
                "_id": 1,
                "num": 42,
                "str": "hello",
                "arr": [1, 2, 3],
            }
        )

        set_accumulation_mode(True)
        try:
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate(
                        [
                            {"$match": {"_id": 1}},
                            {"$project": {"is_num": {"$isNumber": "$num"}}},
                        ]
                    )
                )
                mongo_is_number = result[0].get("is_num") if result else None
                print(f"Mongo $isNumber: {mongo_is_number}")
            finally:
                end_mongo_timing()
        except Exception as e:
            mongo_is_number = f"Error: {e}"
            print(f"Mongo $isNumber: Error - {e}")

    reporter.record_comparison(
        "Type Operators",
        "$isNumber",
        neo_is_number,
        mongo_is_number,
        skip_reason="MongoDB not available" if not client else None,
    )
