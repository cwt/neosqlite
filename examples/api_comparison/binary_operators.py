"""Module for comparing binary data operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_binary_operators():
    """Compare binary data operators ($binarySize)"""
    print("\n=== Binary Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_binary_ops
        from neosqlite.binary import Binary

        neo_collection.insert_one({"_id": 1, "data": Binary(b"hello")})

        # Test $binarySize
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"size": {"$binarySize": "$data"}}},
                    ]
                )
            )
            val = result[0].get("size") if result else None
            neo_binary_size = (
                f"{val} bytes"
                if isinstance(val, int) and val > 0
                else "invalid"
            )
            print(f"Neo $binarySize: {neo_binary_size}")
        except Exception as e:
            neo_binary_size = f"Error: {e}"
            print(f"Neo $binarySize: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    mongo_binary_size = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_binary_ops
        mongo_collection.delete_many({})

        from bson.binary import Binary as MongoBinary

        mongo_collection.insert_one({"_id": 1, "data": MongoBinary(b"hello")})

        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"size": {"$binarySize": "$data"}}},
                    ]
                )
            )
            val = result[0].get("size") if result else None
            mongo_binary_size = (
                f"{val} bytes"
                if isinstance(val, int) and val > 0
                else "invalid"
            )
            print(f"Mongo $binarySize: {mongo_binary_size}")
        except Exception as e:
            mongo_binary_size = f"Error: {e}"
            print(f"Mongo $binarySize: Error - {e}")

        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Binary Operators",
        "$binarySize",
        neo_binary_size,
        mongo_binary_size,
        skip_reason="MongoDB not available" if not client else None,
    )
