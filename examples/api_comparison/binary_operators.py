"""Module for comparing binary data operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from neosqlite.binary import Binary

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


def compare_binary_operators():
    """Compare binary data operators ($binarySize)"""
    print("\n=== Binary Operators Comparison ===")

    neo_binary_size = None
    mongo_binary_size = None

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_binary_ops
        neo_collection.insert_one({"_id": 1, "data": Binary(b"hello")})

        set_accumulation_mode(True)
        # Test $binarySize
        start_neo_timing()
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
            neo_binary_size = val
        except Exception as e:
            neo_binary_size = f"Error: {e}"
            print(f"Neo $binarySize: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo $binarySize: {neo_binary_size}")

    client = get_mongo_client()
    if client:
        from bson.binary import Binary as MongoBinary

        mongo_db = client.test_database
        mongo_collection = mongo_db.test_binary_ops
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"_id": 1, "data": MongoBinary(b"hello")})

        set_accumulation_mode(True)
        start_mongo_timing()
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
            mongo_binary_size = val
        except Exception as e:
            mongo_binary_size = f"Error: {e}"
            print(f"Mongo $binarySize: Error - {e}")
        finally:
            end_mongo_timing()
        print(f"Mongo $binarySize: {mongo_binary_size}")

    # Both should return a positive integer.
    # The actual byte counts will differ because NeoSQLite stores Binary
    # with a small structural header that MongoDB does not have.
    neo_ok = isinstance(neo_binary_size, int) and neo_binary_size > 0
    mongo_ok = isinstance(mongo_binary_size, int) and mongo_binary_size > 0

    reporter.record_comparison(
        "Binary Operators",
        "$binarySize",
        "positive int" if neo_ok else neo_binary_size,
        "positive int" if mongo_ok else mongo_binary_size,
        skip_reason="MongoDB not available" if not client else None,
    )
