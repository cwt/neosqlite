"""Module for comparing raw batch operations between NeoSQLite and PyMongo"""

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


def compare_raw_batch_operations():
    """Compare raw batch operations"""
    print("\n=== Raw Batch Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": f"doc{i}", "value": i} for i in range(10)]
        )

        try:
            cursor = neo_collection.find_raw_batches(
                {"value": {"$gte": 5}}, batch_size=3
            )
            neo_raw_batches = sum(1 for _ in cursor)
            print(f"Neo find_raw_batches: {neo_raw_batches} batches")
        except Exception as e:
            neo_raw_batches = f"Error: {e}"
            print(f"Neo find_raw_batches: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_raw_batches = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": f"doc{i}", "value": i} for i in range(10)]
        )

        try:
            cursor = mongo_collection.find_raw_batches(
                {"value": {"$gte": 5}}, batch_size=3
            )
            mongo_raw_batches = sum(1 for _ in cursor)
            print(f"Mongo find_raw_batches: {mongo_raw_batches} batches")
        except Exception as e:
            mongo_raw_batches = f"Error: {e}"
            print(f"Mongo find_raw_batches: Error - {e}")
        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Raw Batch Operations",
        "find_raw_batches",
        (
            neo_raw_batches
            if not isinstance(neo_raw_batches, str)
            else neo_raw_batches
        ),
        mongo_raw_batches if mongo_raw_batches is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
