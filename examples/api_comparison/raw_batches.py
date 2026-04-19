"""Module for comparing raw batch operations between NeoSQLite and PyMongo"""

import os
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

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_raw_batch_operations():
    """Compare raw batch operations"""
    print("\n=== Raw Batch Operations Comparison ===")

    neo_raw_batches = None
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": f"doc{i}", "value": i} for i in range(10)]
        )

        set_accumulation_mode(True)
        # Test NeoSQLite find_raw_batches
        start_neo_timing()
        try:
            cursor = neo_collection.find_raw_batches(
                {"value": {"$gte": 5}}, batch_size=3
            )
            neo_raw_batches = sum(1 for _ in cursor)
            print(f"Neo find_raw_batches: {neo_raw_batches} batches")
        except Exception as e:
            neo_raw_batches = f"Error: {e}"
            print(f"Neo find_raw_batches: Error - {e}")
        finally:
            end_neo_timing()

    client = get_mongo_client()
    mongo_raw_batches = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": f"doc{i}", "value": i} for i in range(10)]
        )

        set_accumulation_mode(True)
        # Test MongoDB find_raw_batches
        start_mongo_timing()
        try:
            cursor = mongo_collection.find_raw_batches(
                {"value": {"$gte": 5}}, batch_size=3
            )
            mongo_raw_batches = sum(1 for _ in cursor)
            print(f"Mongo find_raw_batches: {mongo_raw_batches} batches")
        except Exception as e:
            mongo_raw_batches = f"Error: {e}"
            print(f"Mongo find_raw_batches: Error - {e}")
        finally:
            end_mongo_timing()

    if IS_NX27017_BACKEND:
        skip_reason = (
            "NX-27017 does not implement batch_size for find_raw_batches"
        )
    elif not client:
        skip_reason = "MongoDB not available"
    else:
        skip_reason = None

    reporter.record_result(
        "Raw Batches",
        "find_raw_batches",
        passed=(
            neo_raw_batches == mongo_raw_batches
            if mongo_raw_batches is not None and skip_reason is None
            else True
        ),
        neo_result=(
            neo_raw_batches
            if not isinstance(neo_raw_batches, str)
            else neo_raw_batches
        ),
        mongo_result=(
            mongo_raw_batches if mongo_raw_batches is not None else None
        ),
        skip_reason=skip_reason,
    )
