"""Module for comparing find and modify operations between NeoSQLite and PyMongo"""

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


def compare_find_and_modify():
    """Compare find and modify operations"""
    print("\n=== Find and Modify Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        )

        set_accumulation_mode(True)
        # Test find_one_and_delete
        start_neo_timing()
        try:
            doc = neo_collection.find_one_and_delete({"name": "Bob"})
        finally:

            end_neo_timing()
        neo_foad = doc is not None

        # Reset for next test (not timed)
        neo_collection.insert_one({"name": "Bob", "age": 25})

        # Test find_one_and_replace
        start_neo_timing()
        try:
            doc = neo_collection.find_one_and_replace(
                {"name": "Alice"}, {"name": "Alice Smith", "age": 31}
            )
        finally:

            end_neo_timing()
        neo_foar = doc is not None

        # Test find_one_and_update
        start_neo_timing()
        try:
            doc = neo_collection.find_one_and_update(
                {"name": "Alice Smith"}, {"$inc": {"age": 1}}
            )
        finally:

            end_neo_timing()
        neo_foau = doc is not None

        print(
            f"NeoSQLite: find_one_and_delete={neo_foad}, find_one_and_replace={neo_foar}, find_one_and_update={neo_foau}"
        )

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None
    mongo_db = None
    mongo_foad = None
    mongo_foar = None
    mongo_foau = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        )

        set_accumulation_mode(True)
        # Test find_one_and_delete
        start_mongo_timing()
        try:
            doc = mongo_collection.find_one_and_delete({"name": "Bob"})
        finally:

            end_mongo_timing()
        mongo_foad = doc is not None

        # Reset for next test
        mongo_collection.insert_one({"name": "Bob", "age": 25})

        # Test find_one_and_replace
        start_mongo_timing()
        try:
            doc = mongo_collection.find_one_and_replace(
                {"name": "Alice"}, {"name": "Alice Smith", "age": 31}
            )
        finally:

            end_mongo_timing()
        mongo_foar = doc is not None

        # Test find_one_and_update
        start_mongo_timing()
        try:
            doc = mongo_collection.find_one_and_update(
                {"name": "Alice Smith"}, {"$inc": {"age": 1}}
            )
        finally:

            end_mongo_timing()
        mongo_foau = doc is not None

        print(
            f"PyMongo: find_one_and_delete={mongo_foad}, find_one_and_replace={mongo_foar}, find_one_and_update={mongo_foau}"
        )
        client.close()

    reporter.record_comparison(
        "Find and Modify",
        "find_one_and_delete",
        neo_foad if neo_foad else "FAIL",
        mongo_foad if mongo_foad else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Find and Modify",
        "find_one_and_replace",
        neo_foar if neo_foar else "FAIL",
        mongo_foar if mongo_foar else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Find and Modify",
        "find_one_and_update",
        neo_foau if neo_foau else "FAIL",
        mongo_foau if mongo_foau else None,
        skip_reason="MongoDB not available" if not client else None,
    )
