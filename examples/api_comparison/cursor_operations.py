"""Module for comparing cursor operations between NeoSQLite and PyMongo"""

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


def compare_cursor_operations():
    """Compare cursor operations"""
    print("\n=== Cursor Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([{"i": i} for i in range(10)])

        set_accumulation_mode(True)
        # Test cursor iteration
        start_neo_timing()
        cursor = neo_collection.find()
        neo_count = sum(1 for _ in cursor)
        end_neo_timing()

        # Test limit
        start_neo_timing()
        neo_limit = len(list(neo_collection.find().limit(5)))
        end_neo_timing()

        # Test skip
        start_neo_timing()
        neo_skip = len(list(neo_collection.find().skip(5)))
        end_neo_timing()

        # Test sort
        start_neo_timing()
        neo_sorted = list(neo_collection.find().sort("i", neosqlite.DESCENDING))
        end_neo_timing()

        neo_sort_ok = neo_sorted[0]["i"] == 9 if neo_sorted else False

        print(
            f"Neo cursor: count={neo_count}, limit={neo_limit}, skip={neo_skip}, sort={neo_sort_ok}"
        )

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None
    mongo_count = None
    mongo_db = None
    mongo_limit = None
    mongo_skip = None
    mongo_sort_ok = None
    mongo_sorted = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many([{"i": i} for i in range(10)])

        set_accumulation_mode(True)

        start_mongo_timing()
        cursor = mongo_collection.find()
        mongo_count = sum(1 for _ in cursor)
        end_mongo_timing()

        start_mongo_timing()
        mongo_limit = len(list(mongo_collection.find().limit(5)))
        end_mongo_timing()

        start_mongo_timing()
        mongo_skip = len(list(mongo_collection.find().skip(5)))
        end_mongo_timing()

        start_mongo_timing()
        from pymongo import DESCENDING as MONGO_DESCENDING

        mongo_sorted = list(mongo_collection.find().sort("i", MONGO_DESCENDING))
        end_mongo_timing()

        mongo_sort_ok = mongo_sorted[0]["i"] == 9 if mongo_sorted else False

        print(
            f"Mongo cursor: count={mongo_count}, limit={mongo_limit}, skip={mongo_skip}, sort={mongo_sort_ok}"
        )
        client.close()

    reporter.record_comparison(
        "Cursor Operations",
        "iteration",
        neo_count,
        mongo_count if mongo_count is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Operations",
        "limit",
        neo_limit,
        mongo_limit if mongo_limit is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Operations",
        "skip",
        neo_skip,
        mongo_skip if mongo_skip is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Operations",
        "sort",
        neo_sorted,
        mongo_sorted if mongo_sorted is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
