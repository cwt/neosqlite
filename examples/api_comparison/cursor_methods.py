"""Module for comparing cursor methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_cursor_methods():
    """Compare cursor methods"""
    print("\n=== Cursor Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_cursor
        neo_collection.insert_many(
            [{"name": f"Doc{i}", "value": i} for i in range(10)]
        )
        neo_collection.create_index("value")

        # Test cursor with multiple methods chained
        try:
            cursor = (
                neo_collection.find({"value": {"$gte": 3}})
                .limit(5)
                .skip(1)
                .sort("value", neosqlite.DESCENDING)
            )
            results = list(cursor)
            neo_cursor_methods = len(results) <= 5
            print(f"Neo cursor chained methods: {len(results)} results")
        except Exception as e:
            neo_cursor_methods = False
            print(f"Neo cursor chained methods: Error - {e}")

        # Test batch_size
        try:
            cursor = neo_collection.find({}).batch_size(3)
            neo_batch_size = cursor is not None
            print(f"Neo batch_size: {'OK' if neo_batch_size else 'FAIL'}")
        except Exception as e:
            neo_batch_size = False
            print(f"Neo batch_size: Error - {e}")

        # Test hint
        try:
            cursor = neo_collection.find({"value": 5}).hint(
                "idx_test_cursor_value"
            )
            results = list(cursor)
            neo_hint = len(results) >= 0
            print(f"Neo hint: {'OK' if neo_hint else 'FAIL'}")
        except Exception as e:
            neo_hint = False
            print(f"Neo hint: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_batch_size = None

    mongo_collection = None

    mongo_cursor_methods = None

    mongo_db = None

    mongo_hint = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_cursor
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": f"Doc{i}", "value": i} for i in range(10)]
        )
        mongo_collection.create_index("value")

        # Test cursor with multiple methods chained
        try:
            from pymongo import DESCENDING as MONGO_DESCENDING

            cursor = (
                mongo_collection.find({"value": {"$gte": 3}})
                .limit(5)
                .skip(1)
                .sort("value", MONGO_DESCENDING)
            )
            results = list(cursor)
            mongo_cursor_methods = len(results) <= 5
            print(f"Mongo cursor chained methods: {len(results)} results")
        except Exception as e:
            mongo_cursor_methods = False
            print(f"Mongo cursor chained methods: Error - {e}")

        # Test batch_size
        try:
            cursor = mongo_collection.find({}).batch_size(3)
            mongo_batch_size = cursor is not None
            print(f"Mongo batch_size: {'OK' if mongo_batch_size else 'FAIL'}")
        except Exception as e:
            mongo_batch_size = False
            print(f"Mongo batch_size: Error - {e}")

        # Test hint
        try:
            cursor = mongo_collection.find({"value": 5}).hint("value_1")
            results = list(cursor)
            mongo_hint = len(results) >= 0
            print(f"Mongo hint: {'OK' if mongo_hint else 'FAIL'}")
        except Exception as e:
            mongo_hint = False
            print(f"Mongo hint: Error - {e}")

        client.close()

        reporter.record_result(
            "Cursor Methods",
            "chained_methods",
            neo_cursor_methods,
            neo_cursor_methods,
            mongo_cursor_methods,
        )
        reporter.record_result(
            "Cursor Methods",
            "batch_size",
            neo_batch_size,
            neo_batch_size,
            mongo_batch_size,
        )
        reporter.record_result(
            "Cursor Methods", "hint", neo_hint, neo_hint, mongo_hint
        )
