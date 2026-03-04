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

        # Test to_list()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            results = cursor.to_list()
            neo_to_list = len(results) >= 0
            print(f"Neo to_list(): {len(results)} documents")
        except Exception as e:
            neo_to_list = False
            print(f"Neo to_list(): Error - {e}")

        # Test to_list() with length
        try:
            cursor = neo_collection.find({})
            results = cursor.to_list(3)
            neo_to_list_length = len(results) == 3
            print(f"Neo to_list(3): {len(results)} documents")
        except Exception as e:
            neo_to_list_length = False
            print(f"Neo to_list(3): Error - {e}")

        # Test clone()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}}).limit(3)
            cloned = cursor.clone()
            results_original = list(cursor)
            results_clone = list(cloned)
            neo_clone = len(results_original) == len(results_clone)
            print(
                f"Neo clone(): {len(results_clone)} documents (original: {len(results_original)})"
            )
        except Exception as e:
            neo_clone = False
            print(f"Neo clone(): Error - {e}")

        # Test explain()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            plan = cursor.explain()
            neo_explain = "queryPlanner" in plan
            print(
                f"Neo explain(): {'OK' if neo_explain else 'FAIL'} (plan has {len(plan.get('queryPlanner', {}).get('winningPlan', []))} stages)"
            )
        except Exception as e:
            neo_explain = False
            print(f"Neo explain(): Error - {e}")

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

        # Test to_list()
        try:
            cursor = mongo_collection.find({"value": {"$gte": 5}})
            results = cursor.to_list()
            mongo_to_list = len(results) >= 0
            print(f"Mongo to_list(): {len(results)} documents")
        except Exception as e:
            mongo_to_list = False
            print(f"Mongo to_list(): Error - {e}")

        # Test to_list() with length
        try:
            cursor = mongo_collection.find({})
            results = cursor.to_list(3)
            mongo_to_list_length = len(results) == 3
            print(f"Mongo to_list(3): {len(results)} documents")
        except Exception as e:
            mongo_to_list_length = False
            print(f"Mongo to_list(3): Error - {e}")

        # Test clone()
        try:
            cursor = mongo_collection.find({"value": {"$gte": 5}}).limit(3)
            cloned = cursor.clone()
            results_original = list(cursor)
            results_clone = list(cloned)
            mongo_clone = len(results_original) == len(results_clone)
            print(
                f"Mongo clone(): {len(results_clone)} documents (original: {len(results_original)})"
            )
        except Exception as e:
            mongo_clone = False
            print(f"Mongo clone(): Error - {e}")

        # Test explain()
        try:
            cursor = mongo_collection.find({"value": {"$gte": 5}})
            plan = cursor.explain()
            mongo_explain = "queryPlanner" in plan
            print(
                f"Mongo explain(): {'OK' if mongo_explain else 'FAIL'} (plan has {len(plan.get('queryPlanner', {}).get('winningPlan', []))} stages)"
            )
        except Exception as e:
            mongo_explain = False
            print(f"Mongo explain(): Error - {e}")

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
        reporter.record_result(
            "Cursor Methods",
            "to_list",
            neo_to_list,
            neo_to_list,
            mongo_to_list,
        )
        reporter.record_result(
            "Cursor Methods",
            "to_list_length",
            neo_to_list_length,
            neo_to_list_length,
            mongo_to_list_length,
        )
        reporter.record_result(
            "Cursor Methods",
            "clone",
            neo_clone,
            neo_clone,
            mongo_clone,
        )
        reporter.record_result(
            "Cursor Methods",
            "explain",
            neo_explain,
            neo_explain,
            mongo_explain,
        )
