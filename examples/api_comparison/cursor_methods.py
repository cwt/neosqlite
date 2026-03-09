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

        # Test comment()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}}).comment(
                "test comment"
            )
            results = list(cursor)
            neo_comment = (
                len(results) >= 0 and cursor._comment == "test comment"
            )
            print(
                f"Neo comment(): {'OK' if neo_comment else 'FAIL'} ({len(results)} results)"
            )
        except Exception as e:
            neo_comment = False
            print(f"Neo comment(): Error - {e}")

        # Test retrieved property
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            results = list(cursor)
            neo_retrieved = cursor.retrieved == len(results)
            print(
                f"Neo retrieved: {'OK' if neo_retrieved else 'FAIL'} ({cursor.retrieved} docs)"
            )
        except Exception as e:
            neo_retrieved = False
            print(f"Neo retrieved: Error - {e}")

        # Test alive property
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            neo_alive_initial = cursor.alive is True
            list(cursor)
            neo_alive_after = isinstance(cursor.alive, bool)
            neo_alive = neo_alive_initial and neo_alive_after
            print(f"Neo alive: {'OK' if neo_alive else 'FAIL'}")
        except Exception as e:
            neo_alive = False
            print(f"Neo alive: Error - {e}")

        # Test collection property
        try:
            cursor = neo_collection.find({})
            neo_collection_prop = cursor.collection is neo_collection
            print(f"Neo collection: {'OK' if neo_collection_prop else 'FAIL'}")
        except Exception as e:
            neo_collection_prop = False
            print(f"Neo collection: Error - {e}")

        # Test address property
        try:
            cursor = neo_collection.find({})
            # Before iteration, should be None (matching PyMongo)
            neo_address_before = cursor.address is None
            list(cursor)
            # After iteration, should be tuple with database path
            neo_address_after = (
                isinstance(cursor.address, tuple)
                and len(cursor.address) == 2
                and cursor.address[0].startswith("sqlite:")
                and cursor.address[1] == 0
            )
            neo_address = neo_address_before and neo_address_after
            print(
                f"Neo address: {'OK' if neo_address else 'FAIL'} (before=None, after={cursor.address})"
            )
        except Exception as e:
            neo_address = False
            print(f"Neo address: Error - {e}")

        # Test min() method
        try:
            neo_collection.delete_many({})
            neo_collection.insert_many([{"value": i} for i in range(20)])
            cursor = neo_collection.find({}).min({"value": 10})
            results = list(cursor)
            neo_min = len(results) == 10 and all(
                doc["value"] >= 10 for doc in results
            )
            print(
                f"Neo min(): {'OK' if neo_min else 'FAIL'} ({len(results)} results)"
            )
        except Exception as e:
            neo_min = False
            print(f"Neo min(): Error - {e}")

        # Test max() method
        try:
            cursor = neo_collection.find({}).max({"value": 10})
            results = list(cursor)
            neo_max = len(results) == 10 and all(
                doc["value"] < 10 for doc in results
            )
            print(
                f"Neo max(): {'OK' if neo_max else 'FAIL'} ({len(results)} results)"
            )
        except Exception as e:
            neo_max = False
            print(f"Neo max(): Error - {e}")

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
            [{"name": f"Doc{i}", "value": i} for i in range(20)]
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

        # Test comment()
        try:
            cursor = mongo_collection.find({"value": {"$gte": 5}}).comment(
                "test comment"
            )
            results = list(cursor)
            mongo_comment = len(results) >= 0
            print(
                f"Mongo comment(): {'OK' if mongo_comment else 'FAIL'} ({len(results)} results)"
            )
        except Exception as e:
            mongo_comment = False
            print(f"Mongo comment(): Error - {e}")

        # Test retrieved property
        try:
            cursor = mongo_collection.find({"value": {"$gte": 5}})
            results = list(cursor)
            mongo_retrieved = cursor.retrieved == len(results)
            print(
                f"Mongo retrieved: {'OK' if mongo_retrieved else 'FAIL'} ({cursor.retrieved} docs)"
            )
        except Exception as e:
            mongo_retrieved = False
            print(f"Mongo retrieved: Error - {e}")

        # Test alive property
        try:
            cursor = mongo_collection.find({"value": {"$gte": 5}})
            mongo_alive_initial = cursor.alive is True
            list(cursor)
            mongo_alive_after = isinstance(cursor.alive, bool)
            mongo_alive = mongo_alive_initial and mongo_alive_after
            print(f"Mongo alive: {'OK' if mongo_alive else 'FAIL'}")
        except Exception as e:
            mongo_alive = False
            print(f"Mongo alive: Error - {e}")

        # Test collection property
        try:
            cursor = mongo_collection.find({})
            mongo_collection_prop = cursor.collection is mongo_collection
            print(
                f"Mongo collection: {'OK' if mongo_collection_prop else 'FAIL'}"
            )
        except Exception as e:
            mongo_collection_prop = False
            print(f"Mongo collection: Error - {e}")

        # Test address property
        try:
            cursor = mongo_collection.find({})
            # Before iteration, should be None
            mongo_address_before = cursor.address is None
            list(cursor)
            # After iteration, should be tuple (host, port)
            mongo_address_after = (
                isinstance(cursor.address, tuple) and len(cursor.address) == 2
            )
            mongo_address = mongo_address_before and mongo_address_after
            print(
                f"Mongo address: {'OK' if mongo_address else 'FAIL'} (before=None, after={cursor.address})"
            )
        except Exception as e:
            mongo_address = False
            print(f"Mongo address: Error - {e}")

        # Test min() method
        # Note: MongoDB min() takes a list of (field, value) tuples and requires hint()
        try:
            cursor = (
                mongo_collection.find({}).hint("value_1").min([("value", 10)])
            )
            results = list(cursor)
            mongo_min = len(results) == 10 and all(
                doc["value"] >= 10 for doc in results
            )
            print(
                f"Mongo min(): {'OK' if mongo_min else 'FAIL'} ({len(results)} results)"
            )
        except Exception as e:
            mongo_min = False
            print(f"Mongo min(): Error - {e}")

        # Test max() method
        # Note: MongoDB max() takes a list of (field, value) tuples and requires hint()
        try:
            cursor = (
                mongo_collection.find({}).hint("value_1").max([("value", 10)])
            )
            results = list(cursor)
            mongo_max = len(results) == 10 and all(
                doc["value"] < 10 for doc in results
            )
            print(
                f"Mongo max(): {'OK' if mongo_max else 'FAIL'} ({len(results)} results)"
            )
        except Exception as e:
            mongo_max = False
            print(f"Mongo max(): Error - {e}")

        # Test collation() method
        # Note: MongoDB collation requires server support and specific locale format
        try:
            cursor = neo_collection.find({}).collation(
                {"locale": "en_US", "strength": 2}
            )
            neo_collation = cursor._collation == {
                "locale": "en_US",
                "strength": 2,
            }
            print(f"Neo collation(): {'OK' if neo_collation else 'FAIL'}")
        except Exception as e:
            neo_collation = False
            print(f"Neo collation(): Error - {e}")

        try:
            from pymongo.collation import Collation

            cursor = mongo_collection.find({}).collation(
                Collation(locale="en_US", strength=2)
            )
            # Just verify the cursor accepts collation without error
            mongo_collation = cursor is not None
            print(f"Mongo collation(): {'OK' if mongo_collation else 'FAIL'}")
        except Exception as e:
            mongo_collation = False
            print(f"Mongo collation(): Error - {e}")

        # Test where() method (Python function filter)
        # Note: MongoDB $where uses JavaScript, NeoSQLite uses Python functions
        # This is a NeoSQLite-specific implementation, not directly comparable
        try:
            # Just verify the method exists and accepts a callable
            cursor = neo_collection.find({}).where(lambda doc: True)
            print("Neo where(): OK (NeoSQLite-specific, Python filter)")
        except Exception as e:
            print(f"Neo where(): Error - {e}")

        client.close()

        reporter.record_comparison(
            "Cursor Methods",
            "chained_methods",
            neo_cursor_methods if neo_cursor_methods else "FAIL",
            mongo_cursor_methods if mongo_cursor_methods else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "batch_size",
            neo_batch_size if neo_batch_size else "FAIL",
            mongo_batch_size if mongo_batch_size else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "hint",
            neo_hint if neo_hint else "FAIL",
            mongo_hint if mongo_hint else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "to_list",
            neo_to_list if neo_to_list else "FAIL",
            mongo_to_list if mongo_to_list else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "to_list_length",
            neo_to_list_length if neo_to_list_length else "FAIL",
            mongo_to_list_length if mongo_to_list_length else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "clone",
            neo_clone if neo_clone else "FAIL",
            mongo_clone if mongo_clone else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "explain",
            neo_explain if neo_explain else "FAIL",
            mongo_explain if mongo_explain else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "comment",
            neo_comment if neo_comment else "FAIL",
            mongo_comment if mongo_comment else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "retrieved",
            neo_retrieved if neo_retrieved else "FAIL",
            mongo_retrieved if mongo_retrieved else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "alive",
            neo_alive if neo_alive else "FAIL",
            mongo_alive if mongo_alive else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "collection",
            neo_collection_prop if neo_collection_prop else "FAIL",
            mongo_collection_prop if mongo_collection_prop else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "address",
            neo_address if neo_address else "FAIL",
            mongo_address if mongo_address else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "min",
            neo_min if neo_min else "FAIL",
            mongo_min if mongo_min else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "max",
            neo_max if neo_max else "FAIL",
            mongo_max if mongo_max else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "collation",
            neo_collation if neo_collation else "FAIL",
            mongo_collation if mongo_collation else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Cursor Methods",
            "where",
            "NeoSQLite-specific (Python filter)",
            None,
            skip_reason="NeoSQLite uses Python function filter; MongoDB uses JavaScript $where",
        )
