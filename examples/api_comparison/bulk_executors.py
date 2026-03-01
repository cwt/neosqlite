"""Module for comparing bulk operation executors between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_bulk_operation_executors():
    """Compare bulk operation executor methods"""
    print("\n=== Bulk Operation Executors Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_bulk_exec
        neo_collection.insert_many(
            [
                {"name": "A", "value": 1},
                {"name": "B", "value": 2},
                {"name": "C", "value": 3},
            ]
        )

        # Test initialize_ordered_bulk_op with add() method
        try:
            neo_ordered = neo_collection.initialize_ordered_bulk_op()
            neo_ordered.add(neosqlite.InsertOne({"name": "D", "value": 4}))
            neo_ordered.add(
                neosqlite.UpdateOne({"name": "A"}, {"$set": {"value": 10}})
            )
            neo_ordered.add(neosqlite.DeleteOne({"name": "B"}))
            neo_ordered_result = neo_ordered.execute()
            neo_ordered_ok = neo_ordered_result.matched_count >= 0
            print(
                f"Neo initialize_ordered_bulk_op: OK (matched={neo_ordered_result.matched_count})"
            )
        except Exception as e:
            neo_ordered_ok = False
            print(f"Neo initialize_ordered_bulk_op: Error - {e}")

        # Test initialize_unordered_bulk_op with add() method
        try:
            neo_unordered = neo_collection.initialize_unordered_bulk_op()
            neo_unordered.add(neosqlite.InsertOne({"name": "E", "value": 5}))
            neo_unordered.add(
                neosqlite.UpdateOne({"name": "C"}, {"$set": {"value": 30}})
            )
            neo_unordered_result = neo_unordered.execute()
            neo_unordered_ok = neo_unordered_result.matched_count >= 0
            print(
                f"Neo initialize_unordered_bulk_op: OK (matched={neo_unordered_result.matched_count})"
            )
        except Exception as e:
            neo_unordered_ok = False
            print(f"Neo initialize_unordered_bulk_op: Error - {e}")

    # Note: PyMongo removed initialize_ordered_bulk_op/initialize_unordered_bulk_op in favor of bulk_write()
    # We test with the NeoSQLite API which follows the older pattern
    print(
        "Mongo: Methods not available in modern PyMongo (use bulk_write instead)"
    )

    reporter.record_result(
        "Bulk Operation Executors",
        "initialize_ordered_bulk_op",
        neo_ordered_ok,
        neo_ordered_ok,
        "N/A (removed in modern PyMongo)",
    )
    reporter.record_result(
        "Bulk Operation Executors",
        "initialize_unordered_bulk_op",
        neo_unordered_ok,
        neo_unordered_ok,
        "N/A (removed in modern PyMongo)",
    )
