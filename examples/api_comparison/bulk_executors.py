"""Module for comparing bulk operation executors between NeoSQLite and PyMongo"""

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


def compare_bulk_operation_executors():
    """Compare bulk operation executor methods"""
    print("\n=== Bulk Operation Executors Comparison ===")

    neo_ordered_ok = False
    neo_unordered_ok = False

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_bulk_exec
        neo_collection.insert_many(
            [
                {"name": "A", "value": 1},
                {"name": "B", "value": 2},
                {"name": "C", "value": 3},
            ]
        )

        set_accumulation_mode(True)

        # Test initialize_ordered_bulk_op with add() method
        start_neo_timing()
        try:
            try:
                neo_ordered = neo_collection.initialize_ordered_bulk_op()
                neo_ordered.add(neosqlite.InsertOne({"name": "D", "value": 4}))
                neo_ordered.add(
                    neosqlite.UpdateOne({"name": "A"}, {"$set": {"value": 10}})
                )
                neo_ordered.add(neosqlite.DeleteOne({"name": "B"}))
                neo_ordered_result = neo_ordered.execute()

                neo_ordered_ok = (
                    neo_ordered_result is not None
                    and neo_ordered_result.matched_count >= 0
                )
                print(
                    f"Neo initialize_ordered_bulk_op: OK (matched={neo_ordered_result.matched_count})"
                )
            except Exception as e:
                print(f"Neo initialize_ordered_bulk_op: Error - {e}")
                neo_ordered_ok = False
        finally:
            end_neo_timing()

        # Test initialize_unordered_bulk_op with add() method
        start_neo_timing()
        try:
            try:
                neo_unordered = neo_collection.initialize_unordered_bulk_op()
                neo_unordered.add(
                    neosqlite.InsertOne({"name": "E", "value": 5})
                )
                neo_unordered.add(
                    neosqlite.UpdateOne({"name": "C"}, {"$set": {"value": 30}})
                )
                neo_unordered_result = neo_unordered.execute()

                neo_unordered_ok = (
                    neo_unordered_result is not None
                    and neo_unordered_result.matched_count >= 0
                )
                print(
                    f"Neo initialize_unordered_bulk_op: OK (matched={neo_unordered_result.matched_count})"
                )
            except Exception as e:
                print(f"Neo initialize_unordered_bulk_op: Error - {e}")
                neo_unordered_ok = False
        finally:
            end_neo_timing()

    # Note: The old initialize_ordered_bulk_op/initialize_unordered_bulk_op API
    # was deprecated in PyMongo 3.5 and completely removed in PyMongo 4.x.
    # Use bulk_write() instead. We skip this test since NeoSQLite supports
    # the old API for backward compatibility but PyMongo doesn't.
    print(
        "Mongo: initialize_ordered_bulk_op/initialize_unordered_bulk_op "
        "removed in PyMongo 4.x (use bulk_write instead)"
    )

    client = get_mongo_client()
    if client:
        from .reporter import benchmark_reporter

        start_mongo_timing()
        try:
            pass
        finally:
            end_mongo_timing()

        if benchmark_reporter:
            benchmark_reporter.mark_mongo_skipped(
                "Bulk Executors",
                "initialize_ordered/unordered_bulk_op removed in PyMongo 4.x (use bulk_write instead)",
            )

    reporter.record_result(
        "Bulk Executors",
        "initialize_ordered_bulk_op",
        neo_ordered_ok,
        neo_ordered_ok,
        None,
        skip_reason="Deprecated/removed in PyMongo 4.x (use bulk_write)",
    )
    reporter.record_result(
        "Bulk Executors",
        "initialize_unordered_bulk_op",
        neo_unordered_ok,
        neo_unordered_ok,
        None,
        skip_reason="Deprecated/removed in PyMongo 4.x (use bulk_write)",
    )
