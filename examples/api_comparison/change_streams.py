"""Module for comparing change streams between NeoSQLite and PyMongo"""

import os
import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    set_accumulation_mode,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

# Check if we're running against NX-27017 (NeoSQLite backend)
IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_change_streams():
    """Compare change streams (watch)"""
    print("\n=== Change Streams (watch) Comparison ===")

    # Import benchmark_reporter to mark MongoDB as skipped in benchmark mode
    from .reporter import benchmark_reporter

    neo_watch_ok = False
    mongo_watch_ok = False

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"name": "test"})

        set_accumulation_mode(True)
        try:
            _ = neo_collection.watch()
            neo_watch_ok = True
            print("Neo watch: Supported")
        except Exception as e:
            print(f"Neo watch: Error - {e}")

    client = test_pymongo_connection()

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"name": "test"})

        set_accumulation_mode(True)
        try:
            _ = mongo_collection.watch()
            mongo_watch_ok = True
            print("Mongo watch: Supported")
        except Exception as e:
            print(f"Mongo watch: Error - {e} (requires replica set)")

        client.close()

    # Mark MongoDB as skipped in benchmark mode when not on replica set
    # NX-27017 backend: Both NeoSQLite and NX-27017 support change streams
    # Real MongoDB standalone: Skip because replica set not available
    if not IS_NX27017_BACKEND and not mongo_watch_ok:
        if benchmark_reporter:
            benchmark_reporter.mark_mongo_skipped(
                "Change Streams",
                "Requires MongoDB replica set; NeoSQLite uses SQLite triggers",
            )

    # Record the comparison
    # For benchmark mode with MongoDB standalone, mark MongoDB as skipped
    if not IS_NX27017_BACKEND and not mongo_watch_ok:
        reporter.record_comparison(
            "Change Streams",
            "watch",
            neo_results="OK" if neo_watch_ok else "FAIL",
            mongo_results=None,  # Mark as skipped
            skip_reason="Requires MongoDB replica set; NeoSQLite uses SQLite triggers",
        )
    else:
        # API comparison mode or both supported
        reporter.record_comparison(
            "Change Streams",
            "watch",
            neo_results="OK" if neo_watch_ok else "FAIL",
            mongo_results="OK" if mongo_watch_ok else "FAIL",
            skip_reason=None,
        )
