"""Module for comparing change streams between NeoSQLite and PyMongo"""

import os
import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    set_accumulation_mode,
)
from .utils import get_mongo_client

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

    client = get_mongo_client()

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

    # NeoSQLite-only hardening: resume tokens and $match pipeline filtering.
    # These need a replica set on the MongoDB side, so they are exercised
    # against NeoSQLite here and covered differentially in unit tests.
    neo_resume_ok = False
    neo_match_ok = False
    with neosqlite.Connection(":memory:") as neo_conn:
        jobs = neo_conn.jobs
        set_accumulation_mode(True)
        try:
            first = jobs.watch(max_await_time_ms=2000)
            jobs.insert_one({"status": "pending", "n": 1})
            jobs.insert_one({"status": "done", "n": 2})
            seen = next(first)
            token = first.resume_token
            first.close()
            resumed = jobs.watch(resume_after=token, max_await_time_ms=2000)
            replayed = next(resumed)
            resumed.close()
            neo_resume_ok = (
                token is not None
                and replayed["documentKey"] != seen["documentKey"]
            )
            print("Neo watch resume_after: Supported")
        except Exception as e:
            print(f"Neo watch resume_after: Error - {e}")

        try:
            filtered = jobs.watch(
                pipeline=[{"$match": {"fullDocument.status": "pending"}}],
                full_document="updateLookup",
                max_await_time_ms=2000,
            )
            jobs.insert_one({"status": "pending", "n": 3})
            jobs.insert_one({"status": "done", "n": 4})
            change = next(filtered)
            filtered.close()
            neo_match_ok = change["fullDocument"]["status"] == "pending"
            print("Neo watch $match pipeline: Supported")
        except Exception as e:
            print(f"Neo watch $match pipeline: Error - {e}")

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
    # When using real MongoDB standalone (not NX-27017): skip because no replica set
    # When using NX-27017 backend or real MongoDB replica set: compare actual results
    if not IS_NX27017_BACKEND and not mongo_watch_ok:
        reporter.record_comparison(
            "Change Streams",
            "watch",
            neo_results="OK" if neo_watch_ok else "FAIL",
            mongo_results=None,  # Mark as skipped
            skip_reason="Requires MongoDB replica set; NeoSQLite uses SQLite triggers",
        )
    else:
        # Compare actual results (NX-27017 backend or real MongoDB with replica set)
        reporter.record_comparison(
            "Change Streams",
            "watch",
            neo_results="OK" if neo_watch_ok else "FAIL",
            mongo_results="OK" if mongo_watch_ok else "FAIL",
            skip_reason=None,
        )

    # Resume tokens and pipeline filtering require a replica set on the
    # MongoDB side; NeoSQLite behavior is verified here and against
    # MongoDB semantics in unit tests.
    reporter.record_comparison(
        "Change Streams",
        "watch resume_after",
        neo_results="OK" if neo_resume_ok else "FAIL",
        mongo_results=None,
        skip_reason="Requires MongoDB replica set; covered in unit tests",
    )
    reporter.record_comparison(
        "Change Streams",
        "watch $match pipeline",
        neo_results="OK" if neo_match_ok else "FAIL",
        mongo_results=None,
        skip_reason="Requires MongoDB replica set; covered in unit tests",
    )
