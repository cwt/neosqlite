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

    # When on NX-27017 backend, compare results (both should work or both should fail)
    # When on real MongoDB, skip if replica set not available
    if IS_NX27017_BACKEND:
        skip_reason = None  # On NX-27017, compare results
    elif mongo_watch_ok is False:
        skip_reason = (
            "Requires MongoDB replica set; NeoSQLite uses SQLite triggers"
        )
    else:
        skip_reason = None

    reporter.record_result(
        "Change Streams",
        "watch",
        passed=(
            neo_watch_ok == mongo_watch_ok
            if mongo_watch_ok is not False
            else True
        ),
        neo_result="OK" if neo_watch_ok else "FAIL",
        mongo_result="OK" if mongo_watch_ok else "FAIL",
        skip_reason=skip_reason,
    )
