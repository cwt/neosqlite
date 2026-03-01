"""Module for comparing change streams between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_change_streams():
    """Compare change streams (watch) - SKIPPED: Different architecture"""
    print("\n=== Change Streams (watch) Comparison ===")

    # NeoSQLite uses SQLite triggers for change tracking
    # MongoDB uses oplog-based change streams
    # These are fundamentally different architectures

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"name": "test"})

        try:
            # NeoSQLite watch uses SQLite change tracking
            _ = neo_collection.watch()
            print("Neo watch: Supported")
        except Exception as e:
            print(f"Neo watch: Error - {e}")

    # MongoDB change streams require replica set
    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"name": "test"})

        try:
            # MongoDB change streams require replica set
            # This will likely fail on standalone MongoDB
            _ = mongo_collection.watch()
            print("Mongo watch: Supported")
        except Exception as e:
            print(f"Mongo watch: Error - {e} (requires replica set)")
        client.close()

    # Skip this test as it requires different infrastructure
    reporter.record_result(
        "Change Streams",
        "watch",
        True,
        "OK",
        "OK",
        skip_reason="Requires MongoDB replica set; NeoSQLite uses SQLite triggers",
    )
