"""Module for comparing reindex operation between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_reindex_operation():
    """Compare reindex operation between NeoSQLite and PyMongo"""
    print("\n=== Reindex Operation Comparison ===")

    neo_reindex_ok = False
    mongo_reindex_ok = False

    # NeoSQLite
    try:
        with neosqlite.Connection(":memory:") as neo_conn:
            neo_collection = neo_conn.test_reindex
            neo_collection.insert_many(
                [
                    {"name": "A", "value": 1},
                    {"name": "B", "value": 2},
                ]
            )
            neo_collection.create_index("name")

            # Test reindex
            start_neo_timing()
            try:
                # Use "reindex" (lowercase) which is supported by NeoSQLite command method
                result = neo_conn.command("reindex", "test_reindex")
                if result.get("ok"):
                    neo_reindex_ok = True
                    print("Neo reindex: OK")
                else:
                    print(f"Neo reindex: Failed - {result.get('errmsg')}")
            finally:
                end_neo_timing()
    except Exception as e:
        print(f"NeoSQLite Error: {e}")

    # MongoDB
    client = test_pymongo_connection()
    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_reindex
            # Ensure clean state
            mongo_collection.drop()
            mongo_collection.insert_many(
                [
                    {"name": "A", "value": 1},
                    {"name": "B", "value": 2},
                ]
            )
            mongo_collection.create_index("name")

            # Test reindex
            start_mongo_timing()
            try:
                # MongoDB command name is "reIndex"
                # Note: reIndex is often restricted or unsupported in some environments
                result = mongo_db.command("reIndex", "test_reindex")
                if result.get("ok"):
                    mongo_reindex_ok = True
                    print("Mongo reIndex command: OK")
                else:
                    print(
                        f"Mongo reIndex command: Failed - {result.get('errmsg')}"
                    )
            except Exception as e:
                print(f"Mongo reIndex command: Error - {e}")
            finally:
                end_mongo_timing()
        except Exception as e:
            print(f"MongoDB Error: {e}")
        finally:
            client.close()
    else:
        print("MongoDB not available, skipping Mongo reindex test")

    # Record comparison
    reporter.record_comparison(
        "Reindex",
        "reindex",
        "OK" if neo_reindex_ok else "FAIL",
        "OK" if mongo_reindex_ok else ("FAIL" if client else None),
        skip_reason="MongoDB not available" if not client else None,
    )
