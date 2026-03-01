"""Module for comparing reindex operation between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_reindex_operation():
    """Compare reindex operation"""
    print("\n=== Reindex Operation Comparison ===")

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
        try:
            neo_collection.reindex("test_reindex")
            neo_reindex_ok = True
            print("Neo reindex: OK")
        except Exception as e:
            neo_reindex_ok = False
            print(f"Neo reindex: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_reindex_ok = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_reindex
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "A", "value": 1},
                {"name": "B", "value": 2},
            ]
        )
        mongo_collection.create_index("name")

        # Test reindex (MongoDB command)
        try:
            mongo_db.command("reIndex", "test_reindex")
            mongo_reindex_ok = True
            print("Mongo reIndex command: OK")
        except Exception as e:
            mongo_reindex_ok = False
            print(f"Mongo reIndex command: Error - {e}")

        client.close()

        reporter.record_result(
            "Reindex Operation",
            "reindex",
            neo_reindex_ok,
            neo_reindex_ok,
            mongo_reindex_ok,
        )
