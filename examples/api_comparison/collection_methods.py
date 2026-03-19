"""Module for comparing collection methods between NeoSQLite and PyMongo"""

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
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_collection_methods():
    """Compare collection methods"""
    print("\n=== Collection Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"name": "test"})

        set_accumulation_mode(True)
        # Test options()
        try:
            start_neo_timing()
            neo_options = neo_collection.options()
            end_neo_timing()

            neo_options_ok = (
                isinstance(neo_options, dict) and "name" in neo_options
            )
            print(f"Neo options(): {'OK' if neo_options_ok else 'FAIL'}")
        except Exception as e:
            neo_options_ok = False
            print(f"Neo options(): Error - {e}")

        # Test rename()
        try:
            start_neo_timing()
            neo_collection.rename("renamed_collection")
            end_neo_timing()

            neo_rename = (
                "renamed_collection" in neo_conn.list_collection_names()
            )
            print(f"Neo rename(): {'OK' if neo_rename else 'FAIL'}")
            # Rename back for cleanup
            neo_conn.renamed_collection.rename("test_collection")
        except Exception as e:
            neo_rename = False
            print(f"Neo rename(): Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None
    mongo_db = None
    mongo_options = None
    mongo_options_ok = None
    mongo_rename = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"name": "test"})

        set_accumulation_mode(True)
        # Test options()
        try:
            start_mongo_timing()
            mongo_options = mongo_collection.options()
            end_mongo_timing()

            # MongoDB options() returns a dict with different structure
            mongo_options_ok = isinstance(mongo_options, dict)
            print(
                f"Mongo options(): {'OK' if mongo_options_ok else 'FAIL'} (returns dict)"
            )
        except Exception as e:
            mongo_options_ok = False
            print(f"Mongo options(): Error - {e}")

        # Test rename()
        try:
            start_mongo_timing()
            mongo_collection.rename("renamed_collection")
            end_mongo_timing()

            mongo_rename = (
                "renamed_collection" in mongo_db.list_collection_names()
            )
            print(f"Mongo rename(): {'OK' if mongo_rename else 'FAIL'}")
            # Rename back for cleanup
            mongo_db.renamed_collection.rename("test_collection")
        except Exception as e:
            mongo_rename = False
            print(f"Mongo rename(): Error - {e}")

        client.close()

    reporter.record_comparison(
        "Collection Methods",
        "options",
        neo_options if neo_options_ok else "FAIL",
        None,
        skip_reason=(
            "MongoDB not available"
            if not client
            else "MongoDB returns {} - backend-specific"
        ),
    )
    reporter.record_comparison(
        "Collection Methods",
        "rename",
        neo_rename if neo_rename else "FAIL",
        mongo_rename if mongo_rename else None,
        skip_reason="MongoDB not available" if not client else None,
    )
