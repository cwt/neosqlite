"""Module for comparing $mod query operator between NeoSQLite and PyMongo"""

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


def compare_mod_operator():
    """Compare $mod query operator"""
    print("\n=== $mod Query Operator Comparison ===")

    neo_mod_result = None
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
                {"name": "David", "age": 28},
                {"name": "Eve", "age": 32},
            ]
        )

        set_accumulation_mode(True)
        # Test $mod: age % 5 == 0
        start_neo_timing()
        try:
            neo_mod_result = list(
                neo_collection.find({"age": {"$mod": [5, 0]}})
            )
            print(f"Neo $mod (age % 5 == 0): {len(neo_mod_result)}")
        except Exception as e:
            neo_mod_result = f"Error: {e}"
            print(f"Neo $mod: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    mongo_mod_result = None

    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            mongo_collection.insert_many(
                [
                    {"name": "Alice", "age": 30},
                    {"name": "Bob", "age": 25},
                    {"name": "Charlie", "age": 35},
                    {"name": "David", "age": 28},
                    {"name": "Eve", "age": 32},
                ]
            )

            set_accumulation_mode(True)
            start_mongo_timing()
            try:
                mongo_mod_result = list(
                    mongo_collection.find({"age": {"$mod": [5, 0]}})
                )
                print(f"Mongo $mod (age % 5 == 0): {len(mongo_mod_result)}")
            except Exception as e:
                mongo_mod_result = f"Error: {e}"
                print(f"Mongo $mod: Error - {e}")
            finally:
                end_mongo_timing()
        finally:
            client.close()

    reporter.record_comparison(
        "$mod Operator",
        "$mod",
        neo_mod_result,
        mongo_mod_result,
        skip_reason="MongoDB not available" if not client else None,
    )
