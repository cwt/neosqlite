"""Module for comparing $mod query operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_mod_operator():
    """Compare $mod query operator"""
    print("\n=== $mod Query Operator Comparison ===")

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

        # Test $mod: age % 5 == 0
        try:
            neo_result = list(neo_collection.find({"age": {"$mod": [5, 0]}}))
            neo_mod = len(neo_result)
            print(f"Neo $mod (age % 5 == 0): {neo_mod}")
        except Exception as e:
            neo_mod = f"Error: {e}"
            print(f"Neo $mod: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_mod = None

    mongo_result = None

    if client:
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

        try:
            mongo_result = list(
                mongo_collection.find({"age": {"$mod": [5, 0]}})
            )
            mongo_mod = len(mongo_result)
            print(f"Mongo $mod (age % 5 == 0): {mongo_mod}")
        except Exception as e:
            mongo_mod = f"Error: {e}"
            print(f"Mongo $mod: Error - {e}")
        client.close()

    reporter.record_result(
        "Query Operators",
        "$mod",
        (
            neo_mod == mongo_mod
            if mongo_mod is not None
            else (
                False
                if not isinstance(neo_mod, str)
                and not isinstance(mongo_mod, str)
                else False
            )
        ),
        neo_mod,
        mongo_mod,
    )
