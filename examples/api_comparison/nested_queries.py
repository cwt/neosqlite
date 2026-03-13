"""Module for comparing nested field queries between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_nested_field_queries():
    """Compare nested field query support"""
    print("\n=== Nested Field Queries Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "Alice", "profile": {"age": 30, "city": "NYC"}},
                {"name": "Bob", "profile": {"age": 25, "city": "LA"}},
                {"name": "Charlie", "profile": {"age": 35, "city": "NYC"}},
            ]
        )

        # Dot notation query
        neo_result = list(neo_collection.find({"profile.city": "NYC"}))
        print(f"Neo nested query (profile.city): {len(neo_result)}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_result = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "Alice", "profile": {"age": 30, "city": "NYC"}},
                {"name": "Bob", "profile": {"age": 25, "city": "LA"}},
                {"name": "Charlie", "profile": {"age": 35, "city": "NYC"}},
            ]
        )

        mongo_result = list(mongo_collection.find({"profile.city": "NYC"}))
        print(f"Mongo nested query (profile.city): {len(mongo_result)}")
        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Nested Field Queries",
        "dot_notation",
        neo_result,
        mongo_result if mongo_result is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
