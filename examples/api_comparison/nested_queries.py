"""Module for comparing nested field queries between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_nested_field_queries():
    """Compare nested field query support"""
    print("\n=== Nested Field Queries Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_result = None

    if client:
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
        client.close()

    passed = (
        len(neo_result) == len(mongo_result)
        if mongo_result is not None
        else False
    )
    reporter.record_result(
        "Nested Field Queries",
        "dot_notation",
        passed,
        len(neo_result),
        len(mongo_result) if mongo_result is not None else None,
    )
