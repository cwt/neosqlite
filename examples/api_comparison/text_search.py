"""Module for comparing text search between NeoSQLite and PyMongo"""

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


def compare_text_search():
    """Compare text search capabilities"""
    print("\n=== Text Search Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "description": "Python developer with SQL expertise",
                },
                {"name": "Bob", "description": "Java developer"},
                {
                    "name": "Charlie",
                    "description": "Full-stack developer with Python and JavaScript",
                },
            ]
        )

        # Create FTS index
        neo_collection.create_index("description", fts=True)

        # Text search
        try:
            neo_results = list(
                neo_collection.find(
                    {"description": {"$regex": "Python", "$options": "i"}}
                )
            )
            neo_text_search = len(neo_results)
            print(f"Neo text search (regex): {neo_text_search}")
        except Exception as e:
            neo_text_search = f"Error: {e}"
            print(f"Neo text search: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_results = None

    mongo_text_search = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "description": "Python developer with SQL expertise",
                },
                {"name": "Bob", "description": "Java developer"},
                {
                    "name": "Charlie",
                    "description": "Full-stack developer with Python and JavaScript",
                },
            ]
        )

        # Create text index
        try:
            from pymongo import ASCENDING as MONGO_ASCENDING

            mongo_collection.create_index([("description", MONGO_ASCENDING)])
            mongo_results = list(
                mongo_collection.find(
                    {"description": {"$regex": "Python", "$options": "i"}}
                )
            )
            mongo_text_search = len(mongo_results)
            print(f"Mongo text search (regex): {mongo_text_search}")
        except Exception as e:
            mongo_text_search = f"Error: {e}"
            print(f"Mongo text search: Error - {e}")
        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Text Search",
        "regex_search",
        (
            neo_text_search
            if not isinstance(neo_text_search, str)
            else neo_text_search
        ),
        mongo_text_search if mongo_text_search is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
