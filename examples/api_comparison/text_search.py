"""Module for comparing text search between NeoSQLite and PyMongo"""

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
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_text_search():
    """Compare text search capabilities"""
    print("\n=== Text Search Comparison ===")

    neo_text_search = None
    with neosqlite.Connection(":memory:") as neo_conn:
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

        set_accumulation_mode(True)
        try:
            # Create FTS index
            start_neo_timing()
            try:
                neo_collection.create_index("description", fts=True)
            finally:
                end_neo_timing()

            # Text search (regex)
            start_neo_timing()
            try:
                neo_results = list(
                    neo_collection.find(
                        {"description": {"$regex": "Python", "$options": "i"}}
                    )
                )
            finally:
                end_neo_timing()

            neo_text_search = len(neo_results)
            print(f"Neo text search (regex): {neo_text_search}")
        except Exception as e:
            neo_text_search = f"Error: {e}"
            print(f"Neo text search: Error - {e}")

    client = get_mongo_client()
    mongo_text_search = None

    if client:
        try:
            from pymongo import ASCENDING as MONGO_ASCENDING

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

            set_accumulation_mode(True)

            # Create index
            start_mongo_timing()
            try:
                mongo_collection.create_index(
                    [("description", MONGO_ASCENDING)]
                )
            finally:
                end_mongo_timing()

            # Text search (regex)
            start_mongo_timing()
            try:
                mongo_results = list(
                    mongo_collection.find(
                        {"description": {"$regex": "Python", "$options": "i"}}
                    )
                )
            finally:
                end_mongo_timing()

            mongo_text_search = len(mongo_results)
            print(f"Mongo text search (regex): {mongo_text_search}")
        except Exception as e:
            mongo_text_search = f"Error: {e}"
            print(f"Mongo text search: Error - {e}")

    reporter.record_comparison(
        "Text Search",
        "regex_search",
        neo_text_search,
        mongo_text_search,
        skip_reason="MongoDB not available" if not client else None,
    )
