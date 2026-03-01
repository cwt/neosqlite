"""Module for comparing find and modify operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_find_and_modify():
    """Compare find and modify operations"""
    print("\n=== Find and Modify Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        )

        doc = neo_collection.find_one_and_delete({"name": "Bob"})
        neo_foad = doc is not None

        neo_collection.insert_one({"name": "Bob", "age": 25})
        doc = neo_collection.find_one_and_replace(
            {"name": "Alice"}, {"name": "Alice Smith", "age": 31}
        )
        neo_foar = doc is not None

        doc = neo_collection.find_one_and_update(
            {"name": "Alice Smith"}, {"$inc": {"age": 1}}
        )
        neo_foau = doc is not None

        print(
            f"NeoSQLite: find_one_and_delete={neo_foad}, find_one_and_replace={neo_foar}, find_one_and_update={neo_foau}"
        )

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_foad = None

    mongo_foar = None

    mongo_foau = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        )

        doc = mongo_collection.find_one_and_delete({"name": "Bob"})
        mongo_foad = doc is not None

        mongo_collection.insert_one({"name": "Bob", "age": 25})
        doc = mongo_collection.find_one_and_replace(
            {"name": "Alice"}, {"name": "Alice Smith", "age": 31}
        )
        mongo_foar = doc is not None

        doc = mongo_collection.find_one_and_update(
            {"name": "Alice Smith"}, {"$inc": {"age": 1}}
        )
        mongo_foau = doc is not None

        print(
            f"PyMongo: find_one_and_delete={mongo_foad}, find_one_and_replace={mongo_foar}, find_one_and_update={mongo_foau}"
        )
        client.close()

    reporter.record_result(
        "Find and Modify", "find_one_and_delete", neo_foad, neo_foad, mongo_foad
    )
    reporter.record_result(
        "Find and Modify",
        "find_one_and_replace",
        neo_foar,
        neo_foar,
        mongo_foar,
    )
    reporter.record_result(
        "Find and Modify", "find_one_and_update", neo_foau, neo_foau, mongo_foau
    )
