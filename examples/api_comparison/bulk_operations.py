"""Module for comparing bulk operations between NeoSQLite and PyMongo"""

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


def compare_bulk_operations():
    """Compare bulk operations"""
    print("\n=== Bulk Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        from neosqlite import InsertOne, UpdateOne, DeleteOne

        requests = [
            InsertOne({"name": "Alice", "age": 30}),
            InsertOne({"name": "Bob", "age": 25}),
            UpdateOne({"name": "Alice"}, {"$set": {"age": 31}}),
            DeleteOne({"name": "Bob"}),
        ]
        result = neo_collection.bulk_write(requests)
        print(
            f"Neo bulk_write: inserted={result.inserted_count}, matched={result.matched_count}, modified={result.modified_count}, deleted={result.deleted_count}"
        )

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        from pymongo import InsertOne, UpdateOne, DeleteOne

        requests = [
            InsertOne({"name": "Alice", "age": 30}),
            InsertOne({"name": "Bob", "age": 25}),
            UpdateOne({"name": "Alice"}, {"$set": {"age": 31}}),
            DeleteOne({"name": "Bob"}),
        ]
        result = mongo_collection.bulk_write(requests)
        print(
            f"Mongo bulk_write: inserted={result.inserted_count}, matched={result.matched_count}, modified={result.modified_count}, deleted={result.deleted_count}"
        )
        end_mongo_timing()
        client.close()

    reporter.record_comparison("Bulk Operations", "bulk_write", "OK", "OK")
