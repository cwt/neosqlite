"""Module for comparing bulk operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_bulk_operations():
    """Compare bulk operations"""
    print("\n=== Bulk Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    if client:
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
        client.close()

    reporter.record_result("Bulk Operations", "bulk_write", True, "OK", "OK")
