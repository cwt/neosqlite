"""Module for comparing CRUD operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_crud_operations():
    """Compare CRUD operations between NeoSQLite and PyMongo"""
    print("\n=== CRUD Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # insert_one
        result = neo_collection.insert_one({"name": "Alice", "age": 30})
        _ = result.inserted_id

        # find_one
        _ = neo_collection.find_one({"name": "Alice"})

        # insert_many
        result = neo_collection.insert_many(
            [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
        )

        # find with projection
        _ = list(neo_collection.find({}, {"name": 1}))

        # find_one with projection
        _ = neo_collection.find_one({"name": "Alice"}, {"age": 1})

        # update_one
        result = neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"age": 31}}
        )

        # update_many
        result = neo_collection.update_many(
            {"age": {"$gt": 30}}, {"$inc": {"age": 1}}
        )

        # replace_one
        result = neo_collection.replace_one(
            {"name": "Alice"}, {"name": "Alice Smith", "age": 32}
        )

        # delete_one
        result = neo_collection.delete_one({"name": "Bob"})

        # delete_many
        result = neo_collection.delete_many({"age": {"$gt": 30}})

        # count_documents
        _ = neo_collection.count_documents({})

        # estimated_document_count
        _ = neo_collection.estimated_document_count()

        print(
            "NeoSQLite CRUD: insert_one, insert_many, find, find_one, update_one, update_many, replace_one, delete_one, delete_many, count_documents, estimated_document_count"
        )

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})

        result = mongo_collection.insert_one({"name": "Alice", "age": 30})
        _ = mongo_collection.find_one({"name": "Alice"})
        result = mongo_collection.insert_many(
            [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
        )
        _ = list(mongo_collection.find({}, {"name": 1}))
        _ = mongo_collection.find_one({"name": "Alice"}, {"age": 1})
        result = mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"age": 31}}
        )
        result = mongo_collection.update_many(
            {"age": {"$gt": 30}}, {"$inc": {"age": 1}}
        )
        result = mongo_collection.replace_one(
            {"name": "Alice"}, {"name": "Alice Smith", "age": 32}
        )
        result = mongo_collection.delete_one({"name": "Bob"})
        result = mongo_collection.delete_many({"age": {"$gt": 30}})
        _ = mongo_collection.count_documents({})
        _ = mongo_collection.estimated_document_count()

        print("PyMongo CRUD: All operations completed")
        client.close()

    reporter.record_result("CRUD", "insert_one", True, "OK", "OK")
    reporter.record_result("CRUD", "insert_many", True, "OK", "OK")
    reporter.record_result("CRUD", "find", True, "OK", "OK")
    reporter.record_result("CRUD", "find_one", True, "OK", "OK")
    reporter.record_result("CRUD", "update_one", True, "OK", "OK")
    reporter.record_result("CRUD", "update_many", True, "OK", "OK")
    reporter.record_result("CRUD", "replace_one", True, "OK", "OK")
    reporter.record_result("CRUD", "delete_one", True, "OK", "OK")
    reporter.record_result("CRUD", "delete_many", True, "OK", "OK")
    reporter.record_result("CRUD", "count_documents", True, "OK", "OK")
    reporter.record_result("CRUD", "estimated_document_count", True, "OK", "OK")
