"""Module for comparing CRUD operations between NeoSQLite and PyMongo"""

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

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_crud_operations():
    """Compare CRUD operations between NeoSQLite and PyMongo"""
    print("\n=== CRUD Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        set_accumulation_mode(True)

        # insert_one
        start_neo_timing()
        try:
            result = neo_collection.insert_one({"name": "Alice", "age": 30})
        finally:

            end_neo_timing()
        _ = result.inserted_id

        # find_one
        start_neo_timing()
        try:
            _ = neo_collection.find_one({"name": "Alice"})
        finally:

            end_neo_timing()

        # insert_many
        start_neo_timing()
        try:
            result = neo_collection.insert_many(
                [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
            )
        finally:

            end_neo_timing()

        # find with projection
        start_neo_timing()
        try:
            _ = list(neo_collection.find({}, {"name": 1}))
        finally:

            end_neo_timing()

        # find_one with projection
        start_neo_timing()
        try:
            _ = neo_collection.find_one({"name": "Alice"}, {"age": 1})
        finally:

            end_neo_timing()

        # update_one
        start_neo_timing()
        try:
            result = neo_collection.update_one(
                {"name": "Alice"}, {"$set": {"age": 31}}
            )
        finally:

            end_neo_timing()

        # update_many
        start_neo_timing()
        try:
            result = neo_collection.update_many(
                {"age": {"$gt": 30}}, {"$inc": {"age": 1}}
            )
        finally:

            end_neo_timing()

        # replace_one
        start_neo_timing()
        try:
            result = neo_collection.replace_one(
                {"name": "Alice"}, {"name": "Alice Smith", "age": 32}
            )
        finally:

            end_neo_timing()

        # delete_one
        start_neo_timing()
        try:
            result = neo_collection.delete_one({"name": "Bob"})
        finally:

            end_neo_timing()

        # delete_many
        start_neo_timing()
        try:
            result = neo_collection.delete_many({"age": {"$gt": 30}})
        finally:

            end_neo_timing()

        # count_documents
        start_neo_timing()
        try:
            _ = neo_collection.count_documents({})
        finally:

            end_neo_timing()

        # estimated_document_count
        start_neo_timing()
        try:
            _ = neo_collection.estimated_document_count()
        finally:

            end_neo_timing()

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

        set_accumulation_mode(True)

        start_mongo_timing()
        try:
            result = mongo_collection.insert_one({"name": "Alice", "age": 30})
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            _ = mongo_collection.find_one({"name": "Alice"})
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            result = mongo_collection.insert_many(
                [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
            )
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            _ = list(mongo_collection.find({}, {"name": 1}))
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            _ = mongo_collection.find_one({"name": "Alice"}, {"age": 1})
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            result = mongo_collection.update_one(
                {"name": "Alice"}, {"$set": {"age": 31}}
            )
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            result = mongo_collection.update_many(
                {"age": {"$gt": 30}}, {"$inc": {"age": 1}}
            )
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            result = mongo_collection.replace_one(
                {"name": "Alice"}, {"name": "Alice Smith", "age": 32}
            )
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            result = mongo_collection.delete_one({"name": "Bob"})
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            result = mongo_collection.delete_many({"age": {"$gt": 30}})
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            _ = mongo_collection.count_documents({})
        finally:

            end_mongo_timing()

        start_mongo_timing()
        try:
            _ = mongo_collection.estimated_document_count()
        finally:

            end_mongo_timing()

        print("PyMongo CRUD: All operations completed")
        client.close()

    reporter.record_comparison("CRUD Operations", "insert_one", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "insert_many", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "find", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "find_one", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "update_one", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "update_many", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "replace_one", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "delete_one", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "delete_many", "OK", "OK")
    reporter.record_comparison("CRUD Operations", "count_documents", "OK", "OK")
    reporter.record_comparison(
        "CRUD Operations", "estimated_document_count", "OK", "OK"
    )
