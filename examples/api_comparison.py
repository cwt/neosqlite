#!/usr/bin/env python3
"""
API Comparison Script between NeoSQLite and PyMongo
"""
import neosqlite
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import sys


def test_pymongo_connection():
    """Test connection to MongoDB"""
    try:
        client = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        print("MongoDB connection successful")
        return client
    except ConnectionFailure:
        print("Failed to connect to MongoDB")
        return None


def compare_crud_operations():
    """Compare CRUD operations between NeoSQLite and PyMongo"""
    print("\n=== CRUD Operations Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # insert_one
        result = neo_collection.insert_one({"name": "Alice", "age": 30})
        print(f"insert_one: inserted_id = {result.inserted_id}")

        # find_one
        doc = neo_collection.find_one({"name": "Alice"})
        print(f"find_one: {doc}")

        # insert_many
        result = neo_collection.insert_many(
            [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
        )
        print(f"insert_many: inserted_ids = {result.inserted_ids}")

        # find
        docs = list(neo_collection.find())
        print(f"find: found {len(docs)} documents")

        # update_one
        result = neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"age": 31}}
        )
        print(
            f"update_one: matched={result.matched_count}, modified={result.modified_count}"
        )

        # update_many
        result = neo_collection.update_many(
            {"age": {"$gt": 30}}, {"$inc": {"age": 1}}
        )
        print(
            f"update_many: matched={result.matched_count}, modified={result.modified_count}"
        )

        # delete_one
        result = neo_collection.delete_one({"name": "Bob"})
        print(f"delete_one: deleted_count = {result.deleted_count}")

        # delete_many
        result = neo_collection.delete_many({"age": {"$gt": 30}})
        print(f"delete_many: deleted_count = {result.deleted_count}")

        # count_documents
        count = neo_collection.count_documents({})
        print(f"count_documents: {count}")

        # estimated_document_count
        count = neo_collection.estimated_document_count()
        print(f"estimated_document_count: {count}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection

        # Clean up first
        mongo_collection.delete_many({})

        # insert_one
        result = mongo_collection.insert_one({"name": "Alice", "age": 30})
        print(f"insert_one: inserted_id = {result.inserted_id}")

        # find_one
        doc = mongo_collection.find_one({"name": "Alice"})
        print(f"find_one: {doc}")

        # insert_many
        result = mongo_collection.insert_many(
            [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
        )
        print(f"insert_many: inserted_ids = {result.inserted_ids}")

        # find
        docs = list(mongo_collection.find())
        print(f"find: found {len(docs)} documents")

        # update_one
        result = mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"age": 31}}
        )
        print(
            f"update_one: matched={result.matched_count}, modified={result.modified_count}"
        )

        # update_many
        result = mongo_collection.update_many(
            {"age": {"$gt": 30}}, {"$inc": {"age": 1}}
        )
        print(
            f"update_many: matched={result.matched_count}, modified={result.modified_count}"
        )

        # delete_one
        result = mongo_collection.delete_one({"name": "Bob"})
        print(f"delete_one: deleted_count = {result.deleted_count}")

        # delete_many
        result = mongo_collection.delete_many({"age": {"$gt": 30}})
        print(f"delete_many: deleted_count = {result.deleted_count}")

        # count_documents
        count = mongo_collection.count_documents({})
        print(f"count_documents: {count}")

        # estimated_document_count
        count = mongo_collection.estimated_document_count()
        print(f"estimated_document_count: {count}")

        # Clean up
        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_index_operations():
    """Compare index operations between NeoSQLite and PyMongo"""
    print("\n=== Index Operations Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # create_index
        neo_collection.create_index("name")
        print("create_index: Created index on 'name'")

        # create_indexes
        neo_collection.create_indexes(
            [
                "age",
                [("name", neosqlite.ASCENDING), ("age", neosqlite.DESCENDING)],
            ]
        )
        print("create_indexes: Created multiple indexes")

        # list_indexes
        indexes = neo_collection.list_indexes()
        print(f"list_indexes: {indexes}")

        # index_information
        info = neo_collection.index_information()
        print(f"index_information: {list(info.keys())}")

        # drop_index
        neo_collection.drop_index("name")
        print("drop_index: Dropped index on 'name'")

        # drop_indexes
        neo_collection.drop_indexes()
        print("drop_indexes: Dropped all indexes")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection

        # Clean up first
        mongo_collection.drop_indexes()

        # create_index
        mongo_collection.create_index("name")
        print("create_index: Created index on 'name'")

        # create_indexes
        from pymongo import ASCENDING, DESCENDING
        from pymongo import IndexModel

        mongo_collection.create_indexes(
            [
                IndexModel([("age", ASCENDING)]),
                IndexModel([("name", ASCENDING), ("age", DESCENDING)]),
            ]
        )
        print("create_indexes: Created multiple indexes")

        # list_indexes
        indexes = list(mongo_collection.list_indexes())
        print(f"list_indexes: {[idx['name'] for idx in indexes]}")

        # index_information
        info = mongo_collection.index_information()
        print(f"index_information: {list(info.keys())}")

        # drop_index
        mongo_collection.drop_index("name_1")
        print("drop_index: Dropped index on 'name'")

        # drop_indexes
        mongo_collection.drop_indexes()
        print("drop_indexes: Dropped all indexes")

        # Clean up
        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_aggregation_operations():
    """Compare aggregation operations between NeoSQLite and PyMongo"""
    print("\n=== Aggregation Operations Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # Insert test data
        neo_collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Engineering"},
                {"name": "Charlie", "age": 35, "department": "Marketing"},
                {"name": "David", "age": 28, "department": "Marketing"},
            ]
        )

        # aggregate
        pipeline = [
            {"$match": {"age": {"$gte": 30}}},
            {"$sort": {"age": neosqlite.DESCENDING}},
            {"$limit": 2},
        ]
        result = neo_collection.aggregate(pipeline)
        print(f"aggregate: {len(result)} documents returned")
        for doc in result:
            print(f"  - {doc}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection

        # Clean up first
        mongo_collection.delete_many({})

        # Insert test data
        mongo_collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Engineering"},
                {"name": "Charlie", "age": 35, "department": "Marketing"},
                {"name": "David", "age": 28, "department": "Marketing"},
            ]
        )

        # aggregate
        from pymongo import DESCENDING

        pipeline = [
            {"$match": {"age": {"$gte": 30}}},
            {"$sort": {"age": DESCENDING}},
            {"$limit": 2},
        ]
        result = list(mongo_collection.aggregate(pipeline))
        print(f"aggregate: {len(result)} documents returned")
        for doc in result:
            print(f"  - {doc}")

        # Clean up
        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_find_and_modify_operations():
    """Compare find and modify operations between NeoSQLite and PyMongo"""
    print("\n=== Find and Modify Operations Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # Insert test data
        neo_collection.insert_many(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        )

        # find_one_and_delete
        doc = neo_collection.find_one_and_delete({"name": "Bob"})
        print(f"find_one_and_delete: {doc}")

        # find_one_and_replace
        doc = neo_collection.find_one_and_replace(
            {"name": "Alice"}, {"name": "Alice Cooper", "age": 31}
        )
        print(f"find_one_and_replace: {doc}")

        # find_one_and_update
        doc = neo_collection.find_one_and_update(
            {"name": "Alice Cooper"}, {"$inc": {"age": 1}}
        )
        print(f"find_one_and_update: {doc}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection

        # Clean up first
        mongo_collection.delete_many({})

        # Insert test data
        mongo_collection.insert_many(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        )

        # find_one_and_delete
        doc = mongo_collection.find_one_and_delete({"name": "Bob"})
        print(f"find_one_and_delete: {doc}")

        # find_one_and_replace
        doc = mongo_collection.find_one_and_replace(
            {"name": "Alice"}, {"name": "Alice Cooper", "age": 31}
        )
        print(f"find_one_and_replace: {doc}")

        # find_one_and_update
        doc = mongo_collection.find_one_and_update(
            {"name": "Alice Cooper"}, {"$inc": {"age": 1}}
        )
        print(f"find_one_and_update: {doc}")

        # Clean up
        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_bulk_operations():
    """Compare bulk operations between NeoSQLite and PyMongo"""
    print("\n=== Bulk Operations Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # bulk_write
        from neosqlite import InsertOne, UpdateOne, DeleteOne

        requests = [
            InsertOne({"name": "Alice", "age": 30}),
            InsertOne({"name": "Bob", "age": 25}),
            UpdateOne({"name": "Alice"}, {"$set": {"age": 31}}),
            DeleteOne({"name": "Bob"}),
        ]
        result = neo_collection.bulk_write(requests)
        print(
            f"bulk_write: inserted={result.inserted_count}, "
            f"matched={result.matched_count}, "
            f"modified={result.modified_count}, "
            f"deleted={result.deleted_count}"
        )

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection

        # Clean up first
        mongo_collection.delete_many({})

        # bulk_write
        from pymongo import InsertOne, UpdateOne, DeleteOne

        requests = [
            InsertOne({"name": "Alice", "age": 30}),
            InsertOne({"name": "Bob", "age": 25}),
            UpdateOne({"name": "Alice"}, {"$set": {"age": 31}}),
            DeleteOne({"name": "Bob"}),
        ]
        result = mongo_collection.bulk_write(requests)
        print(
            f"bulk_write: inserted={result.inserted_count}, "
            f"matched={result.matched_count}, "
            f"modified={result.modified_count}, "
            f"deleted={result.deleted_count}"
        )

        # Clean up
        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def main():
    """Main function to run all comparisons"""
    print("NeoSQLite vs PyMongo API Comparison")
    print("=" * 40)

    compare_crud_operations()
    compare_index_operations()
    compare_aggregation_operations()
    compare_find_and_modify_operations()
    compare_bulk_operations()

    print("\n=== Comparison Summary ===")
    print(
        "This script has compared the core APIs between NeoSQLite and PyMongo."
    )
    print("Both libraries provide similar interfaces for database operations.")


if __name__ == "__main__":
    main()
