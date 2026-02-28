#!/usr/bin/env python3
"""
API Comparison Script between NeoSQLite and PyMongo

This script compares all NeoSQLite supported APIs and operators with MongoDB
and reports compatibility statistics.
"""
import neosqlite
from neosqlite import DESCENDING
from pymongo import (
    MongoClient,
    ASCENDING as MONGO_ASCENDING,
    DESCENDING as MONGO_DESCENDING,
)
from pymongo import IndexModel
from pymongo.errors import ConnectionFailure
from typing import Any, Dict, List, Optional
import sys


class CompatibilityReporter:
    """Tracks and reports API compatibility between NeoSQLite and PyMongo"""

    def __init__(self):
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = []
        self.skipped_tests = []

    def record_result(
        self,
        category: str,
        api_name: str,
        passed: bool,
        neo_result: Any = None,
        mongo_result: Any = None,
        error: Optional[str] = None,
        skip_reason: Optional[str] = None,
    ):
        """Record a test result"""
        self.total_tests += 1
        if skip_reason:
            self.skipped_tests.append(
                {
                    "category": category,
                    "api": api_name,
                    "reason": skip_reason,
                }
            )
        elif passed:
            self.passed_tests += 1
        else:
            self.failed_tests.append(
                {
                    "category": category,
                    "api": api_name,
                    "neo_result": str(neo_result)[:100] if neo_result else None,
                    "mongo_result": (
                        str(mongo_result)[:100] if mongo_result else None
                    ),
                    "error": error,
                }
            )

    def get_compatibility_percentage(self) -> float:
        """Get the compatibility percentage"""
        if self.total_tests == 0:
            return 0.0
        # Exclude skipped tests from percentage calculation
        effective_total = self.total_tests - len(self.skipped_tests)
        if effective_total == 0:
            return 100.0
        return (self.passed_tests / effective_total) * 100

    def print_report(self):
        """Print the compatibility report"""
        print("\n" + "=" * 80)
        print("COMPATIBILITY REPORT")
        print("=" * 80)
        print(f"Total Tests: {self.total_tests}")
        print(f"Passed: {self.passed_tests}")
        print(f"Skipped: {len(self.skipped_tests)}")
        print(f"Failed: {len(self.failed_tests)}")
        print(f"Compatibility: {self.get_compatibility_percentage():.1f}%")
        print("=" * 80)

        if self.failed_tests:
            print("\nINCOMPATIBLE APIs:")
            print("-" * 80)
            for failure in self.failed_tests:
                print(f"\n[{failure['category']}] {failure['api']}")
                if failure["error"]:
                    print(f"  Error: {failure['error']}")
                if failure["neo_result"]:
                    print(f"  NeoSQLite: {failure['neo_result']}")
                if failure["mongo_result"]:
                    print(f"  MongoDB: {failure['mongo_result']}")

        if self.skipped_tests:
            print("\n\nSKIPPED TESTS:")
            print("-" * 80)
            for skip in self.skipped_tests:
                print(f"\n[{skip['category']}] {skip['api']}")
                print(f"  Reason: {skip['reason']}")

        print("\n" + "=" * 80)


# Global reporter instance
reporter = CompatibilityReporter()


def normalize_id(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Remove or normalize _id field for comparison"""
    if "_id" in doc:
        # Don't compare ObjectId values as they differ between implementations
        doc_copy = doc.copy()
        del doc_copy["_id"]
        return doc_copy
    return doc


def normalize_ids(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove _id fields from multiple documents for comparison"""
    return [normalize_id(doc.copy()) for doc in docs]


def compare_values(neo_val: Any, mongo_val: Any, field: str = "") -> bool:
    """Compare two values, handling type differences"""
    # Handle None values
    if neo_val is None and mongo_val is None:
        return True

    # Handle numeric type differences (int vs float)
    if isinstance(neo_val, (int, float)) and isinstance(
        mongo_val, (int, float)
    ):
        return abs(float(neo_val) - float(mongo_val)) < 0.0001

    # Handle list comparison
    if isinstance(neo_val, list) and isinstance(mongo_val, list):
        if len(neo_val) != len(mongo_val):
            return False
        return all(compare_values(n, m) for n, m in zip(neo_val, mongo_val))

    # Handle dict comparison (excluding _id)
    if isinstance(neo_val, dict) and isinstance(mongo_val, dict):
        neo_keys = set(k for k in neo_val.keys() if k != "_id")
        mongo_keys = set(k for k in mongo_val.keys() if k != "_id")
        if neo_keys != mongo_keys:
            return False
        return all(
            compare_values(neo_val[k], mongo_val[k], k)
            for k in neo_keys
            if k != "_id"
        )

    # Direct comparison
    return neo_val == mongo_val


def test_pymongo_connection() -> Optional[MongoClient]:
    """Test connection to MongoDB"""
    try:
        client: MongoClient = MongoClient(
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
        print(f"insert_one: inserted_id (integer) = {result.inserted_id}")

        # find_one
        doc = neo_collection.find_one({"name": "Alice"})
        print(f"find_one: {doc}")
        print(f"   Document _id (ObjectId) = {doc['_id']}")

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
        mongo_collection.create_indexes(
            [
                IndexModel([("age", MONGO_ASCENDING)]),
                IndexModel(
                    [("name", MONGO_ASCENDING), ("age", MONGO_DESCENDING)]
                ),
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
        result = list(neo_collection.aggregate(pipeline))
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
        pipeline = [
            {"$match": {"age": {"$gte": 30}}},
            {"$sort": {"age": MONGO_DESCENDING}},
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


def compare_query_operators():
    """Compare query operators between NeoSQLite and PyMongo"""
    print("\n=== Query Operators Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "scores": [80, 90, 100],
                    "dept": "Engineering",
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "scores": [70, 80],
                    "dept": "Marketing",
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "scores": [90, 95],
                    "dept": "Engineering",
                },
                {
                    "name": "David",
                    "age": 28,
                    "scores": [85],
                    "dept": "Marketing",
                },
                {
                    "name": "Eve",
                    "age": 32,
                    "scores": [88, 92, 96],
                    "dept": "HR",
                },
            ]
        )

        # Test comparison operators
        operators_to_test = [
            # Comparison operators
            ({"age": {"$eq": 30}}, "$eq"),
            ({"age": {"$gt": 30}}, "$gt"),
            ({"age": {"$gte": 30}}, "$gte"),
            ({"age": {"$lt": 30}}, "$lt"),
            ({"age": {"$lte": 30}}, "$lte"),
            ({"age": {"$ne": 30}}, "$ne"),
            ({"age": {"$in": [25, 30, 35]}}, "$in"),
            ({"age": {"$nin": [25, 30]}}, "$nin"),
            # Element operators
            ({"age": {"$exists": True}}, "$exists (true)"),
            ({"age": {"$exists": False}}, "$exists (false)"),
            ({"age": {"$type": 16}}, "$type (int)"),  # 16 = int in MongoDB
            # Array operators
            ({"scores": {"$all": [80, 90]}}, "$all"),
            ({"scores": {"$elemMatch": {"$gte": 90}}}, "$elemMatch"),
            ({"scores": {"$size": 3}}, "$size"),
            # Regex
            ({"name": {"$regex": "A.*"}}, "$regex"),
            # String operator
            ({"name": {"$contains": "li"}}, "$contains"),
        ]

        neo_results = {}
        for query, op_name in operators_to_test:
            try:
                result = list(neo_collection.find(query))
                neo_results[op_name] = len(result)
                print(f"{op_name}: {len(result)} matches")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"{op_name}: Error - {e}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        # Use fresh data for MongoDB (not the same objects as NeoSQLite)
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "scores": [80, 90, 100],
                    "dept": "Engineering",
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "scores": [70, 80],
                    "dept": "Marketing",
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "scores": [90, 95],
                    "dept": "Engineering",
                },
                {
                    "name": "David",
                    "age": 28,
                    "scores": [85],
                    "dept": "Marketing",
                },
                {
                    "name": "Eve",
                    "age": 32,
                    "scores": [88, 92, 96],
                    "dept": "HR",
                },
            ]
        )

        mongo_results = {}
        for query, op_name in operators_to_test:
            try:
                # Convert NeoSQLite operators to MongoDB equivalents if needed
                mongo_query = query.copy()
                if "$contains" in str(query):
                    # MongoDB doesn't have $contains, use $regex
                    field = list(query.keys())[0]
                    value = list(query[field].values())[0]
                    mongo_query = {field: {"$regex": value, "$options": "i"}}

                result = list(mongo_collection.find(mongo_query))
                mongo_results[op_name] = len(result)
                print(f"{op_name}: {len(result)} matches")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"{op_name}: Error - {e}")

        # Compare results
        print("\n--- Comparison ---")
        for op_name in neo_results.keys():
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")

            # Skip error cases
            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
                error = f"Neo: {neo_count}, Mongo: {mongo_count}"
            else:
                passed = neo_count == mongo_count
                error = None

            reporter.record_result(
                "Query Operators",
                op_name,
                passed,
                neo_count,
                mongo_count,
                error,
            )

        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_update_operators():
    """Compare update operators between NeoSQLite and PyMongo"""
    print("\n=== Update Operators Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # Insert test data
        neo_collection.insert_one({"name": "Alice", "age": 30, "score": 100})

        update_ops = [
            ({"$set": {"age": 31}}, "$set"),
            ({"$unset": {"score": ""}}, "$unset"),
            ({"$inc": {"age": 1}}, "$inc"),
            ({"$mul": {"age": 2}}, "$mul"),
            ({"$min": {"age": 25}}, "$min"),
            ({"$max": {"age": 50}}, "$max"),
        ]

        neo_results = {}
        for update, op_name in update_ops:
            try:
                # Reset document
                neo_collection.update_one(
                    {}, {"$set": {"age": 30, "score": 100}}
                )
                result = neo_collection.update_one({}, update)
                doc = neo_collection.find_one({})
                neo_results[op_name] = doc
                print(f"{op_name}: modified={result.modified_count}, doc={doc}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"{op_name}: Error - {e}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"name": "Alice", "age": 30, "score": 100})

        mongo_results = {}
        for update, op_name in update_ops:
            try:
                # Reset document
                mongo_collection.update_one(
                    {}, {"$set": {"age": 30, "score": 100}}
                )
                result = mongo_collection.update_one({}, update)
                doc = mongo_collection.find_one({})
                mongo_results[op_name] = doc
                print(f"{op_name}: modified={result.modified_count}, doc={doc}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"{op_name}: Error - {e}")

        # Compare results (excluding _id)
        print("\n--- Comparison ---")
        for op_name in neo_results.keys():
            neo_val = neo_results[op_name]
            mongo_val = mongo_results.get(op_name, "N/A")

            if isinstance(neo_val, str) or isinstance(mongo_val, str):
                passed = False
                error = f"Neo: {neo_val}, Mongo: {mongo_val}"
            elif neo_val is None or mongo_val is None:
                passed = neo_val == mongo_val
                error = None
            else:
                # Compare documents excluding _id
                neo_normalized = (
                    normalize_id(neo_val.copy())
                    if isinstance(neo_val, dict)
                    else neo_val
                )
                mongo_normalized = (
                    normalize_id(mongo_val.copy())
                    if isinstance(mongo_val, dict)
                    else mongo_val
                )
                passed = compare_values(neo_normalized, mongo_normalized)
                error = (
                    f"Neo: {neo_normalized}, Mongo: {mongo_normalized}"
                    if not passed
                    else None
                )

            reporter.record_result(
                "Update Operators",
                op_name,
                passed,
                neo_val,
                mongo_val,
                error,
            )

        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_aggregation_operators():
    """Compare aggregation operators between NeoSQLite and PyMongo"""
    print("\n=== Aggregation Operators Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "dept": "Engineering",
                    "salary": 50000,
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "dept": "Engineering",
                    "salary": 45000,
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "dept": "Marketing",
                    "salary": 55000,
                },
                {
                    "name": "David",
                    "age": 28,
                    "dept": "Marketing",
                    "salary": 48000,
                },
                {"name": "Eve", "age": 32, "dept": "HR", "salary": 52000},
            ]
        )

        agg_pipelines = [
            (
                [
                    {
                        "$group": {
                            "_id": "$dept",
                            "avg_salary": {"$avg": "$salary"},
                        }
                    }
                ],
                "$group with $avg",
            ),
            (
                [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}],
                "$group with $sum",
            ),
            (
                [{"$group": {"_id": "$dept", "min_sal": {"$min": "$salary"}}}],
                "$group with $min",
            ),
            (
                [{"$group": {"_id": "$dept", "max_sal": {"$max": "$salary"}}}],
                "$group with $max",
            ),
            (
                [
                    {"$group": {"_id": "$dept", "count": {"$count": {}}}},
                ],
                "$group with $count",
            ),
            (
                [
                    {"$match": {"age": {"$gte": 28}}},
                    {"$sort": {"age": DESCENDING}},
                ],
                "$match + $sort",
            ),
            (
                [
                    {"$match": {"age": {"$gte": 28}}},
                    {"$sort": {"age": DESCENDING}},
                    {"$limit": 2},
                ],
                "$match + $sort + $limit",
            ),
            (
                [{"$project": {"name": 1, "age": 1, "_id": 0}}],
                "$project",
            ),
        ]

        neo_results = {}
        for pipeline, op_name in agg_pipelines:
            try:
                result = list(neo_collection.aggregate(pipeline))
                neo_results[op_name] = len(result)
                print(f"{op_name}: {len(result)} results")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"{op_name}: Error - {e}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        # Use fresh data for MongoDB (not the same objects as NeoSQLite)
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "dept": "Engineering",
                    "salary": 50000,
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "dept": "Engineering",
                    "salary": 45000,
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "dept": "Marketing",
                    "salary": 55000,
                },
                {
                    "name": "David",
                    "age": 28,
                    "dept": "Marketing",
                    "salary": 48000,
                },
                {"name": "Eve", "age": 32, "dept": "HR", "salary": 52000},
            ]
        )

        mongo_results = {}
        for pipeline, op_name in agg_pipelines:
            try:
                result = list(mongo_collection.aggregate(pipeline))
                mongo_results[op_name] = len(result)
                print(f"{op_name}: {len(result)} results")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"{op_name}: Error - {e}")

        # Compare results
        print("\n--- Comparison ---")
        for op_name in neo_results.keys():
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")

            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
                error = f"Neo: {neo_count}, Mongo: {mongo_count}"
            else:
                passed = neo_count == mongo_count
                error = None

            reporter.record_result(
                "Aggregation Operators",
                op_name,
                passed,
                neo_count,
                mongo_count,
                error,
            )

        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def compare_distinct_operations():
    """Compare distinct operations between NeoSQLite and PyMongo"""
    print("\n=== Distinct Operations Comparison ===")

    # Test with NeoSQLite
    print("\n--- NeoSQLite ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "Alice", "dept": "Engineering"},
                {"name": "Bob", "dept": "Engineering"},
                {"name": "Charlie", "dept": "Marketing"},
                {"name": "David", "dept": "Marketing"},
                {"name": "Eve", "dept": "HR"},
            ]
        )

        neo_distinct = neo_collection.distinct("dept")
        print(f"distinct('dept'): {sorted(neo_distinct)}")

    # Test with PyMongo
    print("\n--- PyMongo ---")
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        # Use fresh data for MongoDB
        mongo_collection.insert_many(
            [
                {"name": "Alice", "dept": "Engineering"},
                {"name": "Bob", "dept": "Engineering"},
                {"name": "Charlie", "dept": "Marketing"},
                {"name": "David", "dept": "Marketing"},
                {"name": "Eve", "dept": "HR"},
            ]
        )

        mongo_distinct = mongo_collection.distinct("dept")
        print(f"distinct('dept'): {sorted(mongo_distinct)}")

        # Compare results
        passed = set(neo_distinct) == set(mongo_distinct)
        reporter.record_result(
            "Distinct Operations",
            "distinct",
            passed,
            sorted(neo_distinct),
            sorted(mongo_distinct),
            None if passed else "Results differ",
        )

        client.close()
    else:
        print("Skipping PyMongo tests due to connection failure")


def main():
    """Main function to run all comparisons"""
    print("NeoSQLite vs PyMongo API Comparison")
    print("=" * 80)

    compare_crud_operations()
    compare_index_operations()
    compare_aggregation_operations()
    compare_find_and_modify_operations()
    compare_bulk_operations()
    compare_query_operators()
    compare_update_operators()
    compare_aggregation_operators()
    compare_distinct_operations()

    # Print final report
    reporter.print_report()

    # Exit with appropriate code
    if len(reporter.failed_tests) > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
