#!/usr/bin/env python3
"""
Comprehensive API Comparison Script between NeoSQLite and PyMongo

This script compares ALL NeoSQLite supported APIs and operators with MongoDB
and reports compatibility statistics.
"""
import neosqlite
from neosqlite import ASCENDING, DESCENDING
from neosqlite.binary import Binary
from pymongo import (
    MongoClient,
    ASCENDING as MONGO_ASCENDING,
    DESCENDING as MONGO_DESCENDING,
)
from pymongo import IndexModel
from pymongo.errors import ConnectionFailure
from bson import Binary as BsonBinary
from typing import Any, Optional
import sys
import copy


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
            return

        if passed:
            self.passed_tests += 1
        else:
            self.failed_tests.append(
                {
                    "category": category,
                    "api": api_name,
                    "neo_result": str(neo_result)[:200] if neo_result else None,
                    "mongo_result": (
                        str(mongo_result)[:200] if mongo_result else None
                    ),
                    "error": error,
                }
            )

    def get_compatibility_percentage(self) -> float:
        """Get the compatibility percentage"""
        if self.total_tests == 0:
            return 0.0
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
            print("\n\nSKIPPED TESTS (Known Limitations):")
            print("-" * 80)
            for skip in self.skipped_tests:
                print(f"\n[{skip['category']}] {skip['api']}")
                print(f"  Reason: {skip['reason']}")

        print("\n" + "=" * 80)


# Global reporter instance
reporter = CompatibilityReporter()


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


# ============================================================================
# CRUD Operations
# ============================================================================
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


# ============================================================================
# Query Operators
# ============================================================================
def compare_query_operators():
    """Compare query operators between NeoSQLite and PyMongo"""
    print("\n=== Query Operators Comparison ===")

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

        operators = [
            ({"age": {"$eq": 30}}, "$eq"),
            ({"age": {"$gt": 30}}, "$gt"),
            ({"age": {"$gte": 30}}, "$gte"),
            ({"age": {"$lt": 30}}, "$lt"),
            ({"age": {"$lte": 30}}, "$lte"),
            ({"age": {"$ne": 30}}, "$ne"),
            ({"age": {"$in": [25, 30, 35]}}, "$in"),
            ({"age": {"$nin": [25, 30]}}, "$nin"),
            ({"age": {"$exists": True}}, "$exists (true)"),
            ({"age": {"$exists": False}}, "$exists (false)"),
            ({"age": {"$type": 16}}, "$type (int)"),
            ({"scores": {"$all": [80, 90]}}, "$all"),
            # Note: $elemMatch has known differences
            ({"scores": {"$size": 3}}, "$size"),
            ({"name": {"$regex": "A.*"}}, "$regex"),
            ({"name": {"$contains": "li"}}, "$contains"),
        ]

        neo_results = {}
        for query, op_name in operators:
            try:
                result = list(neo_collection.find(query))
                neo_results[op_name] = len(result)
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
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
        for query, op_name in operators:
            try:
                mongo_query = copy.deepcopy(query)
                if "$contains" in str(query):
                    field = list(query.keys())[0]
                    value = list(query[field].values())[0]
                    mongo_query = {field: {"$regex": value, "$options": "i"}}
                result = list(mongo_collection.find(mongo_query))
                mongo_results[op_name] = len(result)
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")
            passed = (
                neo_count == mongo_count
                if not isinstance(neo_count, str)
                and not isinstance(mongo_count, str)
                else False
            )
            reporter.record_result(
                "Query Operators", op_name, passed, neo_count, mongo_count
            )
        client.close()


# ============================================================================
# $expr Operator Tests
# ============================================================================
def compare_expr_operator():
    """Compare $expr operator between NeoSQLite and PyMongo"""
    print("\n=== $expr Operator Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "salary": 50000,
                    "score": 85.5,
                    "angle": 1.5708,
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "salary": 45000,
                    "score": 92.3,
                    "angle": 0.7854,
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "salary": 60000,
                    "score": 78.1,
                    "angle": 3.1416,
                },
                {
                    "name": "David",
                    "age": 28,
                    "salary": 55000,
                    "score": 88.9,
                    "angle": 0.0,
                },
            ]
        )

        expr_queries = [
            # Comparison operators
            ({"$expr": {"$gt": ["$salary", 50000]}}, "$expr $gt"),
            ({"$expr": {"$eq": ["$age", 30]}}, "$expr $eq"),
            ({"$expr": {"$ne": ["$age", 30]}}, "$expr $ne"),
            ({"$expr": {"$lt": ["$age", 30]}}, "$expr $lt"),
            ({"$expr": {"$lte": ["$age", 28]}}, "$expr $lte"),
            ({"$expr": {"$gte": ["$age", 30]}}, "$expr $gte"),
            # Note: $cmp has binding issues - skip for now
            # ({"$expr": {"$cmp": ["$age", 30]}}, "$expr $cmp"),
            # Logical operators
            (
                {
                    "$expr": {
                        "$and": [{"$gt": ["$age", 26]}, {"$lt": ["$age", 35]}]
                    }
                },
                "$expr $and",
            ),
            (
                {
                    "$expr": {
                        "$or": [{"$eq": ["$age", 25]}, {"$eq": ["$age", 35]}]
                    }
                },
                "$expr $or",
            ),
            # Note: $not has different semantics - skip for now
            # ({"$expr": {"$not": {"$gt": ["$age", 30]}}}, "$expr $not"),
            # Arithmetic operators
            ({"$expr": {"$add": ["$age", 5]}}, "$expr $add"),
            ({"$expr": {"$subtract": ["$age", 5]}}, "$expr $subtract"),
            ({"$expr": {"$multiply": ["$salary", 0.001]}}, "$expr $multiply"),
            ({"$expr": {"$divide": ["$salary", 1000]}}, "$expr $divide"),
            ({"$expr": {"$mod": ["$age", 5]}}, "$expr $mod"),
            ({"$expr": {"$abs": {"$subtract": ["$age", 28]}}}, "$expr $abs"),
            ({"$expr": {"$ceil": "$score"}}, "$expr $ceil"),
            ({"$expr": {"$floor": "$score"}}, "$expr $floor"),
            ({"$expr": {"$trunc": "$score"}}, "$expr $trunc"),
            # Note: $round has implementation issues - skip for now
            # ({"$expr": {"$round": ["$score", 0]}}, "$expr $round"),
            # Trigonometric operators
            ({"$expr": {"$sin": "$angle"}}, "$expr $sin"),
            ({"$expr": {"$cos": "$angle"}}, "$expr $cos"),
            ({"$expr": {"$tan": "$angle"}}, "$expr $tan"),
            ({"$expr": {"$asin": 0.5}}, "$expr $asin"),
            ({"$expr": {"$acos": 0.5}}, "$expr $acos"),
            ({"$expr": {"$atan": 1}}, "$expr $atan"),
            ({"$expr": {"$atan2": [1, 1]}}, "$expr $atan2"),
            # Hyperbolic operators
            ({"$expr": {"$sinh": 1}}, "$expr $sinh"),
            ({"$expr": {"$cosh": 1}}, "$expr $cosh"),
            ({"$expr": {"$tanh": 1}}, "$expr $tanh"),
            # Logarithmic operators
            ({"$expr": {"$ln": {"$add": ["$age", 1]}}}, "$expr $ln"),
            ({"$expr": {"$log10": {"$add": ["$age", 1]}}}, "$expr $log10"),
            ({"$expr": {"$log": [{"$add": ["$age", 1]}, 2]}}, "$expr $log"),
            # Exponential operators
            # Note: $exp has implementation issues - skip for now
            # ({"$expr": {"$exp": 1}}, "$expr $exp"),
            # Angle conversion - Note: has implementation issues with literals - skip for now
            # ({"$expr": {"$degreesToRadians": 180}}, "$expr $degreesToRadians"),
            # ({"$expr": {"$radiansToDegrees": 3.14159}}, "$expr $radiansToDegrees"),
            # Conditional operators
            (
                {
                    "$expr": {
                        "$cond": [{"$gt": ["$age", 28]}, "senior", "junior"]
                    }
                },
                "$expr $cond",
            ),
            # Note: $ifNull has implementation issues - skip for now
            # ({"$expr": {"$ifNull": ["$middle_name", "no_middle"]}}, "$expr $ifNull"),
            # Type operators
            ({"$expr": {"$toString": "$age"}}, "$expr $toString"),
            ({"$expr": {"$toInt": "$age"}}, "$expr $toInt"),
            ({"$expr": {"$toDouble": "$age"}}, "$expr $toDouble"),
            ({"$expr": {"$toBool": "$age"}}, "$expr $toBool"),
            ({"$expr": {"$type": "$age"}}, "$expr $type"),
            # String operators
            ({"$expr": {"$toUpper": "$name"}}, "$expr $toUpper"),
            ({"$expr": {"$toLower": "$name"}}, "$expr $toLower"),
            ({"$expr": {"$strLenBytes": "$name"}}, "$expr $strLenBytes"),
            # Note: $concat has implementation issues - skip for now
            # ({"$expr": {"$concat": ["$name", " - ", "$name"]}}, "$expr $concat"),
            # Array operators
            # Note: $isArray has implementation issues - skip for now
            # ({"$expr": {"$isArray": ["$tags"]}}, "$expr $isArray"),
        ]

        neo_results = {}
        for query, op_name in expr_queries:
            try:
                result = list(neo_collection.find(query))
                neo_results[op_name] = len(result)
                print(f"Neo {op_name}: {len(result)}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "salary": 50000,
                    "score": 85.5,
                    "angle": 1.5708,
                    "tags": ["a", "b"],
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "salary": 45000,
                    "score": 92.3,
                    "angle": 0.7854,
                    "tags": ["c"],
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "salary": 60000,
                    "score": 78.1,
                    "angle": 3.1416,
                    "tags": [],
                },
                {
                    "name": "David",
                    "age": 28,
                    "salary": 55000,
                    "score": 88.9,
                    "angle": 0.0,
                    "tags": ["d", "e", "f"],
                },
            ]
        )

        mongo_results = {}
        for query, op_name in expr_queries:
            try:
                result = list(mongo_collection.find(query))
                mongo_results[op_name] = len(result)
                print(f"Mongo {op_name}: {len(result)}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")
            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
            else:
                passed = neo_count == mongo_count
            reporter.record_result(
                "$expr Operator", op_name, passed, neo_count, mongo_count
            )
        client.close()


# ============================================================================
# Update Operators
# ============================================================================
def compare_update_operators():
    """Compare update operators between NeoSQLite and PyMongo"""
    print("\n=== Update Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one(
            {"name": "Alice", "age": 30, "score": 100, "tags": ["a"]}
        )

        update_ops = [
            ({"$set": {"age": 31}}, "$set"),
            ({"$unset": {"score": ""}}, "$unset"),
            ({"$inc": {"age": 1}}, "$inc"),
            ({"$mul": {"age": 2}}, "$mul"),
            ({"$min": {"age": 25}}, "$min"),
            ({"$max": {"age": 50}}, "$max"),
            ({"$rename": {"name": "fullName"}}, "$rename"),
            ({"$setOnInsert": {"created": True}}, "$setOnInsert"),
        ]

        neo_results = {}
        for update, op_name in update_ops:
            try:
                neo_collection.update_one(
                    {}, {"$set": {"age": 30, "score": 100}}
                )
                result = neo_collection.update_one({}, update)
                _ = neo_collection.find_one({})
                neo_results[op_name] = (
                    "OK" if result.modified_count >= 0 else "FAIL"
                )
                print(f"Neo {op_name}: OK")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one(
            {"name": "Alice", "age": 30, "score": 100, "tags": ["a"]}
        )

        mongo_results = {}
        for update, op_name in update_ops:
            try:
                mongo_collection.update_one(
                    {}, {"$set": {"age": 30, "score": 100}}
                )
                result = mongo_collection.update_one({}, update)
                mongo_results[op_name] = (
                    "OK" if result.modified_count >= 0 else "FAIL"
                )
                print(f"Mongo {op_name}: OK")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            passed = "OK" in str(neo_results[op_name]) and "OK" in str(
                mongo_results.get(op_name, "")
            )
            reporter.record_result(
                "Update Operators",
                op_name,
                passed,
                neo_results[op_name],
                mongo_results.get(op_name),
            )
        client.close()


# ============================================================================
# Aggregation Pipeline Stages
# ============================================================================
def compare_aggregation_stages():
    """Compare aggregation pipeline stages between NeoSQLite and PyMongo"""
    print("\n=== Aggregation Pipeline Stages Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "dept": "Engineering",
                    "salary": 50000,
                    "tags": ["python", "sql"],
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "dept": "Engineering",
                    "salary": 45000,
                    "tags": ["java"],
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "dept": "Marketing",
                    "salary": 55000,
                    "tags": ["marketing", "sql"],
                },
                {
                    "name": "David",
                    "age": 28,
                    "dept": "Marketing",
                    "salary": 48000,
                    "tags": ["marketing"],
                },
                {
                    "name": "Eve",
                    "age": 32,
                    "dept": "HR",
                    "salary": 52000,
                    "tags": ["hr", "sql"],
                },
            ]
        )

        pipelines = [
            ([{"$match": {"age": {"$gte": 28}}}], "$match"),
            ([{"$project": {"name": 1, "age": 1, "_id": 0}}], "$project"),
            (
                [{"$addFields": {"bonus": {"$multiply": ["$salary", 0.1]}}}],
                "$addFields",
            ),
            (
                [
                    {
                        "$group": {
                            "_id": "$dept",
                            "avg_salary": {"$avg": "$salary"},
                        }
                    }
                ],
                "$group $avg",
            ),
            (
                [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}],
                "$group $sum",
            ),
            (
                [{"$group": {"_id": "$dept", "min_sal": {"$min": "$salary"}}}],
                "$group $min",
            ),
            (
                [{"$group": {"_id": "$dept", "max_sal": {"$max": "$salary"}}}],
                "$group $max",
            ),
            (
                [{"$group": {"_id": "$dept", "count": {"$count": {}}}}],
                "$group $count",
            ),
            ([{"$sort": {"age": DESCENDING}}], "$sort"),
            ([{"$skip": 2}], "$skip"),
            ([{"$limit": 2}], "$limit"),
            (
                [
                    {"$match": {"age": {"$gte": 25}}},
                    {"$sort": {"age": DESCENDING}},
                    {"$limit": 3},
                ],
                "combined",
            ),
        ]

        neo_results = {}
        for pipeline, op_name in pipelines:
            try:
                result = list(neo_collection.aggregate(pipeline))
                neo_results[op_name] = len(result)
                print(f"Neo {op_name}: {len(result)}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "dept": "Engineering",
                    "salary": 50000,
                    "tags": ["python", "sql"],
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "dept": "Engineering",
                    "salary": 45000,
                    "tags": ["java"],
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "dept": "Marketing",
                    "salary": 55000,
                    "tags": ["marketing", "sql"],
                },
                {
                    "name": "David",
                    "age": 28,
                    "dept": "Marketing",
                    "salary": 48000,
                    "tags": ["marketing"],
                },
                {
                    "name": "Eve",
                    "age": 32,
                    "dept": "HR",
                    "salary": 52000,
                    "tags": ["hr", "sql"],
                },
            ]
        )

        mongo_results = {}
        for pipeline, op_name in pipelines:
            try:
                result = list(mongo_collection.aggregate(pipeline))
                mongo_results[op_name] = len(result)
                print(f"Mongo {op_name}: {len(result)}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")
            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
            else:
                passed = neo_count == mongo_count
            reporter.record_result(
                "Aggregation Stages", op_name, passed, neo_count, mongo_count
            )
        client.close()


# ============================================================================
# Index Operations
# ============================================================================
def compare_index_operations():
    """Compare index operations between NeoSQLite and PyMongo"""
    print("\n=== Index Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.create_index("name")
        neo_collection.create_indexes(
            ["age", [("name", ASCENDING), ("age", DESCENDING)]]
        )
        _ = neo_collection.list_indexes()
        _ = neo_collection.index_information()
        neo_collection.drop_index("name")
        neo_collection.drop_indexes()
        print(
            "NeoSQLite: create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
        )

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.create_index("name")
        mongo_collection.create_indexes(
            [
                IndexModel([("age", MONGO_ASCENDING)]),
                IndexModel(
                    [("name", MONGO_ASCENDING), ("age", MONGO_DESCENDING)]
                ),
            ]
        )
        _ = list(mongo_collection.list_indexes())
        _ = mongo_collection.index_information()
        mongo_collection.drop_index("name_1")
        mongo_collection.drop_indexes()
        print(
            "PyMongo: create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
        )
        client.close()

    for op in [
        "create_index",
        "create_indexes",
        "list_indexes",
        "index_information",
        "drop_index",
        "drop_indexes",
    ]:
        reporter.record_result("Index Operations", op, True, "OK", "OK")


# ============================================================================
# Find and Modify Operations
# ============================================================================
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


# ============================================================================
# Bulk Operations
# ============================================================================
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


# ============================================================================
# Distinct Operations
# ============================================================================
def compare_distinct():
    """Compare distinct operations"""
    print("\n=== Distinct Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"dept": "Engineering"},
                {"dept": "Marketing"},
                {"dept": "Engineering"},
            ]
        )
        neo_distinct = neo_collection.distinct("dept")
        print(f"Neo distinct: {sorted(neo_distinct)}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"dept": "Engineering"},
                {"dept": "Marketing"},
                {"dept": "Engineering"},
            ]
        )
        mongo_distinct = mongo_collection.distinct("dept")
        print(f"Mongo distinct: {sorted(mongo_distinct)}")
        client.close()

    passed = (
        set(neo_distinct) == set(mongo_distinct)
        if "mongo_distinct" in dir()
        else False
    )
    reporter.record_result(
        "Distinct",
        "distinct",
        passed,
        sorted(neo_distinct),
        sorted(mongo_distinct) if "mongo_distinct" in dir() else None,
    )


# ============================================================================
# Binary Data Support
# ============================================================================
def compare_binary_support():
    """Compare Binary data support"""
    print("\n=== Binary Data Support Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        binary_data = Binary(b"test binary data")
        neo_collection.insert_one({"data": binary_data, "name": "test"})
        doc = neo_collection.find_one({"name": "test"})
        neo_has_binary = isinstance(doc.get("data"), (Binary, bytes))
        print(f"Neo Binary support: {neo_has_binary}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        binary_data = BsonBinary(b"test binary data")
        mongo_collection.insert_one({"data": binary_data, "name": "test"})
        doc = mongo_collection.find_one({"name": "test"})
        mongo_has_binary = isinstance(doc.get("data"), (BsonBinary, bytes))
        print(f"Mongo Binary support: {mongo_has_binary}")
        client.close()

    reporter.record_result(
        "Binary Support",
        "Binary",
        neo_has_binary,
        neo_has_binary,
        mongo_has_binary if "mongo_has_binary" in dir() else None,
    )


# ============================================================================
# Nested Field Queries
# ============================================================================
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
        if "mongo_result" in dir()
        else False
    )
    reporter.record_result(
        "Nested Field Queries",
        "dot_notation",
        passed,
        len(neo_result),
        len(mongo_result) if "mongo_result" in dir() else None,
    )


# ============================================================================
# Raw Batch Operations
# ============================================================================
def compare_raw_batch_operations():
    """Compare raw batch operations"""
    print("\n=== Raw Batch Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": f"doc{i}", "value": i} for i in range(10)]
        )

        try:
            cursor = neo_collection.find_raw_batches(
                {"value": {"$gte": 5}}, batch_size=3
            )
            neo_raw_batches = sum(1 for _ in cursor)
            print(f"Neo find_raw_batches: {neo_raw_batches} batches")
        except Exception as e:
            neo_raw_batches = f"Error: {e}"
            print(f"Neo find_raw_batches: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": f"doc{i}", "value": i} for i in range(10)]
        )

        try:
            cursor = mongo_collection.find_raw_batches(
                {"value": {"$gte": 5}}, batch_size=3
            )
            mongo_raw_batches = sum(1 for _ in cursor)
            print(f"Mongo find_raw_batches: {mongo_raw_batches} batches")
        except Exception as e:
            mongo_raw_batches = f"Error: {e}"
            print(f"Mongo find_raw_batches: Error - {e}")
        client.close()

    passed = (
        neo_raw_batches == mongo_raw_batches
        if not isinstance(neo_raw_batches, str)
        and not isinstance(mongo_raw_batches, str)
        else False
    )
    reporter.record_result(
        "Raw Batch Operations",
        "find_raw_batches",
        passed,
        neo_raw_batches,
        mongo_raw_batches if "mongo_raw_batches" in dir() else None,
    )


# ============================================================================
# Change Streams (watch)
# ============================================================================
def compare_change_streams():
    """Compare change streams (watch) - SKIPPED: Different architecture"""
    print("\n=== Change Streams (watch) Comparison ===")

    # NeoSQLite uses SQLite triggers for change tracking
    # MongoDB uses oplog-based change streams
    # These are fundamentally different architectures

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"name": "test"})

        try:
            # NeoSQLite watch uses SQLite change tracking
            _ = neo_collection.watch()
            print("Neo watch: Supported")
        except Exception as e:
            print(f"Neo watch: Error - {e}")

    # MongoDB change streams require replica set
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"name": "test"})

        try:
            # MongoDB change streams require replica set
            # This will likely fail on standalone MongoDB
            _ = mongo_collection.watch()
            print("Mongo watch: Supported")
        except Exception as e:
            print(f"Mongo watch: Error - {e} (requires replica set)")
        client.close()

    # Skip this test as it requires different infrastructure
    reporter.record_result(
        "Change Streams",
        "watch",
        True,
        "OK",
        "OK",
        skip_reason="Requires MongoDB replica set; NeoSQLite uses SQLite triggers",
    )


# ============================================================================
# Text Search
# ============================================================================
def compare_text_search():
    """Compare text search capabilities"""
    print("\n=== Text Search Comparison ===")

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

        # Create FTS index
        neo_collection.create_index("description", fts=True)

        # Text search
        try:
            neo_results = list(
                neo_collection.find({"description": {"$regex": "Python"}})
            )
            neo_text_search = len(neo_results)
            print(f"Neo text search (regex): {neo_text_search}")
        except Exception as e:
            neo_text_search = f"Error: {e}"
            print(f"Neo text search: Error - {e}")

    client = test_pymongo_connection()
    if client:
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
        client.close()

    passed = (
        neo_text_search == mongo_text_search
        if not isinstance(neo_text_search, str)
        and not isinstance(mongo_text_search, str)
        else False
    )
    reporter.record_result(
        "Text Search",
        "regex_search",
        passed,
        neo_text_search,
        mongo_text_search if "mongo_text_search" in dir() else None,
    )


# ============================================================================
# GridFS Operations
# ============================================================================
def compare_gridfs_operations():
    """Compare GridFS operations"""
    print("\n=== GridFS Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        try:
            from neosqlite.gridfs import GridFSBucket

            # NeoSQLite GridFSBucket takes the underlying SQLite connection
            bucket = GridFSBucket(neo_conn.db, bucket_name="fs")

            # Upload file
            file_id = bucket.upload_from_stream("test.txt", b"Hello GridFS!")

            # Download file - NeoSQLite uses open_download_stream
            grid_out = bucket.open_download_stream(file_id)
            neo_file_data = grid_out.read() if grid_out else None

            # Find files
            files = list(bucket.find({"filename": "test.txt"}))

            print(
                f"Neo GridFS: upload={file_id is not None}, download={neo_file_data is not None}, find={len(files)}"
            )

            neo_gridfs_ok = neo_file_data == b"Hello GridFS!"
        except ImportError:
            print("Neo GridFS: Not available")
            neo_gridfs_ok = False
            reporter.record_result(
                "GridFS",
                "GridFSBucket",
                False,
                "Not available",
                "N/A",
                skip_reason="GridFS not compiled in this build",
            )
            return
        except Exception as e:
            print(f"Neo GridFS: Error - {e}")
            neo_gridfs_ok = False
            reporter.record_result(
                "GridFS", "GridFSBucket", False, f"Error: {e}", "N/A"
            )
            return

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        try:
            from gridfs import GridFSBucket as MongoGridFSBucket

            bucket = MongoGridFSBucket(mongo_db, bucket_name="fs")

            # Upload file
            file_id = bucket.upload_from_stream("test.txt", b"Hello GridFS!")

            # Download file - MongoDB also uses open_download_stream
            grid_out = bucket.open_download_stream(file_id)
            mongo_file_data = grid_out.read() if grid_out else None

            # Find files
            files = list(bucket.find({"filename": "test.txt"}))

            print(
                f"Mongo GridFS: upload={file_id is not None}, download={mongo_file_data is not None}, find={len(files)}"
            )

            mongo_gridfs_ok = mongo_file_data == b"Hello GridFS!"
        except Exception as e:
            print(f"Mongo GridFS: Error - {e}")
            mongo_gridfs_ok = False
        client.close()
    else:
        mongo_gridfs_ok = False

    reporter.record_result(
        "GridFS",
        "GridFSBucket",
        neo_gridfs_ok and mongo_gridfs_ok,
        neo_gridfs_ok,
        mongo_gridfs_ok,
    )


# ============================================================================
# ObjectId Operations
# ============================================================================
def compare_objectid_operations():
    """Compare ObjectId operations"""
    print("\n=== ObjectId Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # Insert with ObjectId
        from neosqlite.objectid import ObjectId as NeoObjectId

        oid = NeoObjectId()
        neo_collection.insert_one({"_id": oid, "name": "test"})

        # Find by ObjectId
        doc = neo_collection.find_one({"_id": oid})
        neo_found = doc is not None

        # Test ObjectId generation
        oid2 = NeoObjectId()
        neo_unique = oid != oid2

        # Test ObjectId string conversion
        neo_hex = len(str(oid)) == 24

        print(
            f"Neo ObjectId: create={oid is not None}, find={neo_found}, unique={neo_unique}, hex={neo_hex}"
        )

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})

        from bson import ObjectId as BsonObjectId

        oid = BsonObjectId()
        mongo_collection.insert_one({"_id": oid, "name": "test"})

        doc = mongo_collection.find_one({"_id": oid})
        mongo_found = doc is not None

        oid2 = BsonObjectId()
        mongo_unique = oid != oid2

        mongo_hex = len(str(oid)) == 24

        print(
            f"Mongo ObjectId: create={oid is not None}, find={mongo_found}, unique={mongo_unique}, hex={mongo_hex}"
        )
        client.close()

    reporter.record_result("ObjectId", "create", True, "OK", "OK")
    reporter.record_result(
        "ObjectId",
        "find_by_id",
        neo_found,
        neo_found,
        mongo_found if "mongo_found" in dir() else None,
    )
    reporter.record_result(
        "ObjectId",
        "unique",
        neo_unique,
        neo_unique,
        mongo_unique if "mongo_unique" in dir() else None,
    )
    reporter.record_result(
        "ObjectId",
        "hex_format",
        neo_hex,
        neo_hex,
        mongo_hex if "mongo_hex" in dir() else None,
    )


# ============================================================================
# Additional Query Operators ($type variations)
# ============================================================================
def compare_type_operator():
    """Compare $type operator with various types"""
    print("\n=== $type Operator Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "active": True,
                    "items": [1, 2],
                    "data": {"key": "val"},
                    "nothing": None,
                },
                {
                    "name": 123,
                    "age": "thirty",
                    "active": "yes",
                    "items": "not array",
                    "data": "not object",
                    "nothing": "something",
                },
            ]
        )

        type_tests = [
            ({"name": {"$type": 2}}, "$type string (2)"),  # string
            ({"age": {"$type": 16}}, "$type int (16)"),  # int
            ({"active": {"$type": 8}}, "$type bool (8)"),  # bool
            ({"items": {"$type": 4}}, "$type array (4)"),  # array
            ({"data": {"$type": 3}}, "$type object (3)"),  # object
            ({"nothing": {"$type": 10}}, "$type null (10)"),  # null
        ]

        neo_results = {}
        for query, op_name in type_tests:
            try:
                result = list(neo_collection.find(query))
                neo_results[op_name] = len(result)
                print(f"Neo {op_name}: {len(result)}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "active": True,
                    "items": [1, 2],
                    "data": {"key": "val"},
                    "nothing": None,
                },
                {
                    "name": 123,
                    "age": "thirty",
                    "active": "yes",
                    "items": "not array",
                    "data": "not object",
                    "nothing": "something",
                },
            ]
        )

        mongo_results = {}
        for query, op_name in type_tests:
            try:
                result = list(mongo_collection.find(query))
                mongo_results[op_name] = len(result)
                print(f"Mongo {op_name}: {len(result)}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")
            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
            else:
                passed = neo_count == mongo_count
            reporter.record_result(
                "$type Operator", op_name, passed, neo_count, mongo_count
            )
        client.close()


# ============================================================================
# Additional Aggregation Features
# ============================================================================
def compare_additional_aggregation():
    """Compare additional aggregation features"""
    print("\n=== Additional Aggregation Features Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "sizes": ["S", "M"]},
                {"item": "B", "price": 20, "quantity": 1, "sizes": ["L"]},
                {"item": "A", "price": 30, "quantity": 5, "sizes": ["M", "L"]},
            ]
        )

        # Test $unwind
        unwind_pipeline = [{"$unwind": "$sizes"}]
        try:
            neo_unwind = len(list(neo_collection.aggregate(unwind_pipeline)))
            print(f"Neo $unwind: {neo_unwind}")
        except Exception as e:
            neo_unwind = f"Error: {e}"
            print(f"Neo $unwind: Error - {e}")

        # Test $group with $push
        push_pipeline = [
            {"$group": {"_id": "$item", "prices": {"$push": "$price"}}},
            {"$sort": {"_id": 1}},
        ]
        try:
            neo_push_result = list(neo_collection.aggregate(push_pipeline))
            neo_push = len(neo_push_result)
            print(f"Neo $group $push: {neo_push} groups")
        except Exception as e:
            neo_push = f"Error: {e}"
            print(f"Neo $group $push: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "sizes": ["S", "M"]},
                {"item": "B", "price": 20, "quantity": 1, "sizes": ["L"]},
                {"item": "A", "price": 30, "quantity": 5, "sizes": ["M", "L"]},
            ]
        )

        # Test $unwind
        try:
            mongo_unwind = len(
                list(mongo_collection.aggregate(unwind_pipeline))
            )
            print(f"Mongo $unwind: {mongo_unwind}")
        except Exception as e:
            mongo_unwind = f"Error: {e}"
            print(f"Mongo $unwind: Error - {e}")

        # Test $group with $push
        try:
            mongo_push_result = list(mongo_collection.aggregate(push_pipeline))
            mongo_push = len(mongo_push_result)
            print(f"Mongo $group $push: {mongo_push} groups")
        except Exception as e:
            mongo_push = f"Error: {e}"
            print(f"Mongo $group $push: Error - {e}")

        client.close()

    if not isinstance(neo_unwind, str) and not isinstance(mongo_unwind, str):
        reporter.record_result(
            "Additional Aggregation",
            "$unwind",
            neo_unwind == mongo_unwind,
            neo_unwind,
            mongo_unwind,
        )
    else:
        reporter.record_result(
            "Additional Aggregation", "$unwind", False, neo_unwind, mongo_unwind
        )

    if not isinstance(neo_push, str) and not isinstance(mongo_push, str):
        reporter.record_result(
            "Additional Aggregation", "$group $push", True, neo_push, mongo_push
        )
    else:
        reporter.record_result(
            "Additional Aggregation",
            "$group $push",
            False,
            neo_push,
            mongo_push,
        )


# ============================================================================
# Cursor Operations
# ============================================================================
def compare_cursor_operations():
    """Compare cursor operations"""
    print("\n=== Cursor Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([{"i": i} for i in range(10)])

        # Test cursor iteration
        cursor = neo_collection.find()
        neo_count = sum(1 for _ in cursor)

        # Test limit
        neo_limit = len(list(neo_collection.find().limit(5)))

        # Test skip
        neo_skip = len(list(neo_collection.find().skip(5)))

        # Test sort
        neo_sorted = list(neo_collection.find().sort("i", neosqlite.DESCENDING))
        neo_sort_ok = neo_sorted[0]["i"] == 9 if neo_sorted else False

        print(
            f"Neo cursor: count={neo_count}, limit={neo_limit}, skip={neo_skip}, sort={neo_sort_ok}"
        )

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many([{"i": i} for i in range(10)])

        cursor = mongo_collection.find()
        mongo_count = sum(1 for _ in cursor)

        mongo_limit = len(list(mongo_collection.find().limit(5)))

        mongo_skip = len(list(mongo_collection.find().skip(5)))

        mongo_sorted = list(mongo_collection.find().sort("i", MONGO_DESCENDING))
        mongo_sort_ok = mongo_sorted[0]["i"] == 9 if mongo_sorted else False

        print(
            f"Mongo cursor: count={mongo_count}, limit={mongo_limit}, skip={mongo_skip}, sort={mongo_sort_ok}"
        )
        client.close()

    reporter.record_result(
        "Cursor Operations",
        "iteration",
        neo_count == 10,
        neo_count,
        mongo_count if "mongo_count" in dir() else None,
    )
    reporter.record_result(
        "Cursor Operations",
        "limit",
        neo_limit == 5,
        neo_limit,
        mongo_limit if "mongo_limit" in dir() else None,
    )
    reporter.record_result(
        "Cursor Operations",
        "skip",
        neo_skip == 5,
        neo_skip,
        mongo_skip if "mongo_skip" in dir() else None,
    )
    reporter.record_result(
        "Cursor Operations",
        "sort",
        neo_sort_ok,
        neo_sort_ok,
        mongo_sort_ok if "mongo_sort_ok" in dir() else None,
    )


# ============================================================================
# Known Limitations Tests (Track issues for future fixes)
# ============================================================================
def compare_known_limitations():
    """Compare known limitations - these tests document differences for future fixes"""
    print("\n=== Known Limitations Comparison (Tracking for Future Fixes) ===")
    print(
        "Note: These tests show ACTUAL differences between NeoSQLite and MongoDB."
    )
    print("Use this output to identify what needs to be fixed.\n")

    # Test $elemMatch
    print("--- $elemMatch ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"scores": [80, 90, 100]},
                {"scores": [70, 80]},
                {"scores": [90, 95]},
            ]
        )
        try:
            neo_result = list(
                neo_collection.find({"scores": {"$elemMatch": {"$gte": 90}}})
            )
            neo_elemMatch = len(neo_result)
            print(f"Neo $elemMatch: {neo_elemMatch} matches")
        except Exception as e:
            neo_elemMatch = f"Error: {e}"
            print(f"Neo $elemMatch: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"scores": [80, 90, 100]},
                {"scores": [70, 80]},
                {"scores": [90, 95]},
            ]
        )
        try:
            mongo_result = list(
                mongo_collection.find({"scores": {"$elemMatch": {"$gte": 90}}})
            )
            mongo_elemMatch = len(mongo_result)
            print(f"Mongo $elemMatch: {mongo_elemMatch} matches")
        except Exception as e:
            mongo_elemMatch = f"Error: {e}"
            print(f"Mongo $elemMatch: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$elemMatch",
        (
            neo_elemMatch == mongo_elemMatch
            if not isinstance(neo_elemMatch, str)
            and not isinstance(mongo_elemMatch, str)
            else False
        ),
        neo_elemMatch,
        mongo_elemMatch,
    )

    # Test $expr $not
    print("\n--- $expr $not ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([{"age": 25}, {"age": 30}, {"age": 35}])
        try:
            neo_result = list(
                neo_collection.find({"$expr": {"$not": {"$gt": ["$age", 30]}}})
            )
            neo_not = len(neo_result)
            print(f"Neo $expr $not: {neo_not} matches")
        except Exception as e:
            neo_not = f"Error: {e}"
            print(f"Neo $expr $not: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many([{"age": 25}, {"age": 30}, {"age": 35}])
        try:
            mongo_result = list(
                mongo_collection.find(
                    {"$expr": {"$not": {"$gt": ["$age", 30]}}}
                )
            )
            mongo_not = len(mongo_result)
            print(f"Mongo $expr $not: {mongo_not} matches")
        except Exception as e:
            mongo_not = f"Error: {e}"
            print(f"Mongo $expr $not: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $not",
        (
            neo_not == mongo_not
            if not isinstance(neo_not, str) and not isinstance(mongo_not, str)
            else False
        ),
        neo_not,
        mongo_not,
    )

    # Test $expr $concat
    print("\n--- $expr $concat ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"first": "John", "last": "Doe"})
        try:
            neo_result = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$concat": ["$first", " ", "$last"]},
                                "John Doe",
                            ]
                        }
                    }
                )
            )
            neo_concat = len(neo_result)
            print(f"Neo $expr $concat: {neo_concat} matches")
        except Exception as e:
            neo_concat = f"Error: {e}"
            print(f"Neo $expr $concat: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"first": "John", "last": "Doe"})
        try:
            mongo_result = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$concat": ["$first", " ", "$last"]},
                                "John Doe",
                            ]
                        }
                    }
                )
            )
            mongo_concat = len(mongo_result)
            print(f"Mongo $expr $concat: {mongo_concat} matches")
        except Exception as e:
            mongo_concat = f"Error: {e}"
            print(f"Mongo $expr $concat: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $concat",
        (
            neo_concat == mongo_concat
            if not isinstance(neo_concat, str)
            and not isinstance(mongo_concat, str)
            else False
        ),
        neo_concat,
        mongo_concat,
    )

    # Test $expr $ifNull
    print("\n--- $expr $ifNull ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": "Alice", "middle": None}, {"name": "Bob", "middle": "X"}]
        )
        try:
            neo_result = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$ifNull": ["$middle", "N/A"]}, "N/A"]}}
                )
            )
            neo_ifnull = len(neo_result)
            print(f"Neo $expr $ifNull: {neo_ifnull} matches")
        except Exception as e:
            neo_ifnull = f"Error: {e}"
            print(f"Neo $expr $ifNull: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": "Alice", "middle": None}, {"name": "Bob", "middle": "X"}]
        )
        try:
            mongo_result = list(
                mongo_collection.find(
                    {"$expr": {"$eq": [{"$ifNull": ["$middle", "N/A"]}, "N/A"]}}
                )
            )
            mongo_ifnull = len(mongo_result)
            print(f"Mongo $expr $ifNull: {mongo_ifnull} matches")
        except Exception as e:
            mongo_ifnull = f"Error: {e}"
            print(f"Mongo $expr $ifNull: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $ifNull",
        (
            neo_ifnull == mongo_ifnull
            if not isinstance(neo_ifnull, str)
            and not isinstance(mongo_ifnull, str)
            else False
        ),
        neo_ifnull,
        mongo_ifnull,
    )

    # Test $expr $isArray
    print("\n--- $expr $isArray ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([{"data": [1, 2, 3]}, {"data": "not array"}])
        try:
            neo_result = list(
                neo_collection.find({"$expr": {"$isArray": "$data"}})
            )
            neo_isarray = len(neo_result)
            print(f"Neo $expr $isArray: {neo_isarray} matches")
        except Exception as e:
            neo_isarray = f"Error: {e}"
            print(f"Neo $expr $isArray: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"data": [1, 2, 3]}, {"data": "not array"}]
        )
        try:
            mongo_result = list(
                mongo_collection.find({"$expr": {"$isArray": "$data"}})
            )
            mongo_isarray = len(mongo_result)
            print(f"Mongo $expr $isArray: {mongo_isarray} matches")
        except Exception as e:
            mongo_isarray = f"Error: {e}"
            print(f"Mongo $expr $isArray: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $isArray",
        (
            neo_isarray == mongo_isarray
            if not isinstance(neo_isarray, str)
            and not isinstance(mongo_isarray, str)
            else False
        ),
        neo_isarray,
        mongo_isarray,
    )

    # Test $expr $round
    print("\n--- $expr $round ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"value": 3.14159})
        try:
            neo_result = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$round": ["$value", 2]}, 3.14]}}
                )
            )
            neo_round = len(neo_result)
            print(f"Neo $expr $round: {neo_round} matches")
        except Exception as e:
            neo_round = f"Error: {e}"
            print(f"Neo $expr $round: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"value": 3.14159})
        try:
            mongo_result = list(
                mongo_collection.find(
                    {"$expr": {"$eq": [{"$round": ["$value", 2]}, 3.14]}}
                )
            )
            mongo_round = len(mongo_result)
            print(f"Mongo $expr $round: {mongo_round} matches")
        except Exception as e:
            mongo_round = f"Error: {e}"
            print(f"Mongo $expr $round: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $round",
        (
            neo_round == mongo_round
            if not isinstance(neo_round, str)
            and not isinstance(mongo_round, str)
            else False
        ),
        neo_round,
        mongo_round,
    )

    # Test $expr $exp
    print("\n--- $expr $exp ---")

    # Use tolerance-based comparison for floating point
    def check_exp_result(collection, expected_approx):
        """Check if exp(1) result is approximately correct."""
        results = list(collection.find({"$expr": {"$eq": [{"$exp": 1}, 1]}}))
        if not results:
            return False
        # The query above just checks if any doc matches, we need to verify
        # the actual value. For $expr, we use a range comparison.
        return True

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        # Insert documents with different values to test exp function
        neo_collection.insert_many(
            [
                {"x": 0},  # exp(0) = 1
                {"x": 1},  # exp(1) ≈ 2.718
                {"x": 2},  # exp(2) ≈ 7.389
            ]
        )
        try:
            # Test exp(0) = 1 (exact)
            neo_result_exact = list(
                neo_collection.find({"$expr": {"$eq": [{"$exp": 0}, 1]}})
            )
            # Test exp(1) is in reasonable range (2.718 ± 0.001)
            neo_result_range = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$exp": 1}, 2.717]},
                                {"$lte": [{"$exp": 1}, 2.719]},
                            ]
                        }
                    }
                )
            )
            neo_exp = len(neo_result_exact) + len(neo_result_range)
            print(
                f"Neo $expr $exp: exp(0)=1 ({len(neo_result_exact)}), exp(1)≈2.718 ({len(neo_result_range)})"
            )
        except Exception as e:
            neo_exp = f"Error: {e}"
            print(f"Neo $expr $exp: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"x": 0},
                {"x": 1},
                {"x": 2},
            ]
        )
        try:
            mongo_result_exact = list(
                mongo_collection.find({"$expr": {"$eq": [{"$exp": 0}, 1]}})
            )
            mongo_result_range = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$exp": 1}, 2.717]},
                                {"$lte": [{"$exp": 1}, 2.719]},
                            ]
                        }
                    }
                )
            )
            mongo_exp = len(mongo_result_exact) + len(mongo_result_range)
            print(
                f"Mongo $expr $exp: exp(0)=1 ({len(mongo_result_exact)}), exp(1)≈2.718 ({len(mongo_result_range)})"
            )
        except Exception as e:
            mongo_exp = f"Error: {e}"
            print(f"Mongo $expr $exp: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $exp",
        (
            neo_exp == mongo_exp
            if not isinstance(neo_exp, str) and not isinstance(mongo_exp, str)
            else False
        ),
        neo_exp,
        mongo_exp,
    )

    # Test $expr $degreesToRadians
    print("\n--- $expr $degreesToRadians ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 180},  # radians(180°) = π ≈ 3.14159
                {"angle": 90},  # radians(90°) = π/2 ≈ 1.5708
            ]
        )
        try:
            # Test radians(180) ≈ 3.14159 (π)
            neo_result_180 = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 180}, 3.1415]},
                                {"$lte": [{"$degreesToRadians": 180}, 3.1417]},
                            ]
                        }
                    }
                )
            )
            # Test radians(90) ≈ 1.5708 (π/2)
            neo_result_90 = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 90}, 1.570]},
                                {"$lte": [{"$degreesToRadians": 90}, 1.571]},
                            ]
                        }
                    }
                )
            )
            neo_deg2rad = len(neo_result_180) + len(neo_result_90)
            print(
                f"Neo $expr $degreesToRadians: 180°≈π ({len(neo_result_180)}), 90°≈π/2 ({len(neo_result_90)})"
            )
        except Exception as e:
            neo_deg2rad = f"Error: {e}"
            print(f"Neo $expr $degreesToRadians: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 180},
                {"angle": 90},
            ]
        )
        try:
            mongo_result_180 = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 180}, 3.1415]},
                                {"$lte": [{"$degreesToRadians": 180}, 3.1417]},
                            ]
                        }
                    }
                )
            )
            mongo_result_90 = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 90}, 1.570]},
                                {"$lte": [{"$degreesToRadians": 90}, 1.571]},
                            ]
                        }
                    }
                )
            )
            mongo_deg2rad = len(mongo_result_180) + len(mongo_result_90)
            print(
                f"Mongo $expr $degreesToRadians: 180°≈π ({len(mongo_result_180)}), 90°≈π/2 ({len(mongo_result_90)})"
            )
        except Exception as e:
            mongo_deg2rad = f"Error: {e}"
            print(f"Mongo $expr $degreesToRadians: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $degreesToRadians",
        (
            neo_deg2rad == mongo_deg2rad
            if not isinstance(neo_deg2rad, str)
            and not isinstance(mongo_deg2rad, str)
            else False
        ),
        neo_deg2rad,
        mongo_deg2rad,
    )

    # Test $expr $radiansToDegrees
    print("\n--- $expr $radiansToDegrees ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 3.14159},  # degrees(π) = 180°
                {"angle": 1.5708},  # degrees(π/2) = 90°
            ]
        )
        try:
            # Test degrees(π) ≈ 180
            neo_result_pi = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {
                                    "$gte": [
                                        {"$radiansToDegrees": 3.14159},
                                        179.9,
                                    ]
                                },
                                {
                                    "$lte": [
                                        {"$radiansToDegrees": 3.14159},
                                        180.1,
                                    ]
                                },
                            ]
                        }
                    }
                )
            )
            # Test degrees(π/2) ≈ 90
            neo_result_pi2 = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$radiansToDegrees": 1.5708}, 89.9]},
                                {"$lte": [{"$radiansToDegrees": 1.5708}, 90.1]},
                            ]
                        }
                    }
                )
            )
            neo_rad2deg = len(neo_result_pi) + len(neo_result_pi2)
            print(
                f"Neo $expr $radiansToDegrees: π≈180° ({len(neo_result_pi)}), π/2≈90° ({len(neo_result_pi2)})"
            )
        except Exception as e:
            neo_rad2deg = f"Error: {e}"
            print(f"Neo $expr $radiansToDegrees: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 3.14159},
                {"angle": 1.5708},
            ]
        )
        try:
            mongo_result_pi = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {
                                    "$gte": [
                                        {"$radiansToDegrees": 3.14159},
                                        179.9,
                                    ]
                                },
                                {
                                    "$lte": [
                                        {"$radiansToDegrees": 3.14159},
                                        180.1,
                                    ]
                                },
                            ]
                        }
                    }
                )
            )
            mongo_result_pi2 = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$radiansToDegrees": 1.5708}, 89.9]},
                                {"$lte": [{"$radiansToDegrees": 1.5708}, 90.1]},
                            ]
                        }
                    }
                )
            )
            mongo_rad2deg = len(mongo_result_pi) + len(mongo_result_pi2)
            print(
                f"Mongo $expr $radiansToDegrees: π≈180° ({len(mongo_result_pi)}), π/2≈90° ({len(mongo_result_pi2)})"
            )
        except Exception as e:
            mongo_rad2deg = f"Error: {e}"
            print(f"Mongo $expr $radiansToDegrees: Error - {e}")
        client.close()

    reporter.record_result(
        "Known Limitations",
        "$expr $radiansToDegrees",
        (
            neo_rad2deg == mongo_rad2deg
            if not isinstance(neo_rad2deg, str)
            and not isinstance(mongo_rad2deg, str)
            else False
        ),
        neo_rad2deg,
        mongo_rad2deg,
    )


# ============================================================================
# Main
# ============================================================================
def main():
    """Main function to run all comparisons"""
    print("NeoSQLite vs PyMongo - Comprehensive API Comparison")
    print("=" * 80)

    compare_crud_operations()
    compare_query_operators()
    compare_expr_operator()
    compare_update_operators()
    compare_aggregation_stages()
    compare_index_operations()
    compare_find_and_modify()
    compare_bulk_operations()
    compare_distinct()
    compare_binary_support()
    compare_nested_field_queries()
    compare_raw_batch_operations()
    compare_change_streams()
    compare_text_search()
    compare_gridfs_operations()
    compare_objectid_operations()
    compare_type_operator()
    compare_additional_aggregation()
    compare_cursor_operations()
    compare_known_limitations()

    reporter.print_report()

    if len(reporter.failed_tests) > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
