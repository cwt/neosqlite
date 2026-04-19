"""Module for comparing query operators between NeoSQLite and PyMongo"""

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


def compare_query_operators():
    """Compare query operators between NeoSQLite and PyMongo"""
    print("\n=== Query Operators Comparison ===")

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
        ({"scores": {"$size": 3}}, "$size"),
        ({"name": {"$regex": "A.*"}}, "$regex"),
        (
            {"name": {"$regex": "alice", "$options": "i"}},
            "$regex with $options",
        ),
        ({"$nor": [{"age": 30}, {"name": "Alice"}]}, "$nor"),
        ({"$where": "this.age > 30"}, "$where (NotImplementedError)"),
        (
            {"$function": "function(x) { return x > 30; }"},
            "$function (NotImplementedError)",
        ),
        (
            {"$accumulator": {"init": "function() { return 0; }"}},
            "$accumulator (NotImplementedError)",
        ),
    ]

    neo_results = {}
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

        set_accumulation_mode(True)
        for query, op_name in operators:
            start_neo_timing()
            try:
                try:
                    neo_results[op_name] = list(neo_collection.find(query))
                except Exception as e:
                    neo_results[op_name] = f"Error: {e}"
            finally:
                end_neo_timing()

        # Test $contains (NeoSQLite extension)
        start_neo_timing()
        try:
            try:
                neo_results["$contains"] = list(
                    neo_collection.find({"name": {"$contains": "li"}})
                )
            except Exception as e:
                neo_results["$contains"] = f"Error: {e}"
        finally:
            end_neo_timing()

        if isinstance(neo_results["$contains"], list):
            print(f"Neo $contains: {len(neo_results['$contains'])} documents")
        else:
            print(f"Neo $contains: {neo_results['$contains']}")

        # Test $text separately as it requires index creation
        neo_collection.create_index([("name", "text")])
        start_neo_timing()
        try:
            try:
                neo_results["$text"] = list(
                    neo_collection.find({"$text": {"$search": "Alice"}})
                )
            except Exception as e:
                neo_results["$text"] = f"Error: {e}"
                print(f"Neo $text: Error - {e}")
        finally:
            end_neo_timing()

        if isinstance(neo_results["$text"], list):
            print(f"Neo $text: {len(neo_results['$text'])} documents")

    client = get_mongo_client()
    mongo_results = {}

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

        set_accumulation_mode(True)
        for query, op_name in operators:
            start_mongo_timing()
            try:
                try:
                    mongo_results[op_name] = list(mongo_collection.find(query))
                except Exception as e:
                    mongo_results[op_name] = f"Error: {e}"
            finally:
                end_mongo_timing()

        # MongoDB doesn't support $contains
        mongo_results["$contains"] = None

        # Test $text separately for MongoDB
        mongo_collection.create_index([("name", "text")])
        start_mongo_timing()
        try:
            try:
                mongo_results["$text"] = list(
                    mongo_collection.find({"$text": {"$search": "Alice"}})
                )
            except Exception as e:
                mongo_results["$text"] = f"Error: {e}"
                print(f"Mongo $text: Error - {e}")
        finally:
            end_mongo_timing()

        if isinstance(mongo_results["$text"], list):
            print(f"Mongo $text: {len(mongo_results['$text'])} documents")

        for op_name in neo_results:
            skip_reason = None
            if op_name == "$contains":
                skip_reason = "NeoSQLite extension not in MongoDB"
            elif "(NotImplementedError)" in op_name:
                skip_reason = (
                    "Not supported in NeoSQLite (raises NotImplementedError)"
                )

            reporter.record_comparison(
                "Query Operators",
                op_name,
                neo_results[op_name],
                mongo_results.get(op_name),
                skip_reason=skip_reason,
            )
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for op_name in neo_results:
            reporter.record_comparison(
                "Query Operators",
                op_name,
                neo_results[op_name],
                None,
                skip_reason="MongoDB not available",
            )
