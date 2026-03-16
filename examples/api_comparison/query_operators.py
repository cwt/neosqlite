"""Module for comparing query operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
    set_accumulation_mode,
)
from .utils import test_pymongo_connection

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
            try:
                start_neo_timing()
                result = list(neo_collection.find(query))
                end_neo_timing()
                neo_results[op_name] = result
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"

        # Test $contains (NeoSQLite extension)
        try:
            start_neo_timing()
            neo_results["$contains"] = list(
                neo_collection.find({"name": {"$contains": "li"}})
            )
            end_neo_timing()
            print(f"Neo $contains: {len(neo_results['$contains'])} documents")
        except Exception as e:
            neo_results["$contains"] = f"Error: {e}"

        # Test $text separately as it requires index creation
        try:
            neo_collection.create_index([("name", "text")])

            start_neo_timing()
            neo_results["$text"] = list(
                neo_collection.find({"$text": {"$search": "Alice"}})
            )
            end_neo_timing()
            print(f"Neo $text: {len(neo_results['$text'])} documents")
        except Exception as e:
            neo_results["$text"] = f"Error: {e}"
            print(f"Neo $text: Error - {e}")

    client = test_pymongo_connection()
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
            try:
                start_mongo_timing()
                result = list(mongo_collection.find(query))
                end_mongo_timing()
                mongo_results[op_name] = result
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"

        # MongoDB doesn't support $contains
        mongo_results["$contains"] = None

        # Test $text separately for MongoDB
        try:
            mongo_collection.create_index([("name", "text")])

            start_mongo_timing()
            mongo_results["$text"] = list(
                mongo_collection.find({"$text": {"$search": "Alice"}})
            )
            end_mongo_timing()
            print(f"Mongo $text: {len(mongo_results['$text'])} documents")
        except Exception as e:
            mongo_results["$text"] = f"Error: {e}"
            print(f"Mongo $text: Error - {e}")

        for op_name in neo_results:
            skip_reason = None
            if not client:
                skip_reason = "MongoDB not available"
            elif op_name == "$contains":
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
        client.close()
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
