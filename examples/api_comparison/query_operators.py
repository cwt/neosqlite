"""Module for comparing query operators between NeoSQLite and PyMongo"""

import copy
import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


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
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_count = None

    mongo_db = None

    mongo_query = None

    mongo_results = None

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
                if mongo_count is not None
                else (
                    False
                    if not isinstance(neo_count, str)
                    and not isinstance(mongo_count, str)
                    else False
                )
            )
            reporter.record_result(
                "Query Operators", op_name, passed, neo_count, mongo_count
            )
        client.close()
