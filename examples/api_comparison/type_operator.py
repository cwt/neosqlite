"""Module for comparing $type operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


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
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_count = None

    mongo_db = None

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
                passed = (
                    neo_count == mongo_count
                    if mongo_count is not None
                    else False
                )
            reporter.record_result(
                "$type Operator", op_name, passed, neo_count, mongo_count
            )
        client.close()
