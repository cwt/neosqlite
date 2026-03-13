"""Module for comparing $type operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_type_operator():
    """Compare $type operator with various types"""
    print("\n=== $type Operator Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
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
                neo_results[op_name] = list(neo_collection.find(query))
                print(f"Neo {op_name}: {len(neo_results[op_name])}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_results = {}

    if client:
        start_mongo_timing()
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

        for query, op_name in type_tests:
            try:
                mongo_results[op_name] = list(mongo_collection.find(query))
                print(f"Mongo {op_name}: {len(mongo_results[op_name])}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"

        for op_name in neo_results:
            reporter.record_comparison(
                "$type Operator",
                op_name,
                neo_results[op_name],
                mongo_results.get(op_name),
                skip_reason="MongoDB not available" if not client else None,
            )
        end_mongo_timing()
        client.close()
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for op_name in neo_results:
            reporter.record_comparison(
                "$type Operator",
                op_name,
                neo_results[op_name],
                None,
                skip_reason="MongoDB not available",
            )
