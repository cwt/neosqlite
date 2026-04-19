"""Module for comparing $type operator between NeoSQLite and PyMongo"""

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

        set_accumulation_mode(True)
        neo_results = {}
        for query, op_name in type_tests:
            try:
                start_neo_timing()
                try:
                    result = list(neo_collection.find(query))
                    neo_results[op_name] = result
                    print(f"Neo {op_name}: {len(result)}")
                finally:
                    end_neo_timing()
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

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

        set_accumulation_mode(True)
        for query, op_name in type_tests:
            try:
                start_mongo_timing()
                try:
                    result = list(mongo_collection.find(query))
                    mongo_results[op_name] = result
                    print(f"Mongo {op_name}: {len(result)}")
                finally:
                    end_mongo_timing()
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

            reporter.record_comparison(
                "$type Operator",
                op_name,
                neo_results.get(op_name),
                mongo_results.get(op_name),
                skip_reason=None,
            )
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for query, op_name in type_tests:
            reporter.record_comparison(
                "$type Operator",
                op_name,
                neo_results.get(op_name),
                None,
                skip_reason="MongoDB not available",
            )
