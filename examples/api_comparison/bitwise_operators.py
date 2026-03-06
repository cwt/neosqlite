"""Module for comparing bitwise query operators between NeoSQLite and PyMongo"""

import copy
import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_bitwise_operators():
    """Compare bitwise query operators between NeoSQLite and PyMongo"""
    print("\n=== Bitwise Query Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        # Insert test documents with different bit patterns
        neo_collection.insert_many(
            [
                {"value": 0, "name": "zero"},  # 0000
                {"value": 1, "name": "one"},  # 0001
                {"value": 5, "name": "five"},  # 0101
                {"value": 7, "name": "seven"},  # 0111
                {"value": 8, "name": "eight"},  # 1000
                {"value": 10, "name": "ten"},  # 1010
                {"value": 15, "name": "fifteen"},  # 1111
            ]
        )

        operators = [
            # $bitsAllClear - all specified bits are 0
            ({"value": {"$bitsAllClear": 5}}, "$bitsAllClear (bitmask=5)"),
            (
                {"value": {"$bitsAllClear": [0, 1]}},
                "$bitsAllClear (positions [0,1])",
            ),
            # $bitsAllSet - all specified bits are 1
            ({"value": {"$bitsAllSet": 5}}, "$bitsAllSet (bitmask=5)"),
            (
                {"value": {"$bitsAllSet": [0, 1]}},
                "$bitsAllSet (positions [0,1])",
            ),
            # $bitsAnyClear - any of specified bits are 0
            ({"value": {"$bitsAnyClear": 5}}, "$bitsAnyClear (bitmask=5)"),
            (
                {"value": {"$bitsAnyClear": [0, 1]}},
                "$bitsAnyClear (positions [0,1])",
            ),
            # $bitsAnySet - any of specified bits are 1
            ({"value": {"$bitsAnySet": 5}}, "$bitsAnySet (bitmask=5)"),
            (
                {"value": {"$bitsAnySet": [0, 1]}},
                "$bitsAnySet (positions [0,1])",
            ),
        ]

        neo_results = {}
        for query, op_name in operators:
            try:
                result = list(neo_collection.find(query))
                neo_results[op_name] = len(result)
                print(f"Neo {op_name}: {len(result)} documents")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_results = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"value": 0, "name": "zero"},
                {"value": 1, "name": "one"},
                {"value": 5, "name": "five"},
                {"value": 7, "name": "seven"},
                {"value": 8, "name": "eight"},
                {"value": 10, "name": "ten"},
                {"value": 15, "name": "fifteen"},
            ]
        )

        mongo_results = {}
        for query, op_name in operators:
            try:
                mongo_query = copy.deepcopy(query)
                result = list(mongo_collection.find(mongo_query))
                mongo_results[op_name] = len(result)
                print(f"Mongo {op_name}: {len(result)} documents")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = (
                mongo_results.get(op_name, "N/A") if mongo_results else "N/A"
            )
            passed = (
                neo_count == mongo_count
                if not isinstance(neo_count, str)
                and not isinstance(mongo_count, str)
                else False
            )
            reporter.record_result(
                "Bitwise Operators", op_name, passed, neo_count, mongo_count
            )
        client.close()
    else:
        # No MongoDB connection, just record NeoSQLite results
        for op_name in neo_results:
            neo_count = neo_results[op_name]
            reporter.record_result(
                "Bitwise Operators", op_name, False, neo_count, "N/A"
            )
