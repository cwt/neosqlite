"""Module for comparing date expression operators between NeoSQLite and PyMongo"""

from datetime import datetime, timezone
import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_date_expr_operators():
    """Compare date expression operators in aggregation"""
    print("\n=== Date Expression Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        # Use datetime objects (MongoDB-compatible format)
        neo_collection.insert_many(
            [
                {
                    "event": "A",
                    "date": datetime(
                        2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "event": "B",
                    "date": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                },
            ]
        )

        date_operators = [
            ("$year", {"$year": "$date"}),
            ("$month", {"$month": "$date"}),
            ("$dayOfMonth", {"$dayOfMonth": "$date"}),
            ("$hour", {"$hour": "$date"}),
            ("$minute", {"$minute": "$date"}),
            ("$second", {"$second": "$date"}),
            ("$dayOfWeek", {"$dayOfWeek": "$date"}),
            ("$dayOfYear", {"$dayOfYear": "$date"}),
        ]

        neo_results = {}
        for op_name, op_expr in date_operators:
            try:
                result = list(
                    neo_collection.aggregate([{"$project": {"val": op_expr}}])
                )
                neo_results[op_name] = len(result) == 2
                print(
                    f"Neo {op_name}: {'OK' if neo_results[op_name] else 'FAIL'}"
                )
            except Exception as e:
                neo_results[op_name] = False
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_results = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        # MongoDB requires actual datetime objects
        mongo_collection.insert_many(
            [
                {
                    "event": "A",
                    "date": datetime(
                        2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "event": "B",
                    "date": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                },
            ]
        )

        mongo_results = {}
        for op_name, op_expr in date_operators:
            try:
                result = list(
                    mongo_collection.aggregate([{"$project": {"val": op_expr}}])
                )
                mongo_results[op_name] = len(result) == 2
                print(
                    f"Mongo {op_name}: {'OK' if mongo_results[op_name] else 'FAIL'}"
                )
            except Exception as e:
                mongo_results[op_name] = False
                print(f"Mongo {op_name}: Error - {e}")

        client.close()

        for op_name in neo_results:
            reporter.record_result(
                "Date Expression Operators",
                op_name,
                neo_results[op_name],
                neo_results[op_name],
                mongo_results.get(op_name, False),
            )
