"""Module for comparing $bucket aggregation operator between NeoSQLite and PyMongo"""

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


def compare_bucket_aggregation():
    """Compare $bucket aggregation operator"""
    print("\n=== $bucket Aggregation Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.bucket_test
        neo_collection.insert_many(
            [{"_id": 100 + i, "value": i * 10} for i in range(1, 6)]
        )

        # Test $bucket
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$value",
                                "boundaries": [0, 20, 40, 60],
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )
            neo_bucket = len(result)
            print(f"Neo $bucket: {len(result)} buckets")
        except Exception as e:
            neo_bucket = f"Error: {e}"
            print(f"Neo $bucket: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    mongo_bucket = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.bucket_test
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"_id": 100 + i, "value": i * 10} for i in range(1, 6)]
        )

        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$value",
                                "boundaries": [0, 20, 40, 60],
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )
            mongo_bucket = len(result)
            print(f"Mongo $bucket: {len(result)} buckets")
        except Exception as e:
            mongo_bucket = f"Error: {e}"
            print(f"Mongo $bucket: Error - {e}")

        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Bucket Aggregation",
        "$bucket",
        neo_bucket,
        mongo_bucket,
        skip_reason="MongoDB not available" if not client else None,
    )
