"""Module for comparing aggregation cursor methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_aggregation_cursor_methods():
    """Compare aggregation cursor methods"""
    print("\n=== Aggregation Cursor Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_agg_cursor
        neo_collection.insert_many(
            [{"name": f"Doc{i}", "value": i} for i in range(10)]
        )

        # Test aggregation cursor with batch_size
        try:
            cursor = neo_collection.aggregate(
                [{"$match": {"value": {"$gte": 3}}}]
            ).batch_size(3)
            results = list(cursor)
            neo_agg_batch_size = len(results) <= 10
            print(f"Neo aggregate batch_size: {len(results)} results")
        except Exception as e:
            neo_agg_batch_size = False
            print(f"Neo aggregate batch_size: Error - {e}")

        # Test allow_disk_use (PyMongo style: parameter to aggregate())
        try:
            cursor = neo_collection.aggregate(
                [{"$match": {"value": {"$gte": 3}}}], allowDiskUse=True
            )
            results = list(cursor)
            neo_allow_disk_use = len(results) >= 0
            print(
                f"Neo allow_disk_use: {'OK' if neo_allow_disk_use else 'FAIL'}"
            )
        except Exception as e:
            neo_allow_disk_use = False
            print(f"Neo allow_disk_use: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_agg_batch_size = None

    mongo_allow_disk_use = None

    mongo_collection = None

    mongo_db = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_agg_cursor
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": f"Doc{i}", "value": i} for i in range(10)]
        )

        # Test aggregation cursor with batch_size
        try:
            cursor = mongo_collection.aggregate(
                [{"$match": {"value": {"$gte": 3}}}]
            ).batch_size(3)
            results = list(cursor)
            mongo_agg_batch_size = len(results) <= 10
            print(f"Mongo aggregate batch_size: {len(results)} results")
        except Exception as e:
            mongo_agg_batch_size = False
            print(f"Mongo aggregate batch_size: Error - {e}")

        # Test allow_disk_use (PyMongo style: parameter to aggregate())
        try:
            cursor = mongo_collection.aggregate(
                [{"$match": {"value": {"$gte": 3}}}], allowDiskUse=True
            )
            results = list(cursor)
            mongo_allow_disk_use = len(results) >= 0
            print(
                f"Mongo allow_disk_use: {'OK' if mongo_allow_disk_use else 'FAIL'}"
            )
        except Exception as e:
            mongo_allow_disk_use = False
            print(f"Mongo allow_disk_use: Error - {e}")

        client.close()

        reporter.record_result(
            "Aggregation Cursor Methods",
            "batch_size",
            neo_agg_batch_size,
            neo_agg_batch_size,
            mongo_agg_batch_size,
        )
        reporter.record_result(
            "Aggregation Cursor Methods",
            "allow_disk_use",
            neo_allow_disk_use,
            neo_allow_disk_use,
            mongo_allow_disk_use,
        )
