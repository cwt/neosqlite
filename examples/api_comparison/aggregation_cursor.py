"""Module for comparing aggregation cursor methods between NeoSQLite and PyMongo"""

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

        set_accumulation_mode(True)
        # Test aggregation cursor with batch_size
        start_neo_timing()
        try:
            cursor = neo_collection.aggregate(
                [{"$match": {"value": {"$gte": 3}}}]
            ).batch_size(3)
            results = list(cursor)
            neo_agg_batch_size = len(results) == 7
            print(f"Neo aggregate batch_size: {len(results)} results")
        except Exception as e:
            neo_agg_batch_size = False
            print(f"Neo aggregate batch_size: Error - {e}")
        finally:
            end_neo_timing()

        # Test allow_disk_use (PyMongo style: parameter to aggregate())
        start_neo_timing()
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
        finally:
            end_neo_timing()

        # Test new AggregationCursor properties
        start_neo_timing()
        try:
            cursor = neo_collection.aggregate([{"$match": {}}])
            neo_retrieved = cursor.retrieved == 0
            neo_alive = cursor.alive is True
            neo_coll_prop = cursor.collection == neo_collection
            neo_address_before = cursor.address is None
            neo_session = cursor.session is None
            neo_cursor_id = cursor.cursor_id == 0

            list(cursor)
            neo_retrieved_after = cursor.retrieved == 10
            neo_alive_after = cursor.alive is False
            neo_address_after = cursor.address is not None

            neo_props_ok = all(
                [
                    neo_retrieved,
                    neo_alive,
                    neo_coll_prop,
                    neo_address_before,
                    neo_session,
                    neo_cursor_id,
                    neo_retrieved_after,
                    neo_alive_after,
                    neo_address_after,
                ]
            )
            print(
                f"Neo AggregationCursor properties: {'OK' if neo_props_ok else 'FAIL'}"
            )
        except Exception as e:
            neo_props_ok = False
            print(f"Neo AggregationCursor properties: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_agg_batch_size = None
    mongo_allow_disk_use = None
    mongo_collection = None
    mongo_db = None
    mongo_props_ok = None

    if client:
        mongo_db = client.test_agg_cursor
        mongo_collection = mongo_db.test_agg_cursor
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": f"Doc{i}", "value": i} for i in range(10)]
        )

        set_accumulation_mode(True)
        # Test aggregation cursor with batch_size
        start_mongo_timing()
        try:
            cursor = mongo_collection.aggregate(
                [{"$match": {"value": {"$gte": 3}}}]
            ).batch_size(3)
            results = list(cursor)
            mongo_agg_batch_size = len(results) == 7
            print(f"Mongo aggregate batch_size: {len(results)} results")
        except Exception as e:
            mongo_agg_batch_size = False
            print(f"Mongo aggregate batch_size: Error - {e}")
        finally:
            end_mongo_timing()

        # Test allow_disk_use (PyMongo style: parameter to aggregate())
        start_mongo_timing()
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
        finally:
            end_mongo_timing()

        # Test AggregationCursor properties (called CommandCursor in PyMongo)
        start_mongo_timing()
        try:
            cursor = mongo_collection.aggregate([{"$match": {}}])
            # CommandCursor has fewer properties than standard Cursor in some drivers,
            # but let's check what we can.
            mongo_alive = cursor.alive is True
            mongo_session = cursor.session is None
            mongo_cursor_id = isinstance(cursor.cursor_id, (int, type(None)))

            list(cursor)
            mongo_alive_after = cursor.alive is False

            mongo_props_ok = all(
                [mongo_alive, mongo_session, mongo_cursor_id, mongo_alive_after]
            )
            print(
                f"Mongo AggregationCursor properties: {'OK' if mongo_props_ok else 'FAIL'}"
            )
        except Exception as e:
            mongo_props_ok = False
            print(f"Mongo AggregationCursor properties: Error - {e}")
        finally:
            end_mongo_timing()

        client.close()

    reporter.record_comparison(
        "Aggregation Cursor Methods",
        "batch_size",
        neo_agg_batch_size if neo_agg_batch_size else "FAIL",
        mongo_agg_batch_size if mongo_agg_batch_size else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Cursor Methods",
        "allow_disk_use",
        neo_allow_disk_use if neo_allow_disk_use else "FAIL",
        mongo_allow_disk_use if mongo_allow_disk_use else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Cursor Methods",
        "properties",
        neo_props_ok if neo_props_ok else "FAIL",
        mongo_props_ok if mongo_props_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
