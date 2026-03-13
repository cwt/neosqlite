"""Module for comparing distinct operations between NeoSQLite and PyMongo"""

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


def compare_distinct():
    """Compare distinct operations"""
    print("\n=== Distinct Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"dept": "Engineering"},
                {"dept": "Marketing"},
                {"dept": "Engineering"},
            ]
        )
        neo_distinct = neo_collection.distinct("dept")
        print(f"Neo distinct: {sorted(neo_distinct)}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_distinct = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"dept": "Engineering"},
                {"dept": "Marketing"},
                {"dept": "Engineering"},
            ]
        )
        mongo_distinct = mongo_collection.distinct("dept")
        print(f"Mongo distinct: {sorted(mongo_distinct)}")
        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Distinct",
        "distinct",
        sorted(neo_distinct),
        sorted(mongo_distinct) if mongo_distinct is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
