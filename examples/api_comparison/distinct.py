"""Module for comparing distinct operations between NeoSQLite and PyMongo"""

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


def compare_distinct():
    """Compare distinct operations"""
    print("\n=== Distinct Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"dept": "Engineering"},
                {"dept": "Marketing"},
                {"dept": "Engineering"},
            ]
        )

        set_accumulation_mode(True)
        start_neo_timing()
        try:
            neo_distinct = neo_collection.distinct("dept")
        finally:

            end_neo_timing()
        print(f"Neo distinct: {sorted(neo_distinct)}")

    client = get_mongo_client()
    # Initialize MongoDB result variables

    mongo_collection = None
    mongo_db = None
    mongo_distinct = None

    if client:
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

        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_distinct = mongo_collection.distinct("dept")
        finally:

            end_mongo_timing()
        print(f"Mongo distinct: {sorted(mongo_distinct)}")

    reporter.record_comparison(
        "Distinct",
        "distinct",
        sorted(neo_distinct),
        sorted(mongo_distinct) if mongo_distinct is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
