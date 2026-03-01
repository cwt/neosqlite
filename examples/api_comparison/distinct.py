"""Module for comparing distinct operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

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
        neo_distinct = neo_collection.distinct("dept")
        print(f"Neo distinct: {sorted(neo_distinct)}")

    client = test_pymongo_connection()
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
        mongo_distinct = mongo_collection.distinct("dept")
        print(f"Mongo distinct: {sorted(mongo_distinct)}")
        client.close()

    passed = (
        set(neo_distinct) == set(mongo_distinct)
        if mongo_distinct is not None
        else False if mongo_distinct is not None else False
    )
    reporter.record_result(
        "Distinct",
        "distinct",
        passed,
        sorted(neo_distinct),
        sorted(mongo_distinct) if mongo_distinct is not None else None,
    )
