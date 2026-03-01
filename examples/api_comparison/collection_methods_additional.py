"""Module for comparing additional collection methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_collection_methods():
    """Compare additional collection methods"""
    print("\n=== Additional Collection Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        # Test drop()
        neo_collection = neo_conn.test_drop
        neo_collection.insert_one({"name": "test"})
        try:
            neo_collection.drop()
            neo_drop = "test_drop" not in neo_conn.list_collection_names()
            print(f"Neo drop(): {'OK' if neo_drop else 'FAIL'}")
        except Exception as e:
            neo_drop = False
            print(f"Neo drop(): Error - {e}")

        # Test database property
        neo_collection2 = neo_conn.test
        try:
            neo_db = neo_collection2.database
            neo_db_ok = neo_db is not None
            print(f"Neo database property: {'OK' if neo_db_ok else 'FAIL'}")
        except Exception as e:
            neo_db_ok = False
            print(f"Neo database property: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_collection2 = None

    mongo_db = None

    mongo_db_ok = None

    mongo_db_prop = None

    mongo_drop = None

    if client:
        mongo_db = client.test_database
        # Test drop()
        mongo_collection = mongo_db.test_drop
        mongo_collection.insert_one({"name": "test"})
        try:
            mongo_collection.drop()
            mongo_drop = "test_drop" not in mongo_db.list_collection_names()
            print(f"Mongo drop(): {'OK' if mongo_drop else 'FAIL'}")
        except Exception as e:
            mongo_drop = False
            print(f"Mongo drop(): Error - {e}")

        # Test database property
        mongo_collection2 = mongo_db.test
        try:
            mongo_db_prop = mongo_collection2.database
            mongo_db_ok = mongo_db_prop is not None
            print(f"Mongo database property: {'OK' if mongo_db_ok else 'FAIL'}")
        except Exception as e:
            mongo_db_ok = False
            print(f"Mongo database property: Error - {e}")

        client.close()

    reporter.record_result(
        "Collection Methods", "drop", neo_drop, neo_drop, mongo_drop
    )
    reporter.record_result(
        "Collection Methods",
        "database_property",
        neo_db_ok,
        neo_db_ok,
        mongo_db_ok,
    )
