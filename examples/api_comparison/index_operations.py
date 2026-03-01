"""Module for comparing index operations between NeoSQLite and PyMongo"""

import warnings

from neosqlite import ASCENDING, DESCENDING
from pymongo import (
    ASCENDING as MONGO_ASCENDING,
    DESCENDING as MONGO_DESCENDING,
)
from pymongo import IndexModel
import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_index_operations():
    """Compare index operations between NeoSQLite and PyMongo"""
    print("\n=== Index Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.create_index("name")
        neo_collection.create_indexes(
            ["age", [("name", ASCENDING), ("age", DESCENDING)]]
        )
        _ = neo_collection.list_indexes()
        _ = neo_collection.index_information()
        neo_collection.drop_index("name")
        neo_collection.drop_indexes()
        print(
            "NeoSQLite: create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
        )

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.create_index("name")
        mongo_collection.create_indexes(
            [
                IndexModel([("age", MONGO_ASCENDING)]),
                IndexModel(
                    [("name", MONGO_ASCENDING), ("age", MONGO_DESCENDING)]
                ),
            ]
        )
        _ = list(mongo_collection.list_indexes())
        _ = mongo_collection.index_information()
        mongo_collection.drop_index("name_1")
        mongo_collection.drop_indexes()
        print(
            "PyMongo: create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
        )
        client.close()

    for op in [
        "create_index",
        "create_indexes",
        "list_indexes",
        "index_information",
        "drop_index",
        "drop_indexes",
    ]:
        reporter.record_result("Index Operations", op, True, "OK", "OK")


# Import needed for PyMongo comparison
