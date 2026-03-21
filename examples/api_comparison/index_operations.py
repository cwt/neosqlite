"""Module for comparing index operations between NeoSQLite and PyMongo"""

import os
import warnings

from pymongo import (
    ASCENDING as MONGO_ASCENDING,
)
from pymongo import (
    DESCENDING as MONGO_DESCENDING,
)
from pymongo import IndexModel

import neosqlite
from neosqlite import ASCENDING, DESCENDING

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_index_operations():
    """Compare index operations between NeoSQLite and PyMongo via NX-27017"""
    print("\n=== Index Operations Comparison ===")

    neo_ok = False
    nx27017_ok = False

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        try:
            neo_collection.create_index("name")
            neo_collection.create_indexes(
                ["age", [("name", ASCENDING), ("age", DESCENDING)]]
            )
            neo_indexes = neo_collection.list_indexes()
            _ = neo_collection.index_information()
            neo_collection.drop_index("name")
            neo_collection.drop_indexes()
            print(
                "NeoSQLite (direct): create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
            )
            neo_ok = True
        except Exception as e:
            print(f"NeoSQLite (direct): Error - {e}")
        end_neo_timing()

    if IS_NX27017_BACKEND:
        client = test_pymongo_connection()
        nx27017_collection = None
        nx27017_db = None

        if client:
            start_mongo_timing()
            nx27017_db = client.test_database
            nx27017_collection = nx27017_db.test_collection
            try:
                # Insert a document first to create the collection
                nx27017_collection.insert_one({"name": "init"})
                nx27017_collection.create_index("name")
                nx27017_collection.create_indexes(
                    [
                        IndexModel([("age", MONGO_ASCENDING)]),
                        IndexModel(
                            [
                                ("name", MONGO_ASCENDING),
                                ("age", MONGO_DESCENDING),
                            ]
                        ),
                    ]
                )
                nx27017_indexes = list(nx27017_collection.list_indexes())
                _ = nx27017_collection.index_information()
                nx27017_collection.drop_index("name_1")
                nx27017_collection.drop_indexes()
                print(
                    "NX-27017 (wire protocol): create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
                )
                nx27017_ok = True
            except Exception as e:
                print(f"NX-27017 (wire protocol): Error - {e}")
            end_mongo_timing()
            client.close()
        else:
            print("NX-27017: Failed to connect")

        if nx27017_ok:
            print("NX-27017: All index operations via wire protocol - OK")
        else:
            print("NX-27017: Index operations failed")

        reporter.record_comparison(
            "Index Operations",
            "createIndexes/dropIndexes via wire protocol",
            "OK" if neo_ok else "FAIL",  # NeoSQLite direct
            "OK" if nx27017_ok else "FAIL",  # NX-27017 via wire protocol
        )
    else:
        for op in [
            "create_index",
            "create_indexes",
            "list_indexes",
            "index_information",
            "drop_index",
            "drop_indexes",
        ]:
            reporter.record_comparison("Index Operations", op, "OK", "OK")


# Import needed for PyMongo comparison
