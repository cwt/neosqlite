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
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_index_operations():
    """Compare index operations between NeoSQLite and PyMongo"""
    print("\n=== Index Operations Comparison ===")

    neo_ok = False
    mongo_ok = False

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)
        neo_collection = neo_conn.test_collection

        try:
            # create_index
            start_neo_timing()
            try:
                neo_collection.create_index("name")
            finally:
                end_neo_timing()

            # create_indexes
            start_neo_timing()
            try:
                neo_collection.create_indexes(
                    [
                        IndexModel("age"),
                        IndexModel([("name", ASCENDING), ("age", DESCENDING)]),
                    ]
                )
            finally:
                end_neo_timing()

            # list_indexes
            start_neo_timing()
            try:
                _ = list(neo_collection.list_indexes())
            finally:
                end_neo_timing()

            # index_information
            start_neo_timing()
            try:
                _ = neo_collection.index_information()
            finally:
                end_neo_timing()

            # drop_index
            start_neo_timing()
            try:
                # NeoSQLite uses field name if no explicit name
                neo_collection.drop_index("name")
            finally:
                end_neo_timing()

            # drop_indexes
            start_neo_timing()
            try:
                neo_collection.drop_indexes()
            finally:
                end_neo_timing()

            print(
                "NeoSQLite (direct): create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
            )
            neo_ok = True
        except Exception as e:
            print(f"NeoSQLite (direct): Error - {e}")

    # Test MongoDB via PyMongo (works with both real MongoDB and NX-27017)
    client = test_pymongo_connection()
    if client:
        set_accumulation_mode(True)
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection

            # Insert a document first to create the collection
            mongo_collection.insert_one({"name": "init"})

            # create_index
            start_mongo_timing()
            try:
                mongo_collection.create_index("name")
            finally:
                end_mongo_timing()

            # create_indexes
            start_mongo_timing()
            try:
                mongo_collection.create_indexes(
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
            finally:
                end_mongo_timing()

            # list_indexes
            start_mongo_timing()
            try:
                _ = list(mongo_collection.list_indexes())
            finally:
                end_mongo_timing()

            # index_information
            start_mongo_timing()
            try:
                _ = mongo_collection.index_information()
            finally:
                end_mongo_timing()

            # drop_index
            start_mongo_timing()
            try:
                mongo_collection.drop_index("name_1")
            finally:
                end_mongo_timing()

            # drop_indexes
            start_mongo_timing()
            try:
                mongo_collection.drop_indexes()
            finally:
                end_mongo_timing()

            backend_name = "NX-27017" if IS_NX27017_BACKEND else "MongoDB"
            print(
                f"{backend_name} (PyMongo): create_index, create_indexes, list_indexes, index_information, drop_index, drop_indexes"
            )
            mongo_ok = True
        except Exception as e:
            backend_name = "NX-27017" if IS_NX27017_BACKEND else "MongoDB"
            print(f"{backend_name} (PyMongo): Error - {e}")
        finally:
            client.close()
    else:
        print("MongoDB: Failed to connect")

    reporter.record_comparison(
        "Index Operations",
        "createIndexes/dropIndexes via PyMongo",
        "OK" if neo_ok else "FAIL",
        "OK" if mongo_ok else "FAIL",
    )
