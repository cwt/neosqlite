"""Module for comparing ObjectId operations between NeoSQLite and PyMongo"""

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


def compare_objectid_operations():
    """Compare ObjectId operations"""
    print("\n=== ObjectId Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection

        # Insert with ObjectId
        from neosqlite.objectid import ObjectId as NeoObjectId

        oid = NeoObjectId()
        neo_collection.insert_one({"_id": oid, "name": "test"})

        # Find by ObjectId
        doc = neo_collection.find_one({"_id": oid})
        neo_found = doc is not None

        # Test ObjectId generation
        oid2 = NeoObjectId()
        neo_unique = oid != oid2

        # Test ObjectId string conversion
        neo_hex = len(str(oid)) == 24

        print(
            f"Neo ObjectId: create={oid is not None}, find={neo_found}, unique={neo_unique}, hex={neo_hex}"
        )

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_found = None

    mongo_hex = None

    mongo_unique = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})

        from bson import ObjectId as BsonObjectId

        oid = BsonObjectId()
        mongo_collection.insert_one({"_id": oid, "name": "test"})

        doc = mongo_collection.find_one({"_id": oid})
        mongo_found = doc is not None

        oid2 = BsonObjectId()
        mongo_unique = oid != oid2

        mongo_hex = len(str(oid)) == 24

        print(
            f"Mongo ObjectId: create={oid is not None}, find={mongo_found}, unique={mongo_unique}, hex={mongo_hex}"
        )
        end_mongo_timing()
        client.close()

    reporter.record_comparison("ObjectId", "create", "OK", "OK")
    reporter.record_comparison(
        "ObjectId",
        "find_by_id",
        neo_found if neo_found else "FAIL",
        mongo_found if mongo_found else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "ObjectId",
        "unique",
        neo_unique if neo_unique else "FAIL",
        mongo_unique if mongo_unique else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "ObjectId",
        "hex_format",
        neo_hex if neo_hex else "FAIL",
        mongo_hex if mongo_hex else None,
        skip_reason="MongoDB not available" if not client else None,
    )
