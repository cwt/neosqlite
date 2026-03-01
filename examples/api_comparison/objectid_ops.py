"""Module for comparing ObjectId operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_objectid_operations():
    """Compare ObjectId operations"""
    print("\n=== ObjectId Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_found = None

    mongo_hex = None

    mongo_unique = None

    if client:
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
        client.close()

    reporter.record_result("ObjectId", "create", True, "OK", "OK")
    reporter.record_result(
        "ObjectId",
        "find_by_id",
        neo_found,
        neo_found,
        mongo_found if mongo_found is not None else None,
    )
    reporter.record_result(
        "ObjectId",
        "unique",
        neo_unique,
        neo_unique,
        mongo_unique if mongo_unique is not None else None,
    )
    reporter.record_result(
        "ObjectId",
        "hex_format",
        neo_hex,
        neo_hex,
        mongo_hex if mongo_hex is not None else None,
    )
