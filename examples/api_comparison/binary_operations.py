"""Module for comparing binary data support between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from neosqlite.binary import Binary

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_binary_support():
    """Compare Binary data support"""
    print("\n=== Binary Data Support Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        binary_data = Binary(b"test binary data")
        neo_collection.insert_one({"data": binary_data, "name": "test"})
        doc = neo_collection.find_one({"name": "test"})
        neo_has_binary = isinstance(doc.get("data"), (Binary, bytes))
        print(f"Neo Binary support: {neo_has_binary}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_has_binary = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        from bson import Binary as BsonBinary

        binary_data = BsonBinary(b"test binary data")
        mongo_collection.insert_one({"data": binary_data, "name": "test"})
        doc = mongo_collection.find_one({"name": "test"})
        mongo_has_binary = isinstance(doc.get("data"), (BsonBinary, bytes))
        print(f"Mongo Binary support: {mongo_has_binary}")
        client.close()

    reporter.record_result(
        "Binary Support",
        "Binary",
        neo_has_binary,
        neo_has_binary,
        mongo_has_binary if mongo_has_binary is not None else None,
    )
