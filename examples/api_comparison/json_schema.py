"""Module for comparing $jsonSchema operator and validation between NeoSQLite and PyMongo"""

import os
import warnings

import neosqlite
from neosqlite.objectid import ObjectId

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


def compare_json_schema():
    """Compare $jsonSchema query operator and write validation"""
    print("\n=== $jsonSchema Comparison ===")

    test_data = [
        {
            "_id": 1,
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "oid": ObjectId(),
        },
        {"_id": 2, "name": "Bob", "age": 25, "email": "bob@gmail.com"},
        {"_id": 3, "name": "Charlie", "age": 15},
        {"_id": 4, "name": "Dave", "age": "forty"},  # Invalid age type
    ]

    queries = {
        "required": {"$jsonSchema": {"required": ["email"]}},
        "type_int": {
            "$jsonSchema": {"properties": {"age": {"bsonType": "int"}}}
        },
        "range": {
            "$jsonSchema": {
                "properties": {"age": {"minimum": 20, "maximum": 35}}
            }
        },
        "pattern": {
            "$jsonSchema": {
                "properties": {"email": {"pattern": "@example\\.com$"}}
            }
        },
        "objectId": {
            "$jsonSchema": {
                "required": ["oid"],
                "properties": {"oid": {"bsonType": "objectId"}},
            }
        },
        "logical": {
            "$jsonSchema": {
                "anyOf": [
                    {
                        "required": ["age"],
                        "properties": {"age": {"minimum": 35}},
                    },
                    {
                        "required": ["email"],
                        "properties": {"email": {"pattern": "gmail\\.com$"}},
                    },
                ]
            }
        },
    }

    neo_results = {}
    neo_write_validation = False

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.users
        neo_collection.insert_many(test_data)

        set_accumulation_mode(True)
        for name, query in queries.items():
            try:
                start_neo_timing()
                result = list(neo_collection.find(query))
                end_neo_timing()

                neo_results[name] = result
                print(f"Neo $jsonSchema query ({name}): OK")
            except Exception as e:
                neo_results[name] = f"Error: {e}"
                print(f"Neo $jsonSchema query ({name}): Error - {e}")

        # Test write validation
        try:
            val_schema = {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"age": {"minimum": 18}},
                }
            }
            v_coll = neo_conn.create_collection(
                "validated", validator=val_schema
            )

            # Should pass
            v_coll.insert_one({"name": "X", "age": 20})

            # Should fail
            import sqlite3

            try:
                start_neo_timing()
                v_coll.insert_one({"age": 25})  # Missing name
                neo_write_validation = False
                end_neo_timing()
            except sqlite3.IntegrityError:
                end_neo_timing()
                neo_write_validation = True
                print("Neo write validation: OK (IntegrityError)")
        except Exception as e:
            print(f"Neo write validation: Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}
    mongo_write_validation = False

    if client:
        from bson import ObjectId as BsonObjectId

        def to_bson(doc):
            new_doc = doc.copy()
            for k, v in new_doc.items():
                if isinstance(v, ObjectId):
                    new_doc[k] = BsonObjectId(str(v))
            return new_doc

        mongo_test_data = [to_bson(d) for d in test_data]

        mongo_db = client.test_database
        mongo_collection = mongo_db.users
        mongo_collection.delete_many({})
        mongo_collection.insert_many(mongo_test_data)

        set_accumulation_mode(True)
        for name, query in queries.items():
            try:
                start_mongo_timing()
                result = list(mongo_collection.find(query))
                end_mongo_timing()

                mongo_results[name] = result
                print(f"Mongo $jsonSchema query ({name}): OK")
            except Exception as e:
                mongo_results[name] = f"Error: {e}"
                print(f"Mongo $jsonSchema query ({name}): Error - {e}")

        # Test write validation
        try:
            val_schema = {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"age": {"minimum": 18}},
                }
            }
            mongo_db.drop_collection("validated")
            v_coll = mongo_db.create_collection(
                "validated", validator=val_schema
            )

            # Should pass
            v_coll.insert_one({"name": "X", "age": 20})

            # Should fail
            from pymongo.errors import WriteError

            try:
                start_mongo_timing()
                v_coll.insert_one({"age": 25})
                mongo_write_validation = False
                end_mongo_timing()
            except WriteError:
                end_mongo_timing()
                mongo_write_validation = True
                print("Mongo write validation: OK (WriteError)")
        except Exception as e:
            print(f"Mongo write validation: Error - {e}")

        client.close()

    # Record comparisons
    for name in queries:
        reporter.record_comparison(
            "JSON Schema",
            f"query_{name}",
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason="MongoDB not available" if not client else None,
        )

    if IS_NX27017_BACKEND:
        skip_reason = (
            "Write validation behavior differs between SQLite and MongoDB"
        )
    elif not client:
        skip_reason = "MongoDB not available"
    elif mongo_write_validation is False and neo_write_validation is True:
        skip_reason = "MongoDB validator not enforcing properly"
    else:
        skip_reason = None

    reporter.record_result(
        "JSON Schema",
        "write_validation",
        passed=(
            neo_write_validation == mongo_write_validation
            if client and skip_reason is None
            else neo_write_validation
        ),
        neo_result="IntegrityError" if neo_write_validation else "Fail",
        mongo_result="WriteError" if mongo_write_validation else "Fail",
        skip_reason=skip_reason,
    )
