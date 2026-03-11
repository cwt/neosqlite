"""Module for comparing $jsonSchema operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from neosqlite.objectid import ObjectId
from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_json_schema():
    """Compare $jsonSchema query operator"""
    print("\n=== $jsonSchema Operator Comparison ===")

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
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.users
        neo_collection.insert_many(test_data)

        for name, query in queries.items():
            try:
                neo_results[name] = list(neo_collection.find(query))
                print(f"Neo $jsonSchema ({name}): OK")
            except Exception as e:
                neo_results[name] = f"Error: {e}"
                print(f"Neo $jsonSchema ({name}): Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        from bson import ObjectId as BsonObjectId

        # Convert NeoSQLite ObjectId to BSON ObjectId
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

        for name, query in queries.items():
            try:
                mongo_results[name] = list(mongo_collection.find(query))
                print(f"Mongo $jsonSchema ({name}): OK")
            except Exception as e:
                mongo_results[name] = f"Error: {e}"
                print(f"Mongo $jsonSchema ({name}): Error - {e}")

        client.close()

    # Record comparisons
    for name in queries:
        reporter.record_comparison(
            "JSON Schema",
            f"$jsonSchema ({name})",
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason="MongoDB not available" if not client else None,
        )
