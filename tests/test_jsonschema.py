import pytest
from neosqlite import Connection
from neosqlite.objectid import ObjectId


@pytest.fixture
def collection(tmp_path):
    db_path = str(tmp_path / "test_schema.db")

    with Connection(db_path) as conn:
        coll = conn.collection
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "age": 30,
                    "email": "alice@example.com",
                    "tags": ["staff"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "age": 25,
                    "email": "bob@gmail.com",
                    "tags": ["user"],
                },
                {"_id": 3, "name": "Charlie", "age": 15, "tags": []},
                {
                    "_id": 4,
                    "name": "Dave",
                    "age": 40,
                    "email": "dave@example.com",
                    "oid": ObjectId(),
                },
            ]
        )
        yield coll


def test_jsonschema_find_basic(collection):
    """Test $jsonSchema in find() queries."""
    # Required fields
    results = list(collection.find({"$jsonSchema": {"required": ["email"]}}))
    assert len(results) == 3
    assert {r["_id"] for r in results} == {1, 2, 4}

    # Types and values
    results = list(
        collection.find(
            {
                "$jsonSchema": {
                    "properties": {"age": {"minimum": 20, "maximum": 35}}
                }
            }
        )
    )
    assert len(results) == 2
    assert {r["_id"] for r in results} == {1, 2}


def test_jsonschema_aggregate(collection):
    """Test $jsonSchema in aggregation $match stage."""
    pipeline = [
        {
            "$match": {
                "$jsonSchema": {
                    "required": ["name", "email"],
                    "properties": {"email": {"pattern": "@example\\.com$"}},
                }
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    assert len(results) == 2
    assert {r["_id"] for r in results} == {1, 4}


def test_jsonschema_bson_types(collection):
    """Test MongoDB-specific bsonType support."""
    # objectId type
    results = list(
        collection.find(
            {
                "$jsonSchema": {
                    "required": ["oid"],
                    "properties": {"oid": {"bsonType": "objectId"}},
                }
            }
        )
    )
    assert len(results) == 1
    assert results[0]["_id"] == 4

    # int type
    results = list(
        collection.find(
            {"$jsonSchema": {"properties": {"age": {"bsonType": "int"}}}}
        )
    )
    assert len(results) == 4


def test_jsonschema_logical(collection):
    """Test logical combinations in $jsonSchema."""
    # anyOf
    schema = {
        "anyOf": [
            {"required": ["age"], "properties": {"age": {"minimum": 35}}},
            {
                "required": ["email"],
                "properties": {"email": {"pattern": "gmail\\.com$"}},
            },
        ]
    }
    results = list(collection.find({"$jsonSchema": schema}))
    assert len(results) == 2
    assert {r["_id"] for r in results} == {2, 4}


def test_jsonschema_nested(collection):
    """Test $jsonSchema with nested documents."""
    collection.insert_one({"_id": 10, "profile": {"info": {"bio": "hello"}}})

    schema = {
        "properties": {
            "profile": {"properties": {"info": {"required": ["bio"]}}}
        }
    }
    results = list(collection.find({"$jsonSchema": schema}))
    assert (
        len(results) == 5
    )  # All match because only _id 10 has profile (others are optional)

    # Force required profile
    schema["required"] = ["profile"]
    results = list(collection.find({"$jsonSchema": schema}))
    assert len(results) == 1
    assert results[0]["_id"] == 10
