from datetime import datetime

import pytest

from neosqlite import Connection
from neosqlite.binary import Binary
from neosqlite.collection.query_helper.schema_compiler import (
    compile_schema_to_sql,
)
from neosqlite.collection.query_helper.schema_validator import (
    _validate_node,
    matches_json_schema,
)
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


# =============================================================================
# Schema Validator Unit Tests
# =============================================================================


class TestSchemaValidator:
    """Unit tests for schema_validator.py functions."""

    def test_string_min_length(self):
        """Test minLength validation for strings."""
        schema = {"type": "string", "minLength": 5}
        assert not matches_json_schema("abc", schema)
        assert matches_json_schema("abcdef", schema)

    def test_string_max_length(self):
        """Test maxLength validation for strings."""
        schema = {"type": "string", "maxLength": 5}
        assert matches_json_schema("abc", schema)
        assert not matches_json_schema("abcdef", schema)

    def test_string_pattern(self):
        """Test pattern validation for strings."""
        schema = {"type": "string", "pattern": "^[a-z]+$"}
        assert matches_json_schema("abc", schema)
        assert not matches_json_schema("abc123", schema)

    def test_array_min_items(self):
        """Test minItems validation for arrays."""
        schema = {"type": "array", "minItems": 2}
        assert not matches_json_schema([1], schema)
        assert matches_json_schema([1, 2], schema)

    def test_array_max_items(self):
        """Test maxItems validation for arrays."""
        schema = {"type": "array", "maxItems": 2}
        assert matches_json_schema([1], schema)
        assert matches_json_schema([1, 2], schema)
        assert not matches_json_schema([1, 2, 3], schema)

    def test_array_items(self):
        """Test items validation for arrays."""
        schema = {"type": "array", "items": {"type": "integer"}}
        assert matches_json_schema([1, 2, 3], schema)
        assert not matches_json_schema([1, "two", 3], schema)

    def test_enum(self):
        """Test enum validation."""
        schema = {"enum": ["red", "green", "blue"]}
        assert matches_json_schema("red", schema)
        assert not matches_json_schema("yellow", schema)

    def test_any_of(self):
        """Test anyOf logical combination."""
        schema = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        assert matches_json_schema("hello", schema)
        assert matches_json_schema(42, schema)
        assert not matches_json_schema([1, 2], schema)

    def test_all_of(self):
        """Test allOf logical combination."""
        schema = {
            "allOf": [
                {"type": "integer", "minimum": 10},
                {"type": "integer", "maximum": 20},
            ]
        }
        assert matches_json_schema(15, schema)
        assert not matches_json_schema(5, schema)
        assert not matches_json_schema(25, schema)

    def test_one_of(self):
        """Test oneOf logical combination."""
        schema = {"oneOf": [{"type": "integer"}, {"type": "string"}]}
        assert matches_json_schema(42, schema)
        assert matches_json_schema("hello", schema)
        # Test with overlapping conditions
        schema2 = {
            "oneOf": [
                {"type": "integer", "minimum": 0},
                {"type": "integer", "maximum": 10},
            ]
        }
        assert not matches_json_schema(5, schema2)
        assert matches_json_schema(15, schema2)

    def test_not(self):
        """Test not logical combination."""
        schema = {"not": {"type": "string"}}
        assert matches_json_schema(42, schema)
        assert not matches_json_schema("hello", schema)

    def test_type_list(self):
        """Test type validation with list of types."""
        schema = {"type": ["string", "integer"]}
        assert matches_json_schema("hello", schema)
        assert matches_json_schema(42, schema)
        assert not matches_json_schema([1, 2], schema)

    def test_bson_type_objectid(self):
        """Test bsonType objectId validation."""
        schema = {"bsonType": "objectId"}
        oid = ObjectId()
        assert matches_json_schema(oid, schema)
        assert matches_json_schema({"__neosqlite_objectid__": oid.hex}, schema)
        assert matches_json_schema(oid.hex, schema)
        assert not matches_json_schema("invalid_hex_string", schema)

    def test_bson_type_bindata(self):
        """Test bsonType binData validation."""
        schema = {"bsonType": "binData"}
        assert matches_json_schema(Binary(b"test"), schema)
        assert matches_json_schema(b"bytes", schema)
        assert not matches_json_schema("string", schema)

    def test_bson_type_date(self):
        """Test bsonType date validation."""
        schema = {"bsonType": "date"}
        assert matches_json_schema(datetime.now(), schema)
        assert not matches_json_schema("2023-01-01", schema)

    def test_bson_type_null(self):
        """Test bsonType null validation."""
        schema = {"bsonType": "null"}
        assert matches_json_schema(None, schema)
        assert not matches_json_schema("not null", schema)

    def test_bson_type_boolean(self):
        """Test bsonType boolean validation."""
        schema = {"bsonType": "boolean"}
        assert matches_json_schema(True, schema)
        assert matches_json_schema(False, schema)
        assert not matches_json_schema(1, schema)

    def test_bson_type_list(self):
        """Test bsonType with list of types."""
        schema = {"bsonType": ["string", "objectId"]}
        assert matches_json_schema("hello", schema)
        assert matches_json_schema(ObjectId(), schema)
        assert not matches_json_schema(42, schema)

    def test_unknown_type(self):
        """Test unknown type defaults to True."""
        schema = {"bsonType": "unknownType"}
        assert matches_json_schema("anything", schema)

    def test_validate_node_non_dict_schema(self):
        """Test _validate_node with non-dict schema."""
        assert _validate_node({"a": 1}, "not a dict")
        assert _validate_node({"a": 1}, 123)
        assert _validate_node({"a": 1}, None)

    def test_exclusive_minimum(self):
        """Test exclusiveMinimum validation."""
        schema = {"type": "number", "exclusiveMinimum": 10}
        assert not matches_json_schema(10, schema)
        assert matches_json_schema(10.1, schema)
        assert not matches_json_schema(9.9, schema)

    def test_exclusive_maximum(self):
        """Test exclusiveMaximum validation."""
        schema = {"type": "number", "exclusiveMaximum": 10}
        assert not matches_json_schema(10, schema)
        assert matches_json_schema(9.9, schema)
        assert not matches_json_schema(10.1, schema)


# =============================================================================
# Schema Compiler Unit Tests
# =============================================================================


class TestSchemaCompiler:
    """Unit tests for schema_compiler.py functions."""

    def test_compile_schema_non_dict_schema(self):
        """Test compile_schema_to_sql with non-dict schema."""
        result = compile_schema_to_sql("not a dict")
        assert result == "1"

    def test_compile_schema_required(self):
        """Test compile_schema_to_sql with required fields."""
        schema = {"required": ["name", "age"]}
        result = compile_schema_to_sql(schema)
        assert "IS NOT NULL" in result
        assert "name" in result
        assert "age" in result

    def test_compile_schema_properties(self):
        """Test compile_schema_to_sql with properties."""
        schema = {
            "properties": {
                "name": {"type": "string"},
                "age": {"minimum": 18},
            }
        }
        result = compile_schema_to_sql(schema)
        assert "name" in result
        assert "age" in result

    def test_compile_schema_type_string(self):
        """Test compile_schema_to_sql with string type."""
        schema = {"type": "string"}
        result = compile_schema_to_sql(schema)
        assert "'text'" in result

    def test_compile_schema_type_number(self):
        """Test compile_schema_to_sql with number type."""
        schema = {"type": "number"}
        result = compile_schema_to_sql(schema)
        assert "'integer'" in result
        assert "'real'" in result

    def test_compile_schema_type_integer(self):
        """Test compile_schema_to_sql with integer type."""
        schema = {"type": "integer"}
        result = compile_schema_to_sql(schema)
        assert "'integer'" in result

    def test_compile_schema_type_int(self):
        """Test compile_schema_to_sql with int type."""
        schema = {"type": "int"}
        result = compile_schema_to_sql(schema)
        assert "'integer'" in result

    def test_compile_schema_type_long(self):
        """Test compile_schema_to_sql with long type."""
        schema = {"type": "long"}
        result = compile_schema_to_sql(schema)
        assert "'integer'" in result

    def test_compile_schema_type_double(self):
        """Test compile_schema_to_sql with double type."""
        schema = {"type": "double"}
        result = compile_schema_to_sql(schema)
        assert "'real'" in result

    def test_compile_schema_type_decimal(self):
        """Test compile_schema_to_sql with decimal type."""
        schema = {"type": "decimal"}
        result = compile_schema_to_sql(schema)
        assert "'real'" in result

    def test_compile_schema_type_object(self):
        """Test compile_schema_to_sql with object type."""
        schema = {"type": "object"}
        result = compile_schema_to_sql(schema)
        assert "'object'" in result

    def test_compile_schema_type_array(self):
        """Test compile_schema_to_sql with array type."""
        schema = {"type": "array"}
        result = compile_schema_to_sql(schema)
        assert "'array'" in result

    def test_compile_schema_type_bool(self):
        """Test compile_schema_to_sql with bool type."""
        schema = {"type": "bool"}
        result = compile_schema_to_sql(schema)
        assert result == "1"

    def test_compile_schema_type_boolean(self):
        """Test compile_schema_to_sql with boolean type."""
        schema = {"type": "boolean"}
        result = compile_schema_to_sql(schema)
        assert result == "1"

    def test_compile_schema_type_null(self):
        """Test compile_schema_to_sql with null type."""
        schema = {"type": "null"}
        result = compile_schema_to_sql(schema)
        assert "'null'" in result

    def test_compile_schema_type_list(self):
        """Test compile_schema_to_sql with list of types."""
        schema = {"type": ["string", "integer"]}
        result = compile_schema_to_sql(schema)
        assert "'text'" in result
        assert "'integer'" in result

    def test_compile_schema_minimum(self):
        """Test compile_schema_to_sql with minimum."""
        schema = {"minimum": 10}
        result = compile_schema_to_sql(schema)
        assert ">= 10" in result

    def test_compile_schema_maximum(self):
        """Test compile_schema_to_sql with maximum."""
        schema = {"maximum": 20}
        result = compile_schema_to_sql(schema)
        assert "<= 20" in result

    def test_compile_schema_exclusive_minimum(self):
        """Test compile_schema_to_sql with exclusiveMinimum."""
        schema = {"exclusiveMinimum": 10}
        result = compile_schema_to_sql(schema)
        assert "> 10" in result

    def test_compile_schema_exclusive_maximum(self):
        """Test compile_schema_to_sql with exclusiveMaximum."""
        schema = {"exclusiveMaximum": 20}
        result = compile_schema_to_sql(schema)
        assert "< 20" in result

    def test_compile_schema_jsonb(self):
        """Test compile_schema_to_sql with jsonb=True."""
        schema = {"required": ["name"]}
        result = compile_schema_to_sql(schema, jsonb=True)
        assert "jsonb_extract" in result

    def test_compile_schema_empty(self):
        """Test compile_schema_to_sql with empty schema."""
        schema = {}
        result = compile_schema_to_sql(schema)
        assert result == "1"

    def test_compile_schema_bson_type(self):
        """Test compile_schema_to_sql with bsonType."""
        schema = {"bsonType": "string"}
        result = compile_schema_to_sql(schema)
        assert "'text'" in result

    def test_compile_schema_nested(self):
        """Test compile_schema_to_sql with nested properties."""
        schema = {
            "properties": {
                "profile": {"properties": {"name": {"type": "string"}}}
            }
        }
        result = compile_schema_to_sql(schema)
        assert "profile" in result
        assert "name" in result
