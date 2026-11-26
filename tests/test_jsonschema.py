from datetime import datetime

import pytest

from neosqlite import Connection
from neosqlite.binary import Binary
from neosqlite.collection.query_helper.schema_compiler import (
    compile_query_schema_to_sql,
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
        assert "'true'" in result
        assert "'false'" in result

    def test_compile_schema_type_boolean(self):
        """Test compile_schema_to_sql with boolean type."""
        schema = {"type": "boolean"}
        result = compile_schema_to_sql(schema)
        assert "'true'" in result
        assert "'false'" in result

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
        schema = {"required": ["name"], "properties": {"age": {"minimum": 10}}}
        result = compile_schema_to_sql(schema, jsonb=True)
        assert "json_type" in result
        assert "jsonb_extract" in result

    def test_compile_query_schema_items_honors_json_each_function(self):
        """Use jsonb_each when available; json_each on SQLite 3.45-3.50."""
        schema = {"type": "array", "items": {"type": "integer"}}

        legacy = compile_query_schema_to_sql(schema, jsonb=True)
        assert legacy is not None
        assert "json_each(" in legacy[0]
        assert "jsonb_each" not in legacy[0]

        modern = compile_query_schema_to_sql(
            schema, jsonb=True, json_each_function="jsonb_each"
        )
        assert modern is not None
        assert "jsonb_each(" in modern[0]

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


class TestIssue195JsonSchemaValidatorFixes:
    """Regression tests for GitHub issue #195 $jsonSchema bugs."""

    def test_required_allows_explicit_null_when_type_permits(self):
        """required uses json_type IS NOT NULL so explicit None passes when nullable."""
        import neosqlite
        from neosqlite._sqlite import sqlite3

        schema = {
            "$jsonSchema": {
                "required": ["x"],
                "properties": {"x": {"type": ["string", "null"]}},
            }
        }
        with neosqlite.Connection(":memory:") as conn:
            c = conn.create_collection("nullable_req", validator=schema)
            c.insert_one({"x": None})
            c.insert_one({"x": "ok"})
            assert c.count_documents({}) == 2
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"y": 1})
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"x": 123})

    def test_numeric_bounds_do_not_reject_non_numeric_or_bool(self):
        """minimum/maximum only constrain numeric values, not strings or bools."""
        import neosqlite
        from neosqlite._sqlite import sqlite3

        schema = {
            "$jsonSchema": {"properties": {"v": {"minimum": 1, "maximum": 100}}}
        }
        with neosqlite.Connection(":memory:") as conn:
            c = conn.create_collection("bounds", validator=schema)
            c.insert_one({"v": "hello"})
            c.insert_one({"v": False})
            c.insert_one({"v": 50})
            assert c.count_documents({}) == 3
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"v": 0})
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"v": 101})

    def test_collection_validator_enforces_bool_enum_pattern_items_combinators(
        self,
    ):
        """Collection validator enforces bool, enum, lengths, pattern, items, and combinators."""
        import neosqlite
        from neosqlite._sqlite import sqlite3

        schema = {
            "$jsonSchema": {
                "properties": {
                    "flag": {"bsonType": "bool"},
                    "role": {"enum": ["admin", "user"]},
                    "code": {
                        "type": "string",
                        "minLength": 2,
                        "maxLength": 5,
                        "pattern": r"^[A-Z]+\d+$",
                    },
                    "tags": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 3,
                        "items": {"type": "string"},
                    },
                },
                "anyOf": [{"required": ["flag"]}, {"required": ["role"]}],
            }
        }
        with neosqlite.Connection(":memory:") as conn:
            c = conn.create_collection("strict_docs", validator=schema)
            c.insert_one(
                {
                    "_id": 1,
                    "flag": True,
                    "role": "admin",
                    "code": "A12",
                    "tags": ["ok"],
                }
            )

            # Invalid bool
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"flag": "yes", "role": "admin"})
            # Invalid enum
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"flag": True, "role": "guest"})
            # Invalid pattern on insert
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"flag": True, "code": "bad!"})
            # Invalid items element on insert
            with pytest.raises(sqlite3.IntegrityError):
                c.insert_one({"flag": True, "tags": [123]})
            # Invalid pattern and items on update_one (without mutating row)
            with pytest.raises(sqlite3.IntegrityError):
                c.update_one({"_id": 1}, {"$set": {"code": "bad!"}})
            with pytest.raises(sqlite3.IntegrityError):
                c.update_one({"_id": 1}, {"$push": {"tags": 999}})
            with pytest.raises(sqlite3.IntegrityError):
                c.replace_one({"_id": 1}, {"role": "user", "tags": [123]})
            doc = c.find_one({"_id": 1})
            assert doc["code"] == "A12"
            assert doc["tags"] == ["ok"]

    def test_enum_bool_vs_int_and_dotted_properties_parity(self):
        """enum rejects True for [1, 2] and dotted properties match literal keys in SQL and Python."""
        import neosqlite
        from neosqlite.collection.query_helper import (
            get_force_fallback,
            set_force_fallback,
        )

        assert (
            matches_json_schema(
                {"v": True}, {"properties": {"v": {"enum": [1, 2]}}}
            )
            is False
        )
        assert (
            matches_json_schema(
                {"v": False}, {"properties": {"v": {"enum": [0]}}}
            )
            is False
        )
        assert (
            matches_json_schema(
                {"v": 1}, {"properties": {"v": {"enum": [1, 2]}}}
            )
            is True
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.enum_dot
            coll.insert_many(
                [
                    {"_id": 1, "v": True, "a.b": "lit"},
                    {"_id": 2, "v": 1, "a": {"b": "nested"}},
                ]
            )
            q_enum = {"$jsonSchema": {"properties": {"v": {"enum": [1, 2]}}}}
            q_dot = {
                "$jsonSchema": {
                    "required": ["a.b"],
                    "properties": {"a.b": {"type": "string"}},
                }
            }
            orig = get_force_fallback()
            try:
                for fb in (False, True):
                    set_force_fallback(fb)
                    assert [d["_id"] for d in coll.find(q_enum)] == [2]
                    assert [d["_id"] for d in coll.find(q_dot)] == [1]
            finally:
                set_force_fallback(orig)

    def test_validator_survives_reopen_and_rename(self, tmp_path):
        """Persisted $jsonSchema rules remain enforced after reconnection."""
        import neosqlite
        from neosqlite._sqlite import sqlite3

        schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "properties": {
                    "role": {"bsonType": "string", "pattern": "^admin$"}
                },
            }
        }
        path = str(tmp_path / "validators.db")
        with neosqlite.Connection(path) as conn:
            coll = conn.create_collection("v", validator=schema)
            coll.insert_one({"_id": 1, "role": "admin"})
            with pytest.raises(sqlite3.IntegrityError):
                coll.insert_one({"_id": 2, "role": "bad!"})

        with neosqlite.Connection(path) as conn:
            coll = conn.v
            assert coll._validator_schema == schema["$jsonSchema"]
            with pytest.raises(sqlite3.IntegrityError):
                coll.insert_one({"_id": 3, "role": "bad!"})
            with pytest.raises(sqlite3.IntegrityError):
                coll.update_one({"_id": 1}, {"$set": {"role": "bad!"}})
            conn.rename_collection("v", "v2")

        with neosqlite.Connection(path) as conn:
            coll = conn.v2
            assert coll._validator_schema == schema["$jsonSchema"]
            with pytest.raises(sqlite3.IntegrityError):
                coll.insert_one({"_id": 4, "role": "bad!"})
            assert conn.list_collection_names() == ["v2"]
            coll.drop()
            assert conn.list_collection_names() == []
