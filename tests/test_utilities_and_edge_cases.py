"""
Consolidated tests for utilities, edges cases, and miscellaneous functionality.
"""

import os
import tempfile

import pytest

import neosqlite
from neosqlite import Binary, Connection
from neosqlite.collection.query_helper import (
    QueryHelper,
    _convert_bytes_to_binary,
    get_force_fallback,
    set_force_fallback,
)
from neosqlite.exceptions import MalformedDocument, MalformedQueryException

# ================================
# Database Property Tests
# ================================


def test_database_property():
    """Test that the database property returns the correct database object."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create a connection
        with Connection(tmp_path) as conn:
            # Get a collection
            collection = conn["test_collection"]

            # Verify that the database property returns the connection
            assert collection.database is conn
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_database_property_multiple_collections():
    """Test that the database property works correctly with multiple collections."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create a connection
        with Connection(tmp_path) as conn:
            # Get multiple collections
            collection1 = conn["test_collection1"]
            collection2 = conn["test_collection2"]

            # Verify that both collections have the same database
            assert collection1.database is conn
            assert collection2.database is conn
            assert collection1.database is collection2.database
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_database_property_attribute_access():
    """Test that the database property works with attribute-style access."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create a connection
        with Connection(tmp_path) as conn:
            # Get a collection using attribute access
            collection = conn.test_collection

            # Verify that the database property returns the connection
            assert collection.database is conn
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ================================


# ================================
# Edge Case Tests
# ================================


# ================================
# Simple Coverage Tests
# ================================


def test_database_property_access():
    """Test database property access to improve coverage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        # Access the database property
        db = collection.database
        assert db is conn


def test_collection_attribute_access():
    """Test collection attribute access."""
    with neosqlite.Connection(":memory:") as conn:
        # Access collection via attribute
        collection = conn.test_collection
        assert collection.name == "test_collection"

        # Insert a document
        result = collection.insert_one({"test": "value"})
        assert result.inserted_id is not None
        from neosqlite.objectid import ObjectId

        assert isinstance(result.inserted_id, ObjectId)

        # Find the document
        doc = collection.find_one({"_id": result.inserted_id})
        assert doc is not None
        assert doc["test"] == "value"


# ================================
# Miscellaneous Tests
# ================================


def test_malformed_document_error():
    """Test that inserting a non-dict document raises MalformedDocument."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with a non-dict document
    with pytest.raises(MalformedDocument):
        collection.query_engine.helpers._internal_insert("not a dict")


def test_build_update_clause_unsupported_operator():
    """Test _build_update_clause with an unsupported operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with an unsupported operator
    result = collection.query_engine.helpers._build_update_clause(
        {"$unsupported": {"field": "value"}}
    )
    # Should fall back to None
    assert result is None


def test_get_val_none_value():
    """Test _get_val with None value in path."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with None value in path
    result = collection._get_val({"a": None}, "a.b")
    assert result is None


def test_get_val_dollar_prefix():
    """Test _get_val with dollar prefix in key."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with dollar prefix in key (should strip the $)
    result = collection._get_val({"a": "value"}, "$a")
    assert result == "value"


def test_get_val_non_string_key():
    """Test _get_val with non-string key (literal value like $group _id)."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with integer key - should return the key itself (literal value)
    result = collection._get_val({"a": "value"}, 1)
    assert result == 1

    # Test with float key
    result = collection._get_val({"a": "value"}, 3.14)
    assert result == 3.14

    # Test with null key
    result = collection._get_val({"a": "value"}, None)
    assert result is None


def test_group_with_literal_id():
    """Test $group with literal _id value (integer, not field reference)."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    collection.insert_many(
        [
            {"item": "apple", "qty": 5},
            {"item": "banana", "qty": 3},
            {"item": "cherry", "qty": 5},
        ]
    )

    # Group all docs into one bucket using literal _id: 1
    result = list(
        collection.aggregate(
            [{"$group": {"_id": 1, "total_qty": {"$sum": "$qty"}}}]
        )
    )
    assert len(result) == 1
    assert result[0]["_id"] == 1
    assert result[0]["total_qty"] == 13


def test_load_bytes_data():
    """Test _load with bytes data."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with bytes data
    result = collection._load(1, b'{"test": "value"}')
    assert result == {"_id": 1, "test": "value"}


def test_unsupported_update_operator():
    """Test _internal_update with an unsupported operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with an unsupported operator
    with pytest.raises(
        MalformedQueryException,
        match="Update operator '\\$unsupported' not supported",
    ):
        collection.query_engine.helpers._internal_update(
            1, {"$unsupported": {"field": "value"}}, {"_id": 1}
        )


def test_internal_update_sql_path():
    """Test _internal_update with SQL-based path."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert a document
    doc_id = collection.insert_one(
        {"name": "Alice", "age": 30, "score": 100}
    ).inserted_id

    # Update using SQL-based path (simple operations)
    original_doc = collection.find_one({"_id": doc_id})
    result, modified = collection.query_engine.helpers._internal_update(
        doc_id,
        {"$set": {"name": "Bob"}, "$inc": {"age": 5}, "$mul": {"score": 1.1}},
        original_doc,
    )

    # Verify the update worked
    assert modified
    assert result["name"] == "Bob"
    assert result["age"] == 35
    assert result["score"] == pytest.approx(110.0)  # 100 * 1.1


def test_internal_update_python_path():
    """Test _internal_update with Python-based path for complex operations."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert a document
    doc_id = collection.insert_one(
        {"name": "Alice", "items": [1, 2, 3]}
    ).inserted_id

    # Update using Python-based path (complex operations)
    original_doc = collection.find_one({"_id": doc_id})
    result, modified = collection.query_engine.helpers._internal_update(
        doc_id, {"$push": {"items": 4}, "$set": {"name": "Bob"}}, original_doc
    )

    # Verify the update worked
    assert modified
    assert result["name"] == "Bob"
    assert result["items"] == [1, 2, 3, 4]


def test_internal_update_mixed_operations():
    """Test _internal_update with mixed operations (falls back to Python)."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert a document
    doc_id = collection.insert_one(
        {"name": "Alice", "age": 30, "items": [1, 2, 3]}
    ).inserted_id

    # Update with mixed operations (should fall back to Python)
    original_doc = collection.find_one({"_id": doc_id})
    result, modified = collection.query_engine.helpers._internal_update(
        doc_id,
        {
            "$set": {"name": "Bob"},
            "$inc": {"age": 5},
            "$push": {"items": 4},  # This requires Python fallback
        },
        original_doc,
    )

    # Verify the update worked
    assert result["name"] == "Bob"
    assert result["age"] == 35
    assert result["items"] == [1, 2, 3, 4]


def test_convert_bytes_to_binary_edge_cases():
    """Test edge cases for _convert_bytes_to_binary function."""
    # Test with None
    assert _convert_bytes_to_binary(None) is None

    # Test with integer
    assert _convert_bytes_to_binary(42) == 42

    # Test with string
    assert _convert_bytes_to_binary("test") == "test"

    # Test with empty bytes
    result = _convert_bytes_to_binary(b"")
    assert isinstance(result, Binary)
    assert result == b""
    assert result.subtype == Binary.BINARY_SUBTYPE

    # Test with nested structures containing various types
    test_data = {
        "bytes": b"test",
        "binary": Binary(b"binary", Binary.FUNCTION_SUBTYPE),
        "list": [b"item1", Binary(b"item2", Binary.UUID_SUBTYPE), 42, "string"],
        "nested": {
            "inner_bytes": b"inner",
            "inner_binary": Binary(b"inner_binary", Binary.OLD_BINARY_SUBTYPE),
        },
        "none": None,
        "int": 123,
        "float": 3.14,
    }

    result = _convert_bytes_to_binary(test_data)

    # Check bytes conversion
    assert isinstance(result["bytes"], Binary)
    assert result["bytes"].subtype == Binary.BINARY_SUBTYPE

    # Check binary preservation
    assert isinstance(result["binary"], Binary)
    assert result["binary"].subtype == Binary.FUNCTION_SUBTYPE
    assert result["binary"] is test_data["binary"]  # Same object

    # Check list processing
    assert isinstance(result["list"][0], Binary)
    assert isinstance(result["list"][1], Binary)
    assert result["list"][1].subtype == Binary.UUID_SUBTYPE
    assert result["list"][1] is test_data["list"][1]  # Same object
    assert result["list"][2] == 42  # Int unchanged
    assert result["list"][3] == "string"  # String unchanged

    # Check nested structure
    assert isinstance(result["nested"]["inner_bytes"], Binary)
    assert isinstance(result["nested"]["inner_binary"], Binary)
    assert result["nested"]["inner_binary"].subtype == Binary.OLD_BINARY_SUBTYPE
    assert (
        result["nested"]["inner_binary"] is test_data["nested"]["inner_binary"]
    )

    # Check unchanged values
    assert result["none"] is None
    assert result["int"] == 123
    assert result["float"] == 3.14


def test_force_fallback_functionality():
    """Test force fallback functionality."""
    # Check default state
    assert not get_force_fallback()

    # Turn on fallback
    set_force_fallback(True)
    assert get_force_fallback()

    # Turn off fallback
    set_force_fallback(False)
    assert not get_force_fallback()

    # Test with explicit False
    set_force_fallback(False)
    assert not get_force_fallback()

    # Test with explicit True
    set_force_fallback(True)
    assert get_force_fallback()

    # Reset to default for other tests
    set_force_fallback(False)


def test_query_helper_initialization():
    """Test QueryHelper initialization."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)
        assert helper.collection is collection


def test_can_use_sql_updates_with_binary_values():
    """Test _can_use_sql_updates with binary values."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with binary values in update spec
        update_spec = {"$set": {"data": b"binary_data"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return True because raw bytes don't have encode_for_storage attribute
        # The check is for isinstance(val, bytes) AND hasattr(val, "encode_for_storage")
        # Since raw bytes don't have encode_for_storage, this check fails
        assert result

        # Test with Binary objects in update spec
        update_spec = {"$set": {"data": Binary(b"binary_data")}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return False because Binary objects have encode_for_storage attribute
        assert not result

        # Test with regular values (should return True)
        update_spec = {"$set": {"data": "regular_data"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return True for supported operations
        assert result

        # Test with $rename (now supported in SQL)
        update_spec = {"$rename": {"old": "new"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return True now that $rename is supported in SQL
        assert result

        # Test with upsert (doc_id = 0, should return False)
        update_spec = {"$set": {"data": "regular_data"}}
        result = helper._can_use_sql_updates(update_spec, 0)
        # Should return False for upserts
        assert not result


def test_build_sql_update_clause_with_binary():
    """Test _build_sql_update_clause with binary data."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test $set with raw bytes
        clauses, params = helper._build_sql_update_clause(
            "$set", {"field": b"test_data"}
        )
        assert len(clauses) == 1
        assert clauses[0] == "'$.field', json(?)"
        assert len(params) == 1
        # Params should contain JSON serialized binary data

        # Test $set with Binary object
        binary_obj = Binary(b"binary_data", Binary.FUNCTION_SUBTYPE)
        clauses, params = helper._build_sql_update_clause(
            "$set", {"field": binary_obj}
        )
        assert len(clauses) == 1
        assert clauses[0] == "'$.field', json(?)"
        assert len(params) == 1
        # Params should contain JSON serialized binary data

        # Test $inc with binary (should be handled as regular value)
        clauses, params = helper._build_sql_update_clause(
            "$inc", {"field": b"10"}
        )
        # For $inc, binary should be treated as regular value
        assert len(clauses) == 1
        assert "'$.field'" in clauses[0]
        assert len(params) == 1


def test_internal_insert_with_complex_document():
    """Test _internal_insert with complex document containing various data types."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Create a complex document
        complex_doc = {
            "name": "Test User",
            "age": 30,
            "active": True,
            "scores": [85, 92, 78],
            "address": {"street": "123 Main St", "city": "Test City"},
            "tags": ["tag1", "tag2", "tag3"],
            "binary_data": b"some binary data",
            "binary_object": Binary(b"typed binary", Binary.FUNCTION_SUBTYPE),
            "none_value": None,
            "empty_list": [],
            "empty_dict": {},
        }

        # Insert the document
        doc_id = helper._internal_insert(complex_doc)

        # Verify the document was inserted
        assert doc_id is not None
        assert "_id" in complex_doc
        # With ObjectId implementation, the document should have an ObjectId in _id field
        from neosqlite.objectid import ObjectId

        assert isinstance(complex_doc["_id"], ObjectId)

        # Retrieve and verify the document
        retrieved_doc = collection.find_one({"_id": doc_id})
        assert retrieved_doc is not None
        assert retrieved_doc["name"] == "Test User"
        assert retrieved_doc["age"] == 30
        assert retrieved_doc["active"]
        assert retrieved_doc["scores"] == [85, 92, 78]
        assert retrieved_doc["address"]["street"] == "123 Main St"
        assert retrieved_doc["address"]["city"] == "Test City"
        assert retrieved_doc["tags"] == ["tag1", "tag2", "tag3"]

        # Check binary data handling
        assert isinstance(retrieved_doc["binary_data"], Binary)
        assert bytes(retrieved_doc["binary_data"]) == b"some binary data"
        assert retrieved_doc["binary_data"].subtype == Binary.BINARY_SUBTYPE

        assert isinstance(retrieved_doc["binary_object"], Binary)
        assert bytes(retrieved_doc["binary_object"]) == b"typed binary"
        assert retrieved_doc["binary_object"].subtype == Binary.FUNCTION_SUBTYPE

        assert retrieved_doc["none_value"] is None
        assert retrieved_doc["empty_list"] == []
        assert retrieved_doc["empty_dict"] == {}


def test_internal_insert_edge_cases():
    """Test _internal_insert with edge cases."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with non-dict document (should raise MalformedDocument)
        with pytest.raises(MalformedDocument):
            helper._internal_insert("not a dict")

        # Test with non-dict document (should raise MalformedDocument)
        with pytest.raises(MalformedDocument):
            helper._internal_insert(123)

        # Test with list (should raise MalformedDocument)
        with pytest.raises(MalformedDocument):
            helper._internal_insert([1, 2, 3])


def test_perform_python_update_operations():
    """Test _perform_python_update with various operations."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Insert a test document
        doc = {"name": "Test", "age": 25, "scores": [80, 85], "active": True}
        doc_id = helper._internal_insert(doc)

        # Test $set operation
        update_spec = {"$set": {"age": 30, "city": "New York"}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, doc
        )
        assert modified
        assert result["age"] == 30
        assert result["city"] == "New York"

        # Test $unset operation
        update_spec = {"$unset": {"city": ""}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert "city" not in result

        # Test $inc operation
        update_spec = {"$inc": {"age": 5}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["age"] == 35

        # Test $push operation
        update_spec = {"$push": {"scores": 90}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["scores"] == [80, 85, 90]

        # Test $pull operation
        update_spec = {"$pull": {"scores": 85}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["scores"] == [80, 90]

        # Test $pop operation (pop last element)
        update_spec = {"$pop": {"scores": 1}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["scores"] == [80]

        # Test $pop operation (pop first element)
        update_spec = {"$pop": {"scores": -1}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["scores"] == []

        # Test $rename operation
        update_spec = {"$rename": {"name": "full_name"}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert "name" not in result
        assert result["full_name"] == "Test"

        # Test $mul operation
        update_spec = {"$mul": {"age": 2}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["age"] == 70

        # Test $min operation
        update_spec = {"$min": {"age": 50}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["age"] == 50

        # Test $max operation
        update_spec = {"$max": {"age": 60}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert result["age"] == 60

        # Test $currentDate operation
        update_spec = {"$currentDate": {"lastModified": True}}
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert modified
        assert "lastModified" in result
        # Check that it's an ISO datetime string
        from datetime import datetime

        datetime.fromisoformat(result["lastModified"])

        # Test $setOnInsert operation (only applies on upsert, doc_id=0)
        update_spec = {"$setOnInsert": {"createdAt": "2023-01-01"}}
        # For existing doc (doc_id != 0), should not apply
        result, modified = helper._perform_python_update(
            doc_id, update_spec, result
        )
        assert not modified  # No changes for non-upsert
        assert "createdAt" not in result
        # For upsert (doc_id == 0), should apply
        upsert_doc = {"name": "New"}
        result, modified = helper._perform_python_update(
            0, update_spec, upsert_doc
        )
        assert modified
        assert result["createdAt"] == "2023-01-01"

        # Test unsupported operation (should raise MalformedQueryException)
        update_spec = {"$unsupported": {"field": "value"}}
        with pytest.raises(MalformedQueryException):
            helper._perform_python_update(doc_id, update_spec, result)


def test_internal_replace_and_delete():
    """Test _internal_replace and _internal_delete operations."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Insert a test document
        doc = {"name": "Test", "age": 25}
        doc_id = helper._internal_insert(doc)

        # Verify document exists
        retrieved = collection.find_one({"_id": doc_id})
        assert retrieved is not None
        assert retrieved["name"] == "Test"
        assert retrieved["age"] == 25

        # Test _internal_replace
        new_doc = {"name": "Updated", "age": 30, "city": "New York"}
        helper._internal_replace(doc_id, new_doc)

        # Verify replacement
        retrieved = collection.find_one({"_id": doc_id})
        assert retrieved is not None
        assert retrieved["name"] == "Updated"
        assert retrieved["age"] == 30
        assert retrieved["city"] == "New York"

        # Test _internal_delete
        helper._internal_delete(doc_id)

        # Verify deletion
        retrieved = collection.find_one({"_id": doc_id})
        assert retrieved is None


def test_build_update_clause_edge_cases():
    """Test _build_update_clause with edge cases."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with $rename (should return None to force Python fallback)
        update = {"$rename": {"old_field": "new_field"}}
        result = helper._build_update_clause(update)
        assert result is None

        # Test with unsupported operator (should return None)
        update = {"$unsupported": {"field": "value"}}
        result = helper._build_update_clause(update)
        assert result is None

        # Test with empty $unset (should return None)
        update = {"$unset": {}}
        result = helper._build_update_clause(update)
        assert result is None


def test_is_text_search_query():
    """Test _is_text_search_query function."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with text search query
        query = {"$text": {"$search": "test"}}
        result = helper._is_text_search_query(query)
        assert result

        # Test without text search query
        query = {"name": "test"}
        result = helper._is_text_search_query(query)
        assert not result

        # Test with empty query
        query = {}
        result = helper._is_text_search_query(query)
        assert not result
