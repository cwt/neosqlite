"""
Consolidated tests for utilities, edges cases, and miscellaneous functionality.
"""

import base64
import os
import tempfile
import uuid
from unittest.mock import patch

import pytest
import neosqlite
from neosqlite import Connection, Binary
from neosqlite.collection.query_helper import (
    _convert_bytes_to_binary,
    set_force_fallback,
    get_force_fallback,
    QueryHelper,
)
from neosqlite.exceptions import MalformedDocument, MalformedQueryException


# ================================
# Connection Tests
# ================================


def test_connect():
    conn = neosqlite.Connection(":memory:")
    assert conn.db.isolation_level is None


@patch("neosqlite.connection.sqlite3")
def test_context_manager_closes_connection(sqlite):
    with neosqlite.Connection() as conn:
        pass
    assert conn.db.close.called


@patch("neosqlite.connection.sqlite3")
@patch("neosqlite.connection.Collection")
def test_getitem_returns_collection(mock_collection, sqlite):
    sqlite.connect.return_value = sqlite
    mock_collection.return_value = mock_collection
    conn = neosqlite.Connection()
    assert "foo" not in conn._collections
    assert conn["foo"] == mock_collection


@patch("neosqlite.connection.sqlite3")
def test_getitem_returns_cached_collection(sqlite):
    conn = neosqlite.Connection()
    conn._collections["foo"] = "bar"
    assert conn["foo"] == "bar"


@patch("neosqlite.connection.sqlite3")
def test_drop_collection(sqlite):
    conn = neosqlite.Connection()
    conn.drop_collection("foo")
    conn.db.execute.assert_called_with("DROP TABLE IF EXISTS foo")


@patch("neosqlite.connection.sqlite3")
def test_getattr_returns_attribute(sqlite):
    conn = neosqlite.Connection()
    assert conn.__getattr__("db") is not None


@patch("neosqlite.connection.sqlite3")
def test_getattr_returns_collection(sqlite):
    conn = neosqlite.Connection()
    foo = conn.__getattr__("foo")
    assert isinstance(foo, neosqlite.Collection)


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
# Binary Tests
# ================================


def test_binary_creation():
    """Test Binary creation with different data types."""
    # Test with bytes
    data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
    binary = Binary(data)
    assert isinstance(binary, Binary)
    assert isinstance(binary, bytes)
    assert bytes(binary) == data
    assert binary.subtype == Binary.BINARY_SUBTYPE

    # Test with bytearray
    bytearray_data = bytearray(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")
    binary = Binary(bytearray_data)
    assert bytes(binary) == data

    # Test with memoryview
    memoryview_data = memoryview(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")
    binary = Binary(memoryview_data)
    assert bytes(binary) == data

    # Test with different subtypes
    binary = Binary(data, Binary.FUNCTION_SUBTYPE)
    assert binary.subtype == Binary.FUNCTION_SUBTYPE

    # Test with default subtype
    binary = Binary(data)
    assert binary.subtype == Binary.BINARY_SUBTYPE


def test_binary_creation_errors():
    """Test Binary creation with invalid data."""
    # Test with invalid data type
    with pytest.raises(TypeError):
        Binary("invalid data")

    # Test with invalid subtype type
    with pytest.raises(TypeError):
        Binary(b"data", "invalid subtype")

    # Test with invalid subtype range
    with pytest.raises(ValueError):
        Binary(b"data", -1)

    with pytest.raises(ValueError):
        Binary(b"data", 256)


def test_binary_subtype_constants():
    """Test Binary subtype constants."""
    assert Binary.BINARY_SUBTYPE == 0
    assert Binary.FUNCTION_SUBTYPE == 1
    assert Binary.OLD_BINARY_SUBTYPE == 2
    assert Binary.UUID_SUBTYPE == 4
    assert Binary.MD5_SUBTYPE == 5
    assert Binary.COLUMN_SUBTYPE == 7
    assert Binary.SENSITIVE_SUBTYPE == 8
    assert Binary.VECTOR_SUBTYPE == 9
    assert Binary.USER_DEFINED_SUBTYPE == 128


def test_binary_encode_for_storage():
    """Test Binary encoding for storage."""
    data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
    binary = Binary(data, Binary.FUNCTION_SUBTYPE)

    encoded = binary.encode_for_storage()
    assert isinstance(encoded, dict)
    assert "__neosqlite_binary__" in encoded
    assert encoded["__neosqlite_binary__"] is True
    assert "data" in encoded
    assert "subtype" in encoded
    assert encoded["subtype"] == Binary.FUNCTION_SUBTYPE

    # Check that data is properly base64 encoded
    decoded_data = base64.b64decode(encoded["data"])
    assert decoded_data == data


def test_binary_decode_from_storage():
    """Test Binary decoding from storage."""
    data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
    original_binary = Binary(data, Binary.FUNCTION_SUBTYPE)

    # Encode and then decode
    encoded = original_binary.encode_for_storage()
    decoded_binary = Binary.decode_from_storage(encoded)

    assert isinstance(decoded_binary, Binary)
    assert bytes(decoded_binary) == data
    assert decoded_binary.subtype == Binary.FUNCTION_SUBTYPE


def test_binary_decode_from_storage_errors():
    """Test Binary decoding with invalid data."""
    # Test with invalid data type
    with pytest.raises(ValueError):
        Binary.decode_from_storage("invalid data")

    # Test with missing marker
    with pytest.raises(ValueError):
        Binary.decode_from_storage({"data": "invalid", "subtype": 0})

    # Test with missing data
    with pytest.raises(ValueError):
        Binary.decode_from_storage({"__neosqlite_binary__": True})


def test_binary_from_uuid():
    """Test creating Binary from UUID."""
    test_uuid = uuid.uuid4()
    binary = Binary.from_uuid(test_uuid)

    assert isinstance(binary, Binary)
    assert binary.subtype == Binary.UUID_SUBTYPE
    assert len(binary) == 16  # UUID is 16 bytes

    # Convert back to UUID
    converted_uuid = binary.as_uuid()
    assert converted_uuid == test_uuid


def test_binary_from_uuid_errors():
    """Test Binary.from_uuid with invalid data."""
    with pytest.raises(TypeError):
        Binary.from_uuid("invalid uuid")


def test_binary_as_uuid():
    """Test converting Binary to UUID."""
    test_uuid = uuid.uuid4()
    binary = Binary.from_uuid(test_uuid)

    # Convert back to UUID
    converted_uuid = binary.as_uuid()
    assert isinstance(converted_uuid, uuid.UUID)
    assert converted_uuid == test_uuid


def test_binary_as_uuid_errors():
    """Test Binary.as_uuid with wrong subtype."""
    # Create binary with non-UUID subtype
    binary = Binary(b"\x00" * 16, Binary.BINARY_SUBTYPE)

    # Try to convert to UUID - should fail
    with pytest.raises(ValueError):
        binary.as_uuid()


def test_binary_repr():
    """Test Binary string representation."""
    data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"

    # Test default subtype
    binary = Binary(data)
    repr_str = repr(binary)
    assert "Binary(" in repr_str
    assert repr(data) in repr_str

    # Test with custom subtype
    binary = Binary(data, Binary.FUNCTION_SUBTYPE)
    repr_str = repr(binary)
    assert "Binary(" in repr_str
    assert repr(data) in repr_str
    assert str(Binary.FUNCTION_SUBTYPE) in repr_str


def test_binary_equality():
    """Test Binary equality comparisons."""
    data1 = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
    data2 = b"\x09\x08\x07\x06\x05\x04\x03\x02\x01\x00"

    binary1 = Binary(data1)
    binary2 = Binary(data1)  # Same data
    binary3 = Binary(data2)  # Different data
    binary4 = Binary(
        data1, Binary.FUNCTION_SUBTYPE
    )  # Same data, different subtype

    # Test equality with same Binary
    assert binary1 == binary2
    assert not (binary1 != binary2)

    # Test inequality with different data
    assert binary1 != binary3
    assert not (binary1 == binary3)

    # Test inequality with different subtype
    assert binary1 != binary4
    assert not (binary1 == binary4)

    # Test equality with bytes (only when subtype is BINARY_SUBTYPE)
    assert binary1 == data1
    assert not (binary1 != data1)

    # Test inequality with bytes when subtype is not BINARY_SUBTYPE
    assert binary4 != data1
    assert not (binary4 == data1)

    # Test equality with non-Binary, non-bytes object
    assert not (binary1 == "not binary")
    assert binary1 != "not binary"


def test_binary_hash():
    """Test Binary hash functionality."""
    data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
    binary1 = Binary(data)
    binary2 = Binary(data)  # Same data
    binary3 = Binary(
        data, Binary.FUNCTION_SUBTYPE
    )  # Same data, different subtype

    # Same data and subtype should have same hash
    assert hash(binary1) == hash(binary2)

    # Different subtype should have different hash
    assert hash(binary1) != hash(binary3)


def test_binary_subclassing():
    """Test that Binary can be subclassed."""
    data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"

    class CustomBinary(Binary):
        def custom_method(self):
            return "custom"

    custom_binary = CustomBinary(data)
    assert isinstance(custom_binary, CustomBinary)
    assert isinstance(custom_binary, Binary)
    assert isinstance(custom_binary, bytes)
    assert custom_binary.custom_method() == "custom"
    assert bytes(custom_binary) == data


def test_convert_bytes_to_binary():
    """Test the _convert_bytes_to_binary helper function directly."""
    # Test with raw bytes
    raw_bytes = b"test data"
    result = _convert_bytes_to_binary(raw_bytes)
    assert isinstance(result, Binary)
    assert result == raw_bytes
    assert result.subtype == Binary.BINARY_SUBTYPE

    # Test with existing Binary object (should preserve subtype)
    binary_with_subtype = Binary(b"test data", Binary.FUNCTION_SUBTYPE)
    result = _convert_bytes_to_binary(binary_with_subtype)
    assert isinstance(result, Binary)
    assert result == binary_with_subtype
    assert result.subtype == Binary.FUNCTION_SUBTYPE
    assert result is binary_with_subtype  # Should be the same object

    # Test with dict containing bytes
    test_dict = {
        "data": b"test",
        "binary": Binary(b"binary", Binary.UUID_SUBTYPE),
    }
    result = _convert_bytes_to_binary(test_dict)
    assert isinstance(result, dict)
    assert isinstance(result["data"], Binary)
    assert result["data"].subtype == Binary.BINARY_SUBTYPE
    assert isinstance(result["binary"], Binary)
    assert result["binary"].subtype == Binary.UUID_SUBTYPE
    assert result["binary"] is test_dict["binary"]  # Should be the same object

    # Test with list containing bytes
    test_list = [b"test", Binary(b"binary", Binary.OLD_BINARY_SUBTYPE)]
    result = _convert_bytes_to_binary(test_list)
    assert isinstance(result, list)
    assert isinstance(result[0], Binary)
    assert result[0].subtype == Binary.BINARY_SUBTYPE
    assert isinstance(result[1], Binary)
    assert result[1].subtype == Binary.OLD_BINARY_SUBTYPE
    assert result[1] is test_list[1]  # Should be the same object

    # Test with other types (should be unchanged)
    test_string = "test"
    result = _convert_bytes_to_binary(test_string)
    assert result == test_string

    test_int = 42
    result = _convert_bytes_to_binary(test_int)
    assert result == test_int


def test_insert_raw_bytes_converts_to_binary():
    """Test that raw bytes are converted to Binary objects when inserting."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert document with raw bytes
        raw_bytes = b"\x00\x01\x02\x03"
        result = collection.insert_one({"data": raw_bytes})

        # Retrieve and verify
        doc = collection.find_one({"_id": result.inserted_id})
        assert isinstance(doc["data"], Binary)
        assert doc["data"] == raw_bytes
        assert doc["data"].subtype == Binary.BINARY_SUBTYPE


def test_insert_binary_objects_preserves_subtypes():
    """Test that Binary objects preserve their subtypes when inserting."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert document with Binary objects of different subtypes
        function_binary = Binary(b"function code", Binary.FUNCTION_SUBTYPE)
        uuid_binary = Binary(b"uuid data", Binary.UUID_SUBTYPE)

        result = collection.insert_one(
            {"function_data": function_binary, "uuid_data": uuid_binary}
        )

        # Retrieve and verify subtypes are preserved
        doc = collection.find_one({"_id": result.inserted_id})
        assert isinstance(doc["function_data"], Binary)
        assert doc["function_data"].subtype == Binary.FUNCTION_SUBTYPE
        assert doc["function_data"] == function_binary

        assert isinstance(doc["uuid_data"], Binary)
        assert doc["uuid_data"].subtype == Binary.UUID_SUBTYPE
        assert doc["uuid_data"] == uuid_binary


def test_update_raw_bytes_converts_to_binary():
    """Test that raw bytes are converted to Binary objects when updating."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert a document first
        result = collection.insert_one({"name": "test"})

        # Update with raw bytes
        raw_bytes = b"\x00\x01\x02\x03"
        collection.update_one(
            {"_id": result.inserted_id}, {"$set": {"data": raw_bytes}}
        )

        # Retrieve and verify
        doc = collection.find_one({"_id": result.inserted_id})
        assert isinstance(doc["data"], Binary)
        assert doc["data"] == raw_bytes
        assert doc["data"].subtype == Binary.BINARY_SUBTYPE


def test_update_binary_objects_preserves_subtypes():
    """Test that Binary objects preserve their subtypes when updating."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert a document first
        result = collection.insert_one({"name": "test"})

        # Update with Binary objects of different subtypes
        function_binary = Binary(b"function code", Binary.FUNCTION_SUBTYPE)
        uuid_binary = Binary(b"uuid data", Binary.UUID_SUBTYPE)

        collection.update_one(
            {"_id": result.inserted_id},
            {
                "$set": {
                    "function_data": function_binary,
                    "uuid_data": uuid_binary,
                }
            },
        )

        # Retrieve and verify subtypes are preserved
        doc = collection.find_one({"_id": result.inserted_id})
        assert isinstance(doc["function_data"], Binary)
        assert doc["function_data"].subtype == Binary.FUNCTION_SUBTYPE
        assert doc["function_data"] == function_binary

        assert isinstance(doc["uuid_data"], Binary)
        assert doc["uuid_data"].subtype == Binary.UUID_SUBTYPE
        assert doc["uuid_data"] == uuid_binary


def test_binary_operations_with_different_subtypes():
    """Test various binary operations with different subtypes."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create Binary objects with different subtypes
        binary_objects = {
            "default": Binary(b"default data"),
            "function": Binary(b"function data", Binary.FUNCTION_SUBTYPE),
            "old_binary": Binary(b"old binary data", Binary.OLD_BINARY_SUBTYPE),
            "uuid": Binary(b"uuid data", Binary.UUID_SUBTYPE),
            "md5": Binary(b"md5 data", Binary.MD5_SUBTYPE),
            "user_defined": Binary(b"user data", Binary.USER_DEFINED_SUBTYPE),
        }

        # Create a copy of the keys before insert to avoid issues with dictionary modification
        binary_keys = list(binary_objects.keys())

        # Insert all binary objects
        result = collection.insert_one(binary_objects)

        # Retrieve and verify all subtypes
        doc = collection.find_one({"_id": result.inserted_id})
        for key in binary_keys:
            assert isinstance(
                doc[key], Binary
            ), f"Key {key} should be Binary but is {type(doc[key])}"
            assert doc[key].subtype == binary_objects[key].subtype
            assert doc[key] == binary_objects[key]

        # Update with new binary objects
        new_binary_objects = {
            "new_default": Binary(b"new default data"),
            "new_function": Binary(
                b"new function data", Binary.FUNCTION_SUBTYPE
            ),
        }

        # Create a copy of the keys before update
        new_binary_keys = list(new_binary_objects.keys())

        collection.update_one(
            {"_id": result.inserted_id}, {"$set": new_binary_objects}
        )

        # Retrieve and verify updated subtypes
        doc = collection.find_one({"_id": result.inserted_id})
        for key in new_binary_keys:
            assert isinstance(
                doc[key], Binary
            ), f"Key {key} should be Binary but is {type(doc[key])}"
            assert doc[key].subtype == new_binary_objects[key].subtype
            assert doc[key] == new_binary_objects[key]


def test_binary_in_nested_structures():
    """Test binary data handling in nested structures."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create nested structure with binary data
        nested_data = {
            "level1": {
                "level2": {
                    "binary_data": b"nested binary data",
                    "binary_object": Binary(
                        b"nested object", Binary.UUID_SUBTYPE
                    ),
                },
                "binary_list": [
                    b"list item 1",
                    Binary(b"list item 2", Binary.FUNCTION_SUBTYPE),
                    b"list item 3",
                ],
            }
        }

        # Insert document with nested binary data
        result = collection.insert_one(nested_data)

        # Retrieve and verify nested binary data
        doc = collection.find_one({"_id": result.inserted_id})

        # Check nested binary data
        assert isinstance(doc["level1"]["level2"]["binary_data"], Binary)
        assert (
            doc["level1"]["level2"]["binary_data"].subtype
            == Binary.BINARY_SUBTYPE
        )
        assert doc["level1"]["level2"]["binary_data"] == b"nested binary data"

        # Check nested binary object subtype preservation
        assert isinstance(doc["level1"]["level2"]["binary_object"], Binary)
        assert (
            doc["level1"]["level2"]["binary_object"].subtype
            == Binary.UUID_SUBTYPE
        )
        assert doc["level1"]["level2"]["binary_object"] == Binary(
            b"nested object", Binary.UUID_SUBTYPE
        )

        # Check binary data in list
        binary_list = doc["level1"]["binary_list"]
        assert isinstance(binary_list[0], Binary)
        assert binary_list[0].subtype == Binary.BINARY_SUBTYPE
        assert isinstance(binary_list[1], Binary)
        assert binary_list[1].subtype == Binary.FUNCTION_SUBTYPE
        # Note: We can't check object identity because of deep copying during insert
        assert binary_list[1] == Binary(b"list item 2", Binary.FUNCTION_SUBTYPE)
        assert isinstance(binary_list[2], Binary)
        assert binary_list[2].subtype == Binary.BINARY_SUBTYPE


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
        assert result.inserted_id == 1

        # Find the document
        doc = collection.find_one({"test": "value"})
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


def test_import_with_pysqlite3():
    """Test that the module correctly uses pysqlite3 when available."""

    # This test would pass in an environment where pysqlite3 is available
    # which is our current test environment
    import neosqlite.connection as neosqlite_module

    # In our current environment, pysqlite3 should be available
    try:
        import pysqlite3.dbapi2

        # If we can import it, neosqlite should be using it
        assert neosqlite_module.sqlite3 == pysqlite3.dbapi2
    except ImportError:
        # If pysqlite3 is not available, neosqlite should fall back to standard sqlite3
        import sqlite3

        assert neosqlite_module.sqlite3 == sqlite3


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
    result = collection.query_engine.helpers._internal_update(
        doc_id,
        {"$set": {"name": "Bob"}, "$inc": {"age": 5}, "$mul": {"score": 1.1}},
        original_doc,
    )

    # Verify the update worked
    assert result["name"] == "Bob"
    assert result["age"] == 35
    assert result["score"] == 110  # 100 * 1.1


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
    result = collection.query_engine.helpers._internal_update(
        doc_id, {"$push": {"items": 4}, "$set": {"name": "Bob"}}, original_doc
    )

    # Verify the update worked
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
    result = collection.query_engine.helpers._internal_update(
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

        # Test with unsupported operations (should return False)
        update_spec = {"$rename": {"old": "new"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return False for unsupported operations
        assert not result

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
        result = helper._perform_python_update(doc_id, update_spec, doc)
        assert result["age"] == 30
        assert result["city"] == "New York"

        # Test $unset operation
        update_spec = {"$unset": {"city": ""}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert "city" not in result

        # Test $inc operation
        update_spec = {"$inc": {"age": 5}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 35

        # Test $push operation
        update_spec = {"$push": {"scores": 90}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == [80, 85, 90]

        # Test $pull operation
        update_spec = {"$pull": {"scores": 85}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == [80, 90]

        # Test $pop operation (pop last element)
        update_spec = {"$pop": {"scores": 1}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == [80]

        # Test $pop operation (pop first element)
        update_spec = {"$pop": {"scores": -1}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == []

        # Test $rename operation
        update_spec = {"$rename": {"name": "full_name"}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert "name" not in result
        assert result["full_name"] == "Test"

        # Test $mul operation
        update_spec = {"$mul": {"age": 2}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 70

        # Test $min operation
        update_spec = {"$min": {"age": 50}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 50

        # Test $max operation
        update_spec = {"$max": {"age": 60}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 60

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
