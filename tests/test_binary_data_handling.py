"""
Tests for binary data handling in NeoSQLite.
"""

import pytest
from neosqlite import Connection, Binary


def test_convert_bytes_to_binary_function():
    """Test the _convert_bytes_to_binary helper function directly."""
    from neosqlite.collection.query_helper import _convert_bytes_to_binary

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
        raw_bytes = b"\\x00\\x01\\x02\\x03"
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
        raw_bytes = b"\\x00\\x01\\x02\\x03"
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


if __name__ == "__main__":
    pytest.main([__file__])
