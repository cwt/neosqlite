"""
Tests for Binary class and binary data handling.
"""

import base64
import pytest
import uuid
from neosqlite import Connection, Binary


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
