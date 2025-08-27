# coding: utf-8
"""
Tests for the Binary class in neosqlite.
"""
from neosqlite.binary import Binary
import base64
import pytest
import uuid


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
