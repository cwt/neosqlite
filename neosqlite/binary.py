from typing import Union
import base64


class Binary(bytes):
    """
    A BSON Binary-like class for representing binary data in neosqlite.

    This class provides a PyMongo-compatible interface for storing binary data
    directly in documents (outside of GridFS). The data is automatically
    encoded/decoded when stored in SQLite JSON documents.

    Example usage:
        # Create binary data
        binary_data = Binary(b"some binary content")

        # Store in document
        collection.insert_one({"data": binary_data})

        # Retrieve and use
        doc = collection.find_one({})
        retrieved_data = doc["data"]  # Returns Binary instance
        raw_bytes = bytes(retrieved_data)  # Convert to bytes if needed
    """

    # Type declaration for mypy
    _subtype: int

    # Binary subtypes
    BINARY_SUBTYPE = 0
    FUNCTION_SUBTYPE = 1
    OLD_BINARY_SUBTYPE = 2
    UUID_SUBTYPE = 4
    MD5_SUBTYPE = 5
    COLUMN_SUBTYPE = 7
    SENSITIVE_SUBTYPE = 8
    VECTOR_SUBTYPE = 9
    USER_DEFINED_SUBTYPE = 128

    def __new__(
        cls,
        data: Union[bytes, bytearray, memoryview],
        subtype: int = BINARY_SUBTYPE,
    ):
        """
        Create a new Binary instance.

        Args:
            data: Binary data (bytes, bytearray, or memoryview)
            subtype: Binary subtype (default: BINARY_SUBTYPE)

        Returns:
            A new Binary instance
        """
        if isinstance(data, (bytearray, memoryview)):
            data = bytes(data)
        elif not isinstance(data, bytes):
            raise TypeError("data must be bytes, bytearray, or memoryview")

        if not isinstance(subtype, int):
            raise TypeError("subtype must be an integer")

        if not (0 <= subtype < 256):
            raise ValueError("subtype must be in range [0, 256)")

        instance = super().__new__(cls, data)
        instance._subtype = subtype
        return instance

    @property
    def subtype(self) -> int:
        """Get the binary subtype."""
        return self._subtype

    def encode_for_storage(self) -> dict:
        """
        Encode the binary data for JSON storage.

        Returns:
            A dictionary representation for JSON storage
        """
        return {
            "__neosqlite_binary__": True,
            "data": base64.b64encode(self).decode("utf-8"),
            "subtype": self._subtype,
        }

    @classmethod
    def decode_from_storage(cls, encoded_data: dict) -> "Binary":
        """
        Decode binary data from JSON storage.

        Args:
            encoded_data: Dictionary representation from JSON storage

        Returns:
            A Binary instance
        """
        if (
            not isinstance(encoded_data, dict)
            or "__neosqlite_binary__" not in encoded_data
        ):
            raise ValueError("Invalid encoded binary data")

        if "data" not in encoded_data:
            raise ValueError(
                "Invalid encoded binary data: missing 'data' field"
            )

        data = base64.b64decode(encoded_data["data"])
        subtype = encoded_data.get("subtype", cls.BINARY_SUBTYPE)
        return cls(data, subtype)

    @classmethod
    def from_uuid(cls, uuid_value, uuid_representation=None):
        """
        Create a Binary instance from a UUID.

        Args:
            uuid_value: A UUID instance
            uuid_representation: UUID representation (ignored in neosqlite)

        Returns:
            A Binary instance with UUID_SUBTYPE
        """
        import uuid

        if not isinstance(uuid_value, uuid.UUID):
            raise TypeError("uuid_value must be a UUID instance")

        # Convert UUID to bytes
        return cls(uuid_value.bytes, cls.UUID_SUBTYPE)

    def as_uuid(self, uuid_representation=None):
        """
        Convert this Binary instance to a UUID.

        Returns:
            A UUID instance

        Raises:
            ValueError: If the subtype is not UUID_SUBTYPE
        """
        if self._subtype != self.UUID_SUBTYPE:
            raise ValueError("Binary subtype is not UUID_SUBTYPE")

        import uuid

        return uuid.UUID(bytes=bytes(self))

    def __repr__(self) -> str:
        """Return a string representation of the Binary instance."""
        if self._subtype == self.BINARY_SUBTYPE:
            return f"Binary({super().__repr__()})"
        else:
            return f"Binary({super().__repr__()}, {self._subtype})"

    def __eq__(self, other) -> bool:
        """Check equality with another object."""
        if isinstance(other, Binary):
            return super().__eq__(other) and self._subtype == other._subtype
        elif isinstance(other, bytes):
            return (
                super().__eq__(other) and self._subtype == self.BINARY_SUBTYPE
            )
        return False

    def __ne__(self, other) -> bool:
        """Check inequality with another object."""
        return not self.__eq__(other)

    def __hash__(self) -> int:
        """Return a hash value for the Binary instance."""
        return hash((bytes(self), self._subtype))
