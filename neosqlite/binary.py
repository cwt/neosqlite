"""BSON Binary-like representation for NeoSQLite documents."""

from __future__ import annotations

import base64
import uuid
from typing import Self


class Binary(bytes):
    """A BSON Binary-like class for representing binary data in neosqlite.

    Provides a PyMongo-compatible ``bytes`` subclass with an associated BSON
    subtype integer for storing binary data directly in documents (outside of
    GridFS). Instances are automatically encoded to and decoded from base64
    dictionaries when stored in SQLite JSON documents.

    Example:
        >>> from neosqlite import Connection
        >>> db = Connection(":memory:")
        >>> collection = db.test
        >>> binary_data = Binary(b"some binary content")
        >>> _ = collection.insert_one({"data": binary_data})
        >>> doc = collection.find_one({})
        >>> raw_bytes = bytes(doc["data"])
        >>> raw_bytes
        b'some binary content'
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
        data: bytes | bytearray | memoryview,
        subtype: int = BINARY_SUBTYPE,
    ):
        """Create a new ``Binary`` instance.

        Args:
            data: Binary payload as ``bytes``, ``bytearray``, or
                ``memoryview``.
            subtype: BSON binary subtype in ``[0, 256)`` (defaults to
                ``BINARY_SUBTYPE``).

        Returns:
            Binary: A new ``Binary`` instance wrapping ``data`` and
                ``subtype``.

        Raises:
            TypeError: If ``data`` is not ``bytes``, ``bytearray``, or
                ``memoryview``, or if ``subtype`` is not an ``int``.
            ValueError: If ``subtype`` is outside the range ``[0, 256)``.
        """
        if isinstance(data, (bytearray, memoryview)):
            data = bytes(data)
        elif not isinstance(data, bytes):
            raise TypeError("data must be bytes, bytearray, or memoryview")

        if isinstance(subtype, bool) or not isinstance(subtype, int):
            raise TypeError("subtype must be an integer")

        if not (0 <= subtype < 256):
            raise ValueError("subtype must be in range [0, 256)")

        instance = super().__new__(cls, data)
        instance._subtype = subtype
        return instance

    @property
    def subtype(self) -> int:
        """Return the BSON binary subtype.

        Returns:
            int: The integer subtype code assigned to this instance.
        """
        return self._subtype

    def encode_for_storage(self) -> dict:
        """Encode the binary data for JSON storage.

        Serializes the raw bytes using UTF-8 base64 alongside the
        ``__neosqlite_binary__`` marker and ``subtype``.

        Returns:
            dict: A dictionary representation suitable for JSON storage.
        """
        return {
            "__neosqlite_binary__": True,
            "data": base64.b64encode(self).decode("utf-8"),
            "subtype": self._subtype,
        }

    @classmethod
    def decode_from_storage(cls, encoded_data: dict) -> Self:
        """Decode binary data from JSON storage.

        Args:
            encoded_data: Dictionary representation previously produced by
                ``encode_for_storage``.

        Returns:
            Self: A reconstructed ``Binary`` instance.

        Raises:
            ValueError: If ``encoded_data`` is not a valid encoded binary
                dictionary or is missing the ``"data"`` field.
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
        """Create a ``Binary`` instance from a ``uuid.UUID``.

        Args:
            uuid_value: A ``uuid.UUID`` instance to convert.
            uuid_representation: UUID representation (accepted for PyMongo API
                compatibility and ignored in neosqlite).

        Returns:
            Binary: A ``Binary`` instance containing the UUID bytes with
                ``UUID_SUBTYPE``.

        Raises:
            TypeError: If ``uuid_value`` is not a ``uuid.UUID`` instance.
        """
        if not isinstance(uuid_value, uuid.UUID):
            raise TypeError("uuid_value must be a UUID instance")

        # Convert UUID to bytes
        return cls(uuid_value.bytes, cls.UUID_SUBTYPE)

    def as_uuid(self, uuid_representation=None):
        """Convert this ``Binary`` instance back to a ``uuid.UUID``.

        Args:
            uuid_representation: UUID representation (accepted for PyMongo API
                compatibility and ignored in neosqlite).

        Returns:
            uuid.UUID: The ``UUID`` constructed from the instance bytes.

        Raises:
            ValueError: If this instance's ``subtype`` is not
                ``UUID_SUBTYPE``.
        """
        if self._subtype != self.UUID_SUBTYPE:
            raise ValueError("Binary subtype is not UUID_SUBTYPE")

        return uuid.UUID(bytes=bytes(self))

    def __repr__(self) -> str:
        """Return a string representation of the ``Binary`` instance.

        Omits the subtype argument when it equals ``BINARY_SUBTYPE``.

        Returns:
            str: Printable representation of the binary value and subtype.
        """
        if self._subtype == self.BINARY_SUBTYPE:
            return f"Binary({super().__repr__()})"
        else:
            return f"Binary({super().__repr__()}, {self._subtype})"

    def __eq__(self, other) -> bool:
        """Check equality with another ``Binary`` or ``bytes`` object.

        Another ``Binary`` matches when both bytes and subtype are equal. A
        plain ``bytes`` object matches only when this instance has
        ``BINARY_SUBTYPE``.

        Args:
            other: Object to compare against.

        Returns:
            bool: True if ``other`` is equal to this instance, False otherwise.
        """
        if isinstance(other, Binary):
            return super().__eq__(other) and self._subtype == other._subtype
        elif isinstance(other, bytes):
            return (
                super().__eq__(other) and self._subtype == self.BINARY_SUBTYPE
            )
        return False

    def __ne__(self, other) -> bool:
        """Check inequality with another object.

        Args:
            other: Object to compare against.

        Returns:
            bool: True if ``other`` is not equal to this instance, False
                otherwise.
        """
        return not self.__eq__(other)

    def __hash__(self) -> int:
        """Return a hash value for the ``Binary`` instance.

        Subtype 0 matches PyMongo's ``bson.Binary`` contract by delegating to
        ``bytes.__hash__`` so ``Binary(b"abc", 0) == b"abc"`` preserves hash
        equality.

        Returns:
            int: Hash matching ``bytes`` for ``BINARY_SUBTYPE`` or derived from
            the underlying bytes and subtype otherwise.
        """
        if self._subtype == self.BINARY_SUBTYPE:
            return super().__hash__()
        return hash((bytes(self), self._subtype))
