"""ObjectId implementation for NeoSQLite that follows MongoDB's specification.

Based on MongoDB's ObjectId specification:
- 4 bytes: timestamp (seconds since Unix epoch)
- 5 bytes: random value (generated once per process)
- 3 bytes: counter (incrementing from a random value)

This implementation provides full compatibility with MongoDB ObjectIds
while being optimized for NeoSQLite's local-only architecture.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ObjectId:
    """MongoDB-compatible ObjectId implementation for NeoSQLite.

    Generates 12-byte identifiers following MongoDB's specification:
    - 4 bytes: timestamp (seconds since Unix epoch)
    - 5 bytes: random value (generated once per process)
    - 3 bytes: counter (incrementing from a random value)

    Provides full compatibility with MongoDB ObjectIds while working with
    NeoSQLite.
    """

    _random_bytes: bytes | None = None
    _counter: int | None = None
    _counter_lock = threading.Lock()

    def __init__(self, oid: str | bytes | ObjectId | int | None = None):
        """Initialize a new ObjectId.

        Args:
            oid: A 12-byte binary value, a 24-character hex string, another
                ``ObjectId`` instance, an unsigned 32-bit integer timestamp,
                or ``None`` to generate a new ``ObjectId``.

        Raises:
            TypeError: If ``oid`` has an unsupported type.
            ValueError: If ``oid`` has an invalid length, invalid hex
                characters, or an out-of-range integer timestamp.
        """
        match oid:
            case None:
                # Generate a new ObjectId
                self._id = self._generate_new_id()
            case ObjectId():
                self._id = oid.binary
            case str():
                if len(oid) != 24:
                    raise ValueError(
                        "ObjectId hex string must be exactly 24 characters"
                    )
                try:
                    self._id = bytes.fromhex(oid)
                except ValueError as e:
                    logger.debug(f"Invalid ObjectId hex string '{oid}': {e}")
                    raise ValueError(
                        "ObjectId hex string contains invalid characters"
                    )
            case bytes():
                if len(oid) != 12:
                    raise ValueError("ObjectId must be exactly 12 bytes")
                self._id = oid
            case int():
                # If an integer is provided, it replaces the timestamp part
                # according to MongoDB specification
                if oid < 0 or oid > 0xFFFFFFFF:
                    raise ValueError(
                        "Integer timestamp must be between 0 and 0xFFFFFFFF"
                    )
                # Generate a new ObjectId but with the provided timestamp
                self._id = self._generate_new_id_with_timestamp(oid)
            case _:
                raise TypeError(
                    "ObjectId must be a string, bytes, ObjectId, int, or None"
                )

    @classmethod
    def _generate_new_id(cls) -> bytes:
        """Generate a new 12-byte ObjectId value per MongoDB specification.

        Creates a unique 12-byte identifier consisting of:
        - 4 bytes: timestamp (seconds since Unix epoch)
        - 5 bytes: random value (generated once per process)
        - 3 bytes: counter (incrementing from a random value)

        Thread safety is maintained using ``cls._counter_lock`` when
        initializing shared random bytes and incrementing the counter.

        Returns:
            bytes: A new 12-byte ObjectId value.
        """
        # Ensure thread safety for random bytes and counter
        with cls._counter_lock:
            if cls._random_bytes is None:
                # Generate random bytes once (5 bytes), as per MongoDB spec
                cls._random_bytes = os.urandom(5)

            if cls._counter is None:
                # Initialize counter with a random value
                cls._counter = random.randint(0, 0xFFFFFF)

            # Increment counter and keep only 3 bytes
            cls._counter = (cls._counter + 1) % 0x1000000
            counter_val = cls._counter
            random_bytes = cls._random_bytes

        # Build the 12-byte ObjectId according to MongoDB specification:
        # 4 bytes: timestamp (Unix timestamp, big-endian)
        timestamp = int(time.time()).to_bytes(4, "big")

        # 3 bytes: counter (big-endian)
        counter = counter_val.to_bytes(3, "big")

        return timestamp + random_bytes + counter

    @classmethod
    def _generate_new_id_with_timestamp(cls, timestamp: int) -> bytes:
        """Generate a new 12-byte ObjectId value with a specific timestamp.

        Creates a unique 12-byte identifier using the provided timestamp and
        MongoDB's format:
        - 4 bytes: provided timestamp (instead of current time)
        - 5 bytes: random value (generated once per process)
        - 3 bytes: counter (incrementing from a random value)

        Thread safety is maintained using ``cls._counter_lock`` when
        initializing shared random bytes and incrementing the counter.

        Args:
            timestamp: Unix timestamp (seconds) to encode in the first 4
                bytes.

        Returns:
            bytes: A new 12-byte ObjectId value with the specified timestamp.
        """
        # Ensure thread safety for random bytes and counter
        with cls._counter_lock:
            if cls._random_bytes is None:
                # Generate random bytes once (5 bytes), as per MongoDB spec
                cls._random_bytes = os.urandom(5)

            if cls._counter is None:
                # Initialize counter with a random value
                cls._counter = random.randint(0, 0xFFFFFF)

            # Increment counter and keep only 3 bytes
            cls._counter = (cls._counter + 1) % 0x1000000
            counter_val = cls._counter
            random_bytes = cls._random_bytes

        # Build the 12-byte ObjectId according to MongoDB specification:
        # 4 bytes: provided timestamp (big-endian)
        timestamp_bytes = timestamp.to_bytes(4, "big")

        # 3 bytes: counter (big-endian)
        counter = counter_val.to_bytes(3, "big")

        return timestamp_bytes + random_bytes + counter

    @classmethod
    def is_valid(cls, oid: Any) -> bool:
        """Check if the given value is a valid ObjectId.

        Args:
            oid: Value to validate (``ObjectId``, 24-char hex ``str``,
                12-byte ``bytes``, or integer timestamp).

        Returns:
            bool: True if ``oid`` represents a valid ObjectId, else False.
        """
        match oid:
            case ObjectId():
                return True
            case str():
                if len(oid) != 24:
                    return False
                try:
                    bytes.fromhex(oid)
                    return True
                except (TypeError, ValueError) as e:
                    logger.debug(f"Invalid ObjectId hex string '{oid}': {e}")
                    return False
            case bytes():
                return len(oid) == 12
            case int():
                return 0 <= oid <= 0xFFFFFFFF
            case _:
                return False

    @property
    def binary(self) -> bytes:
        """Get the binary representation of this ObjectId.

        Returns:
            bytes: Raw 12-byte identifier.
        """
        return self._id

    @property
    def hex(self) -> str:
        """Get the hexadecimal string representation of this ObjectId.

        Returns:
            str: 24-character lowercase hexadecimal string.
        """
        return self._id.hex()

    def __str__(self) -> str:
        """Return the hexadecimal string representation.

        Returns:
            str: 24-character hexadecimal representation.
        """
        return self.hex

    def __repr__(self) -> str:
        """Return a string representation of this ObjectId.

        Returns:
            str: Debug representation in ``ObjectId('<hex>')`` form.
        """
        return f"ObjectId('{self.hex}')"

    def __bytes__(self) -> bytes:
        """Return the binary representation of this ObjectId.

        Returns:
            bytes: Raw 12-byte identifier.
        """
        return self._id

    def __eq__(self, other: Any) -> bool:
        """Check equality with another ObjectId, bytes, or hex string.

        Args:
            other: Value to compare against this ``ObjectId``.

        Returns:
            bool: True if ``other`` represents the same 12-byte value, else
                False.
        """
        match other:
            case ObjectId():
                return self._id == other._id
            case bytes():
                return self._id == other
            case str():
                try:
                    return self._id == bytes.fromhex(other)
                except ValueError as e:
                    logger.debug(
                        f"Failed to compare ObjectId with invalid hex string '{other}': {e}"
                    )
                    return False
            case _:
                return False

    def __ne__(self, other: Any) -> bool:
        """Check inequality with another ObjectId, bytes, or hex string.

        Args:
            other: Value to compare against this ``ObjectId``.

        Returns:
            bool: True if ``other`` does not equal this ``ObjectId``.
        """
        return not self.__eq__(other)

    def _compare_bytes(self, other: Any) -> bytes | None:
        """Return the 12-byte value to order against, if comparable.

        Args:
            other: Value to inspect for ordering comparison.

        Returns:
            bytes | None: Raw 12-byte identifier if ``other`` is an
                ``ObjectId``, else ``None``.
        """
        match other:
            case ObjectId():
                return other._id
            case _:
                return None

    def __lt__(self, other: Any) -> bool:
        """Order by the 12-byte big-endian value (BSON byte order).

        Args:
            other: Object to compare against.

        Returns:
            bool: True if less than ``other``, or ``NotImplemented`` when
                ``other`` is not an ``ObjectId``.
        """
        if (other_id := self._compare_bytes(other)) is None:
            return NotImplemented
        return self._id < other_id

    def __le__(self, other: Any) -> bool:
        """Order by the 12-byte big-endian value (BSON byte order).

        Args:
            other: Object to compare against.

        Returns:
            bool: True if less than or equal to ``other``, or
                ``NotImplemented`` when ``other`` is not an ``ObjectId``.
        """
        if (other_id := self._compare_bytes(other)) is None:
            return NotImplemented
        return self._id <= other_id

    def __gt__(self, other: Any) -> bool:
        """Order by the 12-byte big-endian value (BSON byte order).

        Args:
            other: Object to compare against.

        Returns:
            bool: True if greater than ``other``, or ``NotImplemented`` when
                ``other`` is not an ``ObjectId``.
        """
        if (other_id := self._compare_bytes(other)) is None:
            return NotImplemented
        return self._id > other_id

    def __ge__(self, other: Any) -> bool:
        """Order by the 12-byte big-endian value (BSON byte order).

        Args:
            other: Object to compare against.

        Returns:
            bool: True if greater than or equal to ``other``, or
                ``NotImplemented`` when ``other`` is not an ``ObjectId``.
        """
        if (other_id := self._compare_bytes(other)) is None:
            return NotImplemented
        return self._id >= other_id

    def __hash__(self) -> int:
        """Return a hash value for this ObjectId.

        Returns:
            int: Hash computed from the 12-byte identifier.
        """
        return hash(self._id)

    @property
    def generation_time(self) -> datetime:
        """Get the generation time of this ObjectId as a UTC datetime.

        Returns:
            datetime: Timezone-aware UTC datetime decoded from the first
                4 bytes of the identifier.
        """
        # First 4 bytes contain the timestamp
        timestamp_bytes = self._id[:4]
        timestamp = int.from_bytes(timestamp_bytes, "big")
        return datetime.fromtimestamp(timestamp, tz=UTC)

    def encode_for_storage(self) -> dict:
        """Encode the ObjectId for JSON storage compatibility with NeoSQLite.

        Returns:
            dict: Storage dictionary containing ``__neosqlite_objectid__``
                and ``id`` hex string.
        """
        return {
            "__neosqlite_objectid__": True,
            "id": self.hex,
        }

    @classmethod
    def decode_from_storage(cls, encoded_data: dict) -> Self:
        """Decode an ObjectId from JSON storage format.

        Args:
            encoded_data: Dictionary representation loaded from JSON storage.

        Returns:
            Self: Decoded ``ObjectId`` instance.

        Raises:
            ValueError: If ``encoded_data`` is missing the ObjectId marker
                or ``'id'`` field.
        """
        if (
            not isinstance(encoded_data, dict)
            or "__neosqlite_objectid__" not in encoded_data
        ):
            raise ValueError("Invalid encoded ObjectId data")

        if "id" not in encoded_data:
            raise ValueError(
                "Invalid encoded ObjectId data: missing 'id' field"
            )

        return cls(encoded_data["id"])
