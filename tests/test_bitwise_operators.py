"""
Tests for bitwise query operators.

Covers: $bitsAllClear, $bitsAllSet, $bitsAnyClear, $bitsAnySet
"""

import neosqlite
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


class TestBitwiseOperators:
    """Test bitwise query operators."""

    def test_bits_all_clear_basic(self):
        """Test $bitsAllClear with basic bitmask."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            # Insert test documents with different bit patterns
            # 0 = 0000, 5 = 0101, 8 = 1000, 15 = 1111
            coll.insert_many(
                [
                    {"value": 0},  # 0000 - all bits clear
                    {"value": 5},  # 0101 - bits 0 and 2 set
                    {"value": 8},  # 1000 - bit 3 set
                    {"value": 15},  # 1111 - all bits set
                ]
            )

            # Find documents where bits 0 and 2 are clear (bitmask = 5 = 0101)
            result = list(coll.find({"value": {"$bitsAllClear": 5}}))
            assert len(result) == 2
            values = sorted([doc["value"] for doc in result])
            assert values == [0, 8]

    def test_bits_all_clear_bit_positions(self):
        """Test $bitsAllClear with array of bit positions."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},  # 0000
                    {"value": 1},  # 0001
                    {"value": 2},  # 0010
                    {"value": 4},  # 0100
                    {"value": 8},  # 1000
                ]
            )

            # Find documents where bits 0 and 1 are clear (bitmask from positions [0, 1] = 3)
            result = list(coll.find({"value": {"$bitsAllClear": [0, 1]}}))
            assert len(result) == 3
            values = sorted([doc["value"] for doc in result])
            assert values == [0, 4, 8]

    def test_bits_all_set_basic(self):
        """Test $bitsAllSet with basic bitmask."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},  # 0000
                    {"value": 5},  # 0101 - bits 0 and 2 set
                    {
                        "value": 7
                    },  # 0111 - bits 0, 1, and 2 set (includes 0 and 2)
                    {"value": 15},  # 1111 - all bits set (includes 0 and 2)
                ]
            )

            # Find documents where bits 0 and 2 are set (bitmask = 5 = 0101)
            # 0 (0000): bits 0 and 2 NOT set ✗
            # 5 (0101): bits 0 and 2 set ✓
            # 7 (0111): bits 0 and 2 set ✓ (also has bit 1)
            # 15 (1111): bits 0 and 2 set ✓
            result = list(coll.find({"value": {"$bitsAllSet": 5}}))
            assert len(result) == 3
            values = sorted([doc["value"] for doc in result])
            assert values == [5, 7, 15]

    def test_bits_all_set_bit_positions(self):
        """Test $bitsAllSet with array of bit positions."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 3},  # 0011
                    {"value": 7},  # 0111
                    {"value": 11},  # 1011
                    {"value": 15},  # 1111
                ]
            )

            # Find documents where bits 0 and 1 are set (bitmask from positions [0, 1] = 3)
            result = list(coll.find({"value": {"$bitsAllSet": [0, 1]}}))
            assert len(result) == 4
            values = sorted([doc["value"] for doc in result])
            assert values == [3, 7, 11, 15]

    def test_bits_any_clear_basic(self):
        """Test $bitsAnyClear with basic bitmask."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},  # 0000 - all bits clear
                    {"value": 5},  # 0101
                    {"value": 10},  # 1010
                    {"value": 15},  # 1111 - no bits clear
                ]
            )

            # Find documents where any of bits 0 or 2 are clear (bitmask = 5 = 0101)
            # 0 (0000): both bits 0 and 2 are clear ✓
            # 5 (0101): both bits 0 and 2 are set ✗
            # 10 (1010): both bits 0 and 2 are clear ✓
            # 15 (1111): both bits 0 and 2 are set ✗
            result = list(coll.find({"value": {"$bitsAnyClear": 5}}))
            assert len(result) == 2
            values = sorted([doc["value"] for doc in result])
            assert values == [0, 10]

    def test_bits_any_clear_bit_positions(self):
        """Test $bitsAnyClear with array of bit positions."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},  # 0000
                    {"value": 1},  # 0001
                    {"value": 2},  # 0010
                    {"value": 3},  # 0011
                ]
            )

            # Find documents where any of bits 0 or 1 are clear
            result = list(coll.find({"value": {"$bitsAnyClear": [0, 1]}}))
            assert len(result) == 3
            values = sorted([doc["value"] for doc in result])
            assert values == [0, 1, 2]

    def test_bits_any_set_basic(self):
        """Test $bitsAnySet with basic bitmask."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},  # 0000 - no bits set
                    {"value": 5},  # 0101 - bits 0 and 2 set
                    {"value": 10},  # 1010 - bits 1 and 3 set
                    {"value": 15},  # 1111 - all bits set
                ]
            )

            # Find documents where any of bits 0 or 2 are set (bitmask = 5 = 0101)
            result = list(coll.find({"value": {"$bitsAnySet": 5}}))
            assert len(result) == 2
            values = sorted([doc["value"] for doc in result])
            assert values == [5, 15]

    def test_bits_any_set_bit_positions(self):
        """Test $bitsAnySet with array of bit positions."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},  # 0000
                    {"value": 1},  # 0001
                    {"value": 2},  # 0010
                    {"value": 4},  # 0100
                ]
            )

            # Find documents where any of bits 0 or 1 are set
            result = list(coll.find({"value": {"$bitsAnySet": [0, 1]}}))
            assert len(result) == 2
            values = sorted([doc["value"] for doc in result])
            assert values == [1, 2]

    def test_bits_nonexistent_field(self):
        """Test bitwise operators on nonexistent fields."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"name": "test"})

            # All bitwise operators should return no results for nonexistent fields
            result = list(coll.find({"value": {"$bitsAllClear": 1}}))
            assert len(result) == 0

            result = list(coll.find({"value": {"$bitsAllSet": 1}}))
            assert len(result) == 0

            result = list(coll.find({"value": {"$bitsAnyClear": 1}}))
            assert len(result) == 0

            result = list(coll.find({"value": {"$bitsAnySet": 1}}))
            assert len(result) == 0

    def test_bits_non_integer_field(self):
        """Test bitwise operators on non-integer fields."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": "string"},
                    {"value": 3.14},
                    {"value": None},
                    {"value": True},
                    {"value": False},
                ]
            )

            # All bitwise operators should return no results for non-integer fields
            result = list(coll.find({"value": {"$bitsAllClear": 1}}))
            assert len(result) == 0

    def test_bits_combined_with_other_operators(self):
        """Test bitwise operators combined with other query operators."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0, "type": "A"},
                    {"value": 5, "type": "A"},
                    {"value": 8, "type": "B"},
                    {"value": 15, "type": "B"},
                ]
            )

            # Combine $bitsAllClear with $eq
            result = list(
                coll.find(
                    {"$and": [{"value": {"$bitsAllClear": 5}}, {"type": "A"}]}
                )
            )
            assert len(result) == 1
            assert result[0]["value"] == 0

    def test_bits_large_bitmask(self):
        """Test bitwise operators with large bitmasks."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},
                    {"value": 256},  # 2^8 = bit 8 set
                    {"value": 512},  # 2^9 = bit 9 set
                    {"value": 1024},  # 2^10 = bit 10 set
                ]
            )

            # Test with bit 8 set (bitmask = 256)
            result = list(coll.find({"value": {"$bitsAllSet": 256}}))
            assert len(result) == 1
            assert result[0]["value"] == 256


class TestBitwiseOperatorsKillSwitch:
    """Test bitwise operators with kill switch (Python fallback)."""

    def test_bits_all_clear_kill_switch(self):
        """Test $bitsAllClear with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},
                    {"value": 5},
                    {"value": 8},
                    {"value": 15},
                ]
            )

            # Enable kill switch
            original_state = get_force_fallback()
            try:
                set_force_fallback(True)
                result = list(coll.find({"value": {"$bitsAllClear": 5}}))
                assert len(result) == 2
                values = sorted([doc["value"] for doc in result])
                assert values == [0, 8]
            finally:
                set_force_fallback(original_state)

    def test_bits_all_set_kill_switch(self):
        """Test $bitsAllSet with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},
                    {"value": 5},
                    {"value": 7},
                    {"value": 15},
                ]
            )

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)
                result = list(coll.find({"value": {"$bitsAllSet": 5}}))
                assert len(result) == 3
                values = sorted([doc["value"] for doc in result])
                assert values == [5, 7, 15]
            finally:
                set_force_fallback(original_state)

    def test_bits_any_clear_kill_switch(self):
        """Test $bitsAnyClear with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},
                    {"value": 5},
                    {"value": 10},
                    {"value": 15},
                ]
            )

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)
                result = list(coll.find({"value": {"$bitsAnyClear": 5}}))
                assert len(result) == 2
                values = sorted([doc["value"] for doc in result])
                assert values == [0, 10]
            finally:
                set_force_fallback(original_state)

    def test_bits_any_set_kill_switch(self):
        """Test $bitsAnySet with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},
                    {"value": 5},
                    {"value": 10},
                    {"value": 15},
                ]
            )

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)
                result = list(coll.find({"value": {"$bitsAnySet": 5}}))
                assert len(result) == 2
                values = sorted([doc["value"] for doc in result])
                assert values == [5, 15]
            finally:
                set_force_fallback(original_state)
