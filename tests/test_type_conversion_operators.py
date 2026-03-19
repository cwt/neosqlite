"""
Tests for type conversion operators in $expr.

Covers: $isNumber and verification of existing type conversion operators
"""

import neosqlite
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
)


class TestIsNumberOperator:
    """Test $isNumber operator."""

    def test_is_number_with_integer(self):
        """Test $isNumber with integer value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": 42})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 1  # SQLite returns 1 for true

    def test_is_number_with_float(self):
        """Test $isNumber with float value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": 3.14})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 1

    def test_is_number_with_string(self):
        """Test $isNumber with string value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "42"})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 0  # SQLite returns 0 for false

    def test_is_number_with_boolean(self):
        """Test $isNumber with boolean value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": True})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 0  # Booleans are not numbers

    def test_is_number_with_null(self):
        """Test $isNumber with null value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": None})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 0

    def test_is_number_with_array(self):
        """Test $isNumber with array value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": [1, 2, 3]})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 0

    def test_is_number_with_object(self):
        """Test $isNumber with object value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": {"a": 1}})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["is_num"] == 0

    def test_is_number_with_kill_switch(self):
        """Test $isNumber with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 42},
                    {"value": "hello"},
                    {"value": True},
                    {"value": None},
                ]
            )

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                    )
                )

                assert len(result) == 4
                # First doc (42) should be True
                assert result[0]["is_num"] is True
                # Others should be False
                assert result[1]["is_num"] is False
                assert result[2]["is_num"] is False
                assert result[3]["is_num"] is False
            finally:
                set_force_fallback(original_state)


class TestExistingTypeConversionOperators:
    """Test existing type conversion operators for regression."""

    def test_toString(self):
        """Test $toString operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": 42})

            result = list(
                coll.aggregate([{"$project": {"str": {"$toString": "$value"}}}])
            )

            assert result[0]["str"] == "42"

    def test_toInt(self):
        """Test $toInt operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "42"})

            result = list(
                coll.aggregate([{"$project": {"num": {"$toInt": "$value"}}}])
            )

            assert result[0]["num"] == 42

    def test_toInt_from_float(self):
        """Test $toInt truncates floats."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": 42.9})

            result = list(
                coll.aggregate([{"$project": {"num": {"$toInt": "$value"}}}])
            )

            assert result[0]["num"] == 42

    def test_toDouble(self):
        """Test $toDouble operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "3.14"})

            result = list(
                coll.aggregate([{"$project": {"num": {"$toDouble": "$value"}}}])
            )

            assert result[0]["num"] == 3.14

    def test_toBool_with_number(self):
        """Test $toBool with number values."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 0},
                    {"value": 1},
                    {"value": 42},
                ]
            )

            result = list(
                coll.aggregate([{"$project": {"bool": {"$toBool": "$value"}}}])
            )

            assert result[0]["bool"] is False  # 0 -> False
            assert result[1]["bool"] is True  # 1 -> True
            assert result[2]["bool"] is True  # non-zero -> True

    def test_toBool_with_string(self):
        """Test $toBool with string values."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": ""},
                    {"value": "hello"},
                ]
            )

            result = list(
                coll.aggregate([{"$project": {"bool": {"$toBool": "$value"}}}])
            )

            assert result[0]["bool"] is False  # empty string -> False
            assert result[1]["bool"] is True  # non-empty -> True

    def test_toLong(self):
        """Test $toLong operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "9223372036854775807"})

            result = list(
                coll.aggregate([{"$project": {"num": {"$toLong": "$value"}}}])
            )

            assert result[0]["num"] == 9223372036854775807

    def test_toDecimal(self):
        """Test $toDecimal operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "3.14159"})

            result = list(
                coll.aggregate(
                    [{"$project": {"num": {"$toDecimal": "$value"}}}]
                )
            )

            from decimal import Decimal

            assert result[0]["num"] == Decimal("3.14159")

    def test_toObjectId(self):
        """Test $toObjectId operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "507f1f77bcf86cd799439011"})

            result = list(
                coll.aggregate(
                    [{"$project": {"oid": {"$toObjectId": "$value"}}}]
                )
            )

            from neosqlite.objectid import ObjectId

            assert isinstance(result[0]["oid"], ObjectId)
            assert str(result[0]["oid"]) == "507f1f77bcf86cd799439011"

    def test_toString_with_null(self):
        """Test $toString with null value."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": None})

            result = list(
                coll.aggregate([{"$project": {"str": {"$toString": "$value"}}}])
            )

            assert result[0]["str"] is None

    def test_toInt_with_invalid(self):
        """Test $toInt with invalid value returns None."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": "not a number"})

            result = list(
                coll.aggregate([{"$project": {"num": {"$toInt": "$value"}}}])
            )

            assert result[0]["num"] is None


class TestTypeConversionKillSwitch:
    """Test type conversion operators with kill switch."""

    def test_all_type_conversions_with_kill_switch(self):
        """Test all type conversion operators work with kill switch."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {
                    "num": 42,
                    "str": "hello",
                    "float": 3.14,
                }
            )

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "to_str": {"$toString": "$num"},
                                    "to_int": {"$toInt": "$float"},
                                    "to_double": {"$toDouble": "$num"},
                                    "to_bool": {"$toBool": "$num"},
                                    "is_num_num": {"$isNumber": "$num"},
                                    "is_num_str": {"$isNumber": "$str"},
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["to_str"] == "42"
                assert result[0]["to_int"] == 3
                assert result[0]["to_double"] == 42.0
                assert result[0]["to_bool"] is True
                assert result[0]["is_num_num"] is True
                assert result[0]["is_num_str"] is False
            finally:
                set_force_fallback(original_state)
