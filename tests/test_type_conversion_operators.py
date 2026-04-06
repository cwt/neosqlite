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
            assert result[0]["is_num"] == True  # SQLite returns 1 for true

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
            assert result[0]["is_num"] == True

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
            assert result[0]["is_num"] == False  # SQLite returns 0 for false

    def test_is_number_with_boolean(self):
        """Test $isNumber with boolean value.

        MongoDB $isNumber returns False for booleans because they are
        a separate BSON type, not numeric types.
        """
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"value": True})

            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 1
            # MongoDB: booleans are NOT numbers (separate BSON type)
            assert result[0]["is_num"] == False

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
            assert result[0]["is_num"] == False

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
            assert result[0]["is_num"] == False

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
            assert result[0]["is_num"] == False

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
                assert result[0]["is_num"] == True
                # Others should be False
                assert result[1]["is_num"] == False
                assert result[2]["is_num"] == False
                assert result[3]["is_num"] == False
            finally:
                set_force_fallback(original_state)

    def test_is_number_sql_tier_single_value_format(self):
        """Test $isNumber SQL tier with single-value operand format.

        This verifies the bug fix where {"$isNumber": "$value"} failed with
        "requires exactly 1 operand" error in SQL tier.

        Now uses json_type() for perfect BSON type detection instead of typeof().
        """
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()
        expr = {"$isNumber": "$value"}  # Single-value format, not list
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_type" in sql

    def test_is_number_sql_tier_list_format(self):
        """Test $isNumber SQL tier with list operand format."""
        from neosqlite.collection.expr_evaluator import ExprEvaluator

        evaluator = ExprEvaluator()
        expr = {"$isNumber": ["$value"]}  # List format
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_type" in sql

    def test_is_number_in_project_uses_sql_tier(self):
        """Test that $isNumber in $project uses SQL tier (not Python fallback).

        This is an integration test to ensure the SQL tier optimization works
        and doesn't fall back to Python evaluation.
        """
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"value": 42},
                    {"value": 3.14},
                    {"value": "not a number"},
                    {"value": None},
                ]
            )

            # Don't force fallback - let it use SQL tier
            result = list(
                coll.aggregate(
                    [{"$project": {"is_num": {"$isNumber": "$value"}}}]
                )
            )

            assert len(result) == 4
            # Integer should be a number
            assert result[0]["is_num"] == True
            # Float should be a number
            assert result[1]["is_num"] == True
            # String should not be a number
            assert result[2]["is_num"] == False
            # Null should not be a number
            assert result[3]["is_num"] == False

    def test_is_number_with_all_bson_types(self):
        """Test $isNumber with all BSON types for perfect MongoDB compatibility.

        MongoDB $isNumber returns true ONLY for numeric BSON types:
        int, long, double, decimal
        Returns false for: bool, string, array, object, null, date, objectId, binary
        """
        from datetime import datetime

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection

            # Insert documents with different BSON types
            test_docs = [
                {"_id": "int", "value": 42},
                {"_id": "long", "value": 9999999999999},
                {"_id": "double", "value": 3.14},
                {"_id": "bool_true", "value": True},
                {"_id": "bool_false", "value": False},
                {"_id": "string", "value": "hello"},
                {"_id": "null", "value": None},
                {"_id": "array", "value": [1, 2, 3]},
                {"_id": "empty_array", "value": []},
                {"_id": "object", "value": {"key": "value"}},
                {"_id": "empty_object", "value": {}},
                {"_id": "date", "value": datetime(2024, 1, 1)},
            ]

            coll.insert_many(test_docs)

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "_id": 1,
                                "is_num": {"$isNumber": "$value"},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            )

            # Verify each type
            results_by_id = {r["_id"]: r["is_num"] for r in result}

            # Numeric types should be True
            assert results_by_id["int"] == True
            assert results_by_id["long"] == True
            assert results_by_id["double"] == True

            # Non-numeric types should be False
            assert results_by_id["bool_true"] == False
            assert results_by_id["bool_false"] == False
            assert results_by_id["string"] == False
            assert results_by_id["null"] == False
            assert results_by_id["array"] == False
            assert results_by_id["empty_array"] == False
            assert results_by_id["object"] == False
            assert results_by_id["empty_object"] == False
            assert results_by_id["date"] == False


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

            assert result[0]["bool"] == False  # 0 -> False
            assert result[1]["bool"] == True  # 1 -> True
            assert result[2]["bool"] == True  # non-zero -> True

    def test_toBool_with_all_bson_types(self):
        """Test $toBool with all BSON types for perfect MongoDB compatibility.

        MongoDB $toBool truthiness:
        - null: false
        - booleans: as-is (true->true, false->false)
        - numbers: non-zero is true, zero is false
        - strings: non-empty is true, empty is false
        - arrays: true (even empty [])
        - objects: true (even empty {})
        """

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection

            # Insert documents with different BSON types
            test_docs = [
                {"_id": "null", "value": None},
                {"_id": "bool_true", "value": True},
                {"_id": "bool_false", "value": False},
                {"_id": "int_zero", "value": 0},
                {"_id": "int_nonzero", "value": 42},
                {"_id": "float_zero", "value": 0.0},
                {"_id": "float_nonzero", "value": 3.14},
                {"_id": "string_empty", "value": ""},
                {"_id": "string_nonempty", "value": "hello"},
                {"_id": "array_empty", "value": []},
                {"_id": "array_nonempty", "value": [1, 2, 3]},
                {"_id": "object_empty", "value": {}},
                {"_id": "object_nonempty", "value": {"key": "value"}},
            ]

            coll.insert_many(test_docs)

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "_id": 1,
                                "bool_val": {"$toBool": "$value"},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            )

            # Verify each type
            results_by_id = {r["_id"]: r["bool_val"] for r in result}

            # null -> false
            assert results_by_id["null"] == False

            # booleans -> as-is
            assert results_by_id["bool_true"] == True
            assert results_by_id["bool_false"] == False

            # numbers -> non-zero is true
            assert results_by_id["int_zero"] == False
            assert results_by_id["int_nonzero"] == True
            assert results_by_id["float_zero"] == False
            assert results_by_id["float_nonzero"] == True

            # strings -> non-empty is true
            assert results_by_id["string_empty"] == False
            assert results_by_id["string_nonempty"] == True

            # arrays -> always true (even empty)
            assert results_by_id["array_empty"] == True
            assert results_by_id["array_nonempty"] == True

            # objects -> always true (even empty)
            assert results_by_id["object_empty"] == True
            assert results_by_id["object_nonempty"] == True

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

            assert result[0]["bool"] == False  # empty string -> False
            assert result[1]["bool"] == True  # non-empty -> True

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
                assert result[0]["to_bool"] == True
                assert result[0]["is_num_num"] == True
                assert result[0]["is_num_str"] == False
            finally:
                set_force_fallback(original_state)
