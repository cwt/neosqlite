"""Tests for BSON-aware range comparisons on ``_id`` in the SQL tier.

MongoDB compares ``$gt``/``$gte``/``$lt``/``$lte`` operands only against values
of the same BSON type. These tests verify the generated SQL predicates produce
exactly the same matches as the authoritative Python fallback for every ``_id``
type NeoSQLite stores, and that the SQL path is actually taken.
"""

from datetime import UTC, datetime

import pytest

import neosqlite
from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)
from neosqlite.objectid import ObjectId

OID_1 = ObjectId("507f1f77bcf86cd799439011")
OID_2 = ObjectId("607f1f77bcf86cd799439012")
DATE_1 = datetime(2024, 1, 1, tzinfo=UTC)
DATE_2 = datetime(2025, 1, 1, tzinfo=UTC)


class TestIdRangeSql:
    """SQL vs Python equivalence for ``_id`` range comparisons."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.mixed_ids
        coll.insert_many(
            [
                {"_id": 1, "label": "int-1"},
                {"_id": 10, "label": "int-10"},
                {"_id": 2.5, "label": "float-2.5"},
                {"_id": "abc", "label": "str-abc"},
                {"_id": "xyz", "label": "str-xyz"},
                {"_id": False, "label": "bool-false"},
                {"_id": True, "label": "bool-true"},
                {"_id": OID_1, "label": "oid-1"},
                {"_id": OID_2, "label": "oid-2"},
                {"_id": DATE_1, "label": "date-2024"},
                {"_id": DATE_2, "label": "date-2025"},
            ]
        )
        yield coll
        conn.close()

    def _labels(self, collection, query, force_python):
        set_force_fallback(force_python)
        return sorted(doc["label"] for doc in collection.find(query))

    def _assert_same(self, collection, query, expected):
        set_force_fallback(False)
        built = collection.query_engine.helpers._build_simple_where_clause(
            query
        )
        assert built is not None, f"No SQL clause generated for {query}"
        sql_labels = self._labels(collection, query, force_python=False)
        python_labels = self._labels(collection, query, force_python=True)
        assert sql_labels == python_labels == sorted(expected)

    @pytest.mark.parametrize(
        ("op", "operand", "expected"),
        [
            ("$gt", 1, ["float-2.5", "int-10"]),
            ("$gte", 2, ["float-2.5", "int-10"]),
            ("$lt", 10, ["float-2.5", "int-1"]),
            ("$lte", 2.5, ["float-2.5", "int-1"]),
            ("$gt", 2.0, ["float-2.5", "int-10"]),
        ],
    )
    def test_numeric_ranges(self, collection, op, operand, expected):
        self._assert_same(collection, {"_id": {op: operand}}, expected)

    @pytest.mark.parametrize(
        ("op", "operand", "expected"),
        [
            ("$gt", "abc", ["str-xyz"]),
            ("$lt", "xyz", ["str-abc"]),
            ("$gte", "abc", ["str-abc", "str-xyz"]),
            ("$lte", "xyz", ["str-abc", "str-xyz"]),
        ],
    )
    def test_string_ranges_ignore_non_strings(
        self, collection, op, operand, expected
    ):
        self._assert_same(collection, {"_id": {op: operand}}, expected)

    @pytest.mark.parametrize(
        ("op", "operand", "expected"),
        [
            ("$gt", False, ["bool-true"]),
            ("$lt", True, ["bool-false"]),
            ("$gte", False, ["bool-false", "bool-true"]),
            ("$lte", True, ["bool-false", "bool-true"]),
        ],
    )
    def test_boolean_ranges(self, collection, op, operand, expected):
        self._assert_same(collection, {"_id": {op: operand}}, expected)

    @pytest.mark.parametrize(
        ("op", "operand", "expected"),
        [
            ("$gt", OID_1, ["oid-2"]),
            ("$gte", OID_1, ["oid-1", "oid-2"]),
            ("$lt", OID_2, ["oid-1"]),
            ("$lte", OID_1, ["oid-1"]),
        ],
    )
    def test_objectid_ranges(self, collection, op, operand, expected):
        self._assert_same(collection, {"_id": {op: operand}}, expected)

    @pytest.mark.parametrize(
        ("op", "operand", "expected"),
        [
            ("$gt", DATE_1, ["date-2025"]),
            ("$gte", DATE_1, ["date-2024", "date-2025"]),
            ("$lt", DATE_2, ["date-2024"]),
            ("$lte", DATE_2, ["date-2024", "date-2025"]),
        ],
    )
    def test_datetime_ranges(self, collection, op, operand, expected):
        self._assert_same(collection, {"_id": {op: operand}}, expected)

    def test_null_operands(self, collection):
        self._assert_same(collection, {"_id": {"$gt": None}}, [])
        self._assert_same(collection, {"_id": {"$lt": None}}, [])
        self._assert_same(collection, {"_id": {"$gte": None}}, [])
        self._assert_same(collection, {"_id": {"$lte": None}}, [])

    def test_auto_objectid_is_included_in_objectid_range(self, collection):
        result = collection.insert_one({"label": "auto"})
        auto_id = result.inserted_id
        assert isinstance(auto_id, ObjectId)
        self._assert_same(
            collection,
            {"_id": {"$gt": OID_1}},
            ["oid-2", "auto"],
        )

    def test_unsupported_operand_still_falls_back(self, collection):
        built = collection.query_engine.helpers._build_simple_where_clause(
            {"_id": {"$gt": [1, 2]}}
        )
        assert built is None
        assert (
            self._labels(
                collection, {"_id": {"$gt": [1, 2]}}, force_python=False
            )
            == []
        )

    def test_generated_clause_is_type_guarded(self, collection):
        clause, params = (
            collection.query_engine.helpers._build_id_operator_clause(
                {"$gt": "abc"}
            )
        )
        assert "json_type(data, '$._id') = 'text'" in clause
        assert "typeof(" in clause
        assert params == ["abc"]
