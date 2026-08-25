"""Regression tests for critical audit fixes (#87+).

Each test class documents the pre-fix failure and asserts MongoDB-consistent
behavior after the fix.
"""

import pytest

import neosqlite
from neosqlite.exceptions import MalformedQueryException


class TestUpdateManyIncMulValidation:
    """#87: update_many's direct-SQL path skipped $inc/$mul type validation.

    SQLite coerces 'hello' + 1 to 1, so update_many silently rewrote string
    fields to numbers. It now applies the same validation as update_one and
    falls back to the Python tier, which raises MalformedQueryException.
    """

    def test_inc_on_string_field_raises_instead_of_corrupting(
        self, connection
    ):
        c = connection.t
        c.insert_many([{"a": "hello"}, {"a": "world"}])
        with pytest.raises(MalformedQueryException):
            c.update_many({}, {"$inc": {"a": 1}})
        # No partial corruption from the fast path
        assert sorted(d["a"] for d in c.find({})) == ["hello", "world"]

    def test_mul_on_string_field_raises(self, connection):
        c = connection.t
        c.insert_many([{"a": "hello"}])
        with pytest.raises(MalformedQueryException):
            c.update_many({}, {"$mul": {"a": 2}})

    def test_numeric_inc_fast_path_unaffected(self, connection):
        c = connection.t
        c.insert_many([{"n": 1}, {"n": 2}])
        res = c.update_many({}, {"$inc": {"n": 10}})
        assert res.modified_count == 2
        assert [d["n"] for d in c.find({})] == [11, 12]

    def test_filtered_validation_only_checks_matched_docs(self, connection):
        """Docs excluded by the filter must not block a valid $inc."""
        c = connection.t
        c.insert_many([{"kind": "num", "v": 5}, {"kind": "str", "v": "x"}])
        res = c.update_many({"kind": "num"}, {"$inc": {"v": 1}})
        assert res.modified_count == 1
        docs = {d["kind"]: d["v"] for d in c.find({})}
        assert docs == {"num": 6, "str": "x"}


class TestMinMaxMissingField:
    """#88: SQL-tier $min/$max wrote JSON null when the field was missing."""

    def test_min_sets_value_when_field_missing(self, connection):
        c = connection.t
        c.insert_one({"_id": 1})
        c.update_one({"_id": 1}, {"$min": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 5

    def test_max_sets_value_when_field_missing(self, connection):
        c = connection.t
        c.insert_one({"_id": 1})
        c.update_one({"_id": 1}, {"$max": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 5

    def test_min_keeps_smaller_existing_value(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "score": 3})
        c.update_one({"_id": 1}, {"$min": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 3

    def test_max_keeps_larger_existing_value(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "score": 9})
        c.update_one({"_id": 1}, {"$max": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 9


