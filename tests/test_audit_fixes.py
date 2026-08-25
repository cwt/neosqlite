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


class TestAddToSetNoDocumentNesting:
    """#89: the fallback $addToSet clause assigned the whole document into
    the array field — for existing elements via THEN data, and for new ones
    because json_insert(data, ...) returns the full modified document."""

    def _apply_clause(self, connection, col, value):
        clause, params = col.query_engine.helpers._build_sql_update_clause(
            "$addToSet", value
        )
        wrapped = f"jsonb_set(data, {clause[0]})"
        connection.db.execute(
            f"UPDATE {col.name} SET data = {wrapped} WHERE id = 1", params
        )
        connection.db.commit()

    def test_add_to_set_existing_element_keeps_array_intact(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2]})
        self._apply_clause(connection, c, {"arr": 1})
        doc = c.find_one({"_id": 1})
        assert doc["arr"] == [1, 2], f"got {doc!r}"

    def test_add_to_set_new_element_appends_via_clause(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2]})
        self._apply_clause(connection, c, {"arr": 3})
        assert sorted(c.find_one({"_id": 1})["arr"]) == [1, 2, 3]

    def test_add_to_set_via_update_one_stays_correct(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2]})
        c.update_one({"_id": 1}, {"$addToSet": {"arr": 3}})
        c.update_one({"_id": 1}, {"$addToSet": {"arr": 3}})
        assert sorted(c.find_one({"_id": 1})["arr"]) == [1, 2, 3]
