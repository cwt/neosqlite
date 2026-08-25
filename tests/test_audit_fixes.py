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


class TestNullMatchSemantics:
    """#90: {f: null} must match null-or-missing; $ne must not exclude
    documents whose field is missing — in both SQL tiers."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.t
        c.insert_many([{"a": 1, "b": None}, {"a": 2}, {"a": 3, "b": "x"}])
        return c

    def test_equality_null_matches_null_and_missing(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": None}))
        assert found == [1, 2]

    def test_eq_operator_null_matches_null_and_missing(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": {"$eq": None}}))
        assert found == [1, 2]

    def test_ne_excludes_only_matching_values_not_missing(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": {"$ne": "x"}}))
        assert found == [1, 2]

    def test_ne_still_excludes_the_value_itself(self, docs):
        found = [d["a"] for d in docs.find({"b": {"$ne": None}})]
        assert found == [3]

    def test_tier1_group_pipeline_match_null(self, connection):
        """Tier-1 CTE match builder honors the same semantics."""
        c = connection.t1
        c.insert_many([{"g": None}, {"g": "v"}, {}])
        rows = list(
            c.aggregate(
                [
                    {"$match": {"g": {"$ne": "v"}}},
                    {"$group": {"_id": None, "n": {"$sum": 1}}},
                ]
            )
        )
        assert rows and rows[0]["n"] == 2

    def test_tier1_match_equality_null(self, connection):
        c = connection.t1
        c.insert_many([{"g": None}, {"g": "v"}, {}])
        found = list(c.aggregate([{"$match": {"g": None}}]))
        # Both the explicit-null and the missing-field docs match
        assert len(found) == 2
        assert all(d.get("g") is None for d in found)


class TestTextSearchCombinedFilters:
    """#91: $text used to silently drop every sibling filter condition."""

    @pytest.fixture
    def news(self, connection):
        c = connection.news
        c.insert_many(
            [
                {"title": "war report", "category": "politics"},
                {"title": "war economy", "category": "business"},
            ]
        )
        c.create_search_index("title")
        return c

    def test_text_plus_filter_returns_only_matching_both(self, news):
        found = sorted(
            d["category"]
            for d in news.find(
                {"category": "politics", "$text": {"$search": "war"}}
            )
        )
        assert found == ["politics"]

    def test_text_alone_still_matches_all_indexes(self, news):
        found = sorted(
            d["category"]
            for d in news.find({"$text": {"$search": "war"}})
        )
        assert found == ["business", "politics"]

    def test_text_plus_nonmatching_filter_returns_nothing(self, news):
        found = list(
            news.find({"category": "sports", "$text": {"$search": "war"}})
        )
        assert found == []
