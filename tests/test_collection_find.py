# coding: utf-8
import re
from unittest.mock import Mock, call, patch
from pytest import mark, raises
import sqlite3
import nosqlite


class TestCollection:
    def setup_method(self):
        self.db = sqlite3.connect(":memory:")
        self.collection = nosqlite.Collection(self.db, "foo", create=False)

    def teardown_method(self):
        self.db.close()

    def unformat_sql(self, sql: str) -> str:
        return re.sub(r"[\s]+", " ", sql.strip().replace("\n", ""))

    def test_find_with_sort(self):
        self.collection.create()
        self.collection.save({"a": 1, "b": "c"})
        self.collection.save({"a": 1, "b": "a"})
        self.collection.save({"a": 5, "b": "x"})
        self.collection.save({"a": 3, "b": "x"})
        self.collection.save({"a": 4, "b": "z"})
        assert [
            {"a": 1, "b": "c", "_id": 1},
            {"a": 1, "b": "a", "_id": 2},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
        ] == self.collection.find()
        assert [
            {"a": 1, "b": "c", "_id": 1},
            {"a": 1, "b": "a", "_id": 2},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
            {"a": 5, "b": "x", "_id": 3},
        ] == self.collection.find(sort={"a": nosqlite.ASCENDING})
        assert [
            {"a": 1, "b": "a", "_id": 2},
            {"a": 1, "b": "c", "_id": 1},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
        ] == self.collection.find(sort={"b": nosqlite.ASCENDING})
        assert [
            {"a": 5, "b": "x", "_id": 3},
            {"a": 4, "b": "z", "_id": 5},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 1, "b": "c", "_id": 1},
            {"a": 1, "b": "a", "_id": 2},
        ] == self.collection.find(sort={"a": nosqlite.DESCENDING})
        assert [
            {"a": 4, "b": "z", "_id": 5},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 1, "b": "c", "_id": 1},
            {"a": 1, "b": "a", "_id": 2},
        ] == self.collection.find(sort={"b": nosqlite.DESCENDING})
        assert [
            {"a": 1, "b": "a", "_id": 2},
            {"a": 1, "b": "c", "_id": 1},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
            {"a": 5, "b": "x", "_id": 3},
        ] == self.collection.find(
            sort={"a": nosqlite.ASCENDING, "b": nosqlite.ASCENDING}
        )
        assert [
            {"a": 5, "b": "x", "_id": 3},
            {"a": 4, "b": "z", "_id": 5},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 1, "b": "a", "_id": 2},
            {"a": 1, "b": "c", "_id": 1},
        ] == self.collection.find(
            sort={"a": nosqlite.DESCENDING, "b": nosqlite.ASCENDING}
        )
        assert [
            {"a": 5, "b": "x", "_id": 3},
            {"a": 4, "b": "z", "_id": 5},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 1, "b": "c", "_id": 1},
            {"a": 1, "b": "a", "_id": 2},
        ] == self.collection.find(
            sort={"a": nosqlite.DESCENDING, "b": nosqlite.DESCENDING}
        )

    def test_find_with_skip_and_limit(self):
        self.collection.create()
        self.collection.save({"a": 1, "b": "c"})
        self.collection.save({"a": 1, "b": "a"})
        self.collection.save({"a": 5, "b": "x"})
        self.collection.save({"a": 3, "b": "x"})
        self.collection.save({"a": 4, "b": "z"})
        assert [
            {"a": 1, "b": "c", "_id": 1},
            {"a": 1, "b": "a", "_id": 2},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
        ] == self.collection.find(skip=0, limit=5)
        assert [
            {"a": 1, "b": "a", "_id": 2},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
        ] == self.collection.find(skip=1, limit=5)
        assert [
            {"a": 1, "b": "a", "_id": 2},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
            {"a": 4, "b": "z", "_id": 5},
        ] == self.collection.find(skip=1, limit=4)
        assert [
            {"a": 1, "b": "a", "_id": 2},
            {"a": 5, "b": "x", "_id": 3},
            {"a": 3, "b": "x", "_id": 4},
        ] == self.collection.find(skip=1, limit=3)
        assert [] == self.collection.find(limit=0)

    def test_find_with_sort_on_nested_key(self):
        self.collection.create()
        self.collection.save({"a": {"b": 5}, "c": "B"})
        self.collection.save({"a": {"b": 9}, "c": "A"})
        self.collection.save({"a": {"b": 7}, "c": "C"})
        assert [
            {"a": {"b": 5}, "c": "B", "_id": 1},
            {"a": {"b": 7}, "c": "C", "_id": 3},
            {"a": {"b": 9}, "c": "A", "_id": 2},
        ] == self.collection.find(sort={"a.b": nosqlite.ASCENDING})
        assert [
            {"a": {"b": 9}, "c": "A", "_id": 2},
            {"a": {"b": 7}, "c": "C", "_id": 3},
            {"a": {"b": 5}, "c": "B", "_id": 1},
        ] == self.collection.find(sort={"a.b": nosqlite.DESCENDING})

    @mark.parametrize(
        "strdoc,doc",
        [
            ('{"foo": "bar"}', {"_id": 1, "foo": "bar"}),
            ('{"foo": "☃"}', {"_id": 1, "foo": "☃"}),
        ],
    )
    def test_load(self, strdoc, doc):
        assert doc == self.collection._load(1, strdoc)

    def test_find(self):
        query = {"foo": "bar"}
        documents = [
            (1, {"foo": "bar", "baz": "qux"}),  # Will match
            (2, {"foo": "bar", "bar": "baz"}),  # Will match
            (2, {"foo": "baz", "bar": "baz"}),  # Will not match
            (3, {"baz": "qux"}),  # Will not match
        ]

        collection = nosqlite.Collection(Mock(), "foo", create=False)
        collection.db.execute.return_value = collection.db
        collection.db.fetchall.return_value = documents
        collection._load = lambda id, data: data

        ret = collection.find(query)
        assert len(ret) == 2

    def test_find_honors_limit(self):
        query = {"foo": "bar"}
        documents = [
            (1, {"foo": "bar", "baz": "qux"}),  # Will match
            (2, {"foo": "bar", "bar": "baz"}),  # Will match
            (2, {"foo": "baz", "bar": "baz"}),  # Will not match
            (3, {"baz": "qux"}),  # Will not match
        ]

        collection = nosqlite.Collection(Mock(), "foo", create=False)
        collection.db.execute.return_value = collection.db
        collection.db.fetchall.return_value = documents
        collection._load = lambda id, data: data

        ret = collection.find(query, limit=1)
        assert len(ret) == 1

