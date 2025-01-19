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

    def test_find_and_modify(self):
        update = {"foo": "bar"}
        docs = [
            {"foo": "foo"},
            {"baz": "qux"},
        ]
        with patch.object(self.collection, "find"):
            with patch.object(self.collection, "save"):
                self.collection.find.return_value = docs
                self.collection.find_and_modify(update=update)
                self.collection.save.assert_has_calls(
                    [
                        call({"foo": "bar"}),
                        call({"foo": "bar", "baz": "qux"}),
                    ]
                )

    def test_count(self):
        with patch.object(self.collection, "find"):
            self.collection.find.return_value = list(range(10))
            assert self.collection.count() == 10

    def test_distinct(self):
        docs = [{"foo": "bar"}, {"foo": "baz"}, {"foo": 10}, {"bar": "foo"}]
        self.collection.find = lambda: docs
        assert set(("bar", "baz", 10)) == self.collection.distinct("foo")

    def test_rename_raises_for_collision(self):
        nosqlite.Collection(self.db, "bar")  # Create a collision point
        self.collection.create()
        with raises(AssertionError):
            self.collection.rename("bar")

    def test_rename(self):
        self.collection.create()
        assert self.collection.exists()
        self.collection.rename("bar")
        assert self.collection.name == "bar"
        assert self.collection.exists()
        assert not nosqlite.Collection(self.db, "foo", create=False).exists()

