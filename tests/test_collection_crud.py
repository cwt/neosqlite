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

    def test_create(self):
        collection = nosqlite.Collection(Mock(), "foo", create=False)
        collection.create()
        collection.db.execute.assert_any_call(
            """
            CREATE TABLE IF NOT EXISTS foo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL
            )"""
        )

    def test_clear(self):
        collection = nosqlite.Collection(Mock(), "foo")
        collection.clear()
        collection.db.execute.assert_any_call("DELETE FROM foo")

    def test_exists_when_absent(self):
        assert not self.collection.exists()

    def test_exists_when_present(self):
        self.collection.create()
        assert self.collection.exists()

    def test_insert_actually_save(self):
        doc = {"_id": 1, "foo": "bar"}
        self.collection.save = Mock()
        self.collection.insert(doc)
        self.collection.save.assert_called_with(doc)

    def test_insert(self):
        doc = {"foo": "bar"}
        self.collection.create()
        inserted = self.collection.insert(doc)
        assert inserted["_id"] == 1

    def test_insert_non_dict_raise(self):
        doc = "{'foo': 'bar'}"
        self.collection.create()
        with raises(nosqlite.MalformedDocument):
            self.collection.insert(doc)

    def test_update_without_upsert(self):
        doc = {"foo": "bar"}
        self.collection.create()
        updated = self.collection.update({}, doc)
        assert updated is None

    def test_update_with_upsert(self):
        doc = {"foo": "bar"}
        self.collection.create()
        updated = self.collection.update({}, doc, upsert=True)
        assert isinstance(updated, dict)
        assert updated["_id"] == 1
        assert updated["foo"] == doc["foo"] == "bar"

    def test_save_calls_update(self):
        with patch.object(self.collection, "update"):
            doc = {"foo": "bar"}
            self.collection.save(doc)
            self.collection.update.assert_called_with(
                {"_id": doc.pop("_id", None)}, doc, upsert=True
            )

    def test_save(self):
        doc = {"foo": "bar"}
        self.collection.create()
        doc = self.collection.insert(doc)
        doc["foo"] = "baz"
        updated = self.collection.save(doc)
        assert updated["foo"] == "baz"

    def test_delete_calls_remove(self):
        with patch.object(self.collection, "_remove"):
            doc = {"foo": "bar"}
            self.collection.delete(doc)
            self.collection._remove.assert_called_with(doc)

    def test_remove_raises_when_no_id(self):
        with raises(AssertionError):
            self.collection._remove({"foo": "bar"})

    def test_remove(self):
        self.collection.create()
        doc = self.collection.insert({"foo": "bar"})
        assert 1 == int(
            self.collection.db.execute("SELECT COUNT(1) FROM foo").fetchone()[0]
        )
        self.collection._remove(doc)
        assert 0 == int(
            self.collection.db.execute("SELECT COUNT(1) FROM foo").fetchone()[0]
        )

    def test_delete_one(self):
        self.collection.create()
        doc = {"foo": "bar"}
        self.collection.insert(doc)
        assert 1 == int(
            self.collection.db.execute("SELECT COUNT(1) FROM foo").fetchone()[0]
        )
        self.collection.delete_one(doc)
        assert 0 == int(
            self.collection.db.execute("SELECT COUNT(1) FROM foo").fetchone()[0]
        )
        assert self.collection.delete_one(doc) is None

    def test_insert_bulk_documents_on_a_transaction(self):
        self.collection.create()
        self.collection.begin()
        self.collection.save({"a": 1, "b": "c"})
        self.collection.save({"a": 1, "b": "a"})
        self.collection.rollback()
        assert 0 == self.collection.count({"a": 1})
        self.collection.begin()
        self.collection.save({"a": 1, "b": "c"})
        self.collection.save({"a": 1, "b": "a"})
        self.collection.commit()
        assert 2 == self.collection.count({"a": 1})

