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

    def test_ensure_index(self):
        self.collection.create()
        doc = {"foo": "bar"}
        self.collection.insert(doc)
        self.collection.ensure_index("foo")
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        index_name = f"{self.collection.name}{{foo}}"
        assert (
            index_name
            == self.collection.db.execute(
                cmd.format(name=self.collection.name)
            ).fetchone()[0]
        )

    def test_create_index(self):
        self.collection.create()
        doc = {"foo": "bar"}
        self.collection.insert(doc)
        self.collection.create_index("foo", reindex=False)
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        index_name = f"{self.collection.name}{{foo}}"
        assert (
            index_name
            == self.collection.db.execute(
                cmd.format(name=self.collection.name)
            ).fetchone()[0]
        )

    def test_create_index_on_nested_keys(self):
        self.collection.create()
        doc = {"foo": {"bar": "zzz"}, "bok": "bak"}
        self.collection.insert(doc)
        self.collection.insert({"a": 1, "b": 2})
        self.collection.create_index("foo.bar", reindex=True)
        index = f"[{self.collection.name}{{foo_bar}}]"
        assert index in self.collection.list_indexes()
        self.collection.create_index(["foo_bar", "bok"], reindex=True)
        index = f"[{self.collection.name}{{foo_bar,bok}}]"
        assert index in self.collection.list_indexes()

    def test_index_on_nested_keys(self):
        self.test_create_index_on_nested_keys()
        index_name = f"{self.collection.name}{{foo_bar}}"
        cmd = f"SELECT id, foo_bar FROM [{index_name}]"
        assert (1, '"zzz"') == self.collection.db.execute(cmd).fetchone()
        index_name = f"{self.collection.name}{{foo_bar,bok}}"
        cmd = f"SELECT * FROM [{index_name}]"
        assert (1, '"zzz"', '"bak"') == self.collection.db.execute(
            cmd
        ).fetchone()

    def test_reindex(self):
        self.test_create_index()
        index_name = f"{self.collection.name}{{foo}}"
        self.collection.reindex(f"[{index_name}]")
        cmd = f"SELECT id, foo FROM [{index_name}]"
        assert (1, '"bar"') == self.collection.db.execute(cmd).fetchone()

    def test_insert_auto_index(self):
        self.test_reindex()
        self.collection.insert({"foo": "baz"})
        index_name = f"{self.collection.name}{{foo}}"
        cmd = f"SELECT id, foo FROM [{index_name}]"
        results = self.collection.db.execute(cmd).fetchall()
        assert (1, '"bar"') in results
        assert (2, '"baz"') in results

    def test_create_compound_index(self):
        self.collection.create()
        doc = {"foo": "bar", "far": "boo"}
        self.collection.insert(doc)
        self.collection.create_index(("foo", "far"))
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        assert (
            f"{self.collection.name}{{foo,far}}"
            == self.collection.db.execute(
                cmd.format(name=self.collection.name)
            ).fetchone()[0]
        )

    def test_create_unique_index(self):
        self.collection.create()
        doc = {"foo": "bar"}
        self.collection.insert(doc)
        self.collection.create_index("foo", reindex=False, unique=True)
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        index_name = f"{self.collection.name}{{foo}}"
        assert (
            index_name
            == self.collection.db.execute(
                cmd.format(name=self.collection.name)
            ).fetchone()[0]
        )

    def test_reindex_unique_index(self):
        self.test_create_unique_index()
        index_name = f"{self.collection.name}{{foo}}"
        self.collection.reindex(f"[{index_name}]")
        cmd = f"SELECT id, foo FROM [{index_name}]"
        assert (1, '"bar"') == self.collection.db.execute(cmd).fetchone()

    def test_reindex_skips_sparse_documents(self):
        self.collection.create()
        self.collection.create_index("foo")
        self.collection.insert({"a": 1})
        self.collection.insert({"foo": "bar"})
        index_name = f"{self.collection.name}{{foo}}"
        self.collection.reindex(f"[{index_name}]", sparse=True)
        assert 1 == len(self.collection.db.execute(f"SELECT COUNT(1) FROM [{index_name}]").fetchone())

    def test_uniqueness(self):
        self.test_reindex_unique_index()
        doc = {"foo": "bar"}
        index_name = f"{self.collection.name}{{foo}}"
        cmd = f"SELECT id, foo FROM [{index_name}]"
        results = self.collection.db.execute(cmd).fetchall()
        assert [(1, '"bar"')] == results
        with raises(sqlite3.IntegrityError):
            self.collection.insert(doc)
        assert [(1, '"bar"')] == results

    def test_update_to_break_uniqueness(self):
        self.test_uniqueness()
        doc = {"foo": "baz"}
        self.collection.insert(doc)
        index_name = f"{self.collection.name}{{foo}}"
        cmd = f"SELECT id, foo FROM [{index_name}]"
        results = self.collection.db.execute(cmd).fetchall()
        assert [(1, '"bar"'), (3, '"baz"')] == results
        doc = {"foo": "bar", "_id": 3}
        with raises(sqlite3.IntegrityError):
            self.collection.save(doc)
        assert [(1, '"bar"'), (3, '"baz"')] == results

    def test_create_unique_index_on_non_unique_collection(self):
        self.collection.create()
        self.collection.insert({"foo": "bar", "a": 1})
        self.collection.insert({"foo": "bar", "a": 2})
        assert 2 == self.collection.count({"foo": "bar"})
        with raises(sqlite3.IntegrityError):
            self.collection.create_index("foo", unique=True)
        assert 0 == len(self.collection.list_indexes())

    def test_hint_index(self):
        self.collection.create()
        self.collection.insert({"foo": "bar", "a": 1})
        self.collection.insert({"foo": "bar", "a": 2})
        self.collection.insert({"fox": "baz", "a": 3})
        self.collection.insert({"fox": "bar", "a": 4})
        self.collection.create_index("foo")
        self.collection.db = Mock(wraps=self.db)
        docs_without_hint = self.collection.find({"foo": "bar", "a": 2})
        self.collection.db.execute.assert_any_call("SELECT id, data FROM foo ")
        docs_with_hint = self.collection.find(
            {"foo": "bar", "a": 2}, hint="[foo{foo}]"
        )
        self.collection.db.execute.assert_any_call(
            "SELECT id, data FROM foo WHERE id IN (SELECT id FROM [foo{foo}] WHERE foo='\"bar\"')"
        )
        assert docs_without_hint == docs_with_hint

    def test_list_indexes(self):
        self.test_create_index()
        assert isinstance(self.collection.list_indexes(), list)
        assert isinstance(self.collection.list_indexes()[0], str)
        assert (
            f"[{self.collection.name}{{foo}}]"
            == self.collection.list_indexes()[0]
        )

    def test_list_indexes_as_keys(self):
        self.test_create_index()
        assert isinstance(self.collection.list_indexes(as_keys=True), list)
        assert isinstance(self.collection.list_indexes(as_keys=True)[0], list)
        assert ["foo"] == self.collection.list_indexes(as_keys=True)[0]

    def test_drop_index(self):
        self.test_create_index()
        index_name = f"[{self.collection.name}{{foo}}]"
        self.collection.drop_index(index_name)
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        assert (
            self.collection.db.execute(
                cmd.format(name=self.collection.name)
            ).fetchone()
            is None
        )

    def test_drop_indexes(self):
        self.test_create_index()
        self.collection.drop_indexes()
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        assert (
            self.collection.db.execute(
                cmd.format(name=self.collection.name)
            ).fetchone()
            is None
        )

