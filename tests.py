# coding: utf-8
import re
import sqlite3
from unittest.mock import Mock, call, patch
from pytest import fixture, mark, raises
import nosqlite


@fixture(scope="module")
def db(request) -> sqlite3.Connection:
    _db = sqlite3.connect(":memory:")
    request.addfinalizer(_db.close)
    return _db


@fixture(scope="module")
def collection(db: sqlite3.Connection, request) -> nosqlite.Collection:
    return nosqlite.Collection(db, "foo", create=False)


class TestConnection:
    def test_connect(self):
        conn = nosqlite.Connection(":memory:")
        assert conn.db.isolation_level is None

    @patch("nosqlite.sqlite3")
    def test_context_manager_closes_connection(self, sqlite):
        with nosqlite.Connection() as conn:
            pass
        assert conn.db.close.called

    @patch("nosqlite.sqlite3")
    @patch("nosqlite.Collection")
    def test_getitem_returns_collection(self, mock_collection, sqlite):
        sqlite.connect.return_value = sqlite
        mock_collection.return_value = mock_collection
        conn = nosqlite.Connection()
        assert "foo" not in conn._collections
        assert conn["foo"] == mock_collection

    @patch("nosqlite.sqlite3")
    def test_getitem_returns_cached_collection(self, sqlite):
        conn = nosqlite.Connection()
        conn._collections["foo"] = "bar"
        assert conn["foo"] == "bar"

    @patch("nosqlite.sqlite3")
    def test_drop_collection(self, sqlite):
        conn = nosqlite.Connection()
        conn.drop_collection("foo")
        conn.db.execute.assert_called_with("DROP TABLE IF EXISTS foo")

    @patch("nosqlite.sqlite3")
    def test_getattr_returns_attribute(self, sqlite):
        conn = nosqlite.Connection()
        assert conn.__getattr__("db") in list(conn.__dict__.values())

    @patch("nosqlite.sqlite3")
    def test_getattr_returns_collection(self, sqlite):
        conn = nosqlite.Connection()
        foo = conn.__getattr__("foo")
        assert foo not in list(conn.__dict__.values())
        assert isinstance(foo, nosqlite.Collection)


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

    def test_apply_query_and_type(self):
        query = {"$and": [{"foo": "bar"}, {"baz": "qux"}]}
        assert self.collection._apply_query(query, {"foo": "bar", "baz": "qux"})
        assert not self.collection._apply_query(
            query, {"foo": "bar", "baz": "foo"}
        )

    def test_apply_query_or_type(self):
        query = {"$or": [{"foo": "bar"}, {"baz": "qux"}]}
        assert self.collection._apply_query(query, {"foo": "bar", "abc": "xyz"})
        assert self.collection._apply_query(query, {"baz": "qux", "abc": "xyz"})
        assert not self.collection._apply_query(query, {"abc": "xyz"})

    def test_apply_query_not_type(self):
        query = {"$not": {"foo": "bar"}}
        assert self.collection._apply_query(query, {"foo": "baz"})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_nor_type(self):
        query = {"$nor": [{"foo": "bar"}, {"baz": "qux"}]}
        assert self.collection._apply_query(query, {"foo": "baz", "baz": "bar"})
        assert not self.collection._apply_query(query, {"foo": "bar"})
        assert not self.collection._apply_query(query, {"baz": "qux"})
        assert not self.collection._apply_query(
            query, {"foo": "bar", "baz": "qux"}
        )

    def test_apply_query_gt_operator(self):
        query = {"foo": {"$gt": 5}}
        assert self.collection._apply_query(query, {"foo": 10})
        assert not self.collection._apply_query(query, {"foo": 4})

    def test_apply_query_gte_operator(self):
        query = {"foo": {"$gte": 5}}
        assert self.collection._apply_query(query, {"foo": 5})
        assert not self.collection._apply_query(query, {"foo": 4})

    def test_apply_query_lt_operator(self):
        query = {"foo": {"$lt": 5}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": 10})

    def test_apply_query_lte_operator(self):
        query = {"foo": {"$lte": 5}}
        assert self.collection._apply_query(query, {"foo": 5})
        assert not self.collection._apply_query(query, {"foo": 10})

    def test_apply_query_eq_operator(self):
        query = {"foo": {"$eq": 5}}
        assert self.collection._apply_query(query, {"foo": 5})
        assert not self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_in_operator(self):
        query = {"foo": {"$in": [1, 2, 3]}}
        assert self.collection._apply_query(query, {"foo": 1})
        assert not self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_in_operator_raises(self):
        query = {"foo": {"$in": 5}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": 1})

    def test_apply_query_nin_operator(self):
        query = {"foo": {"$nin": [1, 2, 3]}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert self.collection._apply_query(query, {"foo": "bar"})
        assert not self.collection._apply_query(query, {"foo": 1})

    def test_apply_query_nin_operator_raises(self):
        query = {"foo": {"$nin": 5}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": 1})

    def test_apply_query_ne_operator(self):
        query = {"foo": {"$ne": 5}}
        assert self.collection._apply_query(query, {"foo": 1})
        assert self.collection._apply_query(query, {"foo": "bar"})
        assert not self.collection._apply_query(query, {"foo": 5})

    def test_apply_query_all_operator(self):
        query = {"foo": {"$all": [1, 2, 3]}}
        assert self.collection._apply_query(query, {"foo": list(range(10))})
        assert not self.collection._apply_query(query, {"foo": ["bar", "baz"]})
        assert not self.collection._apply_query(query, {"foo": 3})

    def test_apply_query_all_operator_raises(self):
        query = {"foo": {"$all": 3}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_mod_operator(self):
        query = {"foo": {"$mod": [2, 0]}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": 3})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_mod_operator_raises(self):
        query = {"foo": {"$mod": 2}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": 5})

    def test_apply_query_honors_multiple_operators(self):
        query = {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": 3})
        assert not self.collection._apply_query(query, {"foo": 15})
        assert not self.collection._apply_query(query, {"foo": "foo"})

    def test_apply_query_honors_logical_and_operators(self):
        # 'bar' must be 'baz', and 'foo' must be an even number 0-10
        # or an odd number > 10
        query = {
            "bar": "baz",
            "$or": [
                {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}},
                {"foo": {"$gt": 10, "$mod": [2, 1]}},
            ],
        }

        assert self.collection._apply_query(query, {"bar": "baz", "foo": 4})
        assert self.collection._apply_query(query, {"bar": "baz", "foo": 15})
        assert not self.collection._apply_query(
            query, {"bar": "baz", "foo": 14}
        )
        assert not self.collection._apply_query(query, {"bar": "qux", "foo": 4})

    def test_apply_query_exists(self):
        query_exists = {"foo": {"$exists": True}}
        query_not_exists = {"foo": {"$exists": False}}
        assert self.collection._apply_query(query_exists, {"foo": "bar"})
        assert self.collection._apply_query(query_not_exists, {"bar": "baz"})
        assert not self.collection._apply_query(query_exists, {"baz": "bar"})
        assert not self.collection._apply_query(
            query_not_exists, {"foo": "bar"}
        )

    def test_apply_query_exists_raises(self):
        query = {"foo": {"$exists": "foo"}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": "bar"})

    def test_get_operator_fn_improper_op(self):
        with raises(nosqlite.MalformedQueryException):
            self.collection._get_operator_fn("foo")

    def test_get_operator_fn_valid_op(self):
        assert self.collection._get_operator_fn("$in") == nosqlite._in

    def test_get_operator_fn_no_op(self):
        with raises(nosqlite.MalformedQueryException):
            self.collection._get_operator_fn("$foo")

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


class TestFindOne:
    def test_returns_None_if_collection_does_not_exist(
        self, collection: nosqlite.Collection
    ):
        assert collection.find_one({}) is None

    def test_returns_None_if_document_is_not_found(
        self, collection: nosqlite.Collection
    ):
        collection.create()
        assert collection.find_one({}) is None
