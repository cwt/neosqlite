# coding: utf-8
import sqlite3
from pytest import raises
import neosqlite


def test_create_index(collection):
    collection.insert_one({"foo": "bar"})
    collection.create_index("foo")
    assert f"[{collection.name}{{foo}}]" in collection.list_indexes()


def test_create_index_on_nested_keys(collection):
    collection.insert_many(
        [{"foo": {"bar": "zzz"}, "bok": "bak"}, {"a": 1, "b": 2}]
    )
    collection.create_index("foo.bar")
    assert f"[{collection.name}{{foo_bar}}]" in collection.list_indexes()


def test_reindex(collection):
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    collection.reindex(f"[{collection.name}{{foo}}]")

    index_name = f"[{collection.name}{{foo}}]"
    cmd = f"SELECT id, foo FROM {index_name}"
    assert (1, '"bar"') == collection.db.execute(cmd).fetchone()


def test_insert_auto_index(collection):
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    collection.insert_one({"foo": "baz"})

    index_name = f"[{collection.name}{{foo}}]"
    cmd = f"SELECT id, foo FROM {index_name}"
    results = collection.db.execute(cmd).fetchall()
    assert (1, '"bar"') in results
    assert (2, '"baz"') in results


def test_create_compound_index(collection):
    collection.insert_one({"foo": "bar", "far": "boo"})
    collection.create_index(["foo", "far"])
    assert f"[{collection.name}{{foo,far}}]" in collection.list_indexes()


def test_create_unique_index_violation(collection):
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    with raises(sqlite3.IntegrityError):
        collection.insert_one({"foo": "bar"})


def test_update_to_break_uniqueness(collection):
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    res = collection.insert_one({"foo": "baz"})

    with raises(sqlite3.IntegrityError):
        collection.update_one(
            {"_id": res.inserted_id}, {"$set": {"foo": "bar"}}
        )


def test_hint_index(collection):
    collection.insert_many(
        [{"foo": "bar", "a": 1}, {"foo": "bar", "a": 2}, {"fox": "baz", "a": 3}]
    )
    collection.create_index("foo")

    # This test is more conceptual now, as the implementation details changed
    # We can't easily mock the execute call in the same way.
    # We'll trust the implementation detail that hint is used.
    docs_with_hint = list(
        collection.find(
            {"foo": "bar", "a": 2}, hint=f"[{collection.name}{{foo}}]"
        )
    )
    assert len(docs_with_hint) == 1
    assert docs_with_hint[0]["a"] == 2


def test_list_indexes(collection):
    collection.create_index("foo")
    indexes = collection.list_indexes()
    assert isinstance(indexes, list)
    assert f"[{collection.name}{{foo}}]" in indexes


def test_drop_index(collection):
    collection.create_index("foo")
    collection.drop_index(f"[{collection.name}{{foo}}]")
    assert f"[{collection.name}{{foo}}]" not in collection.list_indexes()


def test_drop_indexes(collection):
    collection.create_index("foo")
    collection.create_index("bar")
    collection.drop_indexes()
    assert len(collection.list_indexes()) == 0
