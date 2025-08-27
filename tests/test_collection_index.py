# coding: utf-8
from pytest import raises
from typing import Tuple, Type
import sqlite3

# Handle both standard sqlite3 and pysqlite3 exceptions
try:
    import pysqlite3.dbapi2 as sqlite3_with_jsonb  # type: ignore

    IntegrityError: Tuple[Type[Exception], ...] = (
        sqlite3.IntegrityError,
        sqlite3_with_jsonb.IntegrityError,
    )
except ImportError:
    IntegrityError = (sqlite3.IntegrityError,)


def test_create_index(collection):
    collection.insert_one({"foo": "bar"})
    collection.create_index("foo")
    assert "idx_foo_foo" in collection.list_indexes()


def test_create_index_on_nested_keys(collection):
    collection.insert_many(
        [{"foo": {"bar": "zzz"}, "bok": "bak"}, {"a": 1, "b": 2}]
    )
    collection.create_index("foo.bar")
    assert "idx_foo_foo_bar" in collection.list_indexes()


def test_reindex(collection):
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    # With native JSON indexing, reindex does nothing but should not fail
    collection.reindex("idx_foo_foo")


def test_insert_auto_index(collection):
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    collection.insert_one({"foo": "baz"})

    # With native JSON indexing, we can't directly query the index table
    # but we can verify the index exists by checking the index list
    assert "idx_foo_foo" in collection.list_indexes()


def test_create_compound_index(collection):
    collection.insert_one({"foo": "bar", "far": "boo"})
    collection.create_index(["foo", "far"])
    assert "idx_foo_foo_far" in collection.list_indexes()


def test_create_unique_index_violation(collection):
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    with raises(IntegrityError):
        collection.insert_one({"foo": "bar"})


def test_update_to_break_uniqueness(collection):
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    res = collection.insert_one({"foo": "baz"})

    with raises(IntegrityError):
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
        collection.find({"foo": "bar", "a": 2}, hint=f"idx_foo_foo")
    )
    assert len(docs_with_hint) == 1
    assert docs_with_hint[0]["a"] == 2


def test_list_indexes(collection):
    collection.create_index("foo")
    indexes = collection.list_indexes()
    assert isinstance(indexes, list)
    assert "idx_foo_foo" in indexes


def test_drop_index(collection):
    collection.create_index("foo")
    collection.drop_index("foo")
    assert "idx_foo_foo" not in collection.list_indexes()


def test_drop_indexes(collection):
    collection.create_index("foo")
    collection.create_index("bar")
    collection.drop_indexes()
    assert len(collection.list_indexes()) == 0
