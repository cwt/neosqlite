# coding: utf-8
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


def test_create_indexes_with_string_keys(collection):
    collection.insert_one({"foo": "bar", "baz": "qux"})
    indexes = collection.create_indexes(["foo", "baz"])
    assert "idx_foo_foo" in indexes
    assert "idx_foo_baz" in indexes
    assert len(indexes) == 2
    assert "idx_foo_foo" in collection.list_indexes()
    assert "idx_foo_baz" in collection.list_indexes()


def test_create_indexes_with_compound_keys(collection):
    collection.insert_one({"foo": "bar", "baz": "qux", "quux": "corge"})
    indexes = collection.create_indexes([["foo", "baz"], ["quux"]])
    assert "idx_foo_foo_baz" in indexes
    assert "idx_foo_quux" in indexes
    assert len(indexes) == 2
    assert "idx_foo_foo_baz" in collection.list_indexes()
    assert "idx_foo_quux" in collection.list_indexes()


def test_create_indexes_with_dict_specifications(collection):
    collection.insert_one({"foo": "bar", "baz": "qux"})
    indexes = collection.create_indexes(
        [{"key": "foo"}, {"key": ["baz"], "unique": True}]
    )
    assert "idx_foo_foo" in indexes
    assert "idx_foo_baz" in indexes
    assert len(indexes) == 2
    assert "idx_foo_foo" in collection.list_indexes()
    assert "idx_foo_baz" in collection.list_indexes()


def test_create_indexes_with_mixed_specifications(collection):
    collection.insert_one({"foo": "bar", "baz": "qux", "quux": "corge"})
    indexes = collection.create_indexes(
        ["foo", ["baz", "quux"], {"key": "quux", "unique": True}]
    )
    assert "idx_foo_foo" in indexes
    assert "idx_foo_baz_quux" in indexes
    assert "idx_foo_quux" in indexes
    assert len(indexes) == 3
    assert "idx_foo_foo" in collection.list_indexes()
    assert "idx_foo_baz_quux" in collection.list_indexes()
    assert "idx_foo_quux" in collection.list_indexes()


def test_create_indexes_on_nested_keys(collection):
    collection.insert_many(
        [{"foo": {"bar": "zzz"}, "bok": "bak"}, {"a": 1, "b": 2}]
    )
    indexes = collection.create_indexes(["foo.bar"])
    assert "idx_foo_foo_bar" in indexes
    assert len(indexes) == 1
    assert "idx_foo_foo_bar" in collection.list_indexes()
