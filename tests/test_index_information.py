# coding: utf-8
import sqlite3
from pytest import raises
import neosqlite


def test_index_information_empty(collection):
    """Test index_information on a collection with no indexes."""
    info = collection.index_information()
    assert isinstance(info, dict)
    assert len(info) == 0


def test_index_information_single_index(collection):
    """Test index_information with a single index."""
    collection.create_index("foo")
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo" in info

    # Check index details
    idx_info = info["idx_foo_foo"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is False
    assert "key" in idx_info
    assert idx_info["key"] == {"foo": 1}


def test_index_information_single_index_unique(collection):
    """Test index_information with a single unique index."""
    collection.create_index("foo", unique=True)
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo" in info

    # Check index details
    idx_info = info["idx_foo_foo"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is True
    assert "key" in idx_info
    assert idx_info["key"] == {"foo": 1}


def test_index_information_nested_key_index(collection):
    """Test index_information with an index on a nested key."""
    collection.create_index("foo.bar")
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo_bar" in info

    # Check index details
    idx_info = info["idx_foo_foo_bar"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is False
    assert "key" in idx_info
    assert idx_info["key"] == {"foo.bar": 1}


def test_index_information_compound_index(collection):
    """Test index_information with a compound index."""
    collection.create_index(["foo", "bar"])
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo_bar" in info

    # Check index details
    idx_info = info["idx_foo_foo_bar"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is False
    assert "key" in idx_info
    # For compound indexes, we expect both keys
    assert "foo" in idx_info["key"]
    assert "bar" in idx_info["key"]
    assert idx_info["key"]["foo"] == 1
    assert idx_info["key"]["bar"] == 1


def test_index_information_multiple_indexes(collection):
    """Test index_information with multiple indexes."""
    collection.create_index("foo")
    collection.create_index("bar", unique=True)
    collection.create_index(["baz", "qux"])

    info = collection.index_information()

    # Should have three indexes
    assert len(info) == 3

    # Check all index names exist
    assert "idx_foo_foo" in info
    assert "idx_foo_bar" in info
    assert "idx_foo_baz_qux" in info

    # Check details for each index
    # foo index
    foo_info = info["idx_foo_foo"]
    assert foo_info["unique"] is False
    assert foo_info["key"] == {"foo": 1}

    # bar index (unique)
    bar_info = info["idx_foo_bar"]
    assert bar_info["unique"] is True
    assert bar_info["key"] == {"bar": 1}

    # baz.qux compound index
    baz_qux_info = info["idx_foo_baz_qux"]
    assert baz_qux_info["unique"] is False
    assert "baz" in baz_qux_info["key"]
    assert "qux" in baz_qux_info["key"]
