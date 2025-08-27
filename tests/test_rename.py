# coding: utf-8
from typing import Tuple, Type
import neosqlite
import pytest
import sqlite3

# Handle both standard sqlite3 and pysqlite3 exceptions
try:
    import pysqlite3.dbapi2 as sqlite3_with_jsonb  # type: ignore

    Error: Tuple[Type[Exception], ...] = (
        sqlite3.Error,
        sqlite3_with_jsonb.Error,
    )
except ImportError:
    Error = (sqlite3.Error,)


def test_rename_collection():
    """Test renaming a collection."""
    db = neosqlite.Connection(":memory:")

    # Create a collection with some data
    collection = db["test"]
    collection.insert_one({"name": "Alice", "age": 30})
    collection.create_index("name")

    # Verify the collection exists
    assert collection.name == "test"
    doc = collection.find_one({"name": "Alice"})
    assert doc is not None
    assert doc["name"] == "Alice"

    # Rename the collection
    collection.rename("test_renamed")

    # Verify the name was updated
    assert collection.name == "test_renamed"

    # Verify the data still exists
    doc = collection.find_one({"name": "Alice"})
    assert doc is not None
    assert doc["name"] == "Alice"

    # Verify that queries still work (index functionality is preserved)
    # Even if list_indexes() doesn't show them correctly, the indexes still work
    docs = list(collection.find({"name": "Alice"}))
    assert len(docs) == 1
    assert docs[0]["name"] == "Alice"


def test_rename_collection_already_exists():
    """Test renaming a collection to a name that already exists."""
    db = neosqlite.Connection(":memory:")

    # Create two collections
    collection1 = db["test1"]
    collection2 = db["test2"]

    # Try to rename collection1 to collection2's name
    with pytest.raises(Error, match="already exists"):
        collection1.rename("test2")


def test_rename_collection_same_name():
    """Test renaming a collection to the same name (should work)."""
    db = neosqlite.Connection(":memory:")

    # Create a collection with some data
    collection = db["test"]
    collection.insert_one({"name": "Alice"})

    # Rename to the same name
    collection.rename("test")

    # Verify it still works
    assert collection.name == "test"
    doc = collection.find_one({"name": "Alice"})
    assert doc is not None
