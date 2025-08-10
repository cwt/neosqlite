# coding: utf-8
import pytest
import neosqlite
from neosqlite import MalformedDocument, MalformedQueryException


def test_malformed_document_error():
    """Test that inserting a non-dict document raises MalformedDocument."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with a non-dict document
    with pytest.raises(MalformedDocument):
        collection._internal_insert("not a dict")


# We can't easily test the lastrowid error case without more complex mocking
# Let's remove this test for now


def test_update_many_fallback():
    """Test update_many with a complex query that requires the fallback path."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
    )

    # Update using a complex query that should trigger the fallback
    result = collection.update_many(
        {"name": {"$regex": "^A"}},  # This should trigger the fallback
        {"$inc": {"age": 1}},
    )

    # Verify the update worked
    alice = collection.find_one({"name": "Alice"})
    assert alice["age"] == 31


def test_replace_one_upsert():
    """Test replace_one with upsert=True."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Replace with upsert when no document matches
    result = collection.replace_one(
        {"name": "Alice"}, {"name": "Alice", "age": 30}, upsert=True
    )

    # Verify it was inserted
    assert result.upserted_id is not None
    assert result.matched_count == 0
    assert result.modified_count == 0

    # Verify the document exists
    doc = collection.find_one({"name": "Alice"})
    assert doc["age"] == 30


def test_build_update_clause_unsupported_operator():
    """Test _build_update_clause with an unsupported operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with an unsupported operator
    result = collection._build_update_clause(
        {"$unsupported": {"field": "value"}}
    )
    # Should fall back to None
    assert result is None


def test_get_val_none_value():
    """Test _get_val with None value in path."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with None value in path
    result = collection._get_val({"a": None}, "a.b")
    assert result is None


def test_get_val_dollar_prefix():
    """Test _get_val with dollar prefix in key."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with dollar prefix in key (should strip the $)
    result = collection._get_val({"a": "value"}, "$a")
    assert result == "value"


def test_load_bytes_data():
    """Test _load with bytes data."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with bytes data
    result = collection._load(1, b'{"test": "value"}')
    assert result == {"_id": 1, "test": "value"}


def test_unsupported_update_operator():
    """Test _internal_update with an unsupported operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test with an unsupported operator
    with pytest.raises(
        MalformedQueryException,
        match="Update operator '\\$unsupported' not supported",
    ):
        collection._internal_update(
            1, {"$unsupported": {"field": "value"}}, {"_id": 1}
        )
