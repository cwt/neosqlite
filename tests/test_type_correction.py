"""
Tests for the type_correction module in NeoSQLite.

This module contains tests for the functions in the type_correction module,
which handles automatic conversion between integer IDs and ObjectIds in queries.
"""

import pytest
from neosqlite.collection.type_correction import normalize_id_query


def test_normalize_id_query_basic_functionality():
    """Test that normalize_id_query returns the query unchanged."""
    query = {"_id": 123}
    result = normalize_id_query(query)
    assert result == query
    assert result is query  # Should return the same object


def test_normalize_id_query_with_dict():
    """Test normalize_id_query with a dictionary query."""
    query = {"_id": 123, "name": "test", "age": 30}
    result = normalize_id_query(query)
    assert result == query


def test_normalize_id_query_empty_dict():
    """Test normalize_id_query with an empty dictionary."""
    query = {}
    result = normalize_id_query(query)
    assert result == {}


def test_normalize_id_query_complex_nested():
    """Test normalize_id_query with nested query structures."""
    query = {
        "$and": [{"_id": 123}, {"name": {"$regex": "test"}}],
        "nested": {"field": "value", "_id": 456},
    }
    result = normalize_id_query(query)
    assert result == query


def test_normalize_id_query_with_objectid():
    """Test normalize_id_query with ObjectId-like structures."""
    # Import ObjectId to create a test object
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {"_id": oid, "name": "test"}
    result = normalize_id_query(query)
    assert result == query
    assert result["_id"] == oid


def test_normalize_id_query_preserves_types():
    """Test that normalize_id_query preserves all data types."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {
        "_id": oid,
        "int_field": 42,
        "float_field": 3.14,
        "str_field": "hello",
        "bool_field": True,
        "list_field": [1, 2, 3],
        "none_field": None,
    }
    result = normalize_id_query(query)
    assert result == query
    assert result["_id"] == oid
    assert result["int_field"] == 42
    assert result["float_field"] == 3.14
    assert result["str_field"] == "hello"
    assert result["bool_field"] is True
    assert result["list_field"] == [1, 2, 3]
    assert result["none_field"] is None


def test_normalize_id_query_special_operators():
    """Test normalize_id_query with MongoDB-style operators."""
    query = {
        "$or": [{"_id": 123}, {"_id": 456}],
        "$and": [{"age": {"$gt": 18}}, {"status": "active"}],
        "name": {"$in": ["John", "Jane"]},
    }
    result = normalize_id_query(query)
    assert result == query


def test_normalize_id_query_immutable_like():
    """Test that normalize_id_query doesn't modify the original query."""
    original_query = {"_id": 123, "name": "test"}
    original_copy = original_query.copy()  # Create a copy to compare

    result = normalize_id_query(original_query)

    # The function should return the same object reference
    assert result is original_query
    # Original query should remain unchanged
    assert original_query == original_copy


if __name__ == "__main__":
    pytest.main([__file__])
