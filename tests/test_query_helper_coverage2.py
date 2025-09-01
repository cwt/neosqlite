"""
Additional tests to improve coverage of query_helper.py - Part 2
"""

import pytest
from neosqlite import Connection, Binary
from neosqlite.collection.query_helper import QueryHelper
from neosqlite.exceptions import MalformedDocument, MalformedQueryException


def test_internal_insert_edge_cases():
    """Test _internal_insert with edge cases."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with non-dict document (should raise MalformedDocument)
        with pytest.raises(MalformedDocument):
            helper._internal_insert("not a dict")

        # Test with non-dict document (should raise MalformedDocument)
        with pytest.raises(MalformedDocument):
            helper._internal_insert(123)

        # Test with list (should raise MalformedDocument)
        with pytest.raises(MalformedDocument):
            helper._internal_insert([1, 2, 3])


def test_perform_python_update_operations():
    """Test _perform_python_update with various operations."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Insert a test document
        doc = {"name": "Test", "age": 25, "scores": [80, 85], "active": True}
        doc_id = helper._internal_insert(doc)

        # Test $set operation
        update_spec = {"$set": {"age": 30, "city": "New York"}}
        result = helper._perform_python_update(doc_id, update_spec, doc)
        assert result["age"] == 30
        assert result["city"] == "New York"

        # Test $unset operation
        update_spec = {"$unset": {"city": ""}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert "city" not in result

        # Test $inc operation
        update_spec = {"$inc": {"age": 5}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 35

        # Test $push operation
        update_spec = {"$push": {"scores": 90}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == [80, 85, 90]

        # Test $pull operation
        update_spec = {"$pull": {"scores": 85}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == [80, 90]

        # Test $pop operation (pop last element)
        update_spec = {"$pop": {"scores": 1}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == [80]

        # Test $pop operation (pop first element)
        update_spec = {"$pop": {"scores": -1}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["scores"] == []

        # Test $rename operation
        update_spec = {"$rename": {"name": "full_name"}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert "name" not in result
        assert result["full_name"] == "Test"

        # Test $mul operation
        update_spec = {"$mul": {"age": 2}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 70

        # Test $min operation
        update_spec = {"$min": {"age": 50}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 50

        # Test $max operation
        update_spec = {"$max": {"age": 60}}
        result = helper._perform_python_update(doc_id, update_spec, result)
        assert result["age"] == 60

        # Test unsupported operation (should raise MalformedQueryException)
        update_spec = {"$unsupported": {"field": "value"}}
        with pytest.raises(MalformedQueryException):
            helper._perform_python_update(doc_id, update_spec, result)


def test_internal_replace_and_delete():
    """Test _internal_replace and _internal_delete operations."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Insert a test document
        doc = {"name": "Test", "age": 25}
        doc_id = helper._internal_insert(doc)

        # Verify document exists
        retrieved = collection.find_one({"_id": doc_id})
        assert retrieved is not None
        assert retrieved["name"] == "Test"
        assert retrieved["age"] == 25

        # Test _internal_replace
        new_doc = {"name": "Updated", "age": 30, "city": "New York"}
        helper._internal_replace(doc_id, new_doc)

        # Verify replacement
        retrieved = collection.find_one({"_id": doc_id})
        assert retrieved is not None
        assert retrieved["name"] == "Updated"
        assert retrieved["age"] == 30
        assert retrieved["city"] == "New York"

        # Test _internal_delete
        helper._internal_delete(doc_id)

        # Verify deletion
        retrieved = collection.find_one({"_id": doc_id})
        assert retrieved is None


def test_build_update_clause_edge_cases():
    """Test _build_update_clause with edge cases."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with $rename (should return None to force Python fallback)
        update = {"$rename": {"old_field": "new_field"}}
        result = helper._build_update_clause(update)
        assert result is None

        # Test with unsupported operator (should return None)
        update = {"$unsupported": {"field": "value"}}
        result = helper._build_update_clause(update)
        assert result is None

        # Test with empty $unset (should return None)
        update = {"$unset": {}}
        result = helper._build_update_clause(update)
        assert result is None


def test_is_text_search_query():
    """Test _is_text_search_query function."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with text search query
        query = {"$text": {"$search": "test"}}
        result = helper._is_text_search_query(query)
        assert result == True

        # Test without text search query
        query = {"name": "test"}
        result = helper._is_text_search_query(query)
        assert result == False

        # Test with empty query
        query = {}
        result = helper._is_text_search_query(query)
        assert result == False


if __name__ == "__main__":
    pytest.main([__file__])
