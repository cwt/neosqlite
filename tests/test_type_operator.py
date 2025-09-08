"""
Tests for the $type query operator.
"""

import pytest
import neosqlite


def test_type_operator_with_string_type(collection):
    """Test $type operator with string type."""
    # Insert test documents
    collection.insert_one({"name": "Alice", "age": 30, "active": True})
    collection.insert_one({"name": 123, "age": "thirty", "active": "yes"})

    # Query for documents where name is a string
    result = list(collection.find({"name": {"$type": 2}}))  # 2 = string

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_type_operator_with_number_type(collection):
    """Test $type operator with number type."""
    # Insert test documents
    collection.insert_one({"value": 42})
    collection.insert_one({"value": "42"})

    # Query for documents where value is a number (int)
    result = list(collection.find({"value": {"$type": 16}}))  # 16 = int

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["value"] == 42


def test_type_operator_with_boolean_type(collection):
    """Test $type operator with boolean type."""
    # Insert test documents
    collection.insert_one({"flag": True})
    collection.insert_one({"flag": "true"})

    # Query for documents where flag is a boolean
    result = list(collection.find({"flag": {"$type": 8}}))  # 8 = boolean

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["flag"] == True


def test_type_operator_with_null_type(collection):
    """Test $type operator with null type."""
    # Insert test documents
    collection.insert_one({"value": None})
    collection.insert_one({"value": "not null"})

    # Query for documents where value is null
    result = list(collection.find({"value": {"$type": 10}}))  # 10 = null

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["value"] is None


def test_type_operator_with_array_type(collection):
    """Test $type operator with array type."""
    # Insert test documents
    collection.insert_one({"items": [1, 2, 3]})
    collection.insert_one({"items": "not an array"})

    # Query for documents where items is an array
    result = list(collection.find({"items": {"$type": 4}}))  # 4 = array

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["items"] == [1, 2, 3]


def test_type_operator_with_object_type(collection):
    """Test $type operator with object type."""
    # Insert test documents
    collection.insert_one({"data": {"key": "value"}})
    collection.insert_one({"data": "not an object"})

    # Query for documents where data is an object
    result = list(collection.find({"data": {"$type": 3}}))  # 3 = object

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["data"] == {"key": "value"}


def test_type_operator_with_direct_type(collection):
    """Test $type operator with direct type objects."""
    # Insert test documents
    collection.insert_one({"name": "Alice"})
    collection.insert_one({"name": 123})

    # Query for documents where name is a string using direct type
    result = list(collection.find({"name": {"$type": str}}))

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_type_operator_with_nonexistent_field(collection):
    """Test $type operator with nonexistent field."""
    # Insert test document
    collection.insert_one({"name": "Alice"})

    # Query for documents where nonexistent field is a string
    result = list(collection.find({"nonexistent": {"$type": 2}}))  # 2 = string

    # Should match no documents
    assert len(result) == 0


def test_type_operator_with_nested_field(collection):
    """Test $type operator with nested field."""
    # Insert test documents
    collection.insert_one({"profile": {"age": 30}})
    collection.insert_one({"profile": {"age": "thirty"}})

    # Query for documents where profile.age is a number
    result = list(collection.find({"profile.age": {"$type": 16}}))  # 16 = int

    # Should match only the first document
    assert len(result) == 1
    assert result[0]["profile"]["age"] == 30
