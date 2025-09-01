"""
Additional tests to improve coverage of query_helper.py
"""
import pytest
from neosqlite import Connection, Binary
from neosqlite.collection.query_helper import (
    _convert_bytes_to_binary,
    set_force_fallback,
    get_force_fallback,
    QueryHelper
)


def test_convert_bytes_to_binary_edge_cases():
    """Test edge cases for _convert_bytes_to_binary function."""
    # Test with None
    assert _convert_bytes_to_binary(None) is None
    
    # Test with integer
    assert _convert_bytes_to_binary(42) == 42
    
    # Test with string
    assert _convert_bytes_to_binary("test") == "test"
    
    # Test with empty bytes
    result = _convert_bytes_to_binary(b"")
    assert isinstance(result, Binary)
    assert result == b""
    assert result.subtype == Binary.BINARY_SUBTYPE
    
    # Test with nested structures containing various types
    test_data = {
        "bytes": b"test",
        "binary": Binary(b"binary", Binary.FUNCTION_SUBTYPE),
        "list": [b"item1", Binary(b"item2", Binary.UUID_SUBTYPE), 42, "string"],
        "nested": {
            "inner_bytes": b"inner",
            "inner_binary": Binary(b"inner_binary", Binary.OLD_BINARY_SUBTYPE)
        },
        "none": None,
        "int": 123,
        "float": 3.14
    }
    
    result = _convert_bytes_to_binary(test_data)
    
    # Check bytes conversion
    assert isinstance(result["bytes"], Binary)
    assert result["bytes"].subtype == Binary.BINARY_SUBTYPE
    
    # Check binary preservation
    assert isinstance(result["binary"], Binary)
    assert result["binary"].subtype == Binary.FUNCTION_SUBTYPE
    assert result["binary"] is test_data["binary"]  # Same object
    
    # Check list processing
    assert isinstance(result["list"][0], Binary)
    assert isinstance(result["list"][1], Binary)
    assert result["list"][1].subtype == Binary.UUID_SUBTYPE
    assert result["list"][1] is test_data["list"][1]  # Same object
    assert result["list"][2] == 42  # Int unchanged
    assert result["list"][3] == "string"  # String unchanged
    
    # Check nested structure
    assert isinstance(result["nested"]["inner_bytes"], Binary)
    assert isinstance(result["nested"]["inner_binary"], Binary)
    assert result["nested"]["inner_binary"].subtype == Binary.OLD_BINARY_SUBTYPE
    assert result["nested"]["inner_binary"] is test_data["nested"]["inner_binary"]
    
    # Check unchanged values
    assert result["none"] is None
    assert result["int"] == 123
    assert result["float"] == 3.14


def test_force_fallback_functionality():
    """Test force fallback functionality."""
    # Check default state
    assert get_force_fallback() == False
    
    # Turn on fallback
    set_force_fallback(True)
    assert get_force_fallback() == True
    
    # Turn off fallback
    set_force_fallback(False)
    assert get_force_fallback() == False
    
    # Test with explicit False
    set_force_fallback(False)
    assert get_force_fallback() == False
    
    # Test with explicit True
    set_force_fallback(True)
    assert get_force_fallback() == True
    
    # Reset to default for other tests
    set_force_fallback(False)


def test_query_helper_initialization():
    """Test QueryHelper initialization."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)
        assert helper.collection is collection


def test_can_use_sql_updates_with_binary_values():
    """Test _can_use_sql_updates with binary values."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)
        
        # Test with binary values in update spec
        update_spec = {"$set": {"data": b"binary_data"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return True because raw bytes don't have encode_for_storage attribute
        # The check is for isinstance(val, bytes) AND hasattr(val, "encode_for_storage")
        # Since raw bytes don't have encode_for_storage, this check fails
        assert result == True
        
        # Test with Binary objects in update spec
        update_spec = {"$set": {"data": Binary(b"binary_data")}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return False because Binary objects have encode_for_storage attribute
        assert result == False
        
        # Test with regular values (should return True)
        update_spec = {"$set": {"data": "regular_data"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return True for supported operations
        assert result == True
        
        # Test with unsupported operations (should return False)
        update_spec = {"$rename": {"old": "new"}}
        result = helper._can_use_sql_updates(update_spec, 1)
        # Should return False for unsupported operations
        assert result == False
        
        # Test with upsert (doc_id = 0, should return False)
        update_spec = {"$set": {"data": "regular_data"}}
        result = helper._can_use_sql_updates(update_spec, 0)
        # Should return False for upserts
        assert result == False


def test_build_sql_update_clause_with_binary():
    """Test _build_sql_update_clause with binary data."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)
        
        # Test $set with raw bytes
        clauses, params = helper._build_sql_update_clause("$set", {"field": b"test_data"})
        assert len(clauses) == 1
        assert clauses[0] == "'$.field', json(?)"
        assert len(params) == 1
        # Params should contain JSON serialized binary data
        
        # Test $set with Binary object
        binary_obj = Binary(b"binary_data", Binary.FUNCTION_SUBTYPE)
        clauses, params = helper._build_sql_update_clause("$set", {"field": binary_obj})
        assert len(clauses) == 1
        assert clauses[0] == "'$.field', json(?)"
        assert len(params) == 1
        # Params should contain JSON serialized binary data
        
        # Test $inc with binary (should be handled as regular value)
        clauses, params = helper._build_sql_update_clause("$inc", {"field": b"10"})
        # For $inc, binary should be treated as regular value
        assert len(clauses) == 1
        assert "'$.field'" in clauses[0]
        assert len(params) == 1


def test_internal_insert_with_complex_document():
    """Test _internal_insert with complex document containing various data types."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)
        
        # Create a complex document
        complex_doc = {
            "name": "Test User",
            "age": 30,
            "active": True,
            "scores": [85, 92, 78],
            "address": {
                "street": "123 Main St",
                "city": "Test City"
            },
            "tags": ["tag1", "tag2", "tag3"],
            "binary_data": b"some binary data",
            "binary_object": Binary(b"typed binary", Binary.FUNCTION_SUBTYPE),
            "none_value": None,
            "empty_list": [],
            "empty_dict": {}
        }
        
        # Insert the document
        doc_id = helper._internal_insert(complex_doc)
        
        # Verify the document was inserted
        assert doc_id is not None
        assert "_id" in complex_doc
        assert complex_doc["_id"] == doc_id
        
        # Retrieve and verify the document
        retrieved_doc = collection.find_one({"_id": doc_id})
        assert retrieved_doc is not None
        assert retrieved_doc["name"] == "Test User"
        assert retrieved_doc["age"] == 30
        assert retrieved_doc["active"] == True
        assert retrieved_doc["scores"] == [85, 92, 78]
        assert retrieved_doc["address"]["street"] == "123 Main St"
        assert retrieved_doc["address"]["city"] == "Test City"
        assert retrieved_doc["tags"] == ["tag1", "tag2", "tag3"]
        
        # Check binary data handling
        assert isinstance(retrieved_doc["binary_data"], Binary)
        assert bytes(retrieved_doc["binary_data"]) == b"some binary data"
        assert retrieved_doc["binary_data"].subtype == Binary.BINARY_SUBTYPE
        
        assert isinstance(retrieved_doc["binary_object"], Binary)
        assert bytes(retrieved_doc["binary_object"]) == b"typed binary"
        assert retrieved_doc["binary_object"].subtype == Binary.FUNCTION_SUBTYPE
        
        assert retrieved_doc["none_value"] is None
        assert retrieved_doc["empty_list"] == []
        assert retrieved_doc["empty_dict"] == {}


if __name__ == "__main__":
    pytest.main([__file__])