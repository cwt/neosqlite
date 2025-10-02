"""
Tests for the type_correction module in NeoSQLite.

This module contains tests for the functions in the type_correction module,
which handles automatic conversion between integer IDs and ObjectIds in queries.
"""

import pytest
from neosqlite.collection.type_correction import (
    normalize_id_query,
    normalize_objectid_for_db_query,
    normalize_id_query_for_db,
)


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


# Tests for normalize_objectid_for_db_query function
def test_normalize_objectid_for_db_query_with_objectid():
    """Test normalize_objectid_for_db_query with ObjectId."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    result = normalize_objectid_for_db_query(oid)
    assert result == str(oid)
    assert isinstance(result, str)


def test_normalize_objectid_for_db_query_with_valid_hex_string():
    """Test normalize_objectid_for_db_query with valid ObjectId hex string."""
    valid_hex = "507f1f77bcf86cd799439011"  # 24-character hex string
    result = normalize_objectid_for_db_query(valid_hex)
    assert result == valid_hex
    assert isinstance(result, str)


def test_normalize_objectid_for_db_query_with_invalid_hex_string():
    """Test normalize_objectid_for_db_query with invalid hex string."""
    invalid_hex = "invalid_hex_string_123456"  # Invalid ObjectId hex
    result = normalize_objectid_for_db_query(invalid_hex)
    assert result == invalid_hex
    assert isinstance(result, str)


def test_normalize_objectid_for_db_query_with_short_string():
    """Test normalize_objectid_for_db_query with short string (not ObjectId)."""
    short_str = "12345"
    result = normalize_objectid_for_db_query(short_str)
    assert result == short_str


def test_normalize_objectid_for_db_query_with_int():
    """Test normalize_objectid_for_db_query with integer."""
    int_val = 123
    result = normalize_objectid_for_db_query(int_val)
    assert result == int_val


def test_normalize_objectid_for_db_query_with_none():
    """Test normalize_objectid_for_db_query with None."""
    result = normalize_objectid_for_db_query(None)
    assert result is None


def test_normalize_objectid_for_db_query_with_other_types():
    """Test normalize_objectid_for_db_query with other data types."""
    # Test with list
    result = normalize_objectid_for_db_query([1, 2, 3])
    assert result == [1, 2, 3]

    # Test with dict
    result = normalize_objectid_for_db_query({"key": "value"})
    assert result == {"key": "value"}


# Tests for normalize_id_query_for_db function
def test_normalize_id_query_for_db_with_none():
    """Test normalize_id_query_for_db with non-dict value."""
    result = normalize_id_query_for_db("not a dict")
    assert result == "not a dict"


def test_normalize_id_query_for_db_empty_dict():
    """Test normalize_id_query_for_db with empty dictionary."""
    query = {}
    result = normalize_id_query_for_db(query)
    assert result == {}


def test_normalize_id_query_for_db_id_with_objectid():
    """Test normalize_id_query_for_db with 'id' field containing ObjectId."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {"id": oid}
    result = normalize_id_query_for_db(query)
    # Should convert to _id with string representation
    assert result == {"_id": str(oid)}
    assert "_id" in result
    assert "id" not in result


def test_normalize_id_query_for_db_id_with_valid_hex_string():
    """Test normalize_id_query_for_db with 'id' field containing valid ObjectId hex string."""
    valid_hex = "507f1f77bcf86cd799439011"  # Valid 24-char hex
    query = {"id": valid_hex}
    result = normalize_id_query_for_db(query)
    # Should convert to _id with the same hex string
    assert result == {"_id": valid_hex}


def test_normalize_id_query_for_db_id_with_invalid_hex_not_int():
    """Test normalize_id_query_for_db with 'id' field containing invalid hex (not integer)."""
    invalid_hex = (
        "invalid_hex_st1234567890"  # Invalid ObjectId hex and not integer
    )
    query = {"id": invalid_hex}
    result = normalize_id_query_for_db(query)
    # Should keep as is since it's not valid ObjectId and not an integer string
    assert result == {"id": invalid_hex}


def test_normalize_id_query_for_db_id_with_integer_string():
    """Test normalize_id_query_for_db with 'id' field containing integer string."""
    query = {"id": "123"}
    result = normalize_id_query_for_db(query)
    # Should convert string to integer
    assert result == {"id": 123}


def test_normalize_id_query_for_db_id_with_non_string_non_objectid():
    """Test normalize_id_query_for_db with 'id' field containing non-string, non-ObjectId values."""
    query = {"id": 456.78}
    result = normalize_id_query_for_db(query)
    # Should keep as is
    assert result == {"id": 456.78}


def test_normalize_id_query_for_db_id_with_short_hex_string():
    """Test normalize_id_query_for_db with 'id' field containing short string that can be parsed as integer."""
    short_str = (
        "12345"  # Not 24 characters, not ObjectId, but can be parsed as int
    )
    query = {"id": short_str}
    result = normalize_id_query_for_db(query)
    # Should convert to integer since it's not 24 chars and can be parsed as int
    assert result == {"id": 12345}


def test_normalize_id_query_for_db_id_with_short_hex_as_integer():
    """Test normalize_id_query_for_db with 'id' field containing short hex that is integer."""
    query = {"id": "123abc"}  # Not valid integer string
    result = normalize_id_query_for_db(query)
    # Should remain unchanged since it's not an ObjectId and not a valid integer string
    assert result == {"id": "123abc"}


def test_normalize_id_query_for_db__id_with_objectid():
    """Test normalize_id_query_for_db with '_id' field containing ObjectId."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {"_id": oid}
    result = normalize_id_query_for_db(query)
    # Should convert ObjectId to string representation
    assert result == {"_id": str(oid)}


def test_normalize_id_query_for_db__id_with_integer_string():
    """Test normalize_id_query_for_db with '_id' field containing integer string."""
    query = {"_id": "456"}
    result = normalize_id_query_for_db(query)
    # Should convert string to integer
    assert result == {"_id": 456}


def test_normalize_id_query_for_db__id_with_valid_hex_string():
    """Test normalize_id_query_for_db with '_id' field containing valid ObjectId hex."""
    valid_hex = "507f1f77bcf86cd799439011"
    query = {"_id": valid_hex}
    result = normalize_id_query_for_db(query)
    # Should keep as is since it's a valid ObjectId hex
    assert result == {"_id": valid_hex}


def test_normalize_id_query_for_db__id_with_invalid_hex():
    """Test normalize_id_query_for_db with '_id' field containing invalid hex."""
    invalid_hex = "invalid_hex_st1234567890"
    query = {"_id": invalid_hex}
    result = normalize_id_query_for_db(query)
    # Should keep as is since it's not a valid ObjectId hex
    assert result == {"_id": invalid_hex}


def test_normalize_id_query_for_db__id_with_non_string_non_objectid():
    """Test normalize_id_query_for_db with '_id' field containing non-string, non-ObjectId values."""
    query = {"_id": 789.12}
    result = normalize_id_query_for_db(query)
    # Should keep as is
    assert result == {"_id": 789.12}


def test_normalize_id_query_for_db_non_special_keys():
    """Test normalize_id_query_for_db with non-special keys."""
    query = {"name": "test", "age": "30"}
    result = normalize_id_query_for_db(query)
    # Should keep as is
    assert result == {"name": "test", "age": "30"}


def test_normalize_id_query_for_db_recursive_nested():
    """Test normalize_id_query_for_db with nested dictionaries."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {"id": "123", "nested": {"_id": oid, "field": "value"}}
    result = normalize_id_query_for_db(query)
    # Should recursively process nested dict
    expected = {"id": 123, "nested": {"_id": str(oid), "field": "value"}}
    assert result == expected


def test_normalize_id_query_for_db_list_with_dicts():
    """Test normalize_id_query_for_db with lists containing dictionaries."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {"tags": ["a", "b", "c"], "$or": [{"id": "123"}, {"_id": oid}]}
    result = normalize_id_query_for_db(query)
    # Should process list items that are dicts
    expected = {
        "tags": ["a", "b", "c"],
        "$or": [{"id": 123}, {"_id": str(oid)}],
    }
    assert result == expected


def test_normalize_id_query_for_db_list_with_non_dicts():
    """Test normalize_id_query_for_db with lists containing non-dictionary values."""
    query = {"tags": ["a", "b", "c"], "values": [1, 2, 3]}
    result = normalize_id_query_for_db(query)
    # Should keep list items as is when they're not dictionaries
    assert result == {"tags": ["a", "b", "c"], "values": [1, 2, 3]}


def test_normalize_id_query_for_db_complex_mixed():
    """Test normalize_id_query_for_db with complex mixed query."""
    from neosqlite.objectid import ObjectId

    oid1 = ObjectId()
    oid2 = ObjectId()
    query = {
        "id": "42",
        "_id": oid1,
        "name": "test",
        "$and": [
            {"id": oid2, "status": "active"},
            {"_id": "a1b2c3d4e5f6789012345678"},  # Invalid hex for ObjectId
        ],
        "nested": {
            "id": "999",
            "_id": "507f1f77bcf86cd799439011",  # Valid hex for ObjectId
        },
    }
    result = normalize_id_query_for_db(query)

    expected = {
        "id": 42,
        "_id": str(oid1),
        "name": "test",
        "$and": [
            {"_id": str(oid2), "status": "active"},
            {"_id": "a1b2c3d4e5f6789012345678"},  # Keep invalid hex as string
        ],
        "nested": {
            "id": 999,
            "_id": "507f1f77bcf86cd799439011",  # Valid hex kept as is
        },
    }
    assert result == expected


def test_normalize_id_query_for_db_with_valid_objectid_hex_in_id_field():
    """Test normalize_id_query_for_db with 'id' field containing valid 24-char hex string."""
    valid_hex = "507f1f77bcf86cd799439011"
    query = {"id": valid_hex}
    result = normalize_id_query_for_db(query)
    assert result == {
        "_id": valid_hex
    }  # Should convert 'id' to '_id' when hex is valid ObjectId


def test_normalize_id_query_for_db_with_invalid_objectid_hex_try_parse_int():
    """Test normalize_id_query_for_db with 'id' field containing invalid hex that might be int."""
    # Test with a 24-char string that's not a valid ObjectId hex but might be parsed as int
    # This is actually not possible since ObjectId hex only contains [0-9a-fA-F] chars
    # Let's test the case where it's not 24 chars but could be an integer
    query = {
        "id": "123456789012345678901234"
    }  # 24 chars but not valid hex (contains invalid chars)
    # Actually ObjectId validation will fail for this, so let's create a different scenario
    # We'll test the scenario where it's a string that could be converted to int
    query = {"id": "123456"}
    result = normalize_id_query_for_db(query)
    assert result == {"id": 123456}  # String converted to int


def test_normalize_id_query_for_db_id_with_24char_invalid_hex_that_fails_int_parsing():
    """Test normalize_id_query_for_db with 'id' field containing 24-char invalid hex that also can't be parsed as int."""
    # This tests the exception handling where a 24-char string fails ObjectId validation
    # and also fails integer parsing
    invalid_hex = (
        "zzzzzzzzzzzzzzzzzzzzzzzz"  # 24 chars but not valid hex, not int
    )
    query = {"id": invalid_hex}
    result = normalize_id_query_for_db(query)
    assert result == {"id": invalid_hex}  # Should remain unchanged


def test_normalize_id_query_for_db__id_with_24char_invalid_hex():
    """Test normalize_id_query_for_db with '_id' field containing 24-char invalid hex."""
    # Similar to above but for _id field - tests the branch where 24-char string
    # fails ObjectId validation but is not integer string
    invalid_hex = (
        "invalidhexzzzzzzzzzzzzzz"  # 24 chars but not valid ObjectId hex
    )
    query = {"_id": invalid_hex}
    result = normalize_id_query_for_db(query)
    assert result == {
        "_id": invalid_hex
    }  # Should remain unchanged after validation fails


def test_normalize_id_query_for_db__id_with_invalid_int_string():
    """Test normalize_id_query_for_db with '_id' field containing invalid integer string."""
    # Test the case where an '_id' field has a string that can't be parsed as an integer
    invalid_int_str = "not_a_number"
    query = {"_id": invalid_int_str}
    result = normalize_id_query_for_db(query)
    assert result == {"_id": invalid_int_str}  # Should remain unchanged


if __name__ == "__main__":
    pytest.main([__file__])
