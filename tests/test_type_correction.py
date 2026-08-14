"""
Tests for the type_correction module in NeoSQLite.

This module contains tests for the functions in the type_correction module,
which handles automatic conversion between integer IDs and ObjectIds in queries.
"""

from neosqlite.collection.type_correction import (
    normalize_id_query_for_db,
    normalize_objectid_for_db_query,
)


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


def test_normalize_objectid_for_db_query_with_float():
    """Test normalize_objectid_for_db_query with float."""
    float_val = 3.14
    result = normalize_objectid_for_db_query(float_val)
    assert result == float_val


# Tests for normalize_id_query_for_db function
def test_normalize_id_query_for_db_basic():
    """Test basic normalization of _id field."""
    query = {"_id": 123}
    result = normalize_id_query_for_db(query)
    assert result == query


def test_normalize_id_query_for_db_objectid():
    """Test ObjectId conversion."""
    from neosqlite.objectid import ObjectId

    oid = ObjectId()
    query = {"_id": oid}
    result = normalize_id_query_for_db(query)
    assert result["_id"] == str(oid)
    assert isinstance(result["_id"], str)


def test_normalize_id_query_for_db_hex_string():
    """Test hex string to ObjectId conversion."""
    hex_str = "507f1f77bcf86cd799439011"
    query = {"_id": hex_str}
    result = normalize_id_query_for_db(query)
    # Should keep the hex string as-is (it's a valid ObjectId)
    assert result["_id"] == hex_str


def test_normalize_id_query_for_db_invalid_hex_string():
    """Test invalid hex string handling."""
    invalid_hex = "invalid_hex_12345678"
    query = {"_id": invalid_hex}
    result = normalize_id_query_for_db(query)
    # Should keep as-is since it's not a valid ObjectId
    assert result["_id"] == invalid_hex


def test_normalize_id_query_for_db_nested():
    """Test nested query normalization."""
    query = {
        "$and": [{"_id": 123}, {"name": {"$regex": "test"}}],
        "nested": {"field": "value", "_id": 456},
    }
    result = normalize_id_query_for_db(query)
    assert result["nested"]["_id"] == 456


def test_normalize_id_query_for_db_list():
    """Test list normalization."""
    query = {"_id": [123, 456, 789]}
    result = normalize_id_query_for_db(query)
    assert result["_id"] == [123, 456, 789]


def test_normalize_id_query_for_db_objectid_in_list():
    """Test ObjectId in list normalization."""
    from neosqlite.objectid import ObjectId

    oid1, oid2 = ObjectId(), ObjectId()
    query = {"_id": [oid1, oid2]}
    result = normalize_id_query_for_db(query)
    assert result["_id"] == [str(oid1), str(oid2)]


def test_normalize_id_query_for_db_empty():
    """Test empty query."""
    query = {}
    result = normalize_id_query_for_db(query)
    assert result == query


def test_normalize_id_query_for_db_non_dict():
    """Test non-dict input."""
    result = normalize_id_query_for_db("not a dict")
    assert result == "not a dict"


def test_normalize_id_query_for_db_mixed_types():
    """Test mixed type preservation."""
    query = {
        "int_field": 42,
        "float_field": 3.14,
        "str_field": "hello",
        "bool_field": True,
        "list_field": [1, 2, 3],
        "none_field": None,
    }
    result = normalize_id_query_for_db(query)
    assert result == query
    assert result["int_field"] == 42
    assert result["float_field"] == 3.14
    assert result["str_field"] == "hello"
    assert result["bool_field"] is True
    assert result["list_field"] == [1, 2, 3]
    assert result["none_field"] is None


def test_normalize_id_query_for_db_or_conditions():
    """Test $or conditions."""
    query = {
        "$or": [{"_id": 123}, {"_id": 456}],
        "$and": [{"age": {"$gt": 18}}, {"status": "active"}],
        "name": {"$in": ["John", "Jane"]},
    }
    result = normalize_id_query_for_db(query)
    assert result["$or"][0]["_id"] == 123
    assert result["$or"][1]["_id"] == 456


def test_normalize_id_query_for_db_regex():
    """Test regex pattern handling."""
    import re

    pattern = re.compile(r"^test")
    query = {"name": pattern}
    result = normalize_id_query_for_db(query)
    assert result["name"] == pattern


def test_normalize_id_query_for_db_in_operator():
    """Test $in operator with various types."""
    query = {"_id": {"$in": [123, 456, 789]}}
    result = normalize_id_query_for_db(query)
    assert result["_id"]["$in"] == [123, 456, 789]


def test_normalize_id_query_for_db_nin_operator():
    """Test $nin operator."""
    query = {"_id": {"$nin": [123, 456]}}
    result = normalize_id_query_for_db(query)
    assert result["_id"]["$nin"] == [123, 456]


def test_normalize_id_query_for_db_gt_lt():
    """Test comparison operators."""
    query = {"age": {"$gt": 18, "$lt": 65}}
    result = normalize_id_query_for_db(query)
    assert result["age"]["$gt"] == 18
    assert result["age"]["$lt"] == 65
