"""
Tests for the $type query operator.
"""


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
    assert result[0]["flag"]


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


def test_mod_operator_with_mixed_types(collection):
    """Test $mod operator excludes string values that look like numbers.

    This is a regression test for a bug where SQLite's loose type coercion
    would cause $mod to match string values like "25" because SQLite
    silently converts "25" to 25 for numeric operations.
    """
    # Insert test documents with different types for age
    collection.insert_one({"name": "Alice", "age": 30})  # int 30 % 5 = 0
    collection.insert_one(
        {"name": "Bob", "age": "25"}
    )  # string "25" - should NOT match
    collection.insert_one(
        {"name": "Charlie", "age": 35.0}
    )  # float 35.0 % 5 = 0
    collection.insert_one({"name": "David", "age": 25})  # int 25 % 5 = 0

    # Query: age % 5 == 0
    # Should match Alice (30), Charlie (35.0), David (25) - but NOT Bob ("25")
    result = list(collection.find({"age": {"$mod": [5, 0]}}))

    # Verify correct count and values
    assert len(result) == 3
    names = [doc["name"] for doc in result]
    assert "Alice" in names
    assert "Bob" not in names  # String "25" should NOT match
    assert "Charlie" in names
    assert "David" in names


def test_mod_operator_with_string_value(collection):
    """Test $mod operator with string value returns no matches."""
    collection.insert_one({"value": "10"})
    collection.insert_one({"value": 10})

    # String "10" should not match $mod: [3, 1] because 10 % 3 = 1
    # But this is a string, not a number, so it shouldn't match at all
    result = list(collection.find({"value": {"$mod": [3, 1]}}))

    # Only the numeric 10 should potentially match (10 % 3 = 1)
    # The string "10" should not be considered
    assert len(result) == 1
    assert result[0]["value"] == 10
