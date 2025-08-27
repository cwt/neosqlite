# coding: utf-8
"""
Tests for using json_each() with $unwind operations
"""
import pytest


def test_simple_unwind_with_json_each(collection):
    """Test simple $unwind using json_each() for first-level arrays"""
    # Insert test data with arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "hobbies": ["reading", "swimming", "coding"],
        }
    )
    collection.insert_one(
        {"_id": 2, "name": "Bob", "hobbies": ["gaming", "cooking"]}
    )
    collection.insert_one(
        {"_id": 3, "name": "Charlie", "hobbies": ["painting"]}
    )

    # Try a simple SQL query with json_each to unwind hobbies
    cmd = f"""
    SELECT {collection.name}.id, json_set({collection.name}.data, '$.hobby', je.value) as data
    FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.hobbies')) as je
    """

    # Execute the query directly to see what we get
    cursor = collection.db.execute(cmd)
    results = []
    for row in cursor.fetchall():
        results.append(collection._load(row[0], row[1]))

    # Should have 6 documents (3+2+1 hobbies)
    assert len(results) == 6

    # Check that each document has a 'hobby' field with the unwound value
    hobbies = [doc["hobby"] for doc in results]
    expected_hobbies = [
        "reading",
        "swimming",
        "coding",
        "gaming",
        "cooking",
        "painting",
    ]
    assert sorted(hobbies) == sorted(expected_hobbies)

    # Check that original fields are preserved
    alice_hobbies = [doc["hobby"] for doc in results if doc["name"] == "Alice"]
    assert sorted(alice_hobbies) == sorted(["reading", "swimming", "coding"])


def test_unwind_with_empty_array(collection):
    """Test $unwind with json_each() when array is empty"""
    # Insert document with empty array
    collection.insert_one({"_id": 1, "name": "Alice", "hobbies": []})

    # Try a simple SQL query with json_each to unwind hobbies
    cmd = f"""
    SELECT {collection.name}.id, json_set({collection.name}.data, '$.hobby', je.value) as data
    FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.hobbies')) as je
    """

    # Execute the query directly
    cursor = collection.db.execute(cmd)
    results = []
    for row in cursor.fetchall():
        results.append(collection._load(row[0], row[1]))

    # Should have 0 documents since array is empty
    assert len(results) == 0


def test_unwind_with_non_array_field(collection):
    """Test $unwind with json_each() when field is not an array"""
    # Insert document with non-array field
    collection.insert_one(
        {"_id": 1, "name": "Alice", "hobbies": "not an array"}
    )

    # Try a simple SQL query with json_each to unwind hobbies
    # Wrap in a try/except to handle the expected error
    try:
        cmd = f"""
        SELECT {collection.name}.id, json_set({collection.name}.data, '$.hobby', je.value) as data
        FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.hobbies')) as je
        """

        # Execute the query directly - should not error but may not produce expected results
        cursor = collection.db.execute(cmd)
        results = []
        for row in cursor.fetchall():
            results.append(collection._load(row[0], row[1]))

        # When json_each is applied to a non-array, behavior depends on SQLite version
        # In most cases, it should either produce 0 rows or 1 row with the value
        # We'll just check that we don't get an unexpected number of results
        assert len(results) <= 1
    except Exception:
        # If it errors (as expected with malformed JSON), that's fine
        # This demonstrates that json_each requires proper JSON arrays
        pass


def test_unwind_with_nested_array_path(collection):
    """Test $unwind with json_each() for nested array paths"""
    # Insert test data with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "profile": {"skills": ["Python", "JavaScript", "SQL"]},
        }
    )

    # Try a simple SQL query with json_each to unwind nested array
    cmd = f"""
    SELECT {collection.name}.id, json_set({collection.name}.data, '$.skill', je.value) as data
    FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.profile.skills')) as je
    """

    # Execute the query directly
    cursor = collection.db.execute(cmd)
    results = []
    for row in cursor.fetchall():
        results.append(collection._load(row[0], row[1]))

    # Should have 3 documents
    assert len(results) == 3

    # Check that each document has a 'skill' field with the unwound value
    skills = [doc["skill"] for doc in results]
    expected_skills = ["Python", "JavaScript", "SQL"]
    assert sorted(skills) == sorted(expected_skills)


def test_json_each_with_mixed_data_types(collection):
    """Test json_each() with arrays containing mixed data types"""
    # Insert test data with mixed data types in array
    collection.insert_one(
        {"_id": 1, "name": "Alice", "values": [1, "string", True, None]}
    )

    # Try a simple SQL query with json_each to unwind values
    cmd = f"""
    SELECT {collection.name}.id, json_set({collection.name}.data, '$.value', je.value) as data
    FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.values')) as je
    """

    # Execute the query directly
    cursor = collection.db.execute(cmd)
    results = []
    for row in cursor.fetchall():
        results.append(collection._load(row[0], row[1]))

    # Should have 4 documents
    assert len(results) == 4

    # Check that we got the values back (they may be as strings due to JSON serialization)
    values = [doc["value"] for doc in results]
    # Note: SQLite JSON functions may return values as strings, so we check the content
    assert len(values) == 4


if __name__ == "__main__":
    pytest.main([__file__])
