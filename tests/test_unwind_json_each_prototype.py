# coding: utf-8
"""
Prototype for integrating json_each() with $unwind operations in NeoSQLite
"""
import neosqlite
import pytest


def test_prototype_unwind_with_json_each_optimization(collection):
    """Prototype test showing how json_each() could be integrated with $unwind"""
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

    # Current Python-based implementation (what exists now)
    pipeline_python = [{"$unwind": "$hobbies"}]
    result_python = collection.aggregate(pipeline_python)

    # Prototype SQL-based implementation using json_each()
    # This would be integrated into the _build_aggregation_query method
    unwind_field = "hobbies"
    cmd = f"""
    SELECT {collection.name}.id,
           json_set({collection.name}.data, '$.{unwind_field}', je.value) as data
    FROM {collection.name},
         json_each(json_extract({collection.name}.data, '$.{unwind_field}')) as je
    """

    # Execute the SQL-based query
    cursor = collection.db.execute(cmd)
    result_sql = []
    for row in cursor.fetchall():
        result_sql.append(collection._load(row[0], row[1]))

    # Both approaches should produce the same results
    assert len(result_python) == len(result_sql) == 5

    # Check that we get the same unwound values
    python_hobbies = sorted([doc["hobbies"] for doc in result_python])
    sql_hobbies = sorted([doc["hobbies"] for doc in result_sql])
    assert python_hobbies == sql_hobbies

    # Check that other fields are preserved
    python_names = sorted([doc["name"] for doc in result_python])
    sql_names = sorted([doc["name"] for doc in result_sql])
    assert python_names == sql_names


def test_prototype_nested_unwind_with_json_each(collection):
    """Test prototype for nested field unwind with json_each()"""
    # Insert test data with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "profile": {"skills": ["Python", "JavaScript", "SQL"]},
        }
    )

    # Current Python-based implementation
    pipeline_python = [{"$unwind": "$profile.skills"}]
    result_python = collection.aggregate(pipeline_python)

    # Show what the Python implementation produces
    print("Python result structure:")
    for doc in result_python:
        print(f"  {doc}")

    # Prototype SQL-based implementation using json_each()
    # To match the Python behavior exactly, we need to:
    # 1. Extract the array values with json_each
    # 2. Add a new top-level field with the dot notation as key using json_set
    # The key is to use quoted keys in JSON path to create a top-level field with dots in the name
    unwind_field = "profile.skills"
    sql_field_path = unwind_field  # Keep dots for JSON path in json_extract

    # Use quoted key to create a top-level field with dot notation
    cmd = f"""
    SELECT {collection.name}.id,
           json_set({collection.name}.data, '$."{unwind_field}"', je.value) as data
    FROM {collection.name},
         json_each(json_extract({collection.name}.data, '$.{sql_field_path}')) as je
    """

    # Execute the SQL-based query
    cursor = collection.db.execute(cmd)
    result_sql = []
    for row in cursor.fetchall():
        result_sql.append(collection._load(row[0], row[1]))

    # Show what the SQL implementation produces
    print("SQL result structure:")
    for doc in result_sql:
        print(f"  {doc}")

    # Both approaches should produce the same number of results
    assert len(result_python) == len(result_sql) == 3

    # Check that both approaches add the new field with dot notation
    for doc in result_python:
        assert "profile.skills" in doc  # New field with dot notation

    for doc in result_sql:
        assert "profile.skills" in doc  # New field with dot notation

    # Check that we get the same unwound values
    python_unwound = sorted([doc["profile.skills"] for doc in result_python])
    sql_unwound = sorted([doc["profile.skills"] for doc in result_sql])
    assert python_unwound == sql_unwound

    # Check that original nested structure is preserved in both implementations
    for doc in result_python:
        assert "profile" in doc
        assert "skills" in doc["profile"]
        # In Python implementation, original array is preserved
        assert doc["profile"]["skills"] == ["Python", "JavaScript", "SQL"]

    # The SQL implementation should now also preserve the original nested structure
    for doc in result_sql:
        assert "profile" in doc
        assert "skills" in doc["profile"]
        # Original array should be preserved
        assert doc["profile"]["skills"] == ["Python", "JavaScript", "SQL"]


def test_prototype_unwind_performance_benefit(collection):
    """Demonstrate potential performance benefit of json_each() approach"""
    # Insert many documents with arrays
    docs = []
    for i in range(100):
        docs.append(
            {
                "id": i,
                "name": f"User{i}",
                "tags": [f"tag{j}" for j in range(5)],  # 5 tags per user
            }
        )

    collection.insert_many(docs)

    # Time the current Python implementation
    import time

    start = time.time()
    pipeline_python = [{"$unwind": "$tags"}]
    result_python = collection.aggregate(pipeline_python)
    python_time = time.time() - start

    # Time the SQL implementation with json_each()
    start = time.time()
    unwind_field = "tags"
    cmd = f"""
    SELECT {collection.name}.id,
           json_set({collection.name}.data, '$.{unwind_field}', je.value) as data
    FROM {collection.name},
         json_each(json_extract({collection.name}.data, '$.{unwind_field}')) as je
    """
    cursor = collection.db.execute(cmd)
    result_sql = []
    for row in cursor.fetchall():
        result_sql.append(collection._load(row[0], row[1]))
    sql_time = time.time() - start

    # Both should produce 500 results (100 docs * 5 tags each)
    assert len(result_python) == len(result_sql) == 500

    # The SQL approach should generally be faster (though this is a simple test)
    print(f"Python implementation time: {python_time:.4f}s")
    print(f"SQL implementation time: {sql_time:.4f}s")


if __name__ == "__main__":
    pytest.main([__file__])
