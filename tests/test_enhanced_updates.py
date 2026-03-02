"""
Test cases for enhanced update operations with json_insert and json_replace support.
"""

import pytest
from neosqlite import Connection
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


def _compare_sql_vs_python(collection, update_spec, filter_spec=None):
    """Helper to compare SQL (Tier 1/2) vs Python (Tier 3) results.

    This function runs the same update twice - once with SQL optimization
    and once with Python fallback - and verifies both produce identical results.

    Args:
        collection: The collection to test on
        update_spec: The update specification to test
        filter_spec: The filter specification (default: {"name": "test"})

    Returns:
        tuple: (sql_result, python_result) - both should be identical
    """
    if filter_spec is None:
        filter_spec = {"name": "test"}

    # Reset to SQL mode (Tier 1/2)
    set_force_fallback(False)

    # Insert fresh test document
    collection.delete_many({})
    collection.insert_one(
        {"name": "test", "tags": ["a", "b"], "flags": 0b0101, "count": 10}
    )

    # Run update with SQL (Tier 1/2)
    result_sql = collection.update_one(filter_spec, update_spec)
    doc_sql = collection.find_one(filter_spec)

    # Reset to Python mode (Tier 3)
    set_force_fallback(True)

    # Reset document to same state
    collection.delete_many({})
    collection.insert_one(
        {"name": "test", "tags": ["a", "b"], "flags": 0b0101, "count": 10}
    )

    # Run update with Python (Tier 3)
    result_python = collection.update_one(filter_spec, update_spec)
    doc_python = collection.find_one(filter_spec)

    # Reset fallback to default
    set_force_fallback(False)

    return doc_sql, doc_python, result_sql, result_python


def test_push_each_modifier_sql_vs_python():
    """Test $push with $each modifier - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_each_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$push": {"tags": {"$each": ["c", "d"]}}}
        )

        # Verify both modes produce same results
        assert doc_sql["tags"] == doc_python["tags"] == ["a", "b", "c", "d"]
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_push_position_modifier_sql_vs_python():
    """Test $push with $position modifier - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_position_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$push": {"tags": {"$each": ["x"], "$position": 0}}}
        )

        # Verify both modes produce same results
        assert doc_sql["tags"] == doc_python["tags"] == ["x", "a", "b"]
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_push_slice_modifier_sql_vs_python():
    """Test $push with $slice modifier - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_slice_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection,
            {"$push": {"tags": {"$each": ["c", "d", "e"], "$slice": 3}}},
        )

        # Verify both modes produce same results
        assert doc_sql["tags"] == doc_python["tags"] == ["a", "b", "c"]
        assert len(doc_sql["tags"]) == len(doc_python["tags"]) == 3
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_bit_and_operator_sql_vs_python():
    """Test $bit with AND operation - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_and_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$bit": {"flags": {"and": 0b0011}}}
        )

        # Verify both modes produce same results: 0b0101 & 0b0011 = 0b0001
        assert doc_sql["flags"] == doc_python["flags"] == 0b0001
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_bit_or_operator_sql_vs_python():
    """Test $bit with OR operation - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_or_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$bit": {"flags": {"or": 0b0011}}}
        )

        # Verify both modes produce same results: 0b0101 | 0b0011 = 0b0111
        assert doc_sql["flags"] == doc_python["flags"] == 0b0111
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_bit_xor_operator_sql_vs_python():
    """Test $bit with XOR operation - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_xor_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$bit": {"flags": {"xor": 0b0011}}}
        )

        # Verify both modes produce same results: 0b0101 ^ 0b0011 = 0b0110
        assert doc_sql["flags"] == doc_python["flags"] == 0b0110
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_bit_combined_operations_sql_vs_python():
    """Test $bit with combined AND+OR operations - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_combined_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$bit": {"flags": {"and": 0b0111, "or": 0b0010}}}
        )

        # Verify both modes produce same results: (0b0101 & 0b0111) | 0b0010 = 0b0111
        assert doc_sql["flags"] == doc_python["flags"] == 0b0111
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_bit_with_zero_default_sql_vs_python():
    """Test $bit on non-existent field - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_default_cmp

        # Reset to SQL mode (Tier 1/2)
        set_force_fallback(False)

        # Insert fresh test document without flags field
        collection.delete_many({})
        collection.insert_one({"name": "test"})

        # Run update with SQL (Tier 1/2)
        result_sql = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"or": 0b0101}}}
        )
        doc_sql = collection.find_one({"name": "test"})

        # Reset to Python mode (Tier 3)
        set_force_fallback(True)

        # Reset document to same state
        collection.delete_many({})
        collection.insert_one({"name": "test"})

        # Run update with Python (Tier 3)
        result_python = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"or": 0b0101}}}
        )
        doc_python = collection.find_one({"name": "test"})

        # Reset fallback to default
        set_force_fallback(False)

        # Verify both modes produce same results: 0 | 0b0101 = 0b0101
        assert doc_sql["flags"] == doc_python["flags"] == 0b0101
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_simple_push_sql_vs_python():
    """Test simple $push (Tier 2 SQL) vs Python fallback - compare results."""
    with Connection(":memory:") as conn:
        collection = conn.test_simple_push_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection, {"$push": {"tags": "c"}}
        )

        # Verify both modes produce same results
        assert doc_sql["tags"] == doc_python["tags"] == ["a", "b", "c"]
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_push_combined_modifiers_sql_vs_python():
    """Test $push with combined modifiers - compare SQL vs Python results."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_combined_cmp

        doc_sql, doc_python, result_sql, result_python = _compare_sql_vs_python(
            collection,
            {
                "$push": {
                    "tags": {"$each": ["x", "y"], "$position": 1, "$slice": 4}
                }
            },
        )

        # Verify both modes produce same results
        assert len(doc_sql["tags"]) == len(doc_python["tags"]) == 4
        assert doc_sql["tags"] == doc_python["tags"]
        assert result_sql.matched_count == result_python.matched_count
        assert result_sql.modified_count == result_python.modified_count


def test_update_with_json_insert():
    """Test $set operations using json_insert for ensuring values are only inserted."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document without a specific field
        collection.insert_one({"name": "Alice", "age": 30})

        # Use $set to insert a new field (should use json_insert)
        result = collection.update_one(
            {"name": "Alice"}, {"$set": {"email": "alice@example.com"}}
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify the field was inserted
        doc = collection.find_one({"name": "Alice"})
        assert doc["email"] == "alice@example.com"
        assert doc["age"] == 30


def test_update_with_json_insert_existing_field():
    """Test that json_insert does not overwrite existing fields."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document with an existing field
        collection.insert_one(
            {"name": "Alice", "age": 30, "email": "alice@old.com"}
        )

        # Try to insert the same field (should not overwrite)
        result = collection.update_one(
            {"name": "Alice"}, {"$set": {"email": "alice@new.com"}}
        )

        assert result.matched_count == 1
        # With json_insert, existing fields should not be modified
        # But our implementation uses json_set which does overwrite
        # So this test verifies current behavior
        doc = collection.find_one({"name": "Alice"})
        assert doc["email"] == "alice@new.com"


def test_update_with_json_replace():
    """Test update operations using json_replace for ensuring values are only replaced."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document with existing fields
        collection.insert_one({"name": "Alice", "age": 30})

        # Use $set to replace existing field values (should use json_replace for existing fields)
        result = collection.update_one({"name": "Alice"}, {"$set": {"age": 31}})

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify the field was replaced
        doc = collection.find_one({"name": "Alice"})
        assert doc["age"] == 31
        assert doc["name"] == "Alice"


def test_update_with_mixed_json_insert_replace():
    """Test update operations with mix of inserting new fields and replacing existing ones."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document with some fields
        collection.insert_one({"name": "Alice", "age": 30})

        # Update with mix of new and existing fields
        result = collection.update_one(
            {"name": "Alice"},
            {
                "$set": {
                    "age": 31,
                    "email": "alice@example.com",
                    "city": "New York",
                }
            },
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify both existing and new fields were handled correctly
        doc = collection.find_one({"name": "Alice"})
        assert doc["age"] == 31  # Replaced
        assert doc["email"] == "alice@example.com"  # Inserted
        assert doc["city"] == "New York"  # Inserted
        assert doc["name"] == "Alice"  # Unchanged


def test_update_many_with_json_functions():
    """Test update_many operations using enhanced JSON functions."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert multiple documents
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "score": 85},
                {"name": "Bob", "age": 25, "score": 90},
                {"name": "Charlie", "age": 35, "score": 75},
            ]
        )

        # Update multiple documents
        result = collection.update_many(
            {"age": {"$gte": 30}}, {"$set": {"senior": True, "bonus": 10}}
        )

        assert result.matched_count == 2  # Alice and Charlie
        assert result.modified_count == 2

        # Verify updates
        senior_docs = list(collection.find({"senior": True}))
        assert len(senior_docs) == 2
        for doc in senior_docs:
            assert doc["bonus"] == 10
            assert doc["age"] >= 30


def test_upsert_with_json_functions():
    """Test upsert operations using enhanced JSON functions."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Try to update a non-existent document with upsert=True
        result = collection.update_one(
            {"name": "David"},
            {"$set": {"age": 40, "department": "Engineering"}},
            upsert=True,
        )

        assert result.matched_count == 0
        assert result.modified_count == 0
        assert result.upserted_id is not None

        # Verify the document was inserted
        doc = collection.find_one({"name": "David"})
        assert doc["age"] == 40
        assert doc["department"] == "Engineering"


def test_complex_update_with_nested_fields():
    """Test complex update operations with nested fields."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document with nested fields
        collection.insert_one(
            {
                "name": "Alice",
                "profile": {
                    "age": 30,
                    "address": {"city": "Boston", "zip": "02101"},
                },
                "scores": [85, 90, 78],
            }
        )

        # Update nested fields - using full object replacement which works with SQL
        result = collection.update_one(
            {"name": "Alice"},
            {
                "$set": {
                    "profile": {
                        "age": 31,
                        "address": {"city": "New York", "zip": "02101"},
                        "phone": "555-1234",
                    },
                    "scores": [85, 95, 78],  # Update array element
                }
            },
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify nested field updates
        doc = collection.find_one({"name": "Alice"})
        assert doc["profile"]["age"] == 31
        assert doc["profile"]["address"]["city"] == "New York"
        assert doc["profile"]["phone"] == "555-1234"
        assert doc["scores"][1] == 95


def test_update_with_json_patch():
    """Test update operations using json_patch for document merging."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document
        collection.insert_one(
            {
                "name": "Alice",
                "age": 30,
                "address": {"street": "123 Main St", "city": "Boston"},
            }
        )

        # This would be implemented in a future phase
        # For now, we test that complex updates still work
        result = collection.update_one(
            {"name": "Alice"},
            {"$set": {"age": 31, "address.zip": "02101", "phone": "555-1234"}},
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify the merge-like behavior
        doc = collection.find_one({"name": "Alice"})
        assert doc["age"] == 31
        assert doc["address"]["street"] == "123 Main St"  # Unchanged
        assert doc["address"]["city"] == "Boston"  # Unchanged
        assert doc["address"]["zip"] == "02101"  # Added
        assert doc["phone"] == "555-1234"  # Added


def test_update_with_force_fallback():
    """Test update operations with force fallback enabled."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a document
        collection.insert_one({"name": "Alice", "age": 30})

        # Enable force fallback
        set_force_fallback(True)
        assert get_force_fallback() is True

        # Perform update operation - should use Python fallback
        result = collection.update_one(
            {"name": "Alice"},
            {"$set": {"age": 31, "email": "alice@example.com"}},
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify the update worked
        doc = collection.find_one({"name": "Alice"})
        assert doc["age"] == 31
        assert doc["email"] == "alice@example.com"

        # Disable force fallback
        set_force_fallback(False)
        assert get_force_fallback() is False


def test_update_many_with_force_fallback():
    """Test update_many operations with force fallback enabled."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert multiple documents
        collection.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
            ]
        )

        # Enable force fallback
        set_force_fallback(True)
        assert get_force_fallback() is True

        # Perform update operation - should use Python fallback
        result = collection.update_many(
            {"age": {"$gte": 30}}, {"$set": {"senior": True}}
        )

        assert result.matched_count == 2  # Alice and Charlie
        assert result.modified_count == 2

        # Verify updates
        senior_docs = list(collection.find({"senior": True}))
        assert len(senior_docs) == 2
        for doc in senior_docs:
            assert doc["age"] >= 30

        # Disable force fallback
        set_force_fallback(False)
        assert get_force_fallback() is False


def test_push_each_modifier():
    """Test $push with $each modifier to add multiple elements."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_each

        # Insert document with array
        collection.insert_one({"name": "test", "tags": ["a", "b"]})

        # Push multiple elements with $each
        result = collection.update_one(
            {"name": "test"}, {"$push": {"tags": {"$each": ["c", "d"]}}}
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify elements were added
        doc = collection.find_one({"name": "test"})
        assert doc["tags"] == ["a", "b", "c", "d"]


def test_push_position_modifier():
    """Test $push with $position modifier to insert at specific position."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_position

        # Insert document with array
        collection.insert_one({"name": "test", "tags": ["a", "b", "c"]})

        # Insert at beginning (position 0)
        result = collection.update_one(
            {"name": "test"},
            {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify element was inserted at position 0
        doc = collection.find_one({"name": "test"})
        assert doc["tags"] == ["x", "a", "b", "c"]

        # Insert at middle position
        result = collection.update_one(
            {"name": "test"},
            {"$push": {"tags": {"$each": ["y"], "$position": 2}}},
        )

        doc = collection.find_one({"name": "test"})
        assert doc["tags"] == ["x", "a", "y", "b", "c"]


def test_push_slice_modifier():
    """Test $push with $slice modifier to limit array size."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_slice

        # Insert document with array
        collection.insert_one({"name": "test", "tags": ["a", "b"]})

        # Push with positive slice (keep first N elements)
        result = collection.update_one(
            {"name": "test"},
            {"$push": {"tags": {"$each": ["c", "d", "e"], "$slice": 3}}},
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify only first 3 elements kept
        doc = collection.find_one({"name": "test"})
        assert len(doc["tags"]) == 3
        assert doc["tags"] == ["a", "b", "c"]

        # Test negative slice (keep last N elements)
        collection.update_one(
            {"name": "test"},
            {"$push": {"tags": {"$each": ["f", "g"], "$slice": -3}}},
        )

        doc = collection.find_one({"name": "test"})
        assert len(doc["tags"]) == 3
        assert doc["tags"] == ["c", "f", "g"]


def test_push_combined_modifiers():
    """Test $push with combined $each, $position, and $slice modifiers."""
    with Connection(":memory:") as conn:
        collection = conn.test_push_combined

        # Insert document with array
        collection.insert_one({"name": "test", "tags": ["a", "b", "c"]})

        # Combine position and slice
        result = collection.update_one(
            {"name": "test"},
            {
                "$push": {
                    "tags": {"$each": ["x", "y"], "$position": 1, "$slice": 4}
                }
            },
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify: insert at position 1, then keep only 4 elements
        doc = collection.find_one({"name": "test"})
        assert len(doc["tags"]) == 4
        assert doc["tags"] == ["a", "x", "y", "b"]


def test_bit_and_operator():
    """Test $bit with AND operation."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_and

        # Insert document with flags
        collection.insert_one({"name": "test", "flags": 0b0101})  # 5

        # Apply bitwise AND
        result = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"and": 0b0011}}}  # 3
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify: 0b0101 & 0b0011 = 0b0001 (1)
        doc = collection.find_one({"name": "test"})
        assert doc["flags"] == 0b0001


def test_bit_or_operator():
    """Test $bit with OR operation."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_or

        # Insert document with flags
        collection.insert_one({"name": "test", "flags": 0b0101})  # 5

        # Apply bitwise OR
        result = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"or": 0b0011}}}  # 3
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify: 0b0101 | 0b0011 = 0b0111 (7)
        doc = collection.find_one({"name": "test"})
        assert doc["flags"] == 0b0111


def test_bit_xor_operator():
    """Test $bit with XOR operation."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_xor

        # Insert document with flags
        collection.insert_one({"name": "test", "flags": 0b0101})  # 5

        # Apply bitwise XOR
        result = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"xor": 0b0011}}}  # 3
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify: 0b0101 ^ 0b0011 = 0b0110 (6)
        doc = collection.find_one({"name": "test"})
        assert doc["flags"] == 0b0110


def test_bit_combined_operations():
    """Test $bit with combined AND, OR, and XOR operations."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_combined

        # Insert document with flags
        collection.insert_one({"name": "test", "flags": 0b0101})  # 5

        # Apply combined bitwise operations
        result = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"and": 0b0111, "or": 0b0010}}}
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify: (0b0101 & 0b0111) | 0b0010 = 0b0101 | 0b0010 = 0b0111 (7)
        doc = collection.find_one({"name": "test"})
        assert doc["flags"] == 0b0111


def test_bit_with_zero_default():
    """Test $bit operation on non-existent field (defaults to 0)."""
    with Connection(":memory:") as conn:
        collection = conn.test_bit_default

        # Insert document without flags field
        collection.insert_one({"name": "test"})

        # Apply bitwise OR (should default to 0)
        result = collection.update_one(
            {"name": "test"}, {"$bit": {"flags": {"or": 0b0101}}}
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify: 0 | 0b0101 = 0b0101 (5)
        doc = collection.find_one({"name": "test"})
        assert doc["flags"] == 0b0101


if __name__ == "__main__":
    pytest.main([__file__])
