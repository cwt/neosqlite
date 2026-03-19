"""
Tests for $pullAll update operator.

Covers: Basic $pullAll functionality, edge cases, and kill switch compatibility.
"""

import neosqlite
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
)


class TestPullAllOperator:
    """Test $pullAll update operator."""

    def test_pullall_basic(self):
        """Test $pullAll removes all instances of specified values."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"name": "test", "scores": [80, 90, 80, 100, 90, 80]}
            )

            # Remove all 80s and 90s
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"scores": [80, 90]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["scores"] == [100]

    def test_pullall_single_value(self):
        """Test $pullAll with a single value to remove."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"name": "test", "tags": ["a", "b", "c", "a", "b", "a"]}
            )

            # Remove all 'a' values
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"tags": ["a"]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["tags"] == ["b", "c", "b"]

    def test_pullall_no_matches(self):
        """Test $pullAll when no values match."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"name": "test", "numbers": [1, 2, 3, 4, 5]})

            # Try to remove values that don't exist
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"numbers": [10, 20]}}
            )

            # MongoDB behavior: modified_count=0 when no values actually removed
            assert result.matched_count == 1
            assert result.modified_count == 0
            doc = coll.find_one({"name": "test"})
            assert doc["numbers"] == [1, 2, 3, 4, 5]

    def test_pullall_nonexistent_field(self):
        """Test $pullAll on a nonexistent field."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"name": "test", "existing": [1, 2, 3]})

            # Try to pull from nonexistent field
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"nonexistent": [1, 2]}}
            )

            # MongoDB behavior: modified_count=0 when field doesn't exist
            assert result.matched_count == 1
            assert result.modified_count == 0
            doc = coll.find_one({"name": "test"})
            assert "nonexistent" not in doc

    def test_pullall_empty_array(self):
        """Test $pullAll with empty array to remove."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"name": "test", "items": [1, 2, 3]})

            # Pull with empty array (nothing to remove)
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"items": []}}
            )

            # MongoDB behavior: modified_count=0 when nothing to remove
            assert result.matched_count == 1
            assert result.modified_count == 0
            doc = coll.find_one({"name": "test"})
            assert doc["items"] == [1, 2, 3]

    def test_pullall_removes_all_duplicates(self):
        """Test $pullAll removes all duplicate occurrences."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"name": "test", "values": [5, 5, 5, 5, 5]})

            # Remove all 5s
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"values": [5]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["values"] == []

    def test_pullall_with_strings(self):
        """Test $pullAll with string values."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {
                    "name": "test",
                    "fruits": ["apple", "banana", "apple", "orange", "banana"],
                }
            )

            # Remove apples and bananas
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"fruits": ["apple", "banana"]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["fruits"] == ["orange"]

    def test_pullall_with_mixed_types(self):
        """Test $pullAll with mixed type values."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"name": "test", "mixed": [1, "one", 2, "two", 1, "one"]}
            )

            # Remove integer 1 and string "one"
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"mixed": [1, "one"]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["mixed"] == [2, "two"]

    def test_pullall_multiple_fields(self):
        """Test $pullAll on multiple fields in one update."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {
                    "name": "test",
                    "nums": [1, 2, 3, 1, 2],
                    "letters": ["a", "b", "c", "a", "b"],
                }
            )

            # Pull from both fields
            result = coll.update_one(
                {"name": "test"},
                {"$pullAll": {"nums": [1, 2], "letters": ["a", "b"]}},
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["nums"] == [3]
            assert doc["letters"] == ["c"]

    def test_pullall_preserves_order(self):
        """Test $pullAll preserves the order of remaining elements."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"name": "test", "sequence": [1, 2, 3, 4, 5, 2, 4, 6]}
            )

            # Remove 2 and 4
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"sequence": [2, 4]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            assert doc["sequence"] == [1, 3, 5, 6]

    def test_pullall_with_nested_arrays(self):
        """Test $pullAll works with nested arrays (exact matches)."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"name": "test", "nested": [[1, 2], [3, 4], [1, 2]]}
            )

            # Try to remove [1, 2] - should work for exact matches
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"nested": [[1, 2]]}}
            )

            assert result.modified_count == 1
            doc = coll.find_one({"name": "test"})
            # Nested arrays should be compared as whole objects
            assert doc["nested"] == [[3, 4]]

    def test_pullall_non_array_field(self):
        """Test $pullAll on a non-array field (should be no-op)."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"name": "test", "value": 42})

            # Try to pull from non-array field (should not error, just no-op)
            result = coll.update_one(
                {"name": "test"}, {"$pullAll": {"value": [1, 2]}}
            )

            # MongoDB behavior: modified_count=0 when field is not an array
            assert result.matched_count == 1
            assert result.modified_count == 0
            doc = coll.find_one({"name": "test"})
            assert doc["value"] == 42


class TestPullAllKillSwitch:
    """Test $pullAll with kill switch (Python fallback)."""

    def test_pullall_kill_switch(self):
        """Test $pullAll works with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"name": "test", "scores": [80, 90, 80, 100, 90, 80]}
            )

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = coll.update_one(
                    {"name": "test"}, {"$pullAll": {"scores": [80, 90]}}
                )

                assert result.modified_count == 1
                doc = coll.find_one({"name": "test"})
                assert doc["scores"] == [100]
            finally:
                set_force_fallback(original_state)

    def test_pullall_kill_switch_comparison(self):
        """Test $pullAll returns same results with/without kill switch."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection

            # Test without kill switch
            coll.insert_one(
                {"_id": 1, "name": "test1", "values": [1, 2, 3, 1, 2, 1]}
            )

            result_normal = coll.update_one(
                {"_id": 1}, {"$pullAll": {"values": [1, 2]}}
            )
            doc_normal = coll.find_one({"_id": 1})

            # Reset document
            coll.update_one(
                {"_id": 1}, {"$set": {"values": [1, 2, 3, 1, 2, 1]}}
            )

            # Test with kill switch
            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result_fallback = coll.update_one(
                    {"_id": 1}, {"$pullAll": {"values": [1, 2]}}
                )
                doc_fallback = coll.find_one({"_id": 1})
            finally:
                set_force_fallback(original_state)

            # Results should be identical
            assert (
                result_normal.modified_count == result_fallback.modified_count
            )
            assert doc_normal["values"] == doc_fallback["values"]
            assert doc_normal["values"] == [3]
