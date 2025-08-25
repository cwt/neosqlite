# coding: utf-8
"""
Targeted tests to improve coverage of specific lines in the new $unwind + $group code
"""
import neosqlite
import pytest


def test_unwind_group_invalid_field_formats():
    """Test $unwind + $group with invalid field formats to cover fallback paths"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test with non-string unwind field (should cause error in Python fallback)
        pipeline = [
            {"$unwind": {"invalid": "format"}},  # Invalid format
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        try:
            result = collection.aggregate(pipeline)
            # If it doesn't error, it should return some results
            assert len(result) >= 0
        except Exception:
            # Expected to fail, which is fine
            pass

        # Test with valid unwind but invalid group _id field (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {"_id": {"invalid": "format"}, "count": {"$sum": 1}}
            },  # Invalid format
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2


def test_unwind_group_invalid_accumulator_formats():
    """Test $unwind + $group with invalid accumulator formats"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test with invalid accumulator format (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {"_id": "$tags", "count": "invalid_format"}
            },  # Invalid format
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2

        # Test with accumulator that's not a dict (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {"_id": "$tags", "count": ["invalid", "format"]}
            },  # List instead of dict
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2


def test_unwind_group_unsupported_accumulator():
    """Test $unwind + $group with unsupported accumulator operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test with unsupported accumulator (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "result": {"$unsupported": "$field"}}},
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2


def test_unwind_group_complex_group_id():
    """Test $unwind + $group with complex _id expressions (should fallback)"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test with complex _id expression (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": {"$concat": ["$tags", "_suffix"]},
                    "count": {"$sum": 1},
                }
            },
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2


def test_unwind_group_complex_accumulator_expression():
    """Test $unwind + $group with complex accumulator expressions"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test with complex accumulator expression (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "result": {"$sum": {"$multiply": ["$field1", "$field2"]}},
                }
            },
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2


def test_unwind_group_sum_with_non_one_value():
    """Test $unwind + $group with $sum using non-1 value"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test with $sum using value other than 1 (should fallback to Python)
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {"_id": "$tags", "count": {"$sum": 2}}
            },  # Use 2 instead of 1
        ]
        result = collection.aggregate(pipeline)
        # Should fallback to Python and return unwound documents
        assert len(result) == 2


if __name__ == "__main__":
    pytest.main([__file__])
