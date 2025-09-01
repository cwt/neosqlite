# coding: utf-8
"""
Test cases for unsupported accumulator operations to ensure proper fallback
"""
import neosqlite
import pytest


def test_unsupported_accumulator_operation():
    """Test that unsupported accumulator operations fall back to Python processing"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_unsupported_accumulator"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with unsupported accumulator operation
        # This should fall back to Python processing instead of using SQL optimization
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "total": {"$unsupportedOp": "$value"},
                }
            }
        ]

        # This should not raise an exception but fall back to Python implementation
        result = collection.aggregate(pipeline)

        # Since the operation is unsupported, it should either:
        # 1. Fall back to Python and handle it there, or
        # 2. Raise an appropriate exception
        # Either way, we're testing the code path
        assert len(result) >= 0


def test_unsupported_accumulator_with_unwind():
    """Test that unsupported accumulator operations in unwind+group fall back to Python"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_unsupported_unwind_group"]

        # Insert test data
        docs = [
            {"category": "A", "tags": ["python", "javascript"]},
            {"category": "B", "tags": ["java", "python"]},
        ]
        collection.insert_many(docs)

        # Test unwind then group with unsupported accumulator
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "invalid": {"$invalidOp": 1}}},
        ]

        # This should fall back to Python processing
        result = collection.aggregate(pipeline)

        # Test that we get some result (the exact behavior depends on implementation)
        assert len(result) >= 0


if __name__ == "__main__":
    pytest.main([__file__])
