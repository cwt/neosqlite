# coding: utf-8
"""
Test cases for multiple $unwind stages with json_each()
These tests document the enhanced behavior after implementing multiple $unwind support.
"""
import neosqlite
import pytest


def test_multiple_unwind_stages_enhanced_behavior():
    """Test multiple $unwind stages - enhanced behavior (all are processed)"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with multiple arrays
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript"],
                    "categories": ["programming", "web"],
                },
                {
                    "name": "Bob",
                    "tags": ["java", "python"],
                    "categories": ["programming", "enterprise"],
                },
            ]
        )

        # Test multiple $unwind stages
        pipeline = [{"$unwind": "$tags"}, {"$unwind": "$categories"}]
        result = collection.aggregate(pipeline)

        # With enhanced implementation, all $unwind stages are processed
        # Should have 2*2*2 = 8 documents (2 docs * 2 tags * 2 categories each)
        assert len(result) == 8

        # Check that both fields are unwound
        tags = [doc["tags"] for doc in result]
        categories = [doc["categories"] for doc in result]

        # Each original tag should appear 4 times (2 docs * 2 categories)
        assert tags.count("python") == 4
        assert tags.count("javascript") == 2
        assert tags.count("java") == 2

        # Each original category should appear 4 times (2 docs * 2 tags)
        assert categories.count("programming") == 4
        assert categories.count("web") == 2
        assert categories.count("enterprise") == 2


def test_single_unwind_stage_works():
    """Test that single $unwind stages still work correctly"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript"]},
                {"name": "Bob", "tags": ["java", "python"]},
            ]
        )

        # Test single $unwind stage
        pipeline = [{"$unwind": "$tags"}]
        result = collection.aggregate(pipeline)

        # Should have 4 documents (2 docs * 2 tags each)
        assert len(result) == 4

        # Check that tags are unwound
        tags = [doc["tags"] for doc in result]
        assert tags.count("python") == 2
        assert tags.count("javascript") == 1
        assert tags.count("java") == 1


if __name__ == "__main__":
    pytest.main([__file__])
