# coding: utf-8
"""
Test cases for enhanced multiple $unwind stages with json_each()
"""
import neosqlite
import pytest


def test_multiple_unwind_stages_enhanced():
    """Test multiple $unwind stages with enhanced SQL implementation"""
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


def test_multiple_unwind_with_match_enhanced():
    """Test multiple $unwind stages with $match"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "status": "active",
                    "tags": ["python", "javascript"],
                    "categories": ["programming", "web"],
                },
                {
                    "name": "Bob",
                    "status": "inactive",
                    "tags": ["java", "python"],
                    "categories": ["programming", "enterprise"],
                },
                {
                    "name": "Charlie",
                    "status": "active",
                    "tags": ["go", "rust"],
                    "categories": ["systems", "performance"],
                },
            ]
        )

        # Test $match followed by multiple $unwind stages
        pipeline = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$unwind": "$categories"},
        ]
        result = collection.aggregate(pipeline)

        # Should have 2*2*2 = 8 documents from active users only
        assert len(result) == 8

        # Check that only active users are included
        names = [doc["name"] for doc in result]
        assert "Bob" not in names
        assert names.count("Alice") == 4
        assert names.count("Charlie") == 4


def test_three_unwind_stages():
    """Test three consecutive $unwind stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with three arrays
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript"],
                    "categories": ["programming", "web"],
                    "levels": ["beginner", "intermediate"],
                }
            ]
        )

        # Test three $unwind stages
        pipeline = [
            {"$unwind": "$tags"},
            {"$unwind": "$categories"},
            {"$unwind": "$levels"},
        ]
        result = collection.aggregate(pipeline)

        # Should have 2*2*2 = 8 documents
        assert len(result) == 8

        # Check that all fields are unwound
        tags = [doc["tags"] for doc in result]
        categories = [doc["categories"] for doc in result]
        levels = [doc["levels"] for doc in result]

        assert all(tag in ["python", "javascript"] for tag in tags)
        assert all(
            category in ["programming", "web"] for category in categories
        )
        assert all(level in ["beginner", "intermediate"] for level in levels)


def test_single_unwind_still_works():
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
