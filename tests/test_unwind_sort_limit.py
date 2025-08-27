# coding: utf-8
"""
Test cases for $unwind + $sort + $limit optimization with SQL implementation
"""
import neosqlite
import pytest


def test_unwind_sort_limit_basic():
    """Test basic $unwind + $sort + $limit optimization"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript", "go"],
                    "score": 95,
                },
                {
                    "name": "Bob",
                    "tags": ["java", "python", "rust"],
                    "score": 87,
                },
                {
                    "name": "Charlie",
                    "tags": ["javascript", "go", "rust"],
                    "score": 92,
                },
            ]
        )

        # Test $unwind + $sort + $limit
        pipeline = [{"$unwind": "$tags"}, {"$sort": {"tags": 1}}, {"$limit": 5}]
        result = collection.aggregate(pipeline)

        # Should have 5 documents (limited)
        assert len(result) == 5

        # Should be sorted by tags in ascending order
        tags = [doc["tags"] for doc in result]
        assert tags == ["go", "go", "java", "javascript", "javascript"]


def test_unwind_sort_limit_with_match():
    """Test $match + $unwind + $sort + $limit optimization"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "status": "active",
                    "tags": ["python", "javascript", "go"],
                    "score": 95,
                },
                {
                    "name": "Bob",
                    "status": "inactive",
                    "tags": ["java", "python", "rust"],
                    "score": 87,
                },
                {
                    "name": "Charlie",
                    "status": "active",
                    "tags": ["javascript", "go", "rust"],
                    "score": 92,
                },
            ]
        )

        # Test $match + $unwind + $sort + $limit
        pipeline = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$limit": 3},
        ]
        result = collection.aggregate(pipeline)

        # Should have 3 documents (limited) from active users only
        assert len(result) == 3

        # Should be sorted by tags in ascending order
        tags = [doc["tags"] for doc in result]
        assert tags == ["go", "go", "javascript"]

        # Should only contain active users
        names = [doc["name"] for doc in result]
        assert "Bob" not in names


def test_unwind_sort_limit_descending():
    """Test $unwind + $sort + $limit with descending sort"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript", "go"],
                    "score": 95,
                },
                {
                    "name": "Bob",
                    "tags": ["java", "python", "rust"],
                    "score": 87,
                },
            ]
        )

        # Test $unwind + $sort + $limit with descending sort
        pipeline = [
            {"$unwind": "$tags"},
            {"$sort": {"tags": -1}},  # Descending
            {"$limit": 4},
        ]
        result = collection.aggregate(pipeline)

        # Should have 4 documents (limited)
        assert len(result) == 4

        # Should be sorted by tags in descending order
        tags = [doc["tags"] for doc in result]
        assert tags == ["rust", "python", "python", "javascript"]


def test_unwind_sort_limit_sort_by_original_field():
    """Test $unwind + $sort + $limit sorting by original document field"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript"],
                    "score": 95,
                },
                {"name": "Bob", "tags": ["java", "python"], "score": 87},
                {"name": "Charlie", "tags": ["javascript", "go"], "score": 92},
            ]
        )

        # Test $unwind + $sort + $limit sorting by original field (score)
        pipeline = [
            {"$unwind": "$tags"},
            {"$sort": {"score": -1}},  # Sort by score descending
            {"$limit": 4},
        ]
        result = collection.aggregate(pipeline)

        # Should have 4 documents (limited)
        assert len(result) == 4

        # Should be sorted by score in descending order
        scores = [doc["score"] for doc in result]
        assert scores == [
            95,
            95,
            92,
            92,
        ]  # Alice's tags first, then Charlie's, then Bob's


def test_unwind_sort_limit_with_skip():
    """Test $unwind + $sort + $skip + $limit optimization"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript", "go", "rust"],
                    "score": 95,
                },
                {"name": "Bob", "tags": ["java", "python"], "score": 87},
            ]
        )

        # Test $unwind + $sort + $skip + $limit
        pipeline = [
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$skip": 2},
            {"$limit": 3},
        ]
        result = collection.aggregate(pipeline)

        # Should have 3 documents (limited) after skipping 2
        assert len(result) == 3

        # Should be sorted by tags in ascending order, skipping first 2
        tags = [doc["tags"] for doc in result]
        # All tags alphabetically: go, java, javascript, javascript, python, python, rust
        # After skipping 2: javascript, javascript, python
        assert tags == ["javascript", "python", "python"]


if __name__ == "__main__":
    pytest.main([__file__])
