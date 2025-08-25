# coding: utf-8
"""
Test cases for $unwind with $group operations
"""
import neosqlite
import pytest


def test_unwind_then_group_current_behavior():
    """Test $unwind followed by $group - current behavior"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "category": "A",
                    "tags": [
                        "python",
                        "javascript",
                        "python",
                    ],  # Duplicate python
                },
                {
                    "category": "B",
                    "tags": ["java", "python", "java"],  # Duplicate java
                },
            ]
        )

        # Test $unwind then $group
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should count occurrences of each tag across all documents
        assert len(result) == 3  # python, javascript, java

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts["python"] == 3  # 2 from doc1 + 1 from doc2
        assert counts["javascript"] == 1
        assert counts["java"] == 2  # 1 from doc1 + 1 from doc2

        # Sort by tag name for consistent ordering
        result.sort(key=lambda x: x["_id"])
        tags = [doc["_id"] for doc in result]
        assert tags == ["java", "javascript", "python"]


def test_unwind_group_with_match():
    """Test $unwind + $group with preceding $match"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "category": "A",
                    "status": "active",
                    "tags": ["python", "javascript"],
                },
                {
                    "category": "A",
                    "status": "inactive",
                    "tags": ["java", "python"],
                },
                {"category": "B", "status": "active", "tags": ["go", "rust"]},
            ]
        )

        # Test $match then $unwind then $group
        pipeline = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should only count tags from active documents
        assert len(result) == 4  # python, javascript, go, rust

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts["python"] == 1
        assert counts["javascript"] == 1
        assert counts["go"] == 1
        assert counts["rust"] == 1


def test_unwind_group_with_id_field():
    """Test $unwind + $group using _id field"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with _id field
        collection.insert_many(
            [
                {"_id": 1, "category": "A", "tags": ["python", "javascript"]},
                {"_id": 2, "category": "B", "tags": ["java", "python"]},
            ]
        )

        # Test $unwind then $group by category
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should group by category
        assert len(result) == 2  # A, B

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts["A"] == 2
        assert counts["B"] == 2


if __name__ == "__main__":
    pytest.main([__file__])
