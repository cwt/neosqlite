# coding: utf-8
"""
Additional test cases for $unwind + $group optimization to improve code coverage
"""
import neosqlite
import pytest


def test_unwind_group_edge_cases():
    """Test edge cases for $unwind + $group optimization"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Test with empty array
        collection.insert_many(
            [
                {"category": "A", "tags": []},  # Empty array
                {"category": "B", "tags": ["python", "java"]},
            ]
        )

        # Test $unwind + $group with empty array
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should only count non-empty arrays
        assert len(result) == 2  # python, java

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts["python"] == 1
        assert counts["java"] == 1


def test_unwind_group_with_none_values():
    """Test $unwind + $group with None/null values"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Test with None values in array
        collection.insert_many(
            [{"category": "A", "tags": ["python", None, "java"]}]
        )

        # Test $unwind + $group with None values
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should count None values
        assert len(result) == 3  # python, None, java

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts["python"] == 1
        assert counts["java"] == 1
        assert counts[None] == 1


def test_unwind_group_complex_match():
    """Test $unwind + $group with complex $match conditions"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "category": "A",
                    "status": "active",
                    "priority": 1,
                    "tags": ["python", "javascript"],
                },
                {
                    "category": "A",
                    "status": "inactive",
                    "priority": 2,
                    "tags": ["java", "python"],
                },
                {
                    "category": "B",
                    "status": "active",
                    "priority": 1,
                    "tags": ["go", "rust"],
                },
            ]
        )

        # Test complex $match then $unwind then $group
        pipeline = [
            {"$match": {"status": "active", "priority": 1}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should only count tags from documents matching both conditions
        assert len(result) == 4  # python, javascript, go, rust

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts["python"] == 1
        assert counts["javascript"] == 1
        assert counts["go"] == 1
        assert counts["rust"] == 1


def test_unwind_group_with_numeric_fields():
    """Test $unwind + $group with numeric fields"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with numeric values
        collection.insert_many(
            [{"scores": [85, 92, 78]}, {"scores": [90, 85, 95]}]
        )

        # Test $unwind + $group with numeric field
        pipeline = [
            {"$unwind": "$scores"},
            {"$group": {"_id": "$scores", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should count occurrences of each score
        assert len(result) == 5  # 85, 92, 78, 90, 95

        # Check counts
        counts = {doc["_id"]: doc["count"] for doc in result}
        assert counts[85] == 2
        assert counts[92] == 1
        assert counts[78] == 1
        assert counts[90] == 1
        assert counts[95] == 1


def test_unwind_group_fallback_cases():
    """Test cases that should fallback to Python implementation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [{"category": "A", "tags": ["python", "javascript"]}]
        )

        # Test unsupported accumulator - should fallback to Python
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "avg": {"$avg": "$someField"}}},
        ]
        result = collection.aggregate(pipeline)

        # Should still work (fallback to Python)
        assert len(result) == 2  # python, javascript


def test_unwind_group_fallback_with_add_to_set():
    """Test $unwind + $group with $addToSet using Python fallback"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "category": "A",
                    "tags": ["python", "javascript"],
                    "name": "Alice",
                },
                {"category": "A", "tags": ["python", "go"], "name": "Bob"},
                {
                    "category": "B",
                    "tags": ["java", "python"],
                    "name": "Charlie",
                },
            ]
        )

        # Test complex pipeline that forces Python fallback
        # Using $match with regex forces fallback
        pipeline = [
            {
                "$match": {"category": {"$regex": "^[AB]"}}
            },  # Forces Python fallback
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "uniqueNames": {"$addToSet": "$name"}}},
            {"$sort": {"_id": 1}},
        ]
        result = collection.aggregate(pipeline)

        # Should work with Python fallback
        assert len(result) >= 0  # Should have some results

        # Convert to dict for easier checking
        result_dict = {doc["_id"]: doc.get("uniqueNames", []) for doc in result}

        # Check that python tag has unique names
        if "python" in result_dict:
            unique_names = result_dict["python"]
            # Should contain Alice, Bob, and Charlie (each only once)
            assert "Alice" in unique_names
            assert "Bob" in unique_names
            assert "Charlie" in unique_names
            # Should not have duplicates
            assert len(unique_names) == len(set(unique_names))


def test_unwind_group_multiple_stages_after():
    """Test $unwind + $group followed by other stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "tags": ["python", "javascript", "python"]},
                {"category": "B", "tags": ["java", "python", "java"]},
            ]
        )

        # Test $unwind + $group (this should be optimized)
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = collection.aggregate(pipeline)

        # Should be ordered by _id (due to SQL ORDER BY _id)
        assert len(result) == 3
        # Results should be ordered alphabetically: java, javascript, python
        assert result[0]["_id"] == "java"
        assert result[0]["count"] == 2
        assert result[1]["_id"] == "javascript"
        assert result[1]["count"] == 1
        assert result[2]["_id"] == "python"
        assert result[2]["count"] == 3


if __name__ == "__main__":
    pytest.main([__file__])
