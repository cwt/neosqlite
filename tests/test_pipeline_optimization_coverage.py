"""
Targeted tests for complex pipeline optimization in query_helper.py
Specifically targeting lines 1388-1515, 1733-1791, 1838-1886
"""

import pytest
from neosqlite import Connection


def test_unwind_group_optimization_pattern_1():
    """Test $unwind followed by $group optimization (lines 1388-1515)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with arrays
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript", "sql"]},
                {"name": "Bob", "tags": ["python", "java"]},
                {"name": "Charlie", "tags": ["javascript", "sql", "python"]},
            ]
        )

        # Test $unwind + $group pattern that should be optimized
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]

        # This should use SQL optimization
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) == 4  # 4 unique tags
        tag_counts = {doc["_id"]: doc["count"] for doc in results}
        assert tag_counts["python"] == 3
        assert tag_counts["javascript"] == 2
        assert tag_counts["sql"] == 2
        assert tag_counts["java"] == 1


def test_unwind_group_optimization_pattern_2():
    """Test $match followed by $unwind + $group optimization (lines 1733-1791)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with arrays
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "department": "Engineering",
                    "skills": ["python", "sql"],
                },
                {
                    "name": "Bob",
                    "department": "Engineering",
                    "skills": ["java", "python"],
                },
                {
                    "name": "Charlie",
                    "department": "Marketing",
                    "skills": ["design", "sql"],
                },
                {
                    "name": "David",
                    "department": "Engineering",
                    "skills": ["python", "javascript"],
                },
            ]
        )

        # Test $match + $unwind + $group pattern that should be optimized
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$unwind": "$skills"},
            {"$group": {"_id": "$skills", "count": {"$sum": 1}}},
        ]

        # This should use SQL optimization
        results = list(collection.aggregate(pipeline))

        # Verify results - only Engineering department members
        assert len(results) == 4  # 4 unique skills in Engineering
        skill_counts = {doc["_id"]: doc["count"] for doc in results}
        assert skill_counts["python"] == 3  # Alice, Bob, David
        assert skill_counts["sql"] == 1  # Only Alice
        assert skill_counts["java"] == 1  # Only Bob
        assert skill_counts["javascript"] == 1  # Only David


def test_unwind_group_with_push_accumulator():
    """Test $unwind + $group with $push accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with arrays
        collection.insert_many(
            [
                {"category": "fruits", "items": ["apple", "banana"]},
                {"category": "fruits", "items": ["orange", "apple"]},
                {"category": "vegetables", "items": ["carrot", "broccoli"]},
            ]
        )

        # Test $unwind + $group with $push
        pipeline = [
            {"$unwind": "$items"},
            {"$group": {"_id": "$items", "categories": {"$push": "$category"}}},
        ]

        # This should use SQL optimization
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) >= 4  # At least 4 unique items
        item_categories = {doc["_id"]: doc["categories"] for doc in results}
        assert "apple" in item_categories
        assert "banana" in item_categories
        assert "orange" in item_categories
        assert "carrot" in item_categories


def test_unwind_group_with_addtoset_accumulator():
    """Test $unwind + $group with $addToSet accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with duplicate items
        collection.insert_many(
            [
                {"type": "A", "values": ["x", "y"]},
                {"type": "A", "values": ["y", "z"]},  # y appears twice
                {"type": "B", "values": ["x", "z"]},
            ]
        )

        # Test $unwind + $group with $addToSet (unique values only)
        pipeline = [
            {"$unwind": "$values"},
            {"$group": {"_id": "$values", "types": {"$addToSet": "$type"}}},
        ]

        # This should use SQL optimization
        results = list(collection.aggregate(pipeline))

        # Verify results - each value should have unique types
        assert len(results) == 3  # x, y, z
        for doc in results:
            # Each value should appear only once per type (no duplicates)
            types = doc["types"]
            assert len(types) == len(set(types))  # No duplicates


def test_unwind_with_text_search_optimization():
    """Test $unwind with text search optimization (lines 1838-1886)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with arrays of strings
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "keywords": ["python", "machine learning", "data science"],
                },
                {
                    "name": "Bob",
                    "keywords": ["java", "spring", "microservices"],
                },
                {
                    "name": "Charlie",
                    "keywords": ["python", "django", "web development"],
                },
            ]
        )

        # Test $unwind + $match with text search pattern that should be optimized
        pipeline = [
            {"$unwind": "$keywords"},
            {"$match": {"$text": {"$search": "python"}}},
        ]

        # This should use SQL optimization for the text search on unwound elements
        results = list(collection.aggregate(pipeline))

        # Verify results - should find documents with "python" in keywords
        assert len(results) == 2  # Alice and Charlie have "python" in keywords
        keywords_found = [doc["keywords"] for doc in results]
        assert "python" in keywords_found


def test_complex_unwind_group_patterns():
    """Test complex $unwind + $group patterns with various accumulators."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with nested arrays
        collection.insert_many(
            [
                {"team": "A", "scores": [85, 92, 78]},
                {"team": "B", "scores": [88, 95, 82]},
                {"team": "A", "scores": [90, 87, 93]},
                {"team": "C", "scores": [75, 80, 85]},
            ]
        )

        # Test complex $unwind + $group with multiple accumulators
        pipeline = [
            {"$unwind": "$scores"},
            {
                "$group": {
                    "_id": "$team",
                    "totalScores": {"$sum": 1},  # Count of scores
                    "allScores": {"$push": "$scores"},  # All scores
                    "uniqueScores": {"$addToSet": "$scores"},  # Unique scores
                }
            },
        ]

        # This should use SQL optimization
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) == 3  # Teams A, B, C
        teams_data = {doc["_id"]: doc for doc in results}

        # Team A should have 6 scores (3 from each document)
        assert teams_data["A"]["totalScores"] == 6
        assert len(teams_data["A"]["allScores"]) == 6
        assert (
            len(teams_data["A"]["uniqueScores"]) >= 1
        )  # At least some unique scores


def test_unwind_group_edge_cases():
    """Test edge cases for $unwind + $group optimization."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert edge case data
        collection.insert_many(
            [
                {"id": 1, "tags": []},  # Empty array
                {"id": 2, "tags": ["single"]},  # Single element
                {
                    "id": 3,
                    "tags": ["multiple", "tags", "here"],
                },  # Multiple elements
                {"id": 4, "tags": None},  # Null value
            ]
        )

        # Test $unwind + $group with edge cases
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]

        # This should handle edge cases gracefully
        results = list(collection.aggregate(pipeline))

        # Should only include non-null, non-empty array elements
        assert len(results) == 4  # "single", "multiple", "tags", "here"
        tag_counts = {doc["_id"]: doc["count"] for doc in results}
        assert "single" in tag_counts
        assert "multiple" in tag_counts
        assert "tags" in tag_counts
        assert "here" in tag_counts

        # Empty arrays and null values should be filtered out
        # So we shouldn't see any documents with id=1 (empty array) or id=4 (null)


if __name__ == "__main__":
    pytest.main([__file__])
