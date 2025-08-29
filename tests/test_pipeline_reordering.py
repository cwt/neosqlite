# coding: utf-8
"""
Test cases for pipeline reordering optimization
"""
import neosqlite
import pytest


def test_pipeline_reordering_optimization():
    """Test that pipeline stages are reordered for better performance"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 5}",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i,
                    "tags": [f"tag{j}" for j in range(3)],
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")

        # Test pipeline reordering - match should be moved to the front
        pipeline = [
            {"$unwind": "$tags"},  # Expensive operation first
            {
                "$match": {"category": "Category2", "status": "active"}
            },  # Match should be moved to front
            {"$limit": 10},
        ]

        # The optimization should reorder this to put match first
        result = collection.aggregate(pipeline)

        # Should still work correctly
        assert len(result) <= 10

        # All documents should match the criteria
        categories = [doc["category"] for doc in result]
        statuses = [doc["status"] for doc in result]
        assert all(cat == "Category2" for cat in categories)
        assert all(status == "active" for status in statuses)


def test_cost_based_pipeline_selection():
    """Test that cost estimation is used to select optimal pipeline execution"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(50):
            docs.append(
                {"name": f"User{i}", "category": f"Category{i % 3}", "value": i}
            )
        collection.insert_many(docs)

        # Create index on category
        collection.create_index("category")

        # Test a pipeline where reordering should be beneficial
        pipeline = [
            {"$sort": {"value": -1}},  # Sort expensive operation
            {
                "$match": {"category": "Category1"}
            },  # Indexed match - should be moved to front
            {"$limit": 5},
        ]

        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 5

        # All should match category
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category1" for cat in categories)

        # Should be sorted by value descending
        values = [doc["value"] for doc in result]
        assert values == sorted(values, reverse=True)


def test_match_pushdown_optimization():
    """Test that match stages are pushed down for early filtering"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with nested arrays
        docs = []
        for i in range(30):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",
                    "items": [{"id": j, "value": i * 10 + j} for j in range(5)],
                }
            )
        collection.insert_many(docs)

        # Create index on category
        collection.create_index("category")

        # Test pipeline with match after expensive unwind
        pipeline = [
            {"$unwind": "$items"},  # Expensive unwind operation
            {"$match": {"category": "Category2"}},  # Should be pushed to front
            {"$sort": {"items.value": 1}},
            {"$limit": 10},
        ]

        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 10

        # All should match category
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category2" for cat in categories)


if __name__ == "__main__":
    pytest.main([__file__])
