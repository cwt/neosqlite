# coding: utf-8
"""
Test cases for query cost estimation functionality
"""
import neosqlite
import pytest


def test_query_cost_estimation():
    """Test that query cost estimation works correctly"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 25, "city": "New York"},
                {"name": "Bob", "age": 30, "city": "Boston"},
                {"name": "Charlie", "age": 35, "city": "New York"},
            ]
        )

        # Test cost estimation without indexes
        query_helper = collection.query_engine.helpers
        cost_without_indexes = query_helper._estimate_query_cost({"age": 25})
        assert cost_without_indexes == 1.0  # No indexes, so cost is 1.0

        # Create an index on age
        collection.create_index("age")

        # Test cost estimation with index
        cost_with_index = query_helper._estimate_query_cost({"age": 25})
        assert cost_with_index == 0.3  # With index, cost is reduced

        # Test cost estimation with _id field (always indexed)
        cost_with_id = query_helper._estimate_query_cost({"_id": 1})
        assert cost_with_id == 0.1  # _id is always indexed, so very low cost

        # Test cost estimation with multiple fields
        cost_multiple_fields = query_helper._estimate_query_cost(
            {"age": 25, "city": "New York"}
        )
        # age is indexed (0.3) * city is not indexed (1.0) = 0.3
        assert cost_multiple_fields == 0.3

        # Test cost estimation with logical operators
        cost_with_logical = query_helper._estimate_query_cost(
            {"$and": [{"age": 25}, {"city": "New York"}]}
        )
        # Both subqueries should be estimated
        # age is indexed (0.3) * city is not indexed (1.0) = 0.3
        assert cost_with_logical == 0.3


def test_get_indexed_fields():
    """Test that we can correctly identify indexed fields"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Initially no indexes
        query_helper = collection.query_engine.helpers
        indexed_fields = query_helper._get_indexed_fields()
        assert indexed_fields == []

        # Create some indexes
        collection.create_index("age")
        collection.create_index("profile.city")

        # Check that we can identify the indexed fields
        indexed_fields = query_helper._get_indexed_fields()
        assert len(indexed_fields) == 2
        assert "age" in indexed_fields
        assert "profile.city" in indexed_fields


if __name__ == "__main__":
    pytest.main([__file__])
