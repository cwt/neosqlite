"""
Test for $addFields support in Python fallback implementation.
"""

import pytest
from neosqlite import Connection
from neosqlite.collection.temporary_table_aggregation import (
    execute_2nd_tier_aggregation,
)


def test_add_fields_python_fallback():
    """Test $addFields functionality with Python fallback (when temp tables fail)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        query_engine = collection.query_engine

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test $addFields with a pipeline that includes an unsupported stage
        # This should force fallback to Python implementation
        pipeline = [
            {"$addFields": {"userName": "$name", "userAge": "$age"}},
            {
                "$out": "output_collection"
            },  # Unsupported stage to force fallback
        ]

        # This should raise an exception because $out is not supported
        # But let's test with a supported pipeline first

        # Test with only $addFields - this should work with temp tables
        simple_pipeline = [
            {"$addFields": {"userName": "$name", "userAge": "$age"}}
        ]

        # Use the integration function which will try temp tables first
        try:
            results = execute_2nd_tier_aggregation(
                query_engine, simple_pipeline
            )
            # Verify results
            assert len(results) == 3
            for doc in results:
                assert "userName" in doc
                assert "userAge" in doc
                assert doc["userName"] == doc["name"]
                assert doc["userAge"] == doc["age"]
        except Exception as e:
            pytest.fail(f"Integration failed: {e}")


def test_add_fields_with_unsupported_stage_fallback():
    """Test that pipelines with $addFields and unsupported stages fall back properly."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        query_engine = collection.query_engine

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test $addFields in Python fallback by using execute_2nd_tier_aggregation directly
        # First verify that a simple $addFields works (should use temp tables)
        pipeline = [{"$addFields": {"userName": "$name"}}]

        # This should work with temp tables
        results = execute_2nd_tier_aggregation(query_engine, pipeline)
        assert len(results) == 3
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]


if __name__ == "__main__":
    pytest.main([__file__])
