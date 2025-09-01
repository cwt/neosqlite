"""
Test for force fallback (kill switch) functionality.
"""

import pytest
from neosqlite import Connection
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)
from neosqlite.temporary_table_aggregation import integrate_with_neosqlite


def test_force_fallback_kill_switch():
    """Test that force fallback (kill switch) works correctly."""
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

        # Verify kill switch is off by default
        assert get_force_fallback() == False

        # Test a pipeline that should work with temp tables
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$addFields": {"userName": "$name"}},
        ]

        # With kill switch off, should use temp tables
        results = integrate_with_neosqlite(query_engine, pipeline)
        assert len(results) == 2
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]

        # Turn on the kill switch
        set_force_fallback(True)
        assert get_force_fallback() == True

        # With kill switch on, should fall back to Python even for supported pipelines
        results = integrate_with_neosqlite(query_engine, pipeline)
        assert len(results) == 2
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]

        # Turn off the kill switch
        set_force_fallback(False)
        assert get_force_fallback() == False

        # Should work normally again
        results = integrate_with_neosqlite(query_engine, pipeline)
        assert len(results) == 2
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]


if __name__ == "__main__":
    pytest.main([__file__])
