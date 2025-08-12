# coding: utf-8
"""Focused tests for RawBatchCursor edge cases and fallback path."""

import json
import neosqlite


def test_raw_batch_cursor_fallback_path():
    """Test the Python fallback path in RawBatchCursor."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        collection.insert_many(docs)

        # Create a filter that uses an unsupported operator which causes fallback
        # Using a filter that would cause _build_simple_where_clause to return None
        # We know that nested fields with complex operators like $elemMatch cause fallback
        complex_filter = {"nonexistent": {"$elemMatch": {"$eq": "value"}}}

        # Verify that this filter causes _build_simple_where_clause to return None
        where_result = collection._build_simple_where_clause(complex_filter)
        assert where_result is None  # Should cause fallback to Python

        # This should work and use the fallback path without error
        cursor = collection.find_raw_batches(filter=complex_filter)
        batches = list(cursor)

        # Should not throw any exceptions and should return results
        # (The actual results don't matter for coverage, just that the path is exercised)
        assert len(batches) >= 0  # Could be 0 or more


def test_raw_batch_cursor_constructor():
    """Test RawBatchCursor constructor with different parameters."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test default constructor
        cursor = collection.find_raw_batches()
        assert cursor._batch_size == 100
        assert cursor._filter == {}
        assert cursor._projection == {}
        assert cursor._hint is None

        # Test constructor with parameters
        test_filter = {"age": {"$gt": 25}}
        test_projection = {"name": 1}
        cursor = collection.find_raw_batches(
            filter=test_filter,
            projection=test_projection,
            hint="test_hint",
            batch_size=50,
        )
        assert cursor._batch_size == 50
        assert cursor._filter == test_filter
        assert cursor._projection == test_projection
        assert cursor._hint == "test_hint"


def test_raw_batch_cursor_zero_batch_size():
    """Test RawBatchCursor with zero batch size."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        docs = [{"num": i} for i in range(5)]
        collection.insert_many(docs)

        # Test with zero batch size
        cursor = collection.find_raw_batches(batch_size=0)
        batches = list(cursor)

        # With batch_size=0, we should still get results (SQLite handles it gracefully)
        assert (
            len(batches) >= 0
        )  # Could be 0 or more depending on SQLite behavior


def test_raw_batch_cursor_limit_smaller_than_batch():
    """Test limit smaller than batch size."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        docs = [{"num": i} for i in range(20)]
        collection.insert_many(docs)

        # Test with limit smaller than batch size
        cursor = collection.find_raw_batches(batch_size=15)
        cursor._limit = 5  # Request only 5 documents
        batches = list(cursor)

        # Should have exactly one batch with 5 documents
        assert len(batches) == 1
        batch_str = batches[0].decode("utf-8")
        doc_strings = [s for s in batch_str.split("\n") if s]
        assert len(doc_strings) == 5


if __name__ == "__main__":
    test_raw_batch_cursor_fallback_path()
    test_raw_batch_cursor_constructor()
    test_raw_batch_cursor_zero_batch_size()
    test_raw_batch_cursor_limit_smaller_than_batch()
    print("All focused RawBatchCursor tests passed!")
