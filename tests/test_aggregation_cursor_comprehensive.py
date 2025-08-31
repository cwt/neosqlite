"""
Tests for the AggregationCursor class.
"""

import pytest
import neosqlite
from neosqlite.aggregation_cursor import AggregationCursor


class TestAggregationCursor:
    """Test cases for the AggregationCursor class."""

    def test_init(self, collection):
        """Test AggregationCursor initialization."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        assert cursor.collection == collection
        assert cursor.pipeline == pipeline
        assert cursor._results is None
        assert cursor._position == 0
        assert cursor._executed is False
        assert cursor._batch_size == 1000
        assert cursor._memory_threshold == 100 * 1024 * 1024
        assert cursor._use_quez is False

    def test_iter(self, collection):
        """Test AggregationCursor iteration."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return the cursor itself
        iter_result = iter(cursor)
        assert iter_result is cursor

        # Should have executed the pipeline
        assert cursor._executed is True

    def test_next(self, collection):
        """Test AggregationCursor next method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Get first result
        result1 = next(cursor)
        assert result1["a"] in [2, 3]

        # Get second result
        result2 = next(cursor)
        assert result2["a"] in [2, 3]
        assert result1["a"] != result2["a"]

        # Should raise StopIteration when no more results
        with pytest.raises(StopIteration):
            next(cursor)

    def test_len(self, collection):
        """Test AggregationCursor len method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return the count of results
        assert len(cursor) == 2

    def test_getitem(self, collection):
        """Test AggregationCursor getitem method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$sort": {"a": 1}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should be able to access items by index
        assert cursor[0]["a"] == 1
        assert cursor[1]["a"] == 2
        assert cursor[2]["a"] == 3

        # Should raise IndexError for invalid index
        with pytest.raises(IndexError):
            _ = cursor[10]

    def test_sort(self, collection):
        """Test AggregationCursor sort method."""
        collection.insert_many([{"a": 3}, {"a": 1}, {"a": 2}])
        pipeline = [{"$match": {"a": {"$gte": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should be able to sort results
        cursor.sort(key=lambda doc: doc["a"])
        results = list(cursor)
        assert [doc["a"] for doc in results] == [1, 2, 3]

        # Should return self for chaining
        result = cursor.sort(key=lambda doc: doc["a"])
        assert result is cursor

    def test_to_list(self, collection):
        """Test AggregationCursor to_list method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return all results as a list
        results = cursor.to_list()
        assert len(results) == 2
        assert all(doc["a"] in [2, 3] for doc in results)

    def test_batch_size(self, collection):
        """Test AggregationCursor batch_size method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should set batch size and return self for chaining
        result = cursor.batch_size(500)
        assert result is cursor
        assert cursor._batch_size == 500

    def test_max_await_time_ms(self, collection):
        """Test AggregationCursor max_await_time_ms method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should set max await time and return self for chaining
        result = cursor.max_await_time_ms(5000)
        assert result is cursor

    def test_use_quez(self, collection):
        """Test AggregationCursor use_quez method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should set use_quez flag and return self for chaining
        result = cursor.use_quez(True)
        assert result is cursor
        # Note: Actual quez availability depends on system setup

    def test_get_quez_stats(self, collection):
        """Test AggregationCursor get_quez_stats method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return None when quez is not being used
        stats = cursor.get_quez_stats()
        assert stats is None


def test_aggregation_cursor_integration():
    """Integration test for AggregationCursor with actual data."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        test_docs = [
            {"name": "Alice", "age": 30, "department": "Engineering"},
            {"name": "Bob", "age": 25, "department": "Marketing"},
            {"name": "Charlie", "age": 35, "department": "Engineering"},
            {"name": "Diana", "age": 28, "department": "Sales"},
        ]
        collection.insert_many(test_docs)

        # Test a simple aggregation pipeline
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$sort": {"age": 1}},
        ]

        cursor = collection.aggregate(pipeline)

        # Verify it's an AggregationCursor
        assert isinstance(cursor, AggregationCursor)

        # Test iteration
        results = list(cursor)
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Charlie"

        # Test len
        cursor2 = collection.aggregate(pipeline)
        assert len(cursor2) == 2

        # Test to_list
        cursor3 = collection.aggregate(pipeline)
        results_list = cursor3.to_list()
        assert len(results_list) == 2
        assert all(isinstance(doc, dict) for doc in results_list)
