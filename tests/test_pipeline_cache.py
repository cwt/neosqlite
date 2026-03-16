"""
Tests for pipeline translation caching.
"""

import pytest
import neosqlite


class TestPipelineCache:
    """Tests for pipeline caching functionality."""

    def test_cache_default_enabled(self, connection):
        """Test that cache is enabled by default."""
        users = connection.users
        users.insert_one({"a": 1})
        qe = users.query_engine.sql_tier_aggregator
        assert qe.is_cache_enabled() is True
        assert qe.get_cache_stats()["max_size"] == 100

    def test_cache_disabled(self):
        """Test that cache can be disabled."""
        conn = neosqlite.Connection(":memory:", pipeline_cache=0)
        users = conn.users
        users.insert_one({"a": 1})
        qe = users.query_engine.sql_tier_aggregator
        assert qe.is_cache_enabled() is False

    def test_cache_custom_size(self):
        """Test that custom cache size works."""
        conn = neosqlite.Connection(":memory:", pipeline_cache=50)
        users = conn.users
        users.insert_one({"a": 1})
        qe = users.query_engine.sql_tier_aggregator
        assert qe.get_cache_stats()["max_size"] == 50

    def test_cache_miss_then_hit(self, connection):
        """Test cache miss then hit pattern."""
        users = connection.users
        users.insert_many(
            [
                {"status": "active", "name": "Alice"},
                {"status": "inactive", "name": "Bob"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        # First query - cache miss
        list(users.aggregate([{"$match": {"status": "active"}}]))
        stats = qe.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0

        # Same structure query - cache hit
        list(users.aggregate([{"$match": {"status": "inactive"}}]))
        stats = qe.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 1
        assert stats["hit_rate"] == 0.5

    def test_cache_different_structures(self, connection):
        """Test cache with different pipeline structures."""
        users = connection.users
        users.insert_many(
            [
                {"status": "active", "name": "Alice"},
                {"age": 25, "name": "Bob"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        # Different structures create separate cache entries
        list(users.aggregate([{"$match": {"status": "active"}}]))
        list(users.aggregate([{"$match": {"age": {"$gt": 20}}}]))
        list(users.aggregate([{"$match": {"status": "inactive"}}]))

        stats = qe.get_cache_stats()
        assert stats["size"] == 2  # Two different field structures

    def test_cache_clear(self, connection):
        """Test clearing cache."""
        users = connection.users
        users.insert_one({"a": 1})

        qe = users.query_engine.sql_tier_aggregator
        list(users.aggregate([{"$match": {"a": 1}}]))

        assert qe.cache_size() == 1
        qe.clear_cache()
        assert qe.cache_size() == 0

    def test_cache_contains(self, connection):
        """Test checking if pipeline is in cache."""
        users = connection.users
        users.insert_one({"a": 1})

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"a": 1}}]

        assert qe.cache_contains(pipeline) is False
        list(users.aggregate(pipeline))
        assert qe.cache_contains(pipeline) is True

    def test_cache_evict(self, connection):
        """Test evicting specific pipeline from cache."""
        users = connection.users
        users.insert_one({"a": 1})

        qe = users.query_engine.sql_tier_aggregator
        pipeline = [{"$match": {"a": 1}}]

        list(users.aggregate(pipeline))
        assert qe.cache_size() == 1

        result = qe.evict_from_cache(pipeline)
        assert result is True
        assert qe.cache_size() == 0

    def test_cache_resize(self, connection):
        """Test resizing cache at runtime."""
        users = connection.users
        users.insert_one({"a": 1})

        qe = users.query_engine.sql_tier_aggregator
        assert qe.get_cache_stats()["max_size"] == 100

        qe.resize_cache(10)
        assert qe.get_cache_stats()["max_size"] == 10

    def test_cache_stats(self, connection):
        """Test cache statistics."""
        users = connection.users
        users.insert_many(
            [
                {"status": "active", "name": "Alice"},
                {"status": "inactive", "name": "Bob"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        # Run queries - different values for same structure hit cache
        list(
            users.aggregate([{"$match": {"status": "active"}}])
        )  # miss - builds SQL
        list(
            users.aggregate([{"$match": {"status": "active"}}])
        )  # hit - uses cached SQL
        list(
            users.aggregate([{"$match": {"status": "inactive"}}])
        )  # hit - uses cached SQL, new param

        stats = qe.get_cache_stats()

        assert stats["enabled"] is True
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["hit_rate"] == pytest.approx(2 / 3)

    def test_cache_dump(self, connection):
        """Test dumping cache contents."""
        users = connection.users
        users.insert_many(
            [
                {"status": "active"},
                {"name": "Bob"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        list(users.aggregate([{"$match": {"status": "active"}}]))
        list(users.aggregate([{"$match": {"status": "active"}}]))  # hit

        dump = qe.dump_cache()
        assert len(dump) == 1
        assert dump[0]["hit_count"] == 1
        assert "$match" in dump[0]["key"]

    def test_cache_len(self, connection):
        """Test __len__ method."""
        users = connection.users
        users.insert_one({"a": 1})

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        assert len(qe._pipeline_cache) == 0
        list(users.aggregate([{"$match": {"a": 1}}]))
        assert len(qe._pipeline_cache) == 1


@pytest.fixture
def connection():
    """Fixture to provide a clean connection for each test."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()
