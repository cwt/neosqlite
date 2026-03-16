"""
Tests for SQL translation caching.
"""

import pytest
import neosqlite


class TestTranslationCache:
    """Tests for translation caching functionality."""

    def test_cache_default_enabled(self, connection):
        """Test that cache is enabled by default."""
        users = connection.users
        users.insert_one({"a": 1})
        qe = users.query_engine.sql_tier_aggregator
        assert qe.is_cache_enabled() is True
        assert qe.get_cache_stats()["max_size"] == 100

    def test_cache_disabled(self):
        """Test that cache can be disabled."""
        conn = neosqlite.Connection(":memory:", translation_cache=0)
        users = conn.users
        users.insert_one({"a": 1})
        qe = users.query_engine.sql_tier_aggregator
        assert qe.is_cache_enabled() is False

    def test_cache_custom_size(self):
        """Test that custom cache size works."""
        conn = neosqlite.Connection(":memory:", translation_cache=50)
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

        assert len(qe._translation_cache) == 0
        list(users.aggregate([{"$match": {"a": 1}}]))
        assert len(qe._translation_cache) == 1

    def test_cache_no_collision_different_fields_same_operator(self):
        """Regression test: different fields with same operator must not collide.

        Previously, age.$gt and score.$gt would generate the same cache key
        because the key builder only captured the operator ($gt), not the field name.
        """
        from neosqlite.collection.query_helper.translation_cache import (
            TranslationCache,
        )

        cache = TranslationCache()

        # Different fields using same operator must have different keys
        key1 = cache.make_key([{"$match": {"age": {"$gt": 25}}}])
        key2 = cache.make_key([{"$match": {"score": {"$gt": 90}}}])

        assert (
            key1 != key2
        ), "Cache key collision: age.$gt and score.$gt should differ"

        # Same query with different values should have same key (parameterized)
        key3 = cache.make_key([{"$match": {"age": {"$gt": 25}}}])
        key4 = cache.make_key([{"$match": {"age": {"$gt": 999}}}])
        assert key3 == key4, "Same query structure should have same key"

    def test_cache_multiple_parameterized_operators_different_order(self):
        """Test cache key is stable regardless of operator order in query.

        The key builder sorts dict keys, so {$gt: 1, $lt: 10} and {$lt: 10, $gt: 1}
        should produce the same cache key.
        """
        from neosqlite.collection.query_helper.translation_cache import (
            TranslationCache,
        )

        cache = TranslationCache()

        # Different order of same operators should produce same key
        key1 = cache.make_key([{"$match": {"field": {"$gt": 1, "$lt": 10}}}])
        key2 = cache.make_key([{"$match": {"field": {"$lt": 10, "$gt": 1}}}])
        assert key1 == key2, "Different operator order should produce same key"

        # Different fields should have different keys
        key3 = cache.make_key([{"$match": {"a": {"$gt": 1, "$lt": 10}}}])
        key4 = cache.make_key([{"$match": {"b": {"$gt": 1, "$lt": 10}}}])
        assert key3 != key4, "Different fields should have different keys"

    def test_cache_parameterized_limit_skip_sample(self):
        """Test parameterization works for $limit, $skip, $sample."""
        from neosqlite.collection.query_helper.translation_cache import (
            TranslationCache,
        )

        cache = TranslationCache()

        # $limit with different values should have same key
        key1 = cache.make_key([{"$limit": 10}])
        key2 = cache.make_key([{"$limit": 100}])
        assert key1 == key2, "$limit values should be parameterized"

        # $skip with different values should have same key
        key3 = cache.make_key([{"$skip": 5}])
        key4 = cache.make_key([{"$skip": 50}])
        assert key3 == key4, "$skip values should be parameterized"

        # $sample.size with different values should have same key
        key5 = cache.make_key([{"$sample": {"size": 20}}])
        key6 = cache.make_key([{"$sample": {"size": 100}}])
        assert key5 == key6, "$sample.size values should be parameterized"


@pytest.fixture
def connection():
    """Fixture to provide a clean connection for each test."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()
