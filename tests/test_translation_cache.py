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
        """Test cache miss then hit pattern with correct results."""
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
        result1 = list(users.aggregate([{"$match": {"status": "active"}}]))
        stats = qe.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0
        assert len(result1) == 1
        assert result1[0]["name"] == "Alice"

        # Same structure query - cache hit
        result2 = list(users.aggregate([{"$match": {"status": "inactive"}}]))
        stats = qe.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 1
        assert stats["hit_rate"] == 0.5
        assert len(result2) == 1
        assert result2[0]["name"] == "Bob"

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

    def test_cache_count_no_placeholder(self):
        """Test that $count doesn't create unnecessary placeholders.

        $count doesn't take any input parameters, so it shouldn't create
        placeholder parameters. This was a bug where $count would cause
        index desync with subsequent stages like $limit or $skip.
        """
        from neosqlite.collection.query_helper.translation_cache import (
            TranslationCache,
        )

        cache = TranslationCache()

        # $count with different output field names - the SQL is the same
        key1 = cache.make_key([{"$count": "total"}])
        key2 = cache.make_key([{"$count": "count"}])

        # Key includes output field name - this is fine for correctness
        # though could be optimized to share cache entries
        assert key1 == "$count:total"
        assert key2 == "$count:count"

    def test_cache_group_no_collision_different_fields(self):
        """Regression test: $group with different _id fields must not collide.

        Previously, grouping by $dept and $category would generate the same cache key
        because field references were being parameterized. Now they are preserved.
        """
        from neosqlite.collection.query_helper.translation_cache import (
            TranslationCache,
        )

        cache = TranslationCache()

        # Different group fields should have different keys
        key1 = cache.make_key(
            [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}]
        )
        key2 = cache.make_key(
            [{"$group": {"_id": "$category", "total": {"$sum": "$sales"}}}]
        )
        key3 = cache.make_key(
            [{"$group": {"_id": "$status", "total": {"$sum": 1}}}]
        )

        assert (
            key1 != key2
        ), "$group with different _id fields should have different keys"
        assert (
            key2 != key3
        ), "$group with different _id fields should have different keys"

        # Same group field with different accumulator values should have same key
        cache.make_key(
            [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}]
        )
        cache.make_key([{"$group": {"_id": "$dept", "count": {"$count": 1}}}])
        # These should be different because they have different output field names

    def test_cache_group_different_accumulators_same_field(self):
        """Test that $group with same field but different accumulators are distinguished."""
        from neosqlite.collection.query_helper.translation_cache import (
            TranslationCache,
        )

        cache = TranslationCache()

        # Same _id field but different accumulators
        key1 = cache.make_key(
            [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}]
        )
        key2 = cache.make_key(
            [{"$group": {"_id": "$dept", "avg": {"$avg": "$salary"}}}]
        )
        key3 = cache.make_key(
            [{"$group": {"_id": "$dept", "max": {"$max": "$salary"}}}]
        )

        assert key1 != key2, "Different accumulators should have different keys"
        assert key2 != key3, "Different accumulators should have different keys"

        # Same query should have same key
        key4 = cache.make_key(
            [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}]
        )
        assert key1 == key4, "Same query structure should have same key"


class TestTranslationCacheIntegration:
    """Integration tests verifying cached queries return correct results."""

    def test_cached_match_returns_correct_results(self):
        """Test that cached $match queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "score": 90},
                {"status": "inactive", "score": 50},
                {"status": "active", "score": 75},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(users.aggregate([{"$match": {"status": "active"}}]))
        assert len(result1) == 2
        assert all(r["status"] == "active" for r in result1)

        result2 = list(users.aggregate([{"$match": {"status": "active"}}]))
        assert len(result2) == 2
        assert all(r["status"] == "active" for r in result2)

        assert qe.get_cache_stats()["hits"] >= 1

    def test_cached_limit_returns_correct_results(self):
        """Test that cached $limit queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many([{"value": i} for i in range(10)])

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(users.aggregate([{"$limit": 3}]))
        assert len(result1) == 3

        result2 = list(users.aggregate([{"$limit": 5}]))
        assert len(result2) == 5

        result3 = list(users.aggregate([{"$limit": 3}]))
        assert len(result3) == 3

        assert qe.get_cache_stats()["hits"] >= 1

    def test_cached_skip_returns_correct_results(self):
        """Test that cached $skip queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many([{"value": i} for i in range(10)])

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(users.aggregate([{"$skip": 5}]))
        assert len(result1) == 5
        assert result1[0]["value"] == 5

        result2 = list(users.aggregate([{"$skip": 5}]))
        assert len(result2) == 5
        assert result2[0]["value"] == 5

    def test_cached_sample_returns_correct_results(self):
        """Test that cached $sample queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many([{"value": i} for i in range(100)])

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(users.aggregate([{"$sample": {"size": 10}}]))
        assert len(result1) == 10

        result2 = list(users.aggregate([{"$sample": {"size": 10}}]))
        assert len(result2) == 10

    def test_cached_multistage_pipeline(self):
        """Test that cached multi-stage pipelines return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"dept": "Engineering", "salary": 100000},
                {"dept": "Engineering", "salary": 80000},
                {"dept": "Sales", "salary": 90000},
                {"dept": "Sales", "salary": 70000},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {"$match": {"dept": "Engineering"}},
            {"$sort": {"salary": -1}},
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert result1[0]["salary"] == 100000
        assert result1[1]["salary"] == 80000

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2
        assert result2[0]["salary"] == 100000
        assert result2[1]["salary"] == 80000

    def test_cached_group_returns_correct_results(self):
        """Test that cached $group queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"dept": "Engineering", "salary": 100000},
                {"dept": "Engineering", "salary": 80000},
                {"dept": "Sales", "salary": 90000},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

        totals = {r["_id"]: r["total"] for r in result2}
        assert totals["Engineering"] == 180000
        assert totals["Sales"] == 90000

    def test_cached_group_no_collision_different_fields(self):
        """Regression test: $group with different fields must return correct results.

        Previously, grouping by $dept and then $category would collide in cache,
        returning wrong results. Now they have separate cache entries.
        """
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users

        # Data with both dept and category
        users.insert_many(
            [
                {"dept": "Engineering", "category": "A", "value": 100},
                {"dept": "Engineering", "category": "A", "value": 200},
                {"dept": "Sales", "category": "B", "value": 150},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        # First: group by dept
        result1 = list(
            users.aggregate(
                [{"$group": {"_id": "$dept", "total": {"$sum": "$value"}}}]
            )
        )
        assert len(result1) == 2
        totals1 = {r["_id"]: r["total"] for r in result1}
        assert totals1["Engineering"] == 300
        assert totals1["Sales"] == 150

        # Second: group by category - should use DIFFERENT cache key
        result2 = list(
            users.aggregate(
                [{"$group": {"_id": "$category", "total": {"$sum": "$value"}}}]
            )
        )
        assert len(result2) == 2
        totals2 = {r["_id"]: r["total"] for r in result2}
        assert totals2["A"] == 300
        assert totals2["B"] == 150

        # Third: group by dept again - should hit cache
        result3 = list(
            users.aggregate(
                [{"$group": {"_id": "$dept", "total": {"$sum": "$value"}}}]
            )
        )
        assert len(result3) == 2
        totals3 = {r["_id"]: r["total"] for r in result3}
        assert totals3["Engineering"] == 300
        assert totals3["Sales"] == 150

        stats = qe.get_cache_stats()
        assert stats["size"] == 2, "Should have 2 separate cache entries"
        assert stats["hits"] == 1, "Should have 1 cache hit"

    def test_cached_count_returns_correct_results(self):
        """Test that cached $count queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"status": "active"},
                {"status": "active"},
                {"status": "inactive"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(
            users.aggregate(
                [{"$match": {"status": "active"}}, {"$count": "active_count"}]
            )
        )
        assert len(result1) == 1
        assert result1[0]["active_count"] == 2

        result2 = list(
            users.aggregate(
                [{"$match": {"status": "active"}}, {"$count": "active_count"}]
            )
        )
        assert len(result2) == 1
        assert result2[0]["active_count"] == 2

    def test_cached_project_returns_correct_results(self):
        """Test that cached $project queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice", "age": 30, "secret": "hidden"},
                {"name": "Bob", "age": 25, "secret": "hidden"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$project": {"name": 1, "age": 1}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert "name" in result1[0]
        assert "age" in result1[0]
        assert "secret" not in result1[0]

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2
        assert "name" in result2[0]
        assert "age" in result2[0]

    def test_cached_sort_returns_correct_results(self):
        """Test that cached $sort queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Charlie", "score": 50},
                {"name": "Alice", "score": 100},
                {"name": "Bob", "score": 75},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(users.aggregate([{"$sort": {"score": 1}}]))
        assert result1[0]["name"] == "Charlie"
        assert result1[1]["name"] == "Bob"
        assert result1[2]["name"] == "Alice"

        result2 = list(users.aggregate([{"$sort": {"score": 1}}]))
        assert result2[0]["name"] == "Charlie"
        assert result2[1]["name"] == "Bob"
        assert result2[2]["name"] == "Alice"

    def test_cache_disabled_returns_correct_results(self):
        """Test that disabling cache still returns correct results."""
        conn = neosqlite.Connection(":memory:", translation_cache=0)
        users = conn.users
        users.insert_many(
            [
                {"status": "active"},
                {"status": "inactive"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        assert qe.is_cache_enabled() is False

        result = list(users.aggregate([{"$match": {"status": "active"}}]))
        assert len(result) == 1
        assert result[0]["status"] == "active"

    def test_cache_produces_same_results_as_no_cache(self):
        """Verify cached queries produce identical results to non-cached."""
        conn_cached = neosqlite.Connection(":memory:", translation_cache=100)
        conn_uncached = neosqlite.Connection(":memory:", translation_cache=0)

        users_cached = conn_cached.users
        users_uncached = conn_uncached.users

        users_cached.insert_many(
            [{"status": "active", "score": i} for i in range(20)]
        )
        users_uncached.insert_many(
            [{"status": "active", "score": i} for i in range(20)]
        )

        pipeline = [
            {"$match": {"status": "active"}},
            {"$sort": {"score": -1}},
            {"$limit": 5},
        ]

        result_cached = list(users_cached.aggregate(pipeline))
        result_uncached = list(users_uncached.aggregate(pipeline))

        assert len(result_cached) == len(result_uncached) == 5
        for i in range(5):
            assert result_cached[i]["score"] == result_uncached[i]["score"]

    def test_different_operators_same_field_different_keys(self):
        """Test that different operators on same field get different cache keys."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"age": 20},
                {"age": 30},
                {"age": 40},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        list(users.aggregate([{"$match": {"age": {"$gt": 25}}}]))
        list(users.aggregate([{"$match": {"age": {"$lt": 35}}}]))
        list(users.aggregate([{"$match": {"age": {"$gte": 30}}}]))

        stats = qe.get_cache_stats()
        assert stats["size"] == 3

    def test_empty_pipeline(self):
        """Test handling of empty pipeline."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many([{"a": 1}, {"a": 2}])

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        result1 = list(users.aggregate([]))
        assert len(result1) == 2

        result2 = list(users.aggregate([]))
        assert len(result2) == 2


@pytest.fixture
def connection():
    """Fixture to provide a clean connection for each test."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()
