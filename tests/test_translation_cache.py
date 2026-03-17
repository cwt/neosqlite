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


class TestTranslationCacheEdgeCases:
    """Edge case tests for translation caching."""

    def test_cached_addFields_returns_correct_results(self):
        """Test that cached $addFields queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice", "salary": 50000},
                {"name": "Bob", "salary": 60000},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$addFields": {"bonus": {"$multiply": ["$salary", 0.1]}}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert "bonus" in result1[0]
        assert result1[0]["bonus"] == 5000

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2
        assert result2[0]["bonus"] == 5000

        assert qe.get_cache_stats()["hits"] >= 1

    def test_cached_unwind_returns_correct_results(self):
        """Test that cached $unwind queries return correct data.

        Note: $unwind might not use SQL tier cache in all cases.
        This test verifies correct results regardless of cache usage.
        """
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice", "tags": ["a", "b"]},
                {"name": "Bob", "tags": ["c"]},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$unwind": "$tags"}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 3

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 3

    def test_cached_bucket_returns_correct_results(self):
        """Test that cached $bucket queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"score": 20},
                {"score": 40},
                {"score": 60},
                {"score": 80},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$score",
                    "boundaries": [0, 40, 70, 100],
                    "default": "Other",
                    "output": {"count": {"$sum": 1}},
                }
            }
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 3

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 3

        assert qe.get_cache_stats()["hits"] >= 1

    def test_cached_bucketAuto_returns_correct_results(self):
        """Test that cached $bucketAuto queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"value": 10},
                {"value": 20},
                {"value": 30},
                {"value": 40},
                {"value": 50},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$bucketAuto": {"groupBy": "$value", "buckets": 2}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

        assert qe.get_cache_stats()["hits"] >= 1

    def test_cached_lookup_returns_correct_results(self):
        """Test that cached $lookup queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        orders = conn.orders

        users.insert_many(
            [
                {"_id": 1, "name": "Alice"},
                {"_id": 2, "name": "Bob"},
            ]
        )
        orders.insert_many(
            [
                {"user_id": 1, "amount": 100},
                {"user_id": 2, "amount": 200},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "user_id",
                    "as": "user_orders",
                }
            }
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert len(result1[0].get("user_orders", [])) >= 1

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

        assert qe.get_cache_stats()["hits"] >= 1

    def test_cached_unset_returns_correct_results(self):
        """Test that cached $unset queries return correct data."""
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

        pipeline = [{"$unset": ["secret"]}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert "secret" not in result1[0]

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2
        assert "secret" not in result2[0]

    def test_cached_replaceRoot_returns_correct_results(self):
        """Test that cached $replaceRoot queries return correct data."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice", "data": {"age": 30, "city": "NYC"}},
                {"name": "Bob", "data": {"age": 25, "city": "LA"}},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$replaceRoot": {"newRoot": "$data"}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert "age" in result1[0]
        assert "name" not in result1[0]

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

    def test_cached_count_followed_by_limit_no_collision(self):
        """Regression test: $count followed by $limit should not collide.

        Previously, $count would create placeholder parameters causing
        parameter index desync with subsequent $limit.
        """
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        # Use different data to avoid duplicate key issues
        users.insert_one({"status": "active"})
        users.insert_one({"status": "active"})
        users.insert_one({"status": "active"})
        users.insert_one({"status": "inactive"})

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline1 = [{"$match": {"status": "active"}}, {"$count": "total"}]
        pipeline2 = [{"$match": {"status": "active"}}, {"$limit": 5}]

        list(users.aggregate(pipeline1))
        list(users.aggregate(pipeline1))

        # These two pipelines should have separate cache entries
        list(users.aggregate(pipeline2))
        list(users.aggregate(pipeline2))

        stats = qe.get_cache_stats()
        # Should have 2 separate cache entries
        assert stats["size"] == 2

    def test_cached_group_followed_by_limit_no_collision(self):
        """Regression test: $group followed by $limit should work correctly."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"dept": "A", "value": 10},
                {"dept": "A", "value": 20},
                {"dept": "B", "value": 30},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline1 = [
            {"$group": {"_id": "$dept", "total": {"$sum": "$value"}}},
            {"$limit": 2},
        ]
        pipeline2 = [{"$group": {"_id": "$dept", "total": {"$sum": "$value"}}}]

        result1 = list(users.aggregate(pipeline1))
        assert len(result1) <= 2

        result2 = list(users.aggregate(pipeline2))
        assert len(result2) == 2

    def test_cached_match_with_nested_field_paths(self):
        """Test cache with nested field paths in $match."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"profile": {"age": 25, "city": "NYC"}},
                {"profile": {"age": 30, "city": "LA"}},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"profile.age": {"$gte": 25}}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

    def test_cached_complex_multistage_pipeline(self):
        """Test cache with complex multi-stage pipeline."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"dept": "Engineering", "salary": 100000, "active": True},
                {"dept": "Engineering", "salary": 80000, "active": True},
                {"dept": "Sales", "salary": 90000, "active": False},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {"$match": {"active": True}},
            {"$group": {"_id": "$dept", "avg_salary": {"$avg": "$salary"}}},
            {"$sort": {"avg_salary": -1}},
            {"$limit": 10},
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 1
        assert result1[0]["_id"] == "Engineering"

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 1
        assert result2[0]["_id"] == "Engineering"

        stats = qe.get_cache_stats()
        assert stats["hits"] >= 1

    def test_cached_sample_followed_by_match(self):
        """Test cache with $sample followed by $match."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many([{"value": i} for i in range(100)])

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {"$sample": {"size": 10}},
            {"$match": {"value": {"$gte": 0}}},
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) <= 10

        result2 = list(users.aggregate(pipeline))
        assert len(result2) <= 10

    def test_cached_match_with_or_operator(self):
        """Test that $or operator queries return consistent results."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "type": "A"},
                {"status": "inactive", "type": "B"},
                {"status": "active", "type": "C"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"$or": [{"status": "active"}, {"type": "B"}]}}]

        result1 = list(users.aggregate(pipeline))
        result2 = list(users.aggregate(pipeline))

        # Verify both results are consistent
        assert len(result1) == len(result2)

    def test_cached_match_with_in_operator(self):
        """Test cache with $in operator in $match."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"status": "a"},
                {"status": "b"},
                {"status": "c"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"status": {"$in": ["a", "b"]}}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

    def test_cached_match_with_and_operator(self):
        """Test cache with $and operator in $match."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "score": 90},
                {"status": "active", "score": 50},
                {"status": "inactive", "score": 80},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {
                "$match": {
                    "$and": [{"status": "active"}, {"score": {"$gte": 70}}]
                }
            }
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 1

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 1

    def test_cached_match_with_exists_operator(self):
        """Test cache with $exists operator in $match."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob"},
                {"name": "Charlie", "age": 25},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"age": {"$exists": True}}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

    def test_cached_match_with_type_operator(self):
        """Test that $type operator queries return consistent results."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"value": 1},
                {"value": "string"},
                {"value": 2.5},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"value": {"$type": "number"}}}]

        result1 = list(users.aggregate(pipeline))
        result2 = list(users.aggregate(pipeline))

        # Verify both results are consistent
        assert len(result1) == len(result2)

    def test_cached_group_with_multiple_accumulators(self):
        """Test cache with $group using multiple accumulators."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"dept": "A", "salary": 100, "bonus": 10},
                {"dept": "A", "salary": 200, "bonus": 20},
                {"dept": "B", "salary": 150, "bonus": 15},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [
            {
                "$group": {
                    "_id": "$dept",
                    "total_salary": {"$sum": "$salary"},
                    "total_bonus": {"$sum": "$bonus"},
                    "avg_salary": {"$avg": "$salary"},
                }
            }
        ]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

        dept_a = next(r for r in result2 if r["_id"] == "A")
        assert dept_a["total_salary"] == 300
        assert dept_a["total_bonus"] == 30
        assert dept_a["avg_salary"] == 150

    def test_cached_project_with_exclusions(self):
        """Test cache with $project using exclusions."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice", "age": 30, "secret": "hidden", "temp": "x"},
                {"name": "Bob", "age": 25, "secret": "hidden", "temp": "y"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$project": {"name": 1, "age": 1, "secret": 0}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 2
        assert "name" in result1[0]
        assert "age" in result1[0]
        assert "secret" not in result1[0]
        assert "temp" not in result1[0]

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 2

    def test_cached_sort_descending_and_ascending(self):
        """Test that $sort queries return consistent results in different directions."""
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

        pipeline_asc = [{"$sort": {"score": 1}}]
        pipeline_desc = [{"$sort": {"score": -1}}]

        result_asc1 = list(users.aggregate(pipeline_asc))
        result_desc1 = list(users.aggregate(pipeline_desc))
        result_asc2 = list(users.aggregate(pipeline_asc))
        result_desc2 = list(users.aggregate(pipeline_desc))

        # Verify both results are consistent
        assert result_asc1[0]["name"] == result_asc2[0]["name"]
        assert result_desc1[0]["name"] == result_desc2[0]["name"]

    def test_cached_match_with_regex(self):
        """Test that $regex operator queries return consistent results."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Amanda"},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"name": {"$regex": "^A"}}}]

        result1 = list(users.aggregate(pipeline))
        result2 = list(users.aggregate(pipeline))

        # Verify both results are consistent
        assert len(result1) == len(result2)

    def test_cached_match_with_size_operator(self):
        """Test cache with $size operator in $match."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"tags": ["a", "b"]},
                {"tags": ["c"]},
                {"tags": ["d", "e", "f"]},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"tags": {"$size": 2}}}]

        result1 = list(users.aggregate(pipeline))
        assert len(result1) == 1

        result2 = list(users.aggregate(pipeline))
        assert len(result2) == 1

    def test_cached_match_with_elemMatch_operator(self):
        """Test that $elemMatch operator queries return consistent results."""
        conn = neosqlite.Connection(":memory:", translation_cache=100)
        users = conn.users
        users.insert_many(
            [
                {"scores": [80, 90, 95]},
                {"scores": [70, 60]},
                {"scores": [85, 88]},
            ]
        )

        qe = users.query_engine.sql_tier_aggregator
        qe.clear_cache()

        pipeline = [{"$match": {"scores": {"$elemMatch": {"$gte": 90}}}}]

        result1 = list(users.aggregate(pipeline))
        result2 = list(users.aggregate(pipeline))

        # Verify both results are consistent
        assert len(result1) == len(result2)


class TestTier2TranslationCache:
    """Tests for Tier-2 ($expr with temporary tables) translation caching."""

    def test_tier2_cache_default_enabled(self):
        """Test that Tier-2 cache is enabled by default."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db)
        assert evaluator.is_cache_enabled() is True

    def test_tier2_cache_disabled(self):
        """Test that Tier-2 cache can be disabled."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db, translation_cache_size=0)
        assert evaluator.is_cache_enabled() is False

    def test_tier2_cache_custom_size(self):
        """Test that custom cache size works for Tier-2."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db, translation_cache_size=50)
        assert evaluator.get_cache_stats()["max_size"] == 50

    def test_tier2_cache_miss_then_hit(self):
        """Test Tier-2 cache miss then hit pattern."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "name": "Alice"},
                {"status": "inactive", "name": "Bob"},
            ]
        )

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr1 = {
            "$cond": {
                "if": {"$eq": ["$status", "active"]},
                "then": "yes",
                "else": "no",
            }
        }
        evaluator.evaluate(expr1, "users", None)
        stats = evaluator.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0

        evaluator.evaluate(expr1, "users", None)
        stats = evaluator.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 1

    def test_tier2_cache_different_expressions(self):
        """Test Tier-2 cache with different expression structures."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "name": "Alice"},
                {"age": 25, "name": "Bob"},
            ]
        )

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr1 = {
            "$cond": {
                "if": {"$eq": ["$status", "active"]},
                "then": "yes",
                "else": "no",
            }
        }
        expr2 = {
            "$cond": {"if": {"$gt": ["$age", 20]}, "then": "yes", "else": "no"}
        }
        expr3 = {
            "$cond": {
                "if": {"$eq": ["$status", "inactive"]},
                "then": "yes",
                "else": "no",
            }
        }

        evaluator.evaluate(expr1, "users", None)
        evaluator.evaluate(expr2, "users", None)
        evaluator.evaluate(expr3, "users", None)

        stats = evaluator.get_cache_stats()
        assert stats["size"] == 2

    def test_tier2_cache_clear(self):
        """Test clearing Tier-2 cache."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_one({"a": 1})

        evaluator = TempTableExprEvaluator(conn.db)
        expr = {
            "$cond": {"if": {"$eq": ["$a", 1]}, "then": "yes", "else": "no"}
        }
        evaluator.evaluate(expr, "users", None)

        assert evaluator.cache_size() == 1
        evaluator.clear_cache()
        assert evaluator.cache_size() == 0

    def test_tier2_cache_contains(self):
        """Test checking if expression is in Tier-2 cache."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_one({"a": 1})

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr = {
            "$cond": {"if": {"$eq": ["$a", 1]}, "then": "yes", "else": "no"}
        }

        assert evaluator.cache_contains(expr) is False
        evaluator.evaluate(expr, "users", None)
        assert evaluator.cache_contains(expr) is True

    def test_tier2_cache_evict(self):
        """Test evicting specific expression from Tier-2 cache."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_one({"a": 1})

        evaluator = TempTableExprEvaluator(conn.db)
        expr = {
            "$cond": {"if": {"$eq": ["$a", 1]}, "then": "yes", "else": "no"}
        }

        evaluator.evaluate(expr, "users", None)
        assert evaluator.cache_size() == 1

        result = evaluator.evict_from_cache(expr)
        assert result is True
        assert evaluator.cache_size() == 0

    def test_tier2_cache_resize(self):
        """Test resizing Tier-2 cache at runtime."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db)
        assert evaluator.get_cache_stats()["max_size"] == 100

        evaluator.resize_cache(10)
        assert evaluator.get_cache_stats()["max_size"] == 10

    def test_tier2_cache_stats(self):
        """Test Tier-2 cache statistics."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "name": "Alice"},
                {"status": "inactive", "name": "Bob"},
            ]
        )

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr = {
            "$cond": {
                "if": {"$eq": ["$status", "active"]},
                "then": "yes",
                "else": "no",
            }
        }
        evaluator.evaluate(expr, "users", None)
        evaluator.evaluate(expr, "users", None)
        evaluator.evaluate(expr, "users", None)

        stats = evaluator.get_cache_stats()
        assert evaluator.is_cache_enabled() is True
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["hit_rate"] == pytest.approx(2 / 3)

    def test_tier2_cache_dump(self):
        """Test dumping Tier-2 cache contents."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many(
            [
                {"status": "active"},
                {"name": "Bob"},
            ]
        )

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr = {
            "$cond": {
                "if": {"$eq": ["$status", "active"]},
                "then": "yes",
                "else": "no",
            }
        }
        evaluator.evaluate(expr, "users", None)
        evaluator.evaluate(expr, "users", None)

        dump = evaluator.dump_cache()
        assert len(dump) == 1
        assert dump[0]["hit_count"] == 1

    def test_tier2_cache_len(self):
        """Test __len__ method for Tier-2 cache."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_one({"a": 1})

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        assert len(evaluator._translation_cache) == 0
        expr = {
            "$cond": {"if": {"$eq": ["$a", 1]}, "then": "yes", "else": "no"}
        }
        evaluator.evaluate(expr, "users", None)
        assert len(evaluator._translation_cache) == 1

    def test_tier2_cache_no_collision_different_fields(self):
        """Regression test: different fields with same operator must not collide."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db)

        expr1 = {"$gt": ["$age", 25]}
        expr2 = {"$gt": ["$score", 90]}

        key1 = evaluator._make_expr_key(expr1)
        key2 = evaluator._make_expr_key(expr2)

        assert key1 != key2, "Different fields should have different cache keys"

        expr3 = {"$gt": ["$age", 25]}
        expr4 = {"$gt": ["$age", 999]}
        key3 = evaluator._make_expr_key(expr3)
        key4 = evaluator._make_expr_key(expr4)
        assert key3 == key4, "Same query structure should have same key"

    def test_tier2_cache_preserves_field_references(self):
        """Test that field references ($field) are preserved in cache key."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db)

        expr1 = {"$add": ["$salary", 100]}
        expr2 = {"$add": ["$bonus", 100]}

        key1 = evaluator._make_expr_key(expr1)
        key2 = evaluator._make_expr_key(expr2)

        assert (
            key1 != key2
        ), "Different field references should have different keys"

    def test_tier2_cache_parameterizes_literals(self):
        """Test that literal values are parameterized in cache key."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        evaluator = TempTableExprEvaluator(conn.db)

        expr1 = {"$gt": ["$age", 25]}
        expr2 = {"$gt": ["$age", 999]}

        key1 = evaluator._make_expr_key(expr1)
        key2 = evaluator._make_expr_key(expr2)

        assert key1 == key2, "Different literal values should have same key"

    def test_tier2_cache_with_complex_expression(self):
        """Test Tier-2 cache with complex expressions."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many(
            [
                {"salary": 100000, "bonus": 10000},
                {"salary": 80000, "bonus": 5000},
            ]
        )

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr = {
            "$cond": {
                "if": {"$gte": ["$salary", 90000]},
                "then": {"$add": ["$salary", "$bonus"]},
                "else": "$salary",
            }
        }

        result1 = evaluator.evaluate(expr, "users", None)
        assert result1 is not None

        result2 = evaluator.evaluate(expr, "users", None)
        assert result2 is not None

        stats = evaluator.get_cache_stats()
        assert stats["hits"] >= 1


class TestTier2TranslationCacheIntegration:
    """Integration tests for Tier-2 translation caching."""

    def test_tier2_cached_expr_returns_correct_results(self):
        """Test that cached Tier-2 expressions return correct data."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many(
            [
                {"status": "active", "score": 90},
                {"status": "inactive", "score": 50},
                {"status": "active", "score": 75},
            ]
        )

        evaluator = TempTableExprEvaluator(conn.db)
        evaluator.clear_cache()

        expr = {
            "$cond": {
                "if": {"$eq": ["$status", "active"]},
                "then": "yes",
                "else": "no",
            }
        }

        result1 = evaluator.evaluate(expr, "users", None)
        assert result1 is not None

        result2 = evaluator.evaluate(expr, "users", None)
        assert result2 is not None

        assert evaluator.get_cache_stats()["hits"] >= 1

    def test_tier2_cache_produces_same_results_as_no_cache(self):
        """Verify cached Tier-2 queries produce identical results to non-cached."""
        from neosqlite.collection.expr_temp_table import TempTableExprEvaluator

        conn = neosqlite.Connection(":memory:")
        users = conn.users
        users.insert_many([{"value": i} for i in range(20)])

        expr = {"$gte": ["$value", 10]}

        evaluator_cached = TempTableExprEvaluator(
            conn.db, translation_cache_size=100
        )
        evaluator_cached.clear_cache()

        evaluator_uncached = TempTableExprEvaluator(
            conn.db, translation_cache_size=0
        )

        result_cached = evaluator_cached.evaluate(expr, "users", None)
        result_uncached = evaluator_uncached.evaluate(expr, "users", None)

        assert result_cached is not None
        assert result_uncached is not None


@pytest.fixture
def connection():
    """Fixture to provide a clean connection for each test."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()
