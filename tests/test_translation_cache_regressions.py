"""Regression tests for translation-cache correctness (#85, #86).

The cache previously keyed on pipeline *structure* only (literals replaced
by "?") and re-derived parameters on cache hits via an independent walk.
That served stale/wrong SQL: sort directions were erased and parameter
lists diverged from template placeholders. Keys now canonicalize literal
values and hits return the exact stored parameters.
"""


class TestSortDirectionCacheRegression:
    def test_descending_sort_not_served_ascending_from_cache(self, connection):
        c = connection.test
        c.insert_many([{"a": 3}, {"a": 1}, {"a": 2}])
        asc = [d["a"] for d in c.aggregate([{"$sort": {"a": 1}}])]
        desc = [d["a"] for d in c.aggregate([{"$sort": {"a": -1}}])]
        assert asc == [1, 2, 3]
        assert desc == [3, 2, 1]

    def test_multi_field_sort_direction_ordering(self, connection):
        c = connection.test
        c.insert_many(
            [
                {"a": 1, "b": 2},
                {"a": 1, "b": 1},
                {"a": 2, "b": 5},
            ]
        )
        r1 = [
            (d["a"], d["b"]) for d in c.aggregate([{"$sort": {"a": 1, "b": 1}}])
        ]
        r2 = [
            (d["a"], d["b"])
            for d in c.aggregate([{"$sort": {"a": 1, "b": -1}}])
        ]
        assert r1 == [(1, 1), (1, 2), (2, 5)]
        assert r2 == [(1, 2), (1, 1), (2, 5)]


class TestCacheHitParamsRegression:
    def test_double_match_second_run_not_corrupted(self, connection):
        """Two $match stages: cache hit used to resolve both names from the
        first occurrence, returning documents that must be excluded."""
        c = connection.test
        c.insert_many([{"a": 1}, {"a": 2}])
        p = [{"$match": {"a": 1}}, {"$match": {"a": 2}}]
        assert list(c.aggregate(p)) == []
        assert list(c.aggregate(p)) == []

    def test_in_list_params_survive_cache_hit(self, connection):
        c = connection.test
        c.insert_many([{"t": "a"}, {"t": "b"}, {"t": "c"}])
        p = [{"$match": {"t": {"$in": ["a", "b"]}}}]
        first = sorted(d["t"] for d in c.aggregate(p))  # cache miss
        again = sorted(d["t"] for d in c.aggregate(p))  # cache hit
        assert first == again == ["a", "b"]

    def test_limit_values_never_share_entries(self, connection):
        c = connection.test
        c.insert_many([{"i": i} for i in range(10)])
        short = list(c.aggregate([{"$limit": 2}]))
        long = list(c.aggregate([{"$limit": 9}]))
        assert len(short) == 2
        assert len(long) == 9

    def test_identical_pipeline_hits_cache_with_exact_results(self, connection):
        c = connection.test
        c.insert_many([{"g": "x", "n": 1}, {"g": "x", "n": 3}])
        p = [
            {"$match": {"g": "x"}},
            {"$group": {"_id": "$g", "total": {"$sum": "$n"}}},
        ]
        r1 = list(c.aggregate(p))
        r2 = list(c.aggregate(p))
        assert r1 == r2 == [{"_id": "x", "total": 4}]

    def test_expr_literal_bounds_not_shared_across_expressions(
        self, connection
    ):
        """Tier-2 $expr cache must not serve one expression's literals to a
        structurally identical expression with different bounds."""
        qe = connection.exprtest.query_engine
        col = connection.exprtest
        col.insert_many([{"n": v} for v in (1, 5, 10)])
        # Run twice each so the second execution exercises the cache path
        for _ in range(2):
            lo = list(col.find({"$expr": {"$gt": ["$n", 4]}}))
            hi = list(col.find({"$expr": {"$gt": ["$n", 8]}}))
            assert sorted(d["n"] for d in lo) == [5, 10]
            assert sorted(d["n"] for d in hi) == [10]
        assert qe is not None
