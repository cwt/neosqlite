"""Tests for MongoDB stage aliases ($set, $sortByCount)."""

import pytest

import neosqlite
from neosqlite.collection.query_helper.pipeline_aliases import (
    normalize_pipeline_aliases,
)
from neosqlite.collection.query_helper.utils import set_force_fallback


class TestAliasNormalization:
    def test_no_aliases_returns_original(self):
        pipeline = [{"$match": {"a": 1}}, {"$sort": {"b": -1}}]
        assert normalize_pipeline_aliases(pipeline) is pipeline

    def test_set_is_rewritten(self):
        pipeline = [{"$set": {"x": "$y"}}]
        assert normalize_pipeline_aliases(pipeline) == [
            {"$addFields": {"x": "$y"}}
        ]

    def test_set_with_non_dict_spec_raises_clear_error(self):
        import pytest

        from neosqlite.exceptions import MalformedQueryException

        with pytest.raises(MalformedQueryException, match="must be an object"):
            normalize_pipeline_aliases([{"$set": "foo"}])

    def test_sort_by_count_is_rewritten(self):
        pipeline = [{"$sortByCount": "$cat"}]
        assert normalize_pipeline_aliases(pipeline) == [
            {"$group": {"_id": "$cat", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]

    def test_nested_facet_and_lookup_are_rewritten(self):
        pipeline = [
            {"$facet": {"x": [{"$set": {"a": 1}}, {"$sortByCount": "$cat"}]}},
            {
                "$lookup": {
                    "from": "other",
                    "pipeline": [{"$set": {"b": 2}}],
                    "as": "joined",
                }
            },
        ]
        result = normalize_pipeline_aliases(pipeline)
        assert result[0]["$facet"]["x"] == [
            {"$addFields": {"a": 1}},
            {"$group": {"_id": "$cat", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        assert result[1]["$lookup"]["pipeline"] == [{"$addFields": {"b": 2}}]


class TestAliasAggregation:
    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "cat": "a", "v": 3},
                {"_id": 2, "cat": "b", "v": 1},
                {"_id": 3, "cat": "a", "v": 2},
                {"_id": 4, "cat": "b", "v": 5},
                {"_id": 5, "cat": "c", "v": 4},
            ]
        )
        yield coll
        conn.close()

    @pytest.mark.parametrize("force_python", [False, True])
    def test_set_stage(self, collection, force_python):
        set_force_fallback(force_python)
        results = list(
            collection.aggregate(
                [
                    {"$set": {"doubled": {"$multiply": ["$v", 2]}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        assert [doc["doubled"] for doc in results] == [6, 2, 4, 10, 8]
        assert all("v" in doc for doc in results)

    @pytest.mark.parametrize("force_python", [False, True])
    def test_sort_by_count(self, collection, force_python):
        set_force_fallback(force_python)
        results = list(collection.aggregate([{"$sortByCount": "$cat"}]))
        assert {doc["_id"]: doc["count"] for doc in results} == {
            "a": 2,
            "b": 2,
            "c": 1,
        }
        assert [doc["count"] for doc in results] == sorted(
            (doc["count"] for doc in results), reverse=True
        )

    def test_sort_by_count_expression(self, collection):
        results = list(
            collection.aggregate([{"$sortByCount": {"$mod": ["$v", 2]}}])
        )
        assert {doc["_id"]: doc["count"] for doc in results} == {1: 3, 0: 2}

    def test_explain_reports_tier(self, collection):
        plan = collection.query_engine.explain_aggregation([{"$set": {"x": 1}}])
        assert plan["tier"] in (1, 2, 3)
        plan = collection.query_engine.explain_aggregation(
            [{"$sortByCount": "$cat"}]
        )
        assert plan["tier"] in (1, 2, 3)
