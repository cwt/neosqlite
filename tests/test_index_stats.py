"""Tests for the Tier-3 ``$indexStats`` aggregation stage."""

import pytest

import neosqlite
from neosqlite import ASCENDING
from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)
from neosqlite.exceptions import MalformedQueryException


class TestIndexStats:
    """MongoDB-shaped index statistics from ``$indexStats``."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.docs
        coll.insert_many([{"_id": 1, "a": 1, "b": "x"}])
        coll.create_index("a")
        coll.create_index([("b", ASCENDING), ("a", ASCENDING)])
        coll.create_index("b", unique=True)
        yield coll
        conn.close()

    @pytest.mark.parametrize("force_python", [False, True])
    def test_reports_all_indexes_with_mongodb_shape(
        self, collection, force_python
    ):
        set_force_fallback(force_python)
        results = list(collection.aggregate([{"$indexStats": {}}]))

        by_name = {doc["name"]: doc for doc in results}
        assert set(by_name) == {"_id_", "a_1", "b_1_a_1", "b_1"}
        assert by_name["_id_"]["key"] == {"_id": 1}
        assert by_name["a_1"]["key"] == {"a": 1}
        assert by_name["b_1_a_1"]["key"] == {"b": 1, "a": 1}
        assert by_name["b_1"]["spec"]["unique"] is True
        assert "unique" not in by_name["_id_"]["spec"]

        for doc in results:
            assert doc["spec"]["v"] == 2
            assert doc["spec"]["name"] == doc["name"]
            assert doc["spec"]["key"] == doc["key"]
            assert doc["accesses"]["ops"] == 0
            assert "since" in doc["accesses"]
            assert "host" in doc

    def test_uses_tier3(self, collection):
        tiers = []
        collection.query_engine.add_tier_change_callback(
            lambda old, new, pipeline: tiers.append(new)
        )
        list(collection.aggregate([{"$indexStats": {}}]))
        assert "tier3" in tiers

    def test_empty_collection_still_reports_id_index(self):
        with neosqlite.Connection(":memory:") as conn:
            results = list(conn.empty.aggregate([{"$indexStats": {}}]))
        assert len(results) == 1
        assert results[0]["name"] == "_id_"

    def test_only_valid_as_first_stage(self, collection):
        with pytest.raises(MalformedQueryException, match="first stage"):
            list(
                collection.aggregate(
                    [{"$match": {"a": 1}}, {"$indexStats": {}}]
                )
            )

    def test_tier1_rejects_index_stats(self, collection):
        plan = collection.query_engine.explain_aggregation(
            [{"$indexStats": {}}]
        )
        assert plan["tier"] == 3

    def test_index_stats_preserves_descending_explicit_id_and_custom_names(
        self,
    ):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs_custom
            coll.create_index("_id")
            coll.create_index([("score", -1)])
            coll.create_index("custom_field", name="my_custom_idx")

            results = list(coll.aggregate([{"$indexStats": {}}]))
            entries = [(doc["name"], doc["key"]) for doc in results]
            assert ("_id_", {"_id": 1}) in entries
            assert ("score_-1", {"score": -1}) in entries
            assert ("my_custom_idx", {"custom_field": 1}) in entries
            # Both automatic _id_ and explicit _id index are present
            assert sum(1 for _, key in entries if key == {"_id": 1}) == 2
