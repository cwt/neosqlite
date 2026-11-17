"""Tests for MongoDB-compatible ``$meta`` expression handling.

MongoDB only provides text score metadata after a ``$text`` match. Without it,
``$meta: "textScore"`` is an error; ``$meta: "indexKey"`` is a no-op, and the
internal ``_textScore`` field is never returned in aggregation output. These
tests verify the shared validation layer applies uniformly across tiers.
"""

import pytest

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.query_helper.utils import (
    get_force_fallback,
    set_force_fallback,
)
from neosqlite.exceptions import MalformedQueryException


class TestMetaExpression:
    """Validation, normalization and tier behavior for ``$meta``."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Always restore normal tier selection after a test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "title": "Python basics", "category": "lang"},
                {"_id": 2, "title": "Advanced Python", "category": "lang"},
                {"_id": 3, "title": "SQL guide", "category": "db"},
            ]
        )
        coll.create_index("title", fts=True)
        yield coll
        conn.close()

    def test_sql_converter_emits_stored_score(self):
        """The SQL converter reads the persisted score with a 0.0 fallback."""
        sql, params = ExprEvaluator(data_column="data").build_select_expression(
            {"$meta": "textScore"}
        )
        assert "COALESCE" in sql
        assert "_textScore" in sql
        assert params == []

    @pytest.mark.parametrize("force_fallback", [False, True])
    def test_textscore_without_text_match_raises(
        self, collection, force_fallback
    ):
        """Every tier rejects ``textScore`` without text score metadata."""
        set_force_fallback(force_fallback)
        pipeline = [
            {"$addFields": {"score": {"$meta": "textScore"}}},
        ]
        with pytest.raises(
            MalformedQueryException, match="text score metadata"
        ):
            list(collection.aggregate(pipeline))

    def test_textscore_with_text_match_has_no_internal_leak(self, collection):
        """A scored pipeline exposes the score but not ``_textScore``."""
        pipeline = [
            {"$match": {"$text": {"$search": "Python"}}},
            {"$addFields": {"relevance": {"$meta": "textScore"}}},
            {"$sort": {"_id": 1}},
        ]
        assert (
            collection.query_engine.explain_aggregation(pipeline)["tier"] == 2
        )
        results = list(collection.aggregate(pipeline))

        assert [doc["_id"] for doc in results] == [1, 2]
        assert all(doc["relevance"] > 0 for doc in results)
        assert all("_textScore" not in doc for doc in results)

    def test_textscore_in_facet_subpipeline_inherits_metadata(self, collection):
        """``$facet`` sub-pipelines see the outer ``$text`` metadata."""
        pipeline = [
            {"$match": {"$text": {"$search": "Python"}}},
            {
                "$facet": {
                    "scored": [{"$project": {"score": {"$meta": "textScore"}}}]
                }
            },
        ]
        results = list(collection.aggregate(pipeline))
        assert len(results) == 1
        assert all(doc["score"] > 0 for doc in results[0]["scored"])
        assert all("_textScore" not in doc for doc in results[0]["scored"])

    def test_index_key_field_is_omitted(self, collection):
        """A direct ``$meta: indexKey`` field is not added, like MongoDB."""
        pipeline = [
            {
                "$addFields": {
                    "k": {"$meta": "indexKey"},
                    "kept": 1,
                }
            },
            {"$project": {"k": {"$meta": "indexKey"}, "kept": 1}},
            {"$sort": {"_id": 1}},
        ]
        results = list(collection.aggregate(pipeline))
        assert all("k" not in doc for doc in results)
        assert [doc["kept"] for doc in results] == [1, 1, 1]

    def test_index_key_nested_evaluates_to_null(self, collection):
        """Nested ``$meta: indexKey`` evaluates to ``null``, like MongoDB."""
        pipeline = [
            {
                "$addFields": {
                    "value": {"$add": [{"$meta": "indexKey"}, 1]},
                }
            },
            {"$project": {"_id": 1, "value": 1}},
            {"$sort": {"_id": 1}},
        ]
        results = list(collection.aggregate(pipeline))
        assert all(doc["value"] is None for doc in results)

    def test_unknown_meta_field_raises(self, collection):
        """Unknown ``$meta`` fields mirror MongoDB's error."""
        pipeline = [
            {"$addFields": {"value": {"$meta": "unknownField"}}},
        ]
        with pytest.raises(
            MalformedQueryException, match="Unsupported \\$meta field"
        ):
            list(collection.aggregate(pipeline))

    def test_force_fallback_flag_is_still_respected(self, collection):
        """The kill switch continues to override optimized paths."""
        pipeline = [
            {"$match": {"$text": {"$search": "Python"}}},
            {"$project": {"score": {"$meta": "textScore"}}},
        ]
        set_force_fallback(True)
        try:
            plan = collection.query_engine.explain_aggregation(pipeline)
        finally:
            set_force_fallback(False)
        assert get_force_fallback() is False
        assert plan["tier"] == 3
