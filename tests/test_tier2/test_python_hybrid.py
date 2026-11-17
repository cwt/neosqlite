"""Tests for per-stage Python fallback inside Tier 2.

A single stage whose SQL translation declines should not send the whole
pipeline to Tier 3. These tests force a stage to decline and verify the
pipeline stays in Tier 2, that later SQL stages still run, and that results
match the authoritative Python fallback.
"""

from datetime import UTC, datetime

import pytest

import neosqlite
from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)
from neosqlite.collection.sql_translator_unified import SQLTranslator
from neosqlite.collection.temporary_table_aggregation.operators_hybrid import (
    _id_column_value,
    python_hybrid_fallback,
)
from neosqlite.collection.temporary_table_aggregation.operators_sort_proj import (
    OperatorsSortProjMixin,
)
from neosqlite.objectid import ObjectId


class TestPythonHybrid:
    """Per-stage fallback behavior."""

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
                {"_id": 1, "x": 5, "name": "a"},
                {"_id": 2, "x": 0, "name": "b"},
                {"_id": 3, "x": 9, "name": "c"},
            ]
        )
        yield coll
        conn.close()

    def _force_tier2(self, monkeypatch, collection, decline_field):
        """Make Tier 1 decline and a chosen $match field decline in Tier 2."""
        monkeypatch.setattr(
            collection.query_engine.sql_tier_aggregator,
            "can_optimize_pipeline",
            lambda pipeline, **kwargs: False,
        )
        monkeypatch.setattr(
            collection.query_engine.helpers,
            "_build_aggregation_query",
            lambda pipeline: None,
        )
        original = SQLTranslator.translate_match

        def translate_match(self, spec, *args, **kwargs):
            if decline_field in spec:
                return None, []
            return original(self, spec, *args, **kwargs)

        monkeypatch.setattr(SQLTranslator, "translate_match", translate_match)

    def test_declined_match_stays_in_tier2(self, monkeypatch, collection):
        self._force_tier2(monkeypatch, collection, "x")
        tiers: list[str | None] = []
        collection.query_engine.add_tier_change_callback(
            lambda old, new, pipeline: tiers.append(new)
        )

        pipeline = [
            {"$match": {"x": {"$gt": 1}}},
            {"$sort": {"_id": -1}},
        ]
        set_force_fallback(False)
        optimized = list(collection.aggregate(pipeline))
        assert "tier2" in tiers
        assert "tier3" not in tiers

        set_force_fallback(True)
        python_result = list(collection.aggregate(pipeline))
        assert optimized == python_result
        assert [doc["_id"] for doc in optimized] == [3, 1]

    def test_hybrid_output_feeds_later_sql_stage(self, monkeypatch, collection):
        self._force_tier2(monkeypatch, collection, "x")
        sort_calls: list[dict | None] = []
        original_sort = OperatorsSortProjMixin._process_sort_skip_limit_stage

        def recording_sort(
            self, create_temp, current_table, sort_spec, skip, limit
        ):
            sort_calls.append(sort_spec)
            return original_sort(
                self, create_temp, current_table, sort_spec, skip, limit
            )

        monkeypatch.setattr(
            OperatorsSortProjMixin,
            "_process_sort_skip_limit_stage",
            recording_sort,
        )

        pipeline = [
            {"$match": {"x": {"$gte": 5}}},
            {"$sort": {"_id": 1}},
            {"$count": "total"},
        ]
        set_force_fallback(False)
        result = list(collection.aggregate(pipeline))
        assert result == [{"total": 2}]
        assert sort_calls == [{"_id": 1}]

    def test_decorator_uses_single_stage_name(self):
        """The decorator rebuilds the exact stage that declined SQL."""
        calls: list[dict] = []

        class Dummy:
            @python_hybrid_fallback("$replaceRoot")
            def run(self, create_temp, current_table, spec):
                raise NotImplementedError("declined")

            def _process_stage_python_hybrid(
                self, create_temp, current_table, stage
            ):
                calls.append(stage)
                return "handled"

        assert Dummy().run(None, "table", {"newRoot": "$sub"}) == "handled"
        assert calls == [{"$replaceRoot": {"newRoot": "$sub"}}]

    def test_decorator_resolves_stage_from_spec(self):
        calls: list[dict] = []

        def resolve(spec):
            return (
                "$replaceRoot"
                if isinstance(spec, dict) and "newRoot" in spec
                else "$replaceWith"
            )

        class Dummy:
            @python_hybrid_fallback(resolve)
            def run(self, create_temp, current_table, spec):
                raise NotImplementedError("declined")

            def _process_stage_python_hybrid(
                self, create_temp, current_table, stage
            ):
                calls.append(stage)
                return "handled"

        Dummy().run(None, "table", "$sub")
        assert calls == [{"$replaceWith": "$sub"}]

    @pytest.mark.parametrize(
        ("value", "expected_type"),
        [
            (1, int),
            (2.5, float),
            ("abc", str),
            (True, str),
            (None, type(None)),
        ],
    )
    def test_id_column_value_preserves_scalar_types(self, value, expected_type):
        result = _id_column_value(value)
        assert isinstance(result, expected_type)

    def test_id_column_value_serializes_special_ids(self):
        oid = ObjectId("507f1f77bcf86cd799439011")
        assert _id_column_value(oid) == str(oid)
        date = datetime(2024, 1, 1, tzinfo=UTC)
        assert _id_column_value(date) == str(date)
        assert _id_column_value({"a": 1}) == '{"a": 1}'
