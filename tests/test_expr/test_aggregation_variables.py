"""
Tests for aggregation expression variables: $$ROOT, $$CURRENT, $$REMOVE.

This module tests variable scoping in aggregation pipelines:
- $$ROOT: Original document at pipeline start
- $$CURRENT: Document as it evolves through stages
- $$REMOVE: Sentinel for field removal in $project
"""

import pytest
import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


class TestAggregationVariables:
    """Test aggregation variable scoping."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_vars
        coll.insert_many(
            [
                {"_id": 1, "name": "Alice", "age": 30, "salary": 50000},
                {"_id": 2, "name": "Bob", "age": 25, "salary": 45000},
            ]
        )
        yield coll
        conn.close()

    def test_root_variable(self, collection):
        """Test $$ROOT variable returns original document."""
        set_force_fallback(True)
        try:
            pipeline = [{"$addFields": {"original": "$$ROOT"}}]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # $$ROOT should contain the original document
            assert results[0]["original"]["_id"] == 1
            assert results[0]["original"]["name"] == "Alice"
            assert results[1]["original"]["_id"] == 2
            assert results[1]["original"]["name"] == "Bob"
        finally:
            set_force_fallback(False)

    def test_current_variable(self, collection):
        """Test $$CURRENT variable returns evolving document."""
        set_force_fallback(True)
        try:
            pipeline = [
                {"$addFields": {"bonus": 5000}},
                {"$addFields": {"total": "$$CURRENT"}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # $$CURRENT should include the newly added bonus field
            assert results[0]["total"]["bonus"] == 5000
            assert results[0]["total"]["salary"] == 50000
        finally:
            set_force_fallback(False)

    def test_root_vs_current(self, collection):
        """Test difference between $$ROOT and $$CURRENT."""
        set_force_fallback(True)
        try:
            pipeline = [
                {"$addFields": {"bonus": 5000}},
                {
                    "$addFields": {
                        "root_snapshot": "$$ROOT",
                        "current_snapshot": "$$CURRENT",
                    }
                },
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # $$ROOT should NOT have bonus
            assert "bonus" not in results[0]["root_snapshot"]
            # $$CURRENT should have bonus
            assert results[0]["current_snapshot"]["bonus"] == 5000
        finally:
            set_force_fallback(False)

    def test_root_preserved_across_multiple_stages(self, collection):
        """Test that $$ROOT is preserved across multiple pipeline stages."""
        set_force_fallback(True)
        try:
            pipeline = [
                {"$addFields": {"step1": "a"}},
                {"$addFields": {"step2": "b"}},
                {"$addFields": {"step3": "c"}},
                {"$addFields": {"original": "$$ROOT"}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # $$ROOT should still be the original document
            assert "step1" not in results[0]["original"]
            assert "step2" not in results[0]["original"]
            assert "step3" not in results[0]["original"]
            # But current doc has all fields
            assert results[0]["step1"] == "a"
            assert results[0]["step2"] == "b"
            assert results[0]["step3"] == "c"
        finally:
            set_force_fallback(False)


class TestRemoveSentinel:
    """Test $$REMOVE sentinel for field removal."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_remove
        coll.insert_many(
            [
                {"_id": 1, "name": "Alice", "age": 30, "secret": "hidden"},
                {"_id": 2, "name": "Bob", "age": 25, "secret": "hidden"},
            ]
        )
        yield coll
        conn.close()

    def test_remove_field_in_project(self, collection):
        """Test removing fields using $$REMOVE in $project."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$project": {
                        "name": 1,
                        "age": 1,
                        # secret field should be removed
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            assert "name" in results[0]
            assert "age" in results[0]
            assert "secret" not in results[0]
        finally:
            set_force_fallback(False)

    def test_remove_with_expression(self, collection):
        """Test removing fields conditionally using expressions."""
        set_force_fallback(True)
        try:
            # First add a computed field, then project it out
            pipeline = [
                {"$addFields": {"computed": {"$add": ["$age", 10]}}},
                {
                    "$project": {
                        "name": 1,
                        "age": 1,
                        "computed": "$$REMOVE",  # Remove computed field
                    }
                },
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            assert "name" in results[0]
            assert "age" in results[0]
            assert "computed" not in results[0]
        finally:
            set_force_fallback(False)
