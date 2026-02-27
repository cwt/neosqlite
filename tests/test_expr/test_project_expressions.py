"""
Tests for $project stage with expression support.

This module tests the $project aggregation stage with computed fields
using aggregation expressions.
"""

import pytest
import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


class TestProjectWithExpressions:
    """Test $project stage with expression support."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_project
        coll.insert_many(
            [
                {"_id": 1, "name": "Alice", "age": 30, "salary": 50000},
                {"_id": 2, "name": "Bob", "age": 25, "salary": 45000},
            ]
        )
        yield coll
        conn.close()

    def test_project_with_computed_field(self, collection):
        """Test $project with computed field."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$project": {
                        "name": 1,
                        "double_salary": {"$multiply": ["$salary", 2]},
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            assert results[0]["name"] == "Alice"
            assert results[0]["double_salary"] == 100000
            assert results[1]["name"] == "Bob"
            assert results[1]["double_salary"] == 90000
        finally:
            set_force_fallback(False)

    def test_project_with_arithmetic_expression(self, collection):
        """Test $project with arithmetic expression."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$project": {
                        "name": 1,
                        "annual_bonus": {"$multiply": ["$salary", 0.1]},
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            assert results[0]["annual_bonus"] == 5000.0
            assert results[1]["annual_bonus"] == 4500.0
        finally:
            set_force_fallback(False)

    def test_project_with_conditional_expression(self, collection):
        """Test $project with conditional expression."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$project": {
                        "name": 1,
                        "salary_tier": {
                            "$cond": {
                                "if": {"$gt": ["$salary", 48000]},
                                "then": "senior",
                                "else": "junior",
                            }
                        },
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            assert results[0]["salary_tier"] == "senior"  # Alice: 50000 > 48000
            assert results[1]["salary_tier"] == "junior"  # Bob: 45000 < 48000
        finally:
            set_force_fallback(False)
