"""
Tests for $group stage with expression support in accumulators.

This module tests the $group aggregation stage with expressions in:
- Accumulator operators ($sum, $avg, $min, $max, etc.)
- Group key computation
"""

import pytest
import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


class TestGroupWithExpressions:
    """Test $group stage with expression support in accumulators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_group
        coll.insert_many(
            [
                {"_id": 1, "category": "A", "price": 100, "quantity": 5},
                {"_id": 2, "category": "A", "price": 50, "quantity": 10},
                {"_id": 3, "category": "B", "price": 75, "quantity": 8},
                {"_id": 4, "category": "B", "price": 25, "quantity": 20},
            ]
        )
        yield coll
        conn.close()

    def test_group_with_expression_in_sum_accumulator(self, collection):
        """Test $group with expression in $sum accumulator."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$category",
                        "total_revenue": {
                            "$sum": {"$multiply": ["$price", "$quantity"]}
                        },
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # Category A: (100 * 5) + (50 * 10) = 500 + 500 = 1000
            assert results[0]["_id"] == "A"
            assert results[0]["total_revenue"] == 1000
            # Category B: (75 * 8) + (25 * 20) = 600 + 500 = 1100
            assert results[1]["_id"] == "B"
            assert results[1]["total_revenue"] == 1100
        finally:
            set_force_fallback(False)

    def test_group_with_expression_in_avg_accumulator(self, collection):
        """Test $group with expression in $avg accumulator."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$category",
                        "avg_price": {"$avg": {"$multiply": ["$price", 0.9]}},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # Category A: avg((100 * 0.9), (50 * 0.9)) = avg(90, 45) = 67.5
            assert results[0]["_id"] == "A"
            assert results[0]["avg_price"] == 67.5
            # Category B: avg((75 * 0.9), (25 * 0.9)) = avg(67.5, 22.5) = 45.0
            assert results[1]["_id"] == "B"
            assert results[1]["avg_price"] == 45.0
        finally:
            set_force_fallback(False)

    def test_group_with_expression_in_group_key(self, collection):
        """Test $group with expression in group key."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": {"$toUpper": "$category"},
                        "total_quantity": {"$sum": "$quantity"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            assert results[0]["_id"] == "A"
            assert results[0]["total_quantity"] == 15  # 5 + 10
            assert results[1]["_id"] == "B"
            assert results[1]["total_quantity"] == 28  # 8 + 20
        finally:
            set_force_fallback(False)

    def test_group_with_nested_expression(self, collection):
        """Test $group with nested expressions in accumulator."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$category",
                        "total_with_tax": {
                            "$sum": {
                                "$add": [
                                    {"$multiply": ["$price", "$quantity"]},
                                    {"$multiply": ["$price", "$quantity", 0.1]},
                                ]
                            }
                        },
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # Category A: (500 + 50) + (500 + 50) = 1100
            assert results[0]["_id"] == "A"
            assert results[0]["total_with_tax"] == 1100
            # Category B: (600 + 60) + (500 + 50) = 1210
            assert results[1]["_id"] == "B"
            assert results[1]["total_with_tax"] == 1210
        finally:
            set_force_fallback(False)
