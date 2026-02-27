"""
Tests for $addFields stage with expression support.

This module tests the $addFields aggregation stage with various expression types:
- Arithmetic expressions
- Trigonometric expressions
- Conditional expressions
- Nested expressions
- Field references and literals (backward compatibility)
"""

import pytest
import math
import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


class TestAddFieldsWithExpressions:
    """Test $addFields stage with expression support."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_addfields
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "age": 30,
                    "salary": 50000,
                    "bonus": 5000,
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "age": 25,
                    "salary": 45000,
                    "bonus": 3000,
                },
                {
                    "_id": 3,
                    "name": "Charlie",
                    "age": 35,
                    "salary": 60000,
                    "bonus": 7000,
                },
            ]
        )
        yield coll
        conn.close()

    def test_add_fields_with_arithmetic_expression(self, collection):
        """Test $addFields with arithmetic expression."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "total_income": {"$add": ["$salary", "$bonus"]}
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            assert results[0]["total_income"] == 55000  # 50000 + 5000
            assert results[1]["total_income"] == 48000  # 45000 + 3000
            assert results[2]["total_income"] == 67000  # 60000 + 7000
        finally:
            set_force_fallback(False)

    def test_add_fields_with_multiplication(self, collection):
        """Test $addFields with multiplication expression."""
        set_force_fallback(True)
        try:
            pipeline = [
                {"$addFields": {"tax": {"$multiply": ["$salary", 0.2]}}}
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            assert results[0]["tax"] == 10000.0  # 50000 * 0.2
            assert results[1]["tax"] == 9000.0  # 45000 * 0.2
            assert results[2]["tax"] == 12000.0  # 60000 * 0.2
        finally:
            set_force_fallback(False)

    def test_add_fields_with_trig_expression(self, collection):
        """Test $addFields with trigonometric expression."""
        # Insert document with angle
        collection.insert_one({"_id": 4, "angle": math.pi / 2})

        set_force_fallback(True)
        try:
            pipeline = [{"$addFields": {"sin_angle": {"$sin": "$angle"}}}]
            results = list(collection.aggregate(pipeline))

            # Find the document with angle
            result = next(r for r in results if r.get("angle") is not None)
            assert abs(result["sin_angle"] - 1.0) < 0.0001
        finally:
            set_force_fallback(False)

    def test_add_fields_with_conditional_expression(self, collection):
        """Test $addFields with conditional expression."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "status": {
                            "$cond": {
                                "if": {"$gt": ["$salary", 50000]},
                                "then": "high",
                                "else": "standard",
                            }
                        }
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            # Alice: salary = 50000, not > 50000
            assert results[0]["status"] == "standard"
            # Bob: salary = 45000
            assert results[1]["status"] == "standard"
            # Charlie: salary = 60000 > 50000
            assert results[2]["status"] == "high"
        finally:
            set_force_fallback(False)

    def test_add_fields_with_nested_expression(self, collection):
        """Test $addFields with nested expressions."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "total_with_tax": {
                            "$add": ["$salary", {"$multiply": ["$salary", 0.1]}]
                        }
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            # Alice: 50000 + (50000 * 0.1) = 55000
            assert results[0]["total_with_tax"] == 55000.0
            # Bob: 45000 + (45000 * 0.1) = 49500
            assert results[1]["total_with_tax"] == 49500.0
            # Charlie: 60000 + (60000 * 0.1) = 66000
            assert results[2]["total_with_tax"] == 66000.0
        finally:
            set_force_fallback(False)

    def test_add_fields_with_field_reference(self, collection):
        """Test $addFields with simple field reference (backward compatibility)."""
        set_force_fallback(True)
        try:
            pipeline = [{"$addFields": {"income": "$salary"}}]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            assert results[0]["income"] == 50000
            assert results[1]["income"] == 45000
            assert results[2]["income"] == 60000
        finally:
            set_force_fallback(False)

    def test_add_fields_with_literal(self, collection):
        """Test $addFields with literal value (backward compatibility)."""
        set_force_fallback(True)
        try:
            pipeline = [{"$addFields": {"status": "active", "count": 100}}]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            assert all(r["status"] == "active" for r in results)
            assert all(r["count"] == 100 for r in results)
        finally:
            set_force_fallback(False)

    def test_add_fields_with_comparison_expression(self, collection):
        """Test $addFields with comparison expression."""
        set_force_fallback(True)
        try:
            pipeline = [
                {"$addFields": {"is_high_earner": {"$gt": ["$salary", 50000]}}}
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            assert not results[0]["is_high_earner"]  # 50000 not > 50000
            assert not results[1]["is_high_earner"]  # 45000 not > 50000
            assert results[2]["is_high_earner"]  # 60000 > 50000
        finally:
            set_force_fallback(False)

    def test_field_reference_across_stages(self, collection):
        """Test that fields added in one stage can be referenced in later stages."""
        set_force_fallback(True)
        try:
            # Use a simpler test case
            pipeline = [
                {"$addFields": {"multiplier": 2}},
                {
                    "$addFields": {
                        "result": {"$multiply": ["$salary", "$multiplier"]}
                    }
                },
                {
                    "$addFields": {
                        "doubled_result": {"$multiply": ["$result", 2]}
                    }
                },
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 3
            assert results[0]["result"] == 100000  # 50000 * 2
            assert results[0]["doubled_result"] == 200000  # 100000 * 2
            assert results[1]["result"] == 90000  # 45000 * 2
            assert results[1]["doubled_result"] == 180000  # 90000 * 2
        finally:
            set_force_fallback(False)
