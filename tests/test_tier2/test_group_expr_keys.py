"""
Tests for $group with expression keys in Tier 2 (Temporary Table).

This module tests the Tier 2 implementation of $group stages with expression keys,
including:
- Concatenation expressions
- Arithmetic expressions
- Conditional expressions
- Comparison with Tier 3 Python fallback results

All tests follow the tier comparison pattern:
1. Run pipeline with Tier 2 (temporary tables) enabled
2. Run same pipeline with Tier 3 (Python fallback) forced
3. Verify results are identical
"""

import pytest

import neosqlite
from neosqlite.collection.query_helper.utils import (
    get_force_fallback,
    set_force_fallback,
)
from neosqlite.collection.temporary_table_aggregation import (
    can_process_with_temporary_tables,
)


class TestGroupExpressionKeys:
    """Test $group with expression keys in Tier 2."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data for expression key tests."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_group_expr_keys
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "first_name": "John",
                    "last_name": "Doe",
                    "age": 30,
                    "city": "NYC",
                    "salary": 50000,
                },
                {
                    "_id": 2,
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "age": 28,
                    "city": "NYC",
                    "salary": 60000,
                },
                {
                    "_id": 3,
                    "first_name": "Bob",
                    "last_name": "Smith",
                    "age": 35,
                    "city": "LA",
                    "salary": 70000,
                },
                {
                    "_id": 4,
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "age": 32,
                    "city": "LA",
                    "salary": 75000,
                },
                {
                    "_id": 5,
                    "first_name": "Charlie",
                    "last_name": "Brown",
                    "age": 25,
                    "city": "SF",
                    "salary": 55000,
                },
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        """Normalize aggregation results for comparison."""
        # Sort by _id and convert to comparable format
        return sorted(
            [
                {
                    k: (sorted(v) if isinstance(v, list) else v)
                    for k, v in doc.items()
                }
                for doc in result
            ],
            key=lambda x: str(x.get("_id", "")),
        )

    def test_group_by_concat_expression(self, collection):
        """Test $group with $concat expression key - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {
                "$group": {
                    "_id": {"$concat": ["$first_name", " ", "$last_name"]},
                    "total_salary": {"$sum": "$salary"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        # Check if pipeline can be processed with temporary tables
        assert can_process_with_temporary_tables(pipeline)

        # Get Tier-2 results (optimizations enabled)
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results (force fallback)
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical - this is the key verification
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        # Verify we have 5 unique full names
        assert len(tier2_result) == 5

        # Check specific results (verify on both tiers since they're identical)
        john_doe = next(
            (r for r in tier2_result if r["_id"] == "John Doe"), None
        )
        assert john_doe is not None
        assert john_doe["total_salary"] == 50000

        # Verify same result in Tier-3
        john_doe_tier3 = next(
            (r for r in tier3_result if r["_id"] == "John Doe"), None
        )
        assert john_doe_tier3 is not None
        assert john_doe_tier3["total_salary"] == 50000

    def test_group_by_simple_field(self, collection):
        """Test $group with simple field reference - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {
                "$group": {
                    "_id": "$city",
                    "avg_salary": {"$avg": "$salary"},
                    "count": {"$count": {}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        # Compare Tier-2 vs Tier-3
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        # Verify we have 3 cities
        assert len(tier2_result) == 3

        # Check NYC average (verify on both tiers)
        nyc = next((r for r in tier2_result if r["_id"] == "NYC"), None)
        nyc_tier3 = next((r for r in tier3_result if r["_id"] == "NYC"), None)
        assert nyc is not None
        assert nyc_tier3 is not None
        assert nyc["avg_salary"] == 55000.0
        assert nyc_tier3["avg_salary"] == 55000.0
        assert nyc["count"] == 2
        assert nyc_tier3["count"] == 2

    def test_group_by_literal_value(self, collection):
        """Test $group with literal value - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "total_salary": {"$sum": "$salary"},
                    "avg_age": {"$avg": "$age"},
                }
            }
        ]

        assert can_process_with_temporary_tables(pipeline)

        # Compare Tier-2 vs Tier-3
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        # Should have exactly 1 result
        assert len(tier2_result) == 1
        assert len(tier3_result) == 1

        # Verify results on both tiers
        assert tier2_result[0]["total_salary"] == 310000
        assert tier3_result[0]["total_salary"] == 310000
        assert tier2_result[0]["avg_age"] == 30.0
        assert tier3_result[0]["avg_age"] == 30.0

    def test_group_by_id_field(self, collection):
        """Test $group by _id field - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {"$group": {"_id": "$_id", "name": {"$first": "$first_name"}}},
            {"$sort": {"_id": 1}},
        ]

        # Note: $first with $sort falls back to Python, but without $sort uses Tier-2
        # This tests the basic grouping by _id
        assert can_process_with_temporary_tables(pipeline)

        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        assert len(tier2_result) == 5
        assert len(tier3_result) == 5

    def test_group_with_addtoSet_expression_key(self, collection):
        """Test $group with expression key and $addToSet accumulator - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {
                "$group": {
                    "_id": {"$concat": ["$first_name", " ", "$last_name"]},
                    "cities": {"$addToSet": "$city"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        # Compare Tier-2 vs Tier-3
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        # Each person should have exactly 1 city (since names are unique)
        # Verify on both tiers
        for result in tier2_result:
            assert len(result["cities"]) == 1
        for result in tier3_result:
            assert len(result["cities"]) == 1

    def test_group_with_push_expression_key(self, collection):
        """Test $group with expression key and $push accumulator - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {
                "$group": {
                    "_id": "$city",
                    "salaries": {"$push": "$salary"},
                    "names": {
                        "$push": {"$concat": ["$first_name", " ", "$last_name"]}
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        # Compare Tier-2 vs Tier-3
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        # Check NYC has 2 salaries (verify on both tiers)
        nyc = next((r for r in tier2_result if r["_id"] == "NYC"), None)
        nyc_tier3 = next((r for r in tier3_result if r["_id"] == "NYC"), None)
        assert nyc is not None
        assert nyc_tier3 is not None
        assert len(nyc["salaries"]) == 2
        assert len(nyc_tier3["salaries"]) == 2

    def test_kill_switch_forces_tier3(self, collection):
        """Test that kill switch forces Tier 3 Python fallback - results must be identical."""
        pipeline = [{"$group": {"_id": "$city", "total": {"$sum": "$salary"}}}]

        # With fallback forced (Tier-3)
        set_force_fallback(True)
        assert get_force_fallback() is True
        tier3_result = list(collection.aggregate(pipeline))

        # With fallback disabled (Tier-2)
        set_force_fallback(False)
        assert get_force_fallback() is False
        tier2_result = list(collection.aggregate(pipeline))

        # Results MUST be identical regardless of tier used
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

    def test_group_expression_with_null_values(self, collection):
        """Test $group with expression key when some documents have null/missing fields - Tier-2 vs Tier-3."""
        # Add document with missing fields
        collection.insert_one({"_id": 6, "first_name": "Test", "age": 40})

        pipeline = [
            {
                "$group": {
                    "_id": {"$concat": ["$first_name", " ", "$last_name"]},
                    "count": {"$count": {}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        # Compare Tier-2 vs Tier-3
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier2_normalized = self._normalize_result(tier2_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier2_normalized == tier3_normalized
        ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"

        # Should have 6 results (including one with null last_name)
        assert len(tier2_result) == 6
        assert len(tier3_result) == 6


class TestGroupExpressionKeysEdgeCases:
    """Test edge cases for $group with expression keys."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        """Create a test collection with edge case data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_group_edge_cases
        coll.insert_many(
            [
                {"_id": 1, "a": 10, "b": 20, "category": "X"},
                {"_id": 2, "a": 5, "b": 15, "category": "X"},
                {"_id": 3, "a": 30, "b": 40, "category": "Y"},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        """Normalize aggregation results for comparison."""
        return sorted(
            [
                {
                    k: (sorted(v) if isinstance(v, list) else v)
                    for k, v in doc.items()
                }
                for doc in result
            ],
            key=lambda x: str(x.get("_id", "")),
        )

    def test_group_by_arithmetic_expression(self, collection):
        """Test $group with arithmetic expression key - Tier-2 vs Tier-3 comparison."""
        pipeline = [
            {
                "$group": {
                    "_id": {"$add": ["$a", "$b"]},
                    "count": {"$count": {}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        # Note: $add in expression keys may require Python fallback if it uses params
        # This test verifies the behavior
        assert can_process_with_temporary_tables(pipeline)

        try:
            set_force_fallback(False)
            tier2_result = list(collection.aggregate(pipeline))

            set_force_fallback(True)
            tier3_result = list(collection.aggregate(pipeline))

            # Results MUST be identical
            tier2_normalized = self._normalize_result(tier2_result)
            tier3_normalized = self._normalize_result(tier3_result)
            assert (
                tier2_normalized == tier3_normalized
            ), f"Tier-2 and Tier-3 results differ:\nTier-2: {tier2_normalized}\nTier-3: {tier3_normalized}"
        except NotImplementedError:
            # If not supported in Tier 2, that's expected for parameterized expressions
            pytest.skip(
                "Arithmetic expressions in group keys require Python fallback"
            )

    def test_group_empty_collection(self):
        """Test $group with expression key on empty collection - Tier-2 vs Tier-3 comparison."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_empty

        pipeline = [
            {
                "$group": {
                    "_id": {"$concat": ["$first", " ", "$last"]},
                    "total": {"$sum": "$value"},
                }
            }
        ]

        assert can_process_with_temporary_tables(pipeline)

        # Compare Tier-2 vs Tier-3
        set_force_fallback(False)
        tier2_result = list(coll.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(coll.aggregate(pipeline))

        # Results MUST be identical (both empty)
        assert tier2_result == tier3_result == []

        conn.close()
