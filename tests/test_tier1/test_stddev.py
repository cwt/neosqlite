"""
Tests for $stdDevPop and $stdDevSamp accumulator Tier-1 optimization.

Verifies that the SQL implementation produces identical results to the Python fallback.
"""

import pytest

from neosqlite.collection.query_helper.utils import set_force_fallback


class TestStdDev:
    """Test class for $stdDevPop/$stdDevSamp Tier-1 optimization correctness."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self, connection):
        """Create a test collection with sample data."""
        # Use the default collection from conftest but clear it first
        coll = connection["foo"]
        coll.delete_many({})  # Clear existing data

        # Insert test documents with known values for std dev calculation
        # Values: 2, 4, 4, 4, 5, 5, 7, 9
        # Mean: 5
        # Population variance: 4
        # Population std dev: 2
        # Sample variance: 4.571428...
        # Sample std dev: 2.138089...
        coll.insert_many(
            [
                {"category": "A", "value": 2},
                {"category": "A", "value": 4},
                {"category": "A", "value": 4},
                {"category": "A", "value": 4},
                {"category": "A", "value": 5},
                {"category": "A", "value": 5},
                {"category": "A", "value": 7},
                {"category": "A", "value": 9},
            ]
        )

        yield coll
        coll.drop()

    def _normalize_result(self, result):
        """Normalize aggregation results for comparison."""
        normalized = []
        for doc in result:
            norm_doc = {"_id": doc["_id"]}
            for key, value in doc.items():
                if key == "_id":
                    continue
                # Round floats for comparison
                if isinstance(value, float):
                    norm_doc[key] = round(value, 10)
                else:
                    norm_doc[key] = value
            normalized.append(norm_doc)

        return sorted(normalized, key=lambda x: str(x["_id"]))

    def _compare_float(self, a, b, tolerance=1e-10):
        """Compare two floats with tolerance."""
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return abs(a - b) < tolerance

    def test_stdDevPop_basic(self, collection):
        """Verify Tier-1 $stdDevPop produces identical results to Tier-3 Python."""
        pipeline = [
            {"$group": {"_id": "$category", "stddev": {"$stdDevPop": "$value"}}}
        ]

        # Get Tier-1/Tier-2 optimized results
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical (within floating point tolerance)
        tier1_norm = self._normalize_result(tier1_result)
        tier3_norm = self._normalize_result(tier3_result)

        assert len(tier1_norm) == len(tier3_norm)
        for t1, t3 in zip(tier1_norm, tier3_norm):
            assert t1["_id"] == t3["_id"]
            assert self._compare_float(
                t1["stddev"], t3["stddev"], tolerance=1e-9
            )

    def test_stdDevSamp_basic(self, collection):
        """Verify Tier-1 $stdDevSamp produces identical results to Tier-3 Python."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "stddev": {"$stdDevSamp": "$value"},
                }
            }
        ]

        # Get Tier-1/Tier-2 optimized results
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical (within floating point tolerance)
        tier1_norm = self._normalize_result(tier1_result)
        tier3_norm = self._normalize_result(tier3_result)

        assert len(tier1_norm) == len(tier3_norm)
        for t1, t3 in zip(tier1_norm, tier3_norm):
            assert t1["_id"] == t3["_id"]
            assert self._compare_float(
                t1["stddev"], t3["stddev"], tolerance=1e-9
            )

    def test_stdDevPop_with_expression(self, collection):
        """Verify $stdDevPop with expression produces identical results."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "stddev": {"$stdDevPop": {"$multiply": ["$value", 2]}},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_norm = self._normalize_result(tier1_result)
        tier3_norm = self._normalize_result(tier3_result)

        assert len(tier1_norm) == len(tier3_norm)
        for t1, t3 in zip(tier1_norm, tier3_norm):
            assert t1["_id"] == t3["_id"]
            assert self._compare_float(
                t1["stddev"], t3["stddev"], tolerance=1e-9
            )

    def test_stdDevPop_single_value(self, collection):
        """Verify $stdDevPop with single value returns 0."""
        collection.delete_many({})
        collection.insert_one({"category": "A", "value": 5})

        pipeline = [
            {"$group": {"_id": "$category", "stddev": {"$stdDevPop": "$value"}}}
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Both should return 0 for single value
        assert len(tier1_result) == 1
        assert len(tier3_result) == 1
        assert tier1_result[0]["stddev"] == 0
        assert tier3_result[0]["stddev"] == 0

    def test_stdDevSamp_single_value(self, collection):
        """Verify $stdDevSamp with single value returns None/NaN (undefined)."""
        collection.delete_many({})
        collection.insert_one({"category": "A", "value": 5})

        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "stddev": {"$stdDevSamp": "$value"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Both should return None for single value (sample std dev undefined)
        assert len(tier1_result) == 1
        assert len(tier3_result) == 1
        # Sample std dev with n=1 is undefined (division by zero)
        # Both should handle this the same way
        assert (tier1_result[0]["stddev"] is None) == (
            tier3_result[0]["stddev"] is None
        )

    def test_stdDevPop_empty_collection(self, connection):
        """Verify $stdDevPop works on empty collection."""
        coll = connection["foo"]
        coll.delete_many({})  # Clear existing data

        pipeline = [
            {"$group": {"_id": "$category", "stddev": {"$stdDevPop": "$value"}}}
        ]

        set_force_fallback(False)
        tier1_result = list(coll.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(coll.aggregate(pipeline))

        assert len(tier1_result) == len(tier3_result) == 0

        coll.drop()

    def test_stdDevPop_with_nulls(self, collection):
        """Verify $stdDevPop handles null values correctly."""
        # Insert documents with null values
        collection.insert_many(
            [
                {"category": "A", "value": None},
                {"category": "A", "value": None},
            ]
        )

        pipeline = [
            {"$group": {"_id": "$category", "stddev": {"$stdDevPop": "$value"}}}
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Both should handle nulls the same way
        tier1_norm = self._normalize_result(tier1_result)
        tier3_norm = self._normalize_result(tier3_result)

        assert len(tier1_norm) == len(tier3_norm)

    def test_stdDevPop_multiple_groups(self, collection):
        """Verify $stdDevPop with multiple groups."""
        # Add another category
        collection.insert_many(
            [
                {"category": "B", "value": 10},
                {"category": "B", "value": 20},
                {"category": "B", "value": 30},
            ]
        )

        pipeline = [
            {"$group": {"_id": "$category", "stddev": {"$stdDevPop": "$value"}}}
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_norm = self._normalize_result(tier1_result)
        tier3_norm = self._normalize_result(tier3_result)

        assert len(tier1_norm) == len(tier3_norm)
        for t1, t3 in zip(tier1_norm, tier3_norm):
            assert t1["_id"] == t3["_id"]
            assert self._compare_float(
                t1["stddev"], t3["stddev"], tolerance=1e-9
            )

    def test_stdDevPop_known_values(self, collection):
        """Verify $stdDevPop with known standard deviation."""
        # For values [2, 4, 4, 4, 5, 5, 7, 9]:
        # Mean = 5
        # Population variance = ((2-5)^2 + (4-5)^2 + ... + (9-5)^2) / 8 = 32/8 = 4
        # Population std dev = sqrt(4) = 2
        pipeline = [
            {"$group": {"_id": "$category", "stddev": {"$stdDevPop": "$value"}}}
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Check that both tiers produce the expected result
        assert len(tier1_result) == 1
        assert len(tier3_result) == 1

        # Population std dev should be 2.0
        assert abs(tier1_result[0]["stddev"] - 2.0) < 1e-9
        assert abs(tier3_result[0]["stddev"] - 2.0) < 1e-9
