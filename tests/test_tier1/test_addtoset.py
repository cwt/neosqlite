"""
Tests for $addToSet accumulator Tier-1 optimization.

Verifies that the SQL implementation using json_group_array(DISTINCT ...)
produces identical results to the Python fallback.
"""

import pytest

from neosqlite.collection.query_helper.utils import set_force_fallback


class TestAddToSet:
    """Test class for $addToSet Tier-1 optimization correctness."""

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

        # Insert test documents
        coll.insert_many(
            [
                {"category": "A", "tag": "tag1"},
                {"category": "A", "tag": "tag2"},
                {"category": "A", "tag": "tag1"},  # Duplicate
                {"category": "B", "tag": "tag3"},
                {"category": "B", "tag": "tag4"},
                {"category": "B", "tag": "tag3"},  # Duplicate
                {"category": "C", "tag": "tag5"},
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
                # Sort lists for comparison (handle None values)
                if isinstance(value, list):
                    try:
                        norm_doc[key] = sorted(
                            value,
                            key=lambda x: (
                                x is None,
                                str(x) if x is not None else "",
                            ),
                        )
                    except TypeError:
                        # If sorting fails, just convert to sorted string representation
                        norm_doc[key] = sorted(
                            [str(x) if x is not None else None for x in value]
                        )
                else:
                    norm_doc[key] = value
            normalized.append(norm_doc)

        return sorted(normalized, key=lambda x: str(x["_id"]))

    def test_addToSet_basic(self, collection):
        """Verify Tier-1 $addToSet produces identical results to Tier-3 Python."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            }
        ]

        # Get Tier-1/Tier-2 optimized results
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

    def test_addToSet_with_nulls(self, collection):
        """Verify $addToSet handles null values correctly."""
        # Insert document with null tag
        collection.insert_one({"category": "A", "tag": None})

        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

    def test_addToSet_with_missing_field(self, collection):
        """Verify $addToSet handles missing fields correctly."""
        # Insert document without tag field
        collection.insert_one({"category": "A"})

        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

    def test_addToSet_with_expression(self, collection):
        """Verify $addToSet with expression produces identical results."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {
                        "$addToSet": {"$concat": ["prefix_", "$tag"]}
                    },
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

    def test_addToSet_with_literal(self, collection):
        """Verify $addToSet with literal value produces identical results."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "constant": {"$addToSet": "constant_value"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

    def test_addToSet_empty_collection(self, connection):
        """Verify $addToSet works on empty collection."""
        coll = connection["foo"]
        coll.delete_many({})  # Clear existing data

        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(coll.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(coll.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

        coll.delete_many({})

    def test_addToSet_single_document(self, collection):
        """Verify $addToSet works with single document."""
        collection.delete_many({})
        collection.insert_one({"category": "A", "tag": "single"})

        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )

    def test_addToSet_all_duplicates(self, collection):
        """Verify $addToSet with all duplicate values."""
        collection.delete_many({})
        collection.insert_many(
            [
                {"category": "A", "tag": "same"},
                {"category": "A", "tag": "same"},
                {"category": "A", "tag": "same"},
            ]
        )

        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            }
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert self._normalize_result(tier1_result) == self._normalize_result(
            tier3_result
        )
        # Should have exactly one unique value
        assert tier1_result[0]["unique_tags"] == ["same"]
