"""
Tests for $unwind Tier-2 optimization with advanced options.

Verifies that the Tier-2 temporary table implementation produces identical
results to Tier-3 Python fallback.
"""

import pytest

from neosqlite.collection.query_helper.utils import set_force_fallback


class TestUnwindTier2:
    """Test class for $unwind Tier-2 optimization correctness."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self, connection):
        """Create a test collection with sample data."""
        coll = connection["foo"]
        coll.delete_many({})

        # Insert test documents with arrays
        coll.insert_many(
            [
                {"name": "Alice", "tags": ["python", "sql"]},
                {"name": "Bob", "tags": ["java"]},
                {"name": "Charlie", "tags": ["go", "rust", "c"]},
            ]
        )

        yield coll

    def _normalize_result(self, result):
        """Normalize aggregation results for comparison."""
        normalized = []
        for doc in result:
            norm_doc = {}
            for key, value in doc.items():
                # Convert ObjectId to string for comparison
                if key == "_id":
                    norm_doc[key] = (
                        str(value) if hasattr(value, "__str__") else value
                    )
                # Sort array fields for consistent comparison
                elif isinstance(value, list):
                    norm_doc[key] = (
                        sorted(value)
                        if value and isinstance(value[0], (str, int, float))
                        else value
                    )
                else:
                    norm_doc[key] = value
            normalized.append(norm_doc)

        # Sort by name for consistent ordering
        return sorted(normalized, key=lambda x: str(x.get("name", "")))

    def test_unwind_basic_tier2_vs_tier3(self, collection):
        """Verify Tier-2 basic $unwind produces identical results to Tier-3 Python."""
        pipeline = [{"$unwind": "$tags"}]

        # Get Tier-2 results
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        assert self._normalize_result(tier2_result) == self._normalize_result(
            tier3_result
        )
        assert len(tier2_result) == 6  # 2 + 1 + 3 tags

    def test_unwind_preserve_null_and_empty_arrays(self, collection):
        """Verify Tier-2 preserveNullAndEmptyArrays produces identical results to Tier-3.

        Note: MongoDB preserves documents with missing fields, but Tier-3 Python
        implementation currently has a bug and doesn't. Tier-2 correctly matches
        MongoDB. This test compares Tier-2 vs Tier-3 for regression detection.
        """
        # Add documents with empty/null/missing arrays
        collection.insert_many(
            [
                {"name": "David", "tags": []},  # Empty array
                {"name": "Eve", "tags": None},  # Null
                {"name": "Frank"},  # Missing field
            ]
        )

        pipeline = [
            {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}
        ]

        # Get Tier-2 results
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Tier-2 correctly includes Frank (matches MongoDB), Tier-3 doesn't (bug)
        # For this test, we compare only the common documents
        assert (
            len(tier2_result) == 9
        )  # MongoDB-correct: 6 unwound + 3 preserved
        assert len(tier3_result) == 8  # Bug: missing Frank

        # Compare common documents (excluding Frank)
        tier2_no_frank = [d for d in tier2_result if d.get("name") != "Frank"]
        assert self._normalize_result(tier2_no_frank) == self._normalize_result(
            tier3_result
        )

        # Verify Frank is correctly preserved in Tier-2
        frank_docs = [d for d in tier2_result if d.get("name") == "Frank"]
        assert len(frank_docs) == 1
        assert frank_docs[0].get("tags") is None

    def test_unwind_include_array_index(self, collection):
        """Verify Tier-2 includeArrayIndex produces identical results to Tier-3."""
        pipeline = [{"$unwind": {"path": "$tags", "includeArrayIndex": "idx"}}]

        # Get Tier-2 results
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        assert self._normalize_result(tier2_result) == self._normalize_result(
            tier3_result
        )

        # Check that indices are present
        alice_docs = [doc for doc in tier2_result if doc["name"] == "Alice"]
        assert len(alice_docs) == 2
        alice_indices = sorted([doc["idx"] for doc in alice_docs])
        assert alice_indices == [0, 1]

    def test_unwind_combined_options(self, collection):
        """Verify Tier-2 combined options produce identical results to Tier-3.

        Note: MongoDB preserves documents with missing fields, but Tier-3 Python
        implementation currently has a bug and doesn't. Tier-2 correctly matches
        MongoDB. This test compares Tier-2 vs Tier-3 for regression detection.
        """
        # Add documents with empty/null/missing arrays
        collection.insert_many(
            [
                {"name": "David", "tags": []},  # Empty array
                {"name": "Eve", "tags": None},  # Null
                {"name": "Frank"},  # Missing field
            ]
        )

        pipeline = [
            {
                "$unwind": {
                    "path": "$tags",
                    "preserveNullAndEmptyArrays": True,
                    "includeArrayIndex": "idx",
                }
            }
        ]

        # Get Tier-2 results
        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Tier-2 correctly includes Frank (matches MongoDB), Tier-3 doesn't (bug)
        # For this test, we compare only the common documents
        assert (
            len(tier2_result) == 9
        )  # MongoDB-correct: 6 unwound + 3 preserved
        assert len(tier3_result) == 8  # Bug: missing Frank

        # Compare common documents (excluding Frank)
        tier2_no_frank = [d for d in tier2_result if d.get("name") != "Frank"]
        assert self._normalize_result(tier2_no_frank) == self._normalize_result(
            tier3_result
        )

        # Verify Frank is correctly preserved in Tier-2 with null index
        frank_docs = [d for d in tier2_result if d.get("name") == "Frank"]
        assert len(frank_docs) == 1
        assert frank_docs[0].get("tags") is None
        assert frank_docs[0].get("idx") is None

    def test_unwind_empty_collection(self, connection):
        """Verify $unwind works on empty collection."""
        coll = connection["foo"]
        coll.delete_many({})

        pipeline = [{"$unwind": "$tags"}]

        set_force_fallback(False)
        tier2_result = list(coll.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(coll.aggregate(pipeline))

        assert tier2_result == tier3_result == []

    def test_unwind_no_arrays(self, collection):
        """Verify $unwind when no documents have the array field."""
        collection.delete_many({})
        collection.insert_many(
            [
                {"name": "Alice", "score": 100},
                {"name": "Bob", "score": 200},
            ]
        )

        pipeline = [{"$unwind": "$tags"}]

        set_force_fallback(False)
        tier2_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Both should return empty results (no arrays to unwind)
        assert tier2_result == tier3_result == []
