"""
Tests for $split string operator in Tier 1 (SQL).

This module tests the Tier 1 implementation of $split using recursive CTE.
All tests follow the tier comparison pattern:
1. Run pipeline with Tier 1 (SQL optimization) enabled
2. Run same pipeline with Tier 3 (Python fallback) forced
3. Verify results are identical

MongoDB $split syntax:
    {$split: [string, delimiter]}

Returns an array of substrings split by the delimiter.
"""

import pytest

import neosqlite
from neosqlite.collection.query_helper.utils import (
    get_force_fallback,
    set_force_fallback,
)


class TestSplitOperator:
    """Test $split string operator in Tier 1."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data for $split tests."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_split
        coll.insert_many(
            [
                {"_id": 1, "text": "hello world foo", "delimiter": " "},
                {"_id": 2, "text": "apple,banana,cherry", "delimiter": ","},
                {"_id": 3, "text": "one-two-three-four", "delimiter": "-"},
                {"_id": 4, "text": "no_delimiter_here", "delimiter": " "},
                {"_id": 5, "text": "", "delimiter": " "},
                {"_id": 6, "text": "multiple   spaces", "delimiter": " "},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        """Normalize results for comparison."""
        normalized = []
        for doc in result:
            norm_doc = {}
            for k, v in doc.items():
                if isinstance(v, list):
                    norm_doc[k] = list(v)  # Ensure it's a list
                else:
                    norm_doc[k] = v
            normalized.append(norm_doc)
        return sorted(normalized, key=lambda x: x.get("_id", 0))

    def test_split_basic(self, collection):
        """Test $split with basic space delimiter."""
        pipeline = [
            {"$project": {"_id": 1, "words": {"$split": ["$text", " "]}}},
            {"$sort": {"_id": 1}},
        ]

        # Get Tier-1 results (SQL optimization enabled)
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier1_normalized == tier3_normalized
        ), f"Tier-1 and Tier-3 results differ:\nTier-1: {tier1_normalized}\nTier-3: {tier3_normalized}"

        # Verify specific results
        assert tier1_result[0]["words"] == ["hello", "world", "foo"]
        # Document 2 has no spaces, so it returns the original string
        assert tier1_result[1]["words"] == ["apple,banana,cherry"]
        # Document 3 has hyphens, not spaces, so no split occurs
        assert tier1_result[2]["words"] == ["one-two-three-four"]
        # Document 4 has no spaces
        assert tier1_result[3]["words"] == ["no_delimiter_here"]
        # Document 5 is empty string
        assert tier1_result[4]["words"] == [""]
        # Document 6 has multiple spaces
        assert tier1_result[5]["words"] == ["multiple", "", "", "spaces"]

    def test_split_with_comma_delimiter(self, collection):
        """Test $split with comma delimiter."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "items": {"$split": ["$text", ","]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        assert tier1_result[0]["items"] == ["apple", "banana", "cherry"]

    def test_split_with_hyphen_delimiter(self, collection):
        """Test $split with hyphen delimiter."""
        pipeline = [
            {"$match": {"_id": 3}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", "-"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        assert tier1_result[0]["parts"] == ["one", "two", "three", "four"]

    def test_split_no_delimiter_found(self, collection):
        """Test $split when delimiter is not found in string."""
        pipeline = [
            {"$match": {"_id": 4}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", " "]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # When delimiter not found, returns array with original string
        assert tier1_result[0]["parts"] == ["no_delimiter_here"]

    def test_split_empty_string(self, collection):
        """Test $split with empty string."""
        pipeline = [
            {"$match": {"_id": 5}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", " "]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # Empty string split returns array with empty string
        assert tier1_result[0]["parts"] == [""]

    def test_split_multiple_consecutive_delimiters(self, collection):
        """Test $split with multiple consecutive delimiters."""
        pipeline = [
            {"$match": {"_id": 6}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", " "]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # Multiple spaces create empty strings between them
        assert tier1_result[0]["parts"] == ["multiple", "", "", "spaces"]

    def test_split_with_literal_string(self, collection):
        """Test $split with literal string instead of field reference."""
        pipeline = [
            {"$match": {"_id": 1}},
            {"$project": {"_id": 1, "words": {"$split": ["hello world", " "]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        assert tier1_result[0]["words"] == ["hello", "world"]

    def test_split_with_literal_delimiter(self, collection):
        """Test $split with literal delimiter instead of field reference."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "items": {"$split": ["$text", ","]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        assert tier1_result[0]["items"] == ["apple", "banana", "cherry"]

    def test_split_kill_switch_forces_tier3(self, collection):
        """Test that kill switch forces Tier 3 Python fallback."""
        pipeline = [
            {"$project": {"_id": 1, "words": {"$split": ["$text", " "]}}},
            {"$sort": {"_id": 1}},
        ]

        # With fallback forced (Tier-3)
        set_force_fallback(True)
        assert get_force_fallback() is True
        tier3_result = list(collection.aggregate(pipeline))

        # With fallback disabled (Tier-1)
        set_force_fallback(False)
        assert get_force_fallback() is False
        tier1_result = list(collection.aggregate(pipeline))

        # Results MUST be identical regardless of tier used
        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert (
            tier1_normalized == tier3_normalized
        ), f"Tier-1 and Tier-3 results differ:\nTier-1: {tier1_normalized}\nTier-3: {tier3_normalized}"

    def test_split_in_group_context(self, collection):
        """Test $split used in a $group context."""
        pipeline = [
            {"$match": {"_id": {"$lte": 3}}},
            {
                "$group": {
                    "_id": None,
                    "all_words": {"$push": {"$split": ["$text", " "]}},
                }
            },
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # Should have 3 elements (one per document)
        assert len(tier1_result[0]["all_words"]) == 3


class TestSplitEdgeCases:
    """Test edge cases for $split operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        """Create a test collection with edge case data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_split_edge
        coll.insert_many(
            [
                {"_id": 1, "text": "a|b|c", "delimiter": "|"},
                {"_id": 2, "text": "single", "delimiter": "|"},
                {"_id": 3, "text": "|leading", "delimiter": "|"},
                {"_id": 4, "text": "trailing|", "delimiter": "|"},
                {"_id": 5, "text": "||", "delimiter": "|"},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        """Normalize results for comparison."""
        normalized = []
        for doc in result:
            norm_doc = {}
            for k, v in doc.items():
                if isinstance(v, list):
                    norm_doc[k] = list(v)
                else:
                    norm_doc[k] = v
            normalized.append(norm_doc)
        return sorted(normalized, key=lambda x: x.get("_id", 0))

    def test_split_leading_delimiter(self, collection):
        """Test $split with leading delimiter."""
        pipeline = [
            {"$match": {"_id": 3}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", "|"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # Leading delimiter creates empty first element
        assert tier1_result[0]["parts"] == ["", "leading"]

    def test_split_trailing_delimiter(self, collection):
        """Test $split with trailing delimiter."""
        pipeline = [
            {"$match": {"_id": 4}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", "|"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # Trailing delimiter creates empty last element
        assert tier1_result[0]["parts"] == ["trailing", ""]

    def test_split_only_delimiters(self, collection):
        """Test $split with string containing only delimiters."""
        pipeline = [
            {"$match": {"_id": 5}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", "|"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        # "||" split by "|" should give ["", "", ""]
        assert tier1_result[0]["parts"] == ["", "", ""]

    def test_split_single_element(self, collection):
        """Test $split with no delimiters (single element result)."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "parts": {"$split": ["$text", "|"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized

        assert tier1_result[0]["parts"] == ["single"]

    def test_split_empty_collection(self):
        """Test $split on empty collection."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_split_empty

        pipeline = [
            {"$project": {"_id": 1, "parts": {"$split": ["$text", " "]}}}
        ]

        set_force_fallback(False)
        tier1_result = list(coll.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(coll.aggregate(pipeline))

        assert tier1_result == tier3_result == []

        conn.close()
