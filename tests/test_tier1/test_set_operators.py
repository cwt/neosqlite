"""
Tests for Set Operators in Tier 1 (SQL).

This module tests the Tier 1 implementation of MongoDB set operators:
- $setEquals
- $setIntersection
- $setUnion
- $setDifference
- $setIsSubset
- $anyElementTrue
- $allElementsTrue

All tests follow the tier comparison pattern:
1. Run pipeline with Tier 1 (SQL optimization) enabled
2. Run same pipeline with Tier 3 (Python fallback) forced
3. Verify results are identical
"""

import pytest

import neosqlite
from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)


class TestSetEquals:
    """Test $setEquals operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        """Create test collection."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_set_equals
        coll.insert_many(
            [
                {"_id": 1, "a": [1, 2, 3], "b": [3, 2, 1]},
                {"_id": 2, "a": [1, 2, 3], "b": [1, 2, 3]},
                {"_id": 3, "a": [1, 2, 3], "b": [1, 2]},
                {"_id": 4, "a": [], "b": []},
                {"_id": 5, "a": [1], "b": [2]},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        """Normalize results for comparison."""
        return sorted(result, key=lambda x: x.get("_id", 0))

    def test_set_equals_basic(self, collection):
        """Test $setEquals with equal sets (different order)."""
        pipeline = [
            {"$match": {"_id": 1}},
            {"$project": {"_id": 1, "equal": {"$setEquals": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["equal"] == True

    def test_set_equals_identical(self, collection):
        """Test $setEquals with identical sets."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "equal": {"$setEquals": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["equal"] == True

    def test_set_equals_not_equal(self, collection):
        """Test $setEquals with different sets."""
        pipeline = [
            {"$match": {"_id": 3}},
            {"$project": {"_id": 1, "equal": {"$setEquals": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["equal"] == False

    def test_set_equals_empty(self, collection):
        """Test $setEquals with empty arrays."""
        pipeline = [
            {"$match": {"_id": 4}},
            {"$project": {"_id": 1, "equal": {"$setEquals": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["equal"] == True


class TestSetIntersection:
    """Test $setIntersection operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_set_intersection
        coll.insert_many(
            [
                {"_id": 1, "a": [1, 2, 3], "b": [2, 3, 4]},
                {"_id": 2, "a": [1, 2, 3], "b": [4, 5, 6]},
                {"_id": 3, "a": [1, 2, 3], "b": [3, 2, 1]},
                {"_id": 4, "a": [], "b": [1, 2, 3]},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        """Normalize results - sort array elements for comparison."""
        normalized = []
        for doc in result:
            norm_doc = dict(doc)
            for k, v in norm_doc.items():
                if isinstance(v, list):
                    norm_doc[k] = sorted(v)
            normalized.append(norm_doc)
        return sorted(normalized, key=lambda x: x.get("_id", 0))

    def test_set_intersection_basic(self, collection):
        """Test $setIntersection with overlapping sets."""
        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$project": {
                    "_id": 1,
                    "intersection": {"$setIntersection": ["$a", "$b"]},
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
        assert sorted(tier1_result[0]["intersection"]) == [2, 3]

    def test_set_intersection_empty(self, collection):
        """Test $setIntersection with no overlap."""
        pipeline = [
            {"$match": {"_id": 2}},
            {
                "$project": {
                    "_id": 1,
                    "intersection": {"$setIntersection": ["$a", "$b"]},
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
        assert tier1_result[0]["intersection"] == []

    def test_set_intersection_identical(self, collection):
        """Test $setIntersection with identical sets."""
        pipeline = [
            {"$match": {"_id": 3}},
            {
                "$project": {
                    "_id": 1,
                    "intersection": {"$setIntersection": ["$a", "$b"]},
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
        assert sorted(tier1_result[0]["intersection"]) == [1, 2, 3]


class TestSetUnion:
    """Test $setUnion operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_set_union
        coll.insert_many(
            [
                {"_id": 1, "a": [1, 2], "b": [2, 3]},
                {"_id": 2, "a": [1, 2, 3], "b": [4, 5, 6]},
                {"_id": 3, "a": [], "b": [1, 2]},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        normalized = []
        for doc in result:
            norm_doc = dict(doc)
            for k, v in norm_doc.items():
                if isinstance(v, list):
                    norm_doc[k] = sorted(v)
            normalized.append(norm_doc)
        return sorted(normalized, key=lambda x: x.get("_id", 0))

    def test_set_union_basic(self, collection):
        """Test $setUnion with overlapping sets."""
        pipeline = [
            {"$match": {"_id": 1}},
            {"$project": {"_id": 1, "union": {"$setUnion": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized
        assert sorted(tier1_result[0]["union"]) == [1, 2, 3]

    def test_set_union_disjoint(self, collection):
        """Test $setUnion with disjoint sets."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "union": {"$setUnion": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        tier1_normalized = self._normalize_result(tier1_result)
        tier3_normalized = self._normalize_result(tier3_result)
        assert tier1_normalized == tier3_normalized
        assert sorted(tier1_result[0]["union"]) == [1, 2, 3, 4, 5, 6]


class TestSetDifference:
    """Test $setDifference operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_set_difference
        coll.insert_many(
            [
                {"_id": 1, "a": [1, 2, 3, 4], "b": [3, 4, 5]},
                {"_id": 2, "a": [1, 2, 3], "b": [4, 5, 6]},
                {"_id": 3, "a": [1, 2, 3], "b": [1, 2, 3]},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        normalized = []
        for doc in result:
            norm_doc = dict(doc)
            for k, v in norm_doc.items():
                if isinstance(v, list):
                    norm_doc[k] = sorted(v)
            normalized.append(norm_doc)
        return sorted(normalized, key=lambda x: x.get("_id", 0))

    def test_set_difference_basic(self, collection):
        """Test $setDifference with overlapping sets."""
        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$project": {
                    "_id": 1,
                    "difference": {"$setDifference": ["$a", "$b"]},
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
        assert sorted(tier1_result[0]["difference"]) == [1, 2]

    def test_set_difference_no_overlap(self, collection):
        """Test $setDifference with no overlap."""
        pipeline = [
            {"$match": {"_id": 2}},
            {
                "$project": {
                    "_id": 1,
                    "difference": {"$setDifference": ["$a", "$b"]},
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
        assert sorted(tier1_result[0]["difference"]) == [1, 2, 3]

    def test_set_difference_identical(self, collection):
        """Test $setDifference with identical sets."""
        pipeline = [
            {"$match": {"_id": 3}},
            {
                "$project": {
                    "_id": 1,
                    "difference": {"$setDifference": ["$a", "$b"]},
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
        assert tier1_result[0]["difference"] == []


class TestSetIsSubset:
    """Test $setIsSubset operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_set_issubset
        coll.insert_many(
            [
                {"_id": 1, "a": [1, 2], "b": [1, 2, 3]},
                {"_id": 2, "a": [1, 2, 3], "b": [1, 2]},
                {"_id": 3, "a": [1, 2, 3], "b": [1, 2, 3]},
                {"_id": 4, "a": [], "b": [1, 2, 3]},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        return sorted(result, key=lambda x: x.get("_id", 0))

    def test_set_issubset_true(self, collection):
        """Test $setIsSubset when A is subset of B."""
        pipeline = [
            {"$match": {"_id": 1}},
            {"$project": {"_id": 1, "subset": {"$setIsSubset": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["subset"] == True

    def test_set_issubset_false(self, collection):
        """Test $setIsSubset when A is not subset of B."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "subset": {"$setIsSubset": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["subset"] == False

    def test_set_issubset_equal(self, collection):
        """Test $setIsSubset with equal sets."""
        pipeline = [
            {"$match": {"_id": 3}},
            {"$project": {"_id": 1, "subset": {"$setIsSubset": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["subset"] == True

    def test_set_issubset_empty(self, collection):
        """Test $setIsSubset with empty set."""
        pipeline = [
            {"$match": {"_id": 4}},
            {"$project": {"_id": 1, "subset": {"$setIsSubset": ["$a", "$b"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["subset"] == True


class TestAnyElementTrue:
    """Test $anyElementTrue operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_any_element_true
        coll.insert_many(
            [
                {"_id": 1, "arr": [True, False, False]},
                {"_id": 2, "arr": [False, False, False]},
                {"_id": 3, "arr": [0, 1, 0]},
                {"_id": 4, "arr": [0, 0, 0]},
                {"_id": 5, "arr": []},
                {"_id": 6, "arr": [None, False, 0]},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        return sorted(result, key=lambda x: x.get("_id", 0))

    def test_any_element_true_has_true(self, collection):
        """Test $anyElementTrue with at least one true."""
        pipeline = [
            {"$match": {"_id": 1}},
            {"$project": {"_id": 1, "any": {"$anyElementTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["any"] == True

    def test_any_element_true_all_false(self, collection):
        """Test $anyElementTrue with all false."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "any": {"$anyElementTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["any"] == False

    def test_any_element_true_numeric(self, collection):
        """Test $anyElementTrue with numeric truthy values."""
        pipeline = [
            {"$match": {"_id": 3}},
            {"$project": {"_id": 1, "any": {"$anyElementTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["any"] == True

    def test_any_element_true_empty(self, collection):
        """Test $anyElementTrue with empty array."""
        pipeline = [
            {"$match": {"_id": 5}},
            {"$project": {"_id": 1, "any": {"$anyElementTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["any"] == False


class TestAllElementsTrue:
    """Test $allElementsTrue operator."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self):
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_all_element_true
        coll.insert_many(
            [
                {"_id": 1, "arr": [True, True, True]},
                {"_id": 2, "arr": [True, False, True]},
                {"_id": 3, "arr": [1, 2, 3]},
                {"_id": 4, "arr": [1, 0, 3]},
                {"_id": 5, "arr": []},
            ]
        )
        yield coll
        conn.close()

    def _normalize_result(self, result):
        return sorted(result, key=lambda x: x.get("_id", 0))

    def test_all_elements_true_all_true(self, collection):
        """Test $allElementsTrue with all true."""
        pipeline = [
            {"$match": {"_id": 1}},
            {"$project": {"_id": 1, "all": {"$allElementsTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["all"] == True

    def test_all_elements_true_has_false(self, collection):
        """Test $allElementsTrue with one false."""
        pipeline = [
            {"$match": {"_id": 2}},
            {"$project": {"_id": 1, "all": {"$allElementsTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["all"] == False

    def test_all_elements_true_numeric(self, collection):
        """Test $allElementsTrue with numeric truthy values."""
        pipeline = [
            {"$match": {"_id": 3}},
            {"$project": {"_id": 1, "all": {"$allElementsTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        assert tier1_result[0]["all"] == True

    def test_all_elements_true_empty(self, collection):
        """Test $allElementsTrue with empty array."""
        pipeline = [
            {"$match": {"_id": 5}},
            {"$project": {"_id": 1, "all": {"$allElementsTrue": ["$arr"]}}},
        ]

        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier1_result == tier3_result
        # Empty array returns True (vacuous truth, matching Python's all([]))
        assert tier1_result[0]["all"] == True
