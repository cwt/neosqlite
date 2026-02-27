"""
Tests for $expr set operation operators.

Covers: $setEquals, $setIntersection, $setUnion, $setDifference, $setIsSubset, $anyElementTrue, $allElementsTrue
"""

import pytest
import neosqlite


class TestSetOperations:
    """Test set operation operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with set data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_sets
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "set1": [1, 2, 3],
                    "set2": [2, 3, 4],
                    "set3": [1, 2, 3],
                },
                {
                    "_id": 2,
                    "set1": ["a", "b", "c"],
                    "set2": ["b", "c", "d"],
                    "set3": ["a", "b", "c"],
                },
                {"_id": 3, "set1": [1, 2], "set2": [3, 4], "set3": []},
                {
                    "_id": 4,
                    "set1": [True, False],
                    "set2": [True, True],
                    "set3": [False, False],
                },
                {"_id": 5, "set1": [1, 2, 3], "set2": [3, 4, 5]},
            ]
        )
        yield coll
        conn.close()

    def test_setEquals(self, collection):
        """Test $setEquals operator."""
        # Equal sets
        result = list(
            collection.find({"$expr": {"$setEquals": ["$set1", "$set3"]}})
        )
        assert len(result) == 2

        # Not equal sets
        result = list(
            collection.find({"$expr": {"$setEquals": ["$set1", "$set2"]}})
        )
        assert len(result) == 0

    def test_setIntersection(self, collection):
        """Test $setIntersection operator via $expr."""
        result = list(
            collection.find(
                {
                    "$expr": {
                        "$in": [3, {"$setIntersection": ["$set1", "$set2"]}]
                    }
                }
            )
        )
        assert len(result) == 2

    def test_setUnion(self, collection):
        """Test $setUnion operator via $expr."""
        result = list(
            collection.find(
                {"$expr": {"$in": [5, {"$setUnion": ["$set1", "$set2"]}]}}
            )
        )
        assert len(result) == 1

    def test_setDifference(self, collection):
        """Test $setDifference operator via $expr."""
        result = list(
            collection.find(
                {"$expr": {"$in": [1, {"$setDifference": ["$set1", "$set2"]}]}}
            )
        )
        assert len(result) == 3

    def test_setIsSubset(self, collection):
        """Test $setIsSubset operator."""
        result = list(
            collection.find({"$expr": {"$setIsSubset": ["$set1", "$set2"]}})
        )
        assert len(result) == 0

    def test_anyElementTrue(self, collection):
        """Test $anyElementTrue operator."""
        result = list(
            collection.find({"$expr": {"$anyElementTrue": ["$set1"]}})
        )
        assert len(result) == 5

    def test_allElementsTrue(self, collection):
        """Test $allElementsTrue operator."""
        result = list(
            collection.find({"$expr": {"$allElementsTrue": ["$set1"]}})
        )
        assert len(result) == 4
