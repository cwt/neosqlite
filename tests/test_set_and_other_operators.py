"""
Tests for set and other operators in $expr.

Covers: Set operators ($setEquals, $setIntersection, etc.) and
        Other operators ($mergeObjects, $let, $literal, $getField, $rand, $objectToArray)
"""

import neosqlite
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


class TestSetOperators:
    """Test set operators."""

    def test_setEquals(self):
        """Test $setEquals operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": [1, 2, 3], "b": [3, 2, 1], "c": [1, 2, 4]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "ab_equal": {"$setEquals": ["$a", "$b"]},
                                "ac_equal": {"$setEquals": ["$a", "$c"]},
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert (
                result[0]["ab_equal"] is True
            )  # Same elements, different order
            assert result[0]["ac_equal"] is False  # Different elements

    def test_setIntersection(self):
        """Test $setIntersection operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": [1, 2, 3, 4], "b": [3, 4, 5, 6]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "intersection": {
                                    "$setIntersection": ["$a", "$b"]
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Intersection should be [3, 4] (order may vary)
            assert set(result[0]["intersection"]) == {3, 4}

    def test_setUnion(self):
        """Test $setUnion operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": [1, 2, 3], "b": [3, 4, 5]})

            result = list(
                coll.aggregate(
                    [{"$project": {"union": {"$setUnion": ["$a", "$b"]}}}]
                )
            )

            assert len(result) == 1
            # Union should contain all unique elements
            assert set(result[0]["union"]) == {1, 2, 3, 4, 5}

    def test_setDifference(self):
        """Test $setDifference operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": [1, 2, 3, 4], "b": [3, 4, 5]})

            result = list(
                coll.aggregate(
                    [{"$project": {"diff": {"$setDifference": ["$a", "$b"]}}}]
                )
            )

            assert len(result) == 1
            # a - b should be [1, 2]
            assert set(result[0]["diff"]) == {1, 2}

    def test_setIsSubset(self):
        """Test $setIsSubset operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": [1, 2], "b": [1, 2, 3, 4]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "a_subset_b": {"$setIsSubset": ["$a", "$b"]},
                                "b_subset_a": {"$setIsSubset": ["$b", "$a"]},
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["a_subset_b"] is True
            assert result[0]["b_subset_a"] is False

    def test_anyElementTrue(self):
        """Test $anyElementTrue operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"a": [False, False, True], "b": [False, False, False], "c": []}
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "a_any": {"$anyElementTrue": "$a"},
                                "b_any": {"$anyElementTrue": "$b"},
                                "c_any": {"$anyElementTrue": "$c"},
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["a_any"] is True
            assert result[0]["b_any"] is False
            assert result[0]["c_any"] is False

    def test_allElementsTrue(self):
        """Test $allElementsTrue operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"a": [True, True, True], "b": [True, False, True], "c": []}
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "a_all": {"$allElementsTrue": "$a"},
                                "b_all": {"$allElementsTrue": "$b"},
                                "c_all": {"$allElementsTrue": "$c"},
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["a_all"] is True
            assert result[0]["b_all"] is False
            assert result[0]["c_all"] is True  # Empty array returns True


class TestOtherOperators:
    """Test other operators."""

    def test_mergeObjects(self):
        """Test $mergeObjects operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": {"x": 1, "y": 2}, "b": {"y": 3, "z": 4}})

            result = list(
                coll.aggregate(
                    [{"$project": {"merged": {"$mergeObjects": ["$a", "$b"]}}}]
                )
            )

            assert len(result) == 1
            merged = result[0]["merged"]
            assert merged["x"] == 1
            assert merged["y"] == 3  # Second object overwrites
            assert merged["z"] == 4

    def test_getField(self):
        """Test $getField operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"obj": {"name": "test", "value": 42}})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "name": {
                                    "$getField": {
                                        "field": "name",
                                        "input": "$obj",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["name"] == "test"

    def test_literal(self):
        """Test $literal operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "constant": {"$literal": {"$field": "value"}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # $literal should return the operand as-is without evaluation
            assert result[0]["constant"] == {"$field": "value"}

    def test_rand(self):
        """Test $rand operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"_id": 1})

            result = list(
                coll.aggregate([{"$project": {"random": {"$rand": {}}}}])
            )

            assert len(result) == 1
            random_val = result[0]["random"]
            assert 0 <= random_val <= 1

    def test_objectToArray(self):
        """Test $objectToArray operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"obj": {"a": 1, "b": 2}})

            result = list(
                coll.aggregate(
                    [{"$project": {"array": {"$objectToArray": "$obj"}}}]
                )
            )

            assert len(result) == 1
            array = result[0]["array"]
            assert len(array) == 2
            # Check that we have both key-value pairs
            keys = {item["k"] for item in array}
            assert keys == {"a", "b"}

    def test_let(self):
        """Test $let operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"x": 5, "y": 10})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "sum": {
                                    "$let": {
                                        "vars": {"a": "$x", "b": "$y"},
                                        "in": {"$add": ["$$a", "$$b"]},
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["sum"] == 15


class TestSetAndOtherKillSwitch:
    """Test set and other operators with kill switch."""

    def test_set_operators_with_kill_switch(self):
        """Test set operators work with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": [1, 2, 3], "b": [3, 4, 5]})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "intersection": {
                                        "$setIntersection": ["$a", "$b"]
                                    },
                                    "union": {"$setUnion": ["$a", "$b"]},
                                    "diff": {"$setDifference": ["$a", "$b"]},
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert set(result[0]["intersection"]) == {3}
                assert set(result[0]["union"]) == {1, 2, 3, 4, 5}
                assert set(result[0]["diff"]) == {1, 2}
            finally:
                set_force_fallback(original_state)

    def test_other_operators_with_kill_switch(self):
        """Test other operators work with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"obj": {"a": 1, "b": 2}, "x": 5, "y": 10})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "merged": {
                                        "$mergeObjects": ["$obj", {"c": 3}]
                                    },
                                    "array": {"$objectToArray": "$obj"},
                                    "sum": {
                                        "$let": {
                                            "vars": {"a": "$x", "b": "$y"},
                                            "in": {"$add": ["$$a", "$$b"]},
                                        }
                                    },
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["merged"]["a"] == 1
                assert result[0]["merged"]["c"] == 3
                assert len(result[0]["array"]) == 2
                assert result[0]["sum"] == 15
            finally:
                set_force_fallback(original_state)
