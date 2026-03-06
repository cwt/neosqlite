"""
Tests for array operators in $expr.

Covers: $firstN, $lastN, $maxN, $minN, $sortArray
"""

import neosqlite
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


class TestFirstNOperator:
    """Test $firstN operator."""

    def test_firstN_basic(self):
        """Test $firstN with basic array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [1, 2, 3, 4, 5]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "first3": {
                                    "$firstN": {"input": "$values", "n": 3}
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["first3"] == [1, 2, 3]

    def test_firstN_more_than_array(self):
        """Test $firstN when n is larger than array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [1, 2]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "first": {
                                    "$firstN": {"input": "$values", "n": 10}
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["first"] == [1, 2]

    def test_firstN_zero(self):
        """Test $firstN with n=0."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [1, 2, 3]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "first": {
                                    "$firstN": {"input": "$values", "n": 0}
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["first"] == []


class TestLastNOperator:
    """Test $lastN operator."""

    def test_lastN_basic(self):
        """Test $lastN with basic array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [1, 2, 3, 4, 5]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "last3": {
                                    "$lastN": {"input": "$values", "n": 3}
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["last3"] == [3, 4, 5]

    def test_lastN_more_than_array(self):
        """Test $lastN when n is larger than array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [1, 2]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "last": {
                                    "$lastN": {"input": "$values", "n": 10}
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["last"] == [1, 2]

    def test_lastN_zero(self):
        """Test $lastN with n=0."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [1, 2, 3]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "last": {"$lastN": {"input": "$values", "n": 0}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["last"] == []


class TestMaxNOperator:
    """Test $maxN operator."""

    def test_maxN_basic(self):
        """Test $maxN with basic array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [5, 2, 8, 1, 9, 3]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "max3": {"$maxN": {"input": "$values", "n": 3}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Should return 3 largest values in descending order
            assert result[0]["max3"] == [9, 8, 5]

    def test_maxN_with_strings(self):
        """Test $maxN with string array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": ["zebra", "apple", "banana"]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "max2": {"$maxN": {"input": "$values", "n": 2}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Should return 2 largest (alphabetically last) values
            assert result[0]["max2"] == ["zebra", "banana"]


class TestMinNOperator:
    """Test $minN operator."""

    def test_minN_basic(self):
        """Test $minN with basic array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [5, 2, 8, 1, 9, 3]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "min3": {"$minN": {"input": "$values", "n": 3}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Should return 3 smallest values in ascending order
            assert result[0]["min3"] == [1, 2, 3]

    def test_minN_with_strings(self):
        """Test $minN with string array."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": ["zebra", "apple", "banana"]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "min2": {"$minN": {"input": "$values", "n": 2}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Should return 2 smallest (alphabetically first) values
            assert result[0]["min2"] == ["apple", "banana"]


class TestSortArrayOperator:
    """Test $sortArray operator."""

    def test_sortArray_ascending(self):
        """Test $sortArray ascending."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [3, 1, 4, 1, 5, 9, 2, 6]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "sorted": {"$sortArray": {"input": "$values"}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["sorted"] == [1, 1, 2, 3, 4, 5, 6, 9]

    def test_sortArray_descending(self):
        """Test $sortArray descending."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [3, 1, 4, 1, 5]})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "sorted": {"$sortArray": {"input": "$values"}}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            # Default is ascending
            assert result[0]["sorted"] == [1, 1, 3, 4, 5]

    def test_sortArray_objects(self):
        """Test $sortArray with array of objects."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {
                    "items": [
                        {"name": "C", "value": 3},
                        {"name": "A", "value": 1},
                        {"name": "B", "value": 2},
                    ]
                }
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "sorted": {
                                    "$sortArray": {
                                        "input": "$items",
                                        "sortBy": {"name": 1},
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            sorted_items = result[0]["sorted"]
            assert sorted_items[0]["name"] == "A"
            assert sorted_items[1]["name"] == "B"
            assert sorted_items[2]["name"] == "C"

    def test_sortArray_objects_descending(self):
        """Test $sortArray with array of objects descending."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {
                    "items": [
                        {"name": "C", "value": 3},
                        {"name": "A", "value": 1},
                        {"name": "B", "value": 2},
                    ]
                }
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "sorted": {
                                    "$sortArray": {
                                        "input": "$items",
                                        "sortBy": {"value": -1},
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            sorted_items = result[0]["sorted"]
            assert sorted_items[0]["value"] == 3
            assert sorted_items[1]["value"] == 2
            assert sorted_items[2]["value"] == 1


class TestArrayOperatorsKillSwitch:
    """Test array operators with kill switch."""

    def test_array_operators_with_kill_switch(self):
        """Test array operators work with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"values": [5, 2, 8, 1, 9]})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "first2": {
                                        "$firstN": {"input": "$values", "n": 2}
                                    },
                                    "last2": {
                                        "$lastN": {"input": "$values", "n": 2}
                                    },
                                    "max2": {
                                        "$maxN": {"input": "$values", "n": 2}
                                    },
                                    "min2": {
                                        "$minN": {"input": "$values", "n": 2}
                                    },
                                    "sorted": {
                                        "$sortArray": {"input": "$values"}
                                    },
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["first2"] == [5, 2]
                assert result[0]["last2"] == [
                    1,
                    9,
                ]  # Last 2 elements of [5, 2, 8, 1, 9]
                assert result[0]["max2"] == [9, 8]
                assert result[0]["min2"] == [1, 2]
                assert result[0]["sorted"] == [1, 2, 5, 8, 9]
            finally:
                set_force_fallback(original_state)
