"""
Unit tests for the _optimize_unwind_group_pattern helper function
"""

import pytest
from neosqlite import Connection
from neosqlite.collection.query_helper import QueryHelper


def test_optimize_unwind_group_pattern_basic():
    """Test basic $unwind + $group optimization pattern."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test the basic pattern that should be optimized
        pipeline = [
            {"$unwind": "$items"},  # Stage 0
            {
                "$group": {  # Stage 1 - this should be optimized
                    "_id": "$items",
                    "count": {"$sum": 1},
                }
            },
        ]

        # Call the helper function directly
        result = helper._optimize_unwind_group_pattern(1, pipeline)

        # Should return optimized SQL query
        assert result is not None
        cmd, params, output_fields = result
        assert "SELECT" in cmd
        assert "json_each" in cmd
        assert "GROUP BY" in cmd
        assert "je.value AS _id" in cmd
        assert "COUNT(*) AS count" in cmd
        assert "_id" in output_fields
        assert "count" in output_fields


def test_optimize_unwind_group_pattern_with_push():
    """Test $unwind + $group with $push accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with $push accumulator
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$category", "tag_list": {"$push": "$tags"}}},
        ]

        # Call the helper function directly
        result = helper._optimize_unwind_group_pattern(1, pipeline)

        # Should return optimized SQL query
        assert result is not None
        cmd, params, output_fields = result
        assert "SELECT" in cmd
        assert "json_group_array(je.value)" in cmd
        assert '"tag_list"' in cmd
        assert "_id" in output_fields
        assert "tag_list" in output_fields


def test_optimize_unwind_group_pattern_with_addtoset():
    """Test $unwind + $group with $addToSet accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with $addToSet accumulator
        pipeline = [
            {"$unwind": "$skills"},
            {
                "$group": {
                    "_id": "$department",
                    "unique_skills": {"$addToSet": "$skills"},
                }
            },
        ]

        # Call the helper function directly
        result = helper._optimize_unwind_group_pattern(1, pipeline)

        # Should return optimized SQL query
        assert result is not None
        cmd, params, output_fields = result
        assert "SELECT" in cmd
        assert "json_group_array(DISTINCT je.value)" in cmd
        assert '"unique_skills"' in cmd
        assert "_id" in output_fields
        assert "unique_skills" in output_fields


def test_optimize_unwind_group_pattern_with_count():
    """Test $unwind + $group with $count accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with $count accumulator
        pipeline = [
            {"$unwind": "$products"},
            {"$group": {"_id": "$vendor", "product_count": {"$count": {}}}},
        ]

        # Call the helper function directly
        result = helper._optimize_unwind_group_pattern(1, pipeline)

        # Should return optimized SQL query
        assert result is not None
        cmd, params, output_fields = result
        assert "SELECT" in cmd
        assert "COUNT(*) AS product_count" in cmd
        assert "_id" in output_fields
        assert "product_count" in output_fields


def test_optimize_unwind_group_pattern_invalid_conditions():
    """Test cases where optimization should not be applied."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test case 1: Wrong stage index (not 1)
        pipeline1 = [
            {"$unwind": "$items"},
            {"$group": {"_id": "$items", "count": {"$sum": 1}}},
        ]
        result = helper._optimize_unwind_group_pattern(
            2, pipeline1
        )  # Index 2 instead of 1
        assert result is None

        # Test case 2: No $unwind in previous stage
        pipeline2 = [
            {"$match": {"active": True}},
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        ]
        result = helper._optimize_unwind_group_pattern(1, pipeline2)
        assert result is None

        # Test case 3: Invalid unwind stage format
        pipeline3 = [
            {"$unwind": {"path": "$items"}},  # Dictionary instead of string
            {"$group": {"_id": "$items", "count": {"$sum": 1}}},
        ]
        result = helper._optimize_unwind_group_pattern(1, pipeline3)
        assert result is None

        # Test case 4: Invalid group _id format
        pipeline4 = [
            {"$unwind": "$items"},
            {
                "$group": {"_id": {"$toUpper": "$items"}, "count": {"$sum": 1}}
            },  # Expression instead of string
        ]
        result = helper._optimize_unwind_group_pattern(1, pipeline4)
        assert result is None


def test_optimize_unwind_group_pattern_unsupported_accumulator():
    """Test case where accumulator is not supported (should fallback)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test with unsupported accumulator
        pipeline = [
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": "$items",
                    "avg_value": {
                        "$avg": "$price"
                    },  # $avg is not supported in optimization
                }
            },
        ]

        # Should fallback to None (no optimization)
        result = helper._optimize_unwind_group_pattern(1, pipeline)
        assert result is None


def test_optimize_unwind_group_pattern_group_by_different_field():
    """Test $unwind + $group where _id groups by different field than unwind."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test grouping by a different field than the unwind field
        pipeline = [
            {"$unwind": "$scores"},
            {
                "$group": {
                    "_id": "$student_name",  # Grouping by different field
                    "count": {"$sum": 1},
                }
            },
        ]

        # Should still be optimized but with different GROUP BY clause
        result = helper._optimize_unwind_group_pattern(1, pipeline)

        assert result is not None
        cmd, params, output_fields = result
        assert "SELECT" in cmd
        assert (
            "json_extract" in cmd
        )  # Should use json_extract for different field
        assert "GROUP BY json_extract" in cmd
        assert "_id" in output_fields
        assert "count" in output_fields


if __name__ == "__main__":
    pytest.main([__file__])
