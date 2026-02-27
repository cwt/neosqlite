"""
Tests for Tier 2 (Temporary Table) operators.

This module tests the Tier 2 implementation of:
- $replaceRoot / $replaceWith stages
- $first / $last accumulators
- $addToSet accumulator
- $replaceOne string operator
"""

import pytest
import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    can_process_with_temporary_tables,
)


class TestReplaceRootTier2:
    """Test $replaceRoot and $replaceWith stages with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_replace_root
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "profile": {"age": 30, "city": "NYC"},
                },
                {"_id": 2, "name": "Bob", "profile": {"age": 25, "city": "LA"}},
                {
                    "_id": 3,
                    "name": "Charlie",
                    "profile": {"age": 35, "city": "SF"},
                },
            ]
        )
        yield coll
        conn.close()

    def test_replace_root_with_field_reference(self, collection):
        """Test $replaceRoot with field reference."""
        pipeline = [{"$replaceRoot": {"newRoot": "$profile"}}]

        # Check if pipeline can be processed with temporary tables
        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 3
        # Each result should be the profile object
        assert results[0]["age"] == 30
        assert results[0]["city"] == "NYC"
        assert results[1]["age"] == 25
        assert results[1]["city"] == "LA"

    def test_replace_with_field_reference(self, collection):
        """Test $replaceWith with field reference."""
        pipeline = [{"$replaceWith": "$profile"}]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 3
        assert results[0]["age"] == 30
        assert results[0]["city"] == "NYC"


class TestFirstLastAccumulatorsTier2:
    """Test $first and $last accumulators with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_first_last
        coll.insert_many(
            [
                {"_id": 1, "category": "A", "value": 10, "order": 1},
                {"_id": 2, "category": "A", "value": 20, "order": 2},
                {"_id": 3, "category": "A", "value": 30, "order": 3},
                {"_id": 4, "category": "B", "value": 100, "order": 1},
                {"_id": 5, "category": "B", "value": 200, "order": 2},
            ]
        )
        yield coll
        conn.close()

    def test_first_accumulator(self, collection):
        """Test $first accumulator in $group."""
        pipeline = [
            {"$sort": {"order": 1}},  # Ensure ordering
            {
                "$group": {
                    "_id": "$category",
                    "first_value": {"$first": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Category A first value should be 10
        category_a = next(r for r in results if r["_id"] == "A")
        assert category_a["first_value"] == 10
        # Category B first value should be 100
        category_b = next(r for r in results if r["_id"] == "B")
        assert category_b["first_value"] == 100

    def test_last_accumulator(self, collection):
        """Test $last accumulator in $group."""
        pipeline = [
            {"$sort": {"order": 1}},  # Ensure ordering
            {"$group": {"_id": "$category", "last_value": {"$last": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Category A last value should be 30
        category_a = next(r for r in results if r["_id"] == "A")
        assert category_a["last_value"] == 30
        # Category B last value should be 200
        category_b = next(r for r in results if r["_id"] == "B")
        assert category_b["last_value"] == 200


class TestAddToSetAccumulatorTier2:
    """Test $addToSet accumulator with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_add_to_set
        coll.insert_many(
            [
                {"_id": 1, "category": "A", "tag": "x"},
                {"_id": 2, "category": "A", "tag": "y"},
                {"_id": 3, "category": "A", "tag": "x"},  # Duplicate
                {"_id": 4, "category": "B", "tag": "z"},
                {"_id": 5, "category": "B", "tag": "y"},
            ]
        )
        yield coll
        conn.close()

    def test_add_to_set_accumulator(self, collection):
        """Test $addToSet accumulator in $group."""
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_tags": {"$addToSet": "$tag"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Category A should have unique tags ['x', 'y']
        category_a = next(r for r in results if r["_id"] == "A")
        assert "unique_tags" in category_a
        assert len(category_a["unique_tags"]) == 2  # x and y (no duplicates)
        assert "x" in category_a["unique_tags"]
        assert "y" in category_a["unique_tags"]

        # Category B should have unique tags ['z', 'y']
        category_b = next(r for r in results if r["_id"] == "B")
        assert "unique_tags" in category_b
        assert len(category_b["unique_tags"]) == 2


class TestReplaceOneTier2:
    """Test $replaceOne string operator with Tier 2."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_replace_one
        coll.insert_many(
            [
                {"_id": 1, "text": "hello world", "name": "Alice"},
                {"_id": 2, "text": "foo bar", "name": "Bob"},
                {"_id": 3, "text": "hello again", "name": "Charlie"},
            ]
        )
        yield coll
        conn.close()

    def test_replace_one_operator(self, collection):
        """Test $replaceOne in $addFields."""
        pipeline = [
            {
                "$addFields": {
                    "modified_text": {
                        "$replaceOne": {
                            "input": "$text",
                            "find": "hello",
                            "replacement": "hi",
                        }
                    }
                }
            }
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 3
        # First document: "hello world" -> "hi world"
        assert results[0]["modified_text"] == "hi world"
        # Second document: "foo bar" unchanged (no "hello")
        assert results[1]["modified_text"] == "foo bar"
        # Third document: "hello again" -> "hi again"
        assert results[2]["modified_text"] == "hi again"

    def test_replace_one_with_special_chars(self, collection):
        """Test $replaceOne with special characters."""
        # Insert document with special chars
        collection.insert_one({"_id": 10, "text": "a-b-c", "name": "Test"})

        pipeline = [
            {"$match": {"_id": 10}},
            {
                "$addFields": {
                    "modified": {
                        "$replaceOne": {
                            "input": "$text",
                            "find": "-",
                            "replacement": "_",
                        }
                    }
                }
            },
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 1
        # $replaceOne only replaces the FIRST occurrence
        assert results[0]["modified"] == "a_b-c"  # Only first "-" replaced


class TestCombinedPipelineTier2:
    """Test combined pipelines with multiple Tier 2 operators."""

    @pytest.fixture
    def collection(self):
        """Create a test collection with sample data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.test_combined
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "category": "A",
                    "name": "Product 1",
                    "description": "hello world",
                    "price": 100,
                    "order": 1,
                },
                {
                    "_id": 2,
                    "category": "A",
                    "name": "Product 2",
                    "description": "hello again",
                    "price": 50,
                    "order": 2,
                },
                {
                    "_id": 3,
                    "category": "B",
                    "name": "Product 3",
                    "description": "foo bar",
                    "price": 75,
                    "order": 1,
                },
            ]
        )
        yield coll
        conn.close()

    def test_combined_pipeline(self, collection):
        """Test pipeline with multiple Tier 2 operators."""
        pipeline = [
            # Replace "hello" with "hi" in description
            {
                "$addFields": {
                    "modified_desc": {
                        "$replaceOne": {
                            "input": "$description",
                            "find": "hello",
                            "replacement": "hi",
                        }
                    }
                }
            },
            # Group by category
            {
                "$group": {
                    "_id": "$category",
                    "total_price": {"$sum": "$price"},
                    "avg_price": {"$avg": "$price"},
                    "unique_descriptions": {"$addToSet": "$modified_desc"},
                }
            },
        ]

        assert can_process_with_temporary_tables(pipeline)

        results = list(collection.aggregate(pipeline))

        assert len(results) == 2
        # Check that we have the expected fields
        for result in results:
            assert "total_price" in result
            assert "avg_price" in result
            assert "unique_descriptions" in result

        # Check totals (order may vary)
        total_prices = sorted([r["total_price"] for r in results])
        assert total_prices == [75, 150]  # Category B: 75, Category A: 150

        # Check averages
        avg_prices = sorted([r["avg_price"] for r in results])
        assert avg_prices == [75.0, 75.0]
