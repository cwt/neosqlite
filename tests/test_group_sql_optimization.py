# coding: utf-8
"""
Tests for SQL-based $group stage optimization in aggregation pipeline.
"""
import neosqlite
import pytest


def test_group_stage_sql_optimization():
    """Test that $group stage uses SQL instead of Python processing."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group"]

        # Insert test data
        docs = [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
            {"store": "A", "price": 30},
        ]
        collection.insert_many(docs)

        # Test simple group with sum
        pipeline = [
            {"$group": {"_id": "$store", "total": {"$sum": "$price"}}},
            {"$sort": {"_id": 1}},
        ]

        # This should now use SQL-based grouping
        result = collection.aggregate(pipeline)

        assert len(result) == 2
        assert result[0] == {"_id": "A", "total": 40}
        assert result[1] == {"_id": "B", "total": 20}


def test_group_stage_with_multiple_accumulators():
    """Test $group stage with multiple accumulator functions."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_multi"]

        # Insert test data
        docs = [
            {"item": "A", "price": 10, "quantity": 2},
            {"item": "B", "price": 20, "quantity": 1},
            {"item": "A", "price": 30, "quantity": 5},
            {"item": "B", "price": 10, "quantity": 2},
        ]
        collection.insert_many(docs)

        # Test group with multiple accumulators
        pipeline = [
            {
                "$group": {
                    "_id": "$item",
                    "total_quantity": {"$sum": "$quantity"},
                    "avg_price": {"$avg": "$price"},
                    "min_price": {"$min": "$price"},
                    "max_price": {"$max": "$price"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        assert result[0] == {
            "_id": "A",
            "total_quantity": 7,
            "avg_price": 20.0,
            "min_price": 10,
            "max_price": 30,
        }
        assert result[1] == {
            "_id": "B",
            "total_quantity": 3,
            "avg_price": 15.0,
            "min_price": 10,
            "max_price": 20,
        }


def test_group_stage_with_count():
    """Test $group stage with $count accumulator."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_count"]

        # Insert test data
        docs = [
            {"category": "A", "value": 1},
            {"category": "B", "value": 2},
            {"category": "A", "value": 3},
            {"category": "A", "value": 4},
        ]
        collection.insert_many(docs)

        # Test group with count
        pipeline = [
            {"$group": {"_id": "$category", "count": {"$count": {}}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        assert result[0] == {"_id": "A", "count": 3}
        assert result[1] == {"_id": "B", "count": 1}


def test_group_stage_with_match():
    """Test $group stage combined with $match stage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_match"]

        # Insert test data
        docs = [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
            {"store": "A", "price": 30},
            {"store": "C", "price": 40},
        ]
        collection.insert_many(docs)

        # Test group with match
        pipeline = [
            {"$match": {"price": {"$gte": 20}}},
            {"$group": {"_id": "$store", "total": {"$sum": "$price"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 3
        # Only stores with price >= 20
        assert {"_id": "A", "total": 30} in result  # Only the 30 priced item
        assert {"_id": "B", "total": 20} in result
        assert {"_id": "C", "total": 40} in result


def test_group_stage_fallback_to_python():
    """Test that complex group operations still fallback to Python."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_fallback"]

        # Insert test data
        docs = [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
        ]
        collection.insert_many(docs)

        # Test complex grouping that should fallback to Python - using _id: None
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$price"}}}]

        # This should fallback to Python processing but still work
        result = collection.aggregate(pipeline)

        # Should get one document with the total of all prices
        assert len(result) == 1
        assert result[0]["total"] == 30


if __name__ == "__main__":
    test_group_stage_sql_optimization()
    test_group_stage_with_multiple_accumulators()
    test_group_stage_with_count()
    test_group_stage_with_match()
    test_group_stage_fallback_to_python()
    print("All tests passed!")
