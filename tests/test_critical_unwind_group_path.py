"""
Targeted test to hit the specific code path at lines 1388-1515
"""

import pytest
from neosqlite import Connection


def test_unwind_followed_by_group_optimization():
    """Test the specific $unwind followed by $group optimization path."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data that should trigger the optimization
        collection.insert_many(
            [
                {"category": "tech", "items": ["laptop", "phone"]},
                {"category": "tech", "items": ["laptop", "tablet"]},
                {"category": "home", "items": ["sofa", "table", "laptop"]},
            ]
        )

        # This specific pipeline should trigger the optimization path:
        # 1. Exactly 2 stages: $unwind followed by $group
        # 2. Both use string syntax with $ prefix
        # 3. This should hit the code at lines 1388-1515
        pipeline = [
            {"$unwind": "$items"},  # First stage, string syntax with $
            {
                "$group": {  # Second stage ($group), i=1
                    "_id": "$items",  # String syntax with $ prefix
                    "count": {"$sum": 1},  # Simple accumulator
                }
            },
        ]

        # This should use the SQL optimization that covers lines 1388-1515
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) >= 3  # At least laptop, phone, tablet, sofa, table
        item_counts = {doc["_id"]: doc["count"] for doc in results}
        # Laptop should appear in all 3 documents
        assert item_counts["laptop"] == 3


def test_unwind_group_with_push_accumulator():
    """Test $unwind + $group with $push accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"team": "A", "players": ["Alice", "Bob"]},
                {"team": "A", "players": ["Alice", "Charlie"]},
                {"team": "B", "players": ["David", "Eve", "Frank"]},
            ]
        )

        # Test with $push accumulator - this should also hit the optimization path
        pipeline = [
            {"$unwind": "$players"},
            {
                "$group": {
                    "_id": "$team",
                    "all_players": {"$push": "$players"},  # $push accumulator
                }
            },
        ]

        # This should hit the $push case in the match statement (lines ~1470)
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) == 2  # Team A and Team B
        team_data = {doc["_id"]: doc["all_players"] for doc in results}
        assert "A" in team_data
        assert "B" in team_data
        # Team A should have 4 players total (with duplicates)
        assert len(team_data["A"]) == 4
        # Team B should have 3 players
        assert len(team_data["B"]) == 3


def test_unwind_group_with_addtoset_accumulator():
    """Test $unwind + $group with $addToSet accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with duplicate values
        collection.insert_many(
            [
                {"type": "fruits", "names": ["apple", "banana"]},
                {
                    "type": "fruits",
                    "names": ["apple", "orange"],
                },  # apple appears twice
                {"type": "vegetables", "names": ["carrot", "broccoli"]},
            ]
        )

        # Test with $addToSet accumulator - should hit the $addToSet case
        pipeline = [
            {"$unwind": "$names"},
            {
                "$group": {
                    "_id": "$type",
                    "unique_names": {
                        "$addToSet": "$names"
                    },  # $addToSet accumulator
                }
            },
        ]

        # This should hit the $addToSet case in the match statement (lines ~1490)
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) == 2  # fruits and vegetables
        type_data = {doc["_id"]: doc["unique_names"] for doc in results}
        assert "fruits" in type_data
        assert "vegetables" in type_data
        # fruits should have 3 unique names: apple, banana, orange (apple deduplicated)
        assert len(type_data["fruits"]) == 3
        # vegetables should have 2 unique names
        assert len(type_data["vegetables"]) == 2


def test_unwind_group_with_count_accumulator():
    """Test $unwind + $group with $count accumulator."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"region": "north", "cities": ["NYC", "Boston"]},
                {
                    "region": "north",
                    "cities": ["NYC", "Philadelphia"],
                },  # NYC appears twice
                {
                    "region": "south",
                    "cities": ["Miami", "Atlanta", "NYC"],
                },  # NYC appears in south too
            ]
        )

        # Test with $count accumulator - should hit the $count case
        pipeline = [
            {"$unwind": "$cities"},
            {
                "$group": {
                    "_id": "$cities",
                    "appearances": {"$count": {}},  # $count accumulator
                }
            },
        ]

        # This should hit the $count case in the match statement (lines ~1450)
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert (
            len(results) >= 3
        )  # At least NYC, Boston, Miami, Atlanta, Philadelphia
        city_data = {doc["_id"]: doc["appearances"] for doc in results}
        # NYC should appear 3 times total
        assert city_data["NYC"] == 3


def test_unwind_group_fallback_conditions():
    """Test conditions that should cause fallback to Python processing."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"category": "books", "tags": ["fiction", "novel"]},
                {"category": "books", "tags": ["fiction", "mystery"]},
            ]
        )

        # Test with complex accumulator that should cause fallback
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "complex_calc": {
                        "$sum": {"$multiply": ["$tags", 2]}
                    },  # Complex expression should cause fallback
                }
            },
        ]

        # This should gracefully fall back to Python processing when optimization fails
        results = list(collection.aggregate(pipeline))

        # Should still work (fallback to Python)
        assert len(results) >= 2


if __name__ == "__main__":
    pytest.main([__file__])
