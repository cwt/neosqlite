"""
Additional targeted tests to cover lines 1388-1515 in query_helper.py
"""

import pytest
from neosqlite import Connection


def test_unwind_group_specific_optimization_path():
    """Test the specific optimization path that covers lines 1388-1515."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data that will trigger the specific optimization path
        collection.insert_many(
            [
                {"category": "tech", "items": ["laptop", "phone", "tablet"]},
                {"category": "tech", "items": ["laptop", "monitor"]},
                {"category": "home", "items": ["sofa", "table", "laptop"]},
            ]
        )

        # This specific pattern should trigger the optimization path at lines 1388-1515:
        # $unwind followed immediately by $group with string field references
        pipeline = [
            {"$unwind": "$items"},  # String syntax with $ prefix
            {
                "$group": {
                    "_id": "$items",  # String syntax with $ prefix - this is the key part
                    "count": {"$sum": 1},
                }
            },
        ]

        # This should use the SQL optimization path that covers lines 1388-1515
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) >= 3  # At least laptop, phone, tablet
        item_counts = {doc["_id"]: doc["count"] for doc in results}
        assert item_counts["laptop"] == 3  # Appears in all three categories
        # Other items will have count 1 or 2


def test_unwind_group_with_group_by_different_field():
    """Test $unwind + $group where group _id is different from unwind field."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"type": "electronics", "products": ["phone", "tablet"]},
                {"type": "electronics", "products": ["laptop", "phone"]},
                {"type": "furniture", "products": ["chair", "table"]},
            ]
        )

        # Test group by a different field than the unwind field
        pipeline = [
            {"$unwind": "$products"},
            {
                "$group": {
                    "_id": "$type",  # Group by type, not by products (unwind field)
                    "product_count": {"$sum": 1},
                    "all_products": {"$push": "$products"},
                }
            },
        ]

        # This should also trigger the optimization path
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) == 2  # electronics, furniture
        type_data = {doc["_id"]: doc for doc in results}
        assert "electronics" in type_data
        assert "furniture" in type_data
        assert (
            type_data["electronics"]["product_count"] == 4
        )  # 4 products total
        assert type_data["furniture"]["product_count"] == 2  # 2 products total


def test_unwind_group_complex_accumulators():
    """Test $unwind + $group with complex accumulator operations."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"team": "A", "scores": [85, 92]},
                {"team": "B", "scores": [78, 88]},
                {"team": "A", "scores": [90, 87]},
            ]
        )

        # Test with multiple accumulator types
        pipeline = [
            {"$unwind": "$scores"},
            {
                "$group": {
                    "_id": "$team",
                    "total_scores": {"$sum": 1},  # Count accumulator
                    "score_list": {"$push": "$scores"},  # Push accumulator
                    "unique_scores": {
                        "$addToSet": "$scores"
                    },  # addToSet accumulator
                }
            },
        ]

        # This should trigger the complex accumulator handling in lines 1388-1515
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) == 2  # Team A and Team B
        team_data = {doc["_id"]: doc for doc in results}
        assert "A" in team_data
        assert "B" in team_data

        # Team A should have 4 scores (2 documents with 2 scores each)
        assert team_data["A"]["total_scores"] == 4
        assert len(team_data["A"]["score_list"]) == 4
        assert (
            len(team_data["A"]["unique_scores"]) >= 1
        )  # At least some unique scores


def test_unwind_group_edge_case_invalid_accumulator():
    """Test $unwind + $group with invalid accumulator (should fallback)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"category": "books", "tags": ["fiction", "novel"]},
                {"category": "books", "tags": ["fiction", "mystery"]},
            ]
        )

        # Test with an invalid accumulator format that should cause fallback
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "count": {
                        "$invalidOp": 1
                    },  # Invalid operator should cause fallback
                }
            },
        ]

        # This should gracefully fall back to Python processing
        results = list(collection.aggregate(pipeline))

        # Should still work (fallback to Python)
        assert len(results) >= 2  # At least fiction, novel, mystery


def test_unwind_group_with_non_string_field_references():
    """Test $unwind + $group with non-string field references (should fallback)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [{"type": "A", "values": [1, 2]}, {"type": "B", "values": [3, 4]}]
        )

        # Test with complex group specification that should cause fallback
        pipeline = [
            {"$unwind": "$values"},
            {
                "$group": {
                    "_id": "$type",
                    # Complex accumulator that should cause fallback
                    "stats": {
                        "$sum": {
                            "$multiply": ["$values", 2]
                        }  # Complex expression
                    },
                }
            },
        ]

        # This should fall back to Python processing
        results = list(collection.aggregate(pipeline))

        # Should still work (fallback to Python)
        assert len(results) >= 1


if __name__ == "__main__":
    pytest.main([__file__])
