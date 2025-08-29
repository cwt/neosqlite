# coding: utf-8
"""
Test to verify that group operations work correctly with force fallback
"""
import neosqlite


def test_group_with_force_fallback():
    """Test that group operations work correctly when force fallback is enabled"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "price": 10, "quantity": 2},
                {"category": "B", "price": 20, "quantity": 1},
                {"category": "A", "price": 30, "quantity": 5},
            ]
        )

        # Pipeline with group operations
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "total_price": {"$sum": "$price"},
                    "avg_quantity": {"$avg": "$quantity"},
                    "count": {"$sum": 1},  # This should work with our fix
                }
            }
        ]

        # Test with force fallback enabled
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_fallback = collection.aggregate(pipeline)

        # Reset flag
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Verify results
        assert len(result_fallback) == 2
        result_fallback.sort(key=lambda x: x["_id"])

        # Check category A
        assert result_fallback[0]["_id"] == "A"
        assert result_fallback[0]["total_price"] == 40  # 10 + 30
        assert result_fallback[0]["avg_quantity"] == 3.5  # (2 + 5) / 2
        assert result_fallback[0]["count"] == 2  # Count of documents

        # Check category B
        assert result_fallback[1]["_id"] == "B"
        assert result_fallback[1]["total_price"] == 20
        assert result_fallback[1]["avg_quantity"] == 1.0
        assert result_fallback[1]["count"] == 1


if __name__ == "__main__":
    test_group_with_force_fallback()
    print("Group operations with force fallback test passed!")
