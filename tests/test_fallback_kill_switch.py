# coding: utf-8
"""
Test to demonstrate how a 'kill switch' for forcing Python fallback could be implemented
"""
import neosqlite
import os


# Global flag to force fallback - this would be added to the codebase
_FORCE_FALLBACK = False


def set_force_fallback(force=True):
    """Set global flag to force all aggregation queries to use Python fallback"""
    global _FORCE_FALLBACK
    _FORCE_FALLBACK = force


def test_fallback_kill_switch():
    """Test demonstrating how a fallback kill switch would work"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript"]},
                {"name": "Bob", "tags": ["java", "python"]},
            ]
        )

        # Test normal operation (should use SQL optimization)
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]

        result_normal = collection.aggregate(pipeline)
        assert len(result_normal) == 3

        # If we had a kill switch, we would enable it like this:
        # set_force_fallback(True)
        # result_fallback = collection.aggregate(pipeline)
        # set_force_fallback(False)
        #
        # # Results should be identical
        # assert result_normal == result_fallback
        # print("Kill switch test passed - results identical between paths")


if __name__ == "__main__":
    test_fallback_kill_switch()
    print("Demonstration of fallback kill switch concept completed!")
