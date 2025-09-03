#!/usr/bin/env python3
"""
Test to ensure nested array unwind operations produce consistent results
between optimized and fallback implementations.
"""

import neosqlite


def test_nested_array_unwind_consistency():
    """Test that nested array unwind produces identical results in optimized and fallback modes."""
    with neosqlite.Connection(":memory:") as conn:
        users = conn["users"]

        # Insert test data with nested arrays
        user_docs = [
            {
                "_id": 1,
                "name": "User 1",
                "projects": [
                    {
                        "name": "Project 0",
                        "tasks": ["task0", "task1", "task2"],
                    },
                    {
                        "name": "Project 1",
                        "tasks": ["task3", "task4"],
                    },
                ],
            },
            {
                "_id": 2,
                "name": "User 2",
                "projects": [
                    {
                        "name": "Project 2",
                        "tasks": ["task5"],
                    }
                ],
            },
        ]
        users.insert_many(user_docs)

        # Pipeline with nested array unwind operations
        pipeline = [{"$unwind": "$projects"}, {"$unwind": "$projects.tasks"}]

        # Test optimized path
        neosqlite.collection.query_helper.set_force_fallback(False)
        result_optimized = list(users.aggregate(pipeline))

        # Test fallback path
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_fallback = list(users.aggregate(pipeline))

        # Reset to normal operation
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Verify both paths produce identical results
        assert len(result_optimized) == len(
            result_fallback
        ), f"Result counts differ: optimized={len(result_optimized)}, fallback={len(result_fallback)}"

        # Sort both results for consistent comparison
        result_optimized.sort(
            key=lambda x: (x["_id"], x["projects"]["name"], x["projects.tasks"])
        )
        result_fallback.sort(
            key=lambda x: (x["_id"], x["projects"]["name"], x["projects.tasks"])
        )

        # Compare each document
        for i, (opt_doc, fb_doc) in enumerate(
            zip(result_optimized, result_fallback)
        ):
            assert (
                opt_doc == fb_doc
            ), f"Document {i} differs between optimized and fallback:\nOptimized: {opt_doc}\nFallback:  {fb_doc}"

        # Verify we have the expected number of results
        # User 1: 2 projects × (3 + 2 tasks) = 5 results
        # User 2: 1 project × 1 task = 1 result
        # Total: 6 results
        assert (
            len(result_optimized) == 6
        ), f"Expected 6 results, got {len(result_optimized)}"


if __name__ == "__main__":
    test_nested_array_unwind_consistency()
    print("Nested array unwind consistency test passed!")
