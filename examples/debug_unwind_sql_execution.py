#!/usr/bin/env python3
"""
Debug script to verify that unwind operations are executed using SQL optimization.

This script adds debugging information to track which execution path is being used
for the three cases with performance gain less than 1.5x.
"""

import neosqlite
import time
from typing import List, Dict, Any, Callable, Optional

# Add debugging to track SQL execution
original_build_unwind_query: Optional[Callable] = None


def debug_build_unwind_query(
    self,
    pipeline_index: int,
    pipeline: List[Dict[str, Any]],
    unwind_stages: List[str],
):
    """Debug version of _build_unwind_query that logs when SQL optimization is used."""
    print(
        f"  [DEBUG] Attempting SQL optimization for unwind stages: {unwind_stages}"
    )
    # Type check to ensure original_build_unwind_query is not None
    if original_build_unwind_query is not None:
        result = original_build_unwind_query(
            self, pipeline_index, pipeline, unwind_stages
        )
    else:
        result = None
    if result:
        print(
            f"  [DEBUG] SQL optimization SUCCESSFUL for unwind stages: {unwind_stages}"
        )
    else:
        print(
            f"  [DEBUG] SQL optimization FAILED, falling back to Python for unwind stages: {unwind_stages}"
        )
    return result


def setup_debugging():
    """Setup debugging for SQL optimization tracking."""
    global original_build_unwind_query
    if original_build_unwind_query is None:
        original_build_unwind_query = (
            neosqlite.collection.query_helper.QueryHelper._build_unwind_query
        )
        neosqlite.collection.query_helper.QueryHelper._build_unwind_query = (
            debug_build_unwind_query
        )


def teardown_debugging():
    """Restore original functions."""
    global original_build_unwind_query
    if original_build_unwind_query is not None:
        neosqlite.collection.query_helper.QueryHelper._build_unwind_query = (
            original_build_unwind_query
        )
        original_build_unwind_query = None


def test_unwind_execution(
    collection, pipeline: List[Dict[str, Any]], description: str
) -> Dict[str, Any]:
    """Test a specific unwind operation and report execution path."""
    print(f"\n--- {description} ---")

    # Test optimized path
    neosqlite.collection.query_helper.set_force_fallback(False)

    start_time = time.perf_counter()
    cursor_optimized = collection.aggregate(pipeline)
    result_optimized = list(cursor_optimized)
    optimized_time = time.perf_counter() - start_time

    # Test fallback path
    neosqlite.collection.query_helper.set_force_fallback(True)

    start_time = time.perf_counter()
    cursor_fallback = collection.aggregate(pipeline)
    result_fallback = list(cursor_fallback)
    fallback_time = time.perf_counter() - start_time

    # Reset to normal operation
    neosqlite.collection.query_helper.set_force_fallback(False)

    # Verify results are identical
    result_count_match = len(result_optimized) == len(result_fallback)

    speedup = (
        fallback_time / optimized_time if optimized_time > 0 else float("inf")
    )

    print(f"  Optimized: {optimized_time:.4f}s")
    print(f"  Fallback:  {fallback_time:.4f}s")
    print(f"  Speedup:   {speedup:.1f}x faster")
    print(f"  Results match: {result_count_match}")
    print(f"  Result count: {len(result_optimized)} documents")

    return {
        "optimized_time": optimized_time,
        "fallback_time": fallback_time,
        "speedup": speedup,
        "results_match": result_count_match,
        "result_count": len(result_optimized),
    }


def main():
    print("=== NeoSQLite Unwind SQL Execution Debug ===")

    # Setup debugging
    setup_debugging()

    try:
        with neosqlite.Connection(":memory:") as conn:
            # Create collection
            products = conn["products"]

            # Insert test data
            print("1. Preparing test data...")
            product_docs = []
            categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
            for i in range(100):  # Smaller dataset for debugging
                product_docs.append(
                    {
                        "_id": i + 1,
                        "name": f"Product {i + 1}",
                        "category": categories[i % len(categories)],
                        "price": float(10 + (i % 200)),
                        "tags": [
                            f"tag{j}_{i % 5}" for j in range(3)
                        ],  # 3 tags per product
                        "brand": f"Brand {(i // 50) % 5}",
                        "status": "active" if i % 3 != 0 else "inactive",
                    }
                )
            products.insert_many(product_docs)
            print(f"   Inserted {len(product_docs)} products")

            # Create indexes
            print("\n2. Creating indexes...")
            products.create_index("category")
            products.create_index("status")
            products.create_index("price")
            print("   Created indexes on frequently queried fields")

            # Test cases with performance gain less than 1.5x
            results = {}

            # 1. Single $unwind operation
            results["$unwind (single)"] = test_unwind_execution(
                products, [{"$unwind": "$tags"}], "Single $unwind operation"
            )

            # 2. Multiple consecutive $unwind operations
            # Create users collection with nested arrays for this test
            users = conn["users"]
            user_docs = []
            for i in range(50):  # Smaller dataset for debugging
                user_docs.append(
                    {
                        "_id": i + 1,
                        "name": f"User {i + 1}",
                        "department": f"Department {i % 5}",
                        "skills": [
                            f"skill{j}" for j in range(2)
                        ],  # 2 skills per user
                        "scores": [
                            80 + (i % 20) for _ in range(3)
                        ],  # 3 scores per user
                    }
                )
            users.insert_many(user_docs)
            print(
                f"\n   Inserted {len(user_docs)} users with nested arrays for multiple unwind test"
            )

            results["$unwind (multiple)"] = test_unwind_execution(
                users,
                [{"$unwind": "$skills"}, {"$unwind": "$scores"}],
                "Multiple consecutive $unwind operations",
            )

            # 3. Nested array $unwind operations
            # Add projects with tasks to users
            users.delete_many({})  # Clear collection
            user_docs = []
            for i in range(50):  # Smaller dataset for debugging
                user_docs.append(
                    {
                        "_id": i + 1,
                        "name": f"User {i + 1}",
                        "projects": [
                            {
                                "name": f"Project {k}",
                                "tasks": [f"task{t}" for t in range(3)],
                            }
                            for k in range(2)  # 2 projects per user
                        ],
                    }
                )
            users.insert_many(user_docs)
            print(
                f"   Updated users with nested project/task arrays for nested unwind test"
            )

            results["$unwind (nested)"] = test_unwind_execution(
                users,
                [{"$unwind": "$projects"}, {"$unwind": "$projects.tasks"}],
                "Nested array $unwind operations",
            )

            # Summary
            print("\n" + "=" * 60)
            print("DEBUG SUMMARY")
            print("=" * 60)

            for feature, data in results.items():
                print(f"\n{feature}:")
                print(f"  Speedup: {data['speedup']:.1f}x")
                print(f"  Result count: {data['result_count']}")
                print(f"  Results match: {data['results_match']}")

    finally:
        # Teardown debugging
        teardown_debugging()

    print("\n=== Debug Complete ===")


if __name__ == "__main__":
    main()
