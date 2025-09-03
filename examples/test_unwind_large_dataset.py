#!/usr/bin/env python3
"""
Debug script to test unwind operations with larger datasets.

This script tests the same unwind operations with 5,000 documents to see if
performance characteristics change with larger datasets.
"""

import neosqlite
import time
from typing import List, Dict, Any


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
    print("=== NeoSQLite Unwind Performance Test (5,000 documents) ===")

    with neosqlite.Connection(":memory:") as conn:
        # Create collection
        products = conn["products"]

        # Insert larger test data
        print("1. Preparing test data (5,000 documents)...")
        product_docs = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        for i in range(5000):  # Larger dataset
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

        # Create users collection with nested arrays
        users = conn["users"]
        user_docs = []
        for i in range(2500):  # Larger dataset
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
        print(f"   Inserted {len(user_docs)} users with nested arrays")

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
            products,
            [{"$unwind": "$tags"}],
            "Single $unwind operation (5,000 documents)",
        )

        # 2. Multiple consecutive $unwind operations
        results["$unwind (multiple)"] = test_unwind_execution(
            users,
            [{"$unwind": "$skills"}, {"$unwind": "$scores"}],
            "Multiple consecutive $unwind operations (2,500 documents)",
        )

        # 3. Nested array $unwind operations
        results["$unwind (nested)"] = test_unwind_execution(
            users,
            [{"$unwind": "$projects"}, {"$unwind": "$projects.tasks"}],
            "Nested array $unwind operations (2,500 documents)",
        )

        # Summary
        print("\n" + "=" * 60)
        print("LARGE DATASET TEST SUMMARY")
        print("=" * 60)

        for feature, data in results.items():
            print(f"\n{feature}:")
            print(f"  Speedup: {data['speedup']:.1f}x")
            print(f"  Result count: {data['result_count']}")
            print(f"  Results match: {data['results_match']}")

    print("\n=== Large Dataset Test Complete ===")


if __name__ == "__main__":
    main()
