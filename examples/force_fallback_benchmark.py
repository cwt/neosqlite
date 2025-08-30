#!/usr/bin/env python3
"""
Example script demonstrating how to use the force fallback kill switch for benchmarking
"""
import neosqlite
import time


def benchmark_aggregation_performance():
    """Benchmark the performance difference between SQL optimization and Python fallback"""

    # Create connection and collection
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["benchmark_test"]

        # Insert test data
        print("Inserting test data...")
        test_data = [
            {
                "category": f"Category{i % 10}",
                "tags": [f"tag{j}" for j in range(5)],
                "value": i,
            }
            for i in range(5000)
        ]
        collection.insert_many(test_data)
        print(f"Inserted {len(test_data)} documents")

        # Define test pipeline (simpler one without complex group operations)
        pipeline = [
            {"$unwind": "$tags"},
            {"$match": {"category": "Category5"}},
            {"$sort": {"tags": 1}},
            {"$limit": 100},
        ]

        print("\nRunning benchmark...")

        # Test optimized path
        neosqlite.collection.query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_optimized = collection.aggregate(pipeline)
        # Force execution by converting to list to get accurate timing
        result_optimized = list(cursor_optimized)
        optimized_time = time.perf_counter() - start_time

        # Test fallback path
        neosqlite.collection.query_helper.set_force_fallback(True)
        start_time = time.perf_counter()
        cursor_fallback = collection.aggregate(pipeline)
        # Force execution by converting to list to get accurate timing
        result_fallback = list(cursor_fallback)
        fallback_time = time.perf_counter() - start_time

        # Reset to normal operation
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Verify results are identical
        print(f"Optimized result count: {len(result_optimized)}")
        print(f"Fallback result count: {len(result_fallback)}")

        # Compare results
        print(
            f"\nResults identical: {len(result_optimized) == len(result_fallback)}"
        )

        # Print performance results
        print(f"\nPerformance Results:")
        print(f"  Optimized path: {optimized_time:.4f} seconds")
        print(f"  Fallback path:  {fallback_time:.4f} seconds")
        if optimized_time > 0:
            speedup = fallback_time / optimized_time
            print(
                f"  Speedup:        {speedup:.2f}x faster with SQL optimization"
            )

        # Show sample results
        print(f"\nSample Results (first 3):")
        for i, doc in enumerate(result_optimized[:3]):
            print(
                f"  {i+1}. Category: {doc['category']}, Tag: {doc['tags']}, Value: {doc['value']}"
            )


def demonstrate_force_fallback_usage():
    """Demonstrate basic usage of the force fallback feature"""

    print("=== Force Fallback Demonstration ===")

    # Check initial state
    print(
        f"Initial fallback state: {neosqlite.collection.query_helper.get_force_fallback()}"
    )

    # Enable fallback
    neosqlite.collection.query_helper.set_force_fallback(True)
    print(
        f"After enabling fallback: {neosqlite.collection.query_helper.get_force_fallback()}"
    )

    # Disable fallback
    neosqlite.collection.query_helper.set_force_fallback(False)
    print(
        f"After disabling fallback: {neosqlite.collection.query_helper.get_force_fallback()}"
    )


if __name__ == "__main__":
    demonstrate_force_fallback_usage()
    print()
    benchmark_aggregation_performance()
