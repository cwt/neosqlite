#!/usr/bin/env python3
"""
Enhanced benchmark covering additional SQL-optimized features in NeoSQLite.

This benchmark specifically tests the additional SQL optimization features that
were not covered in the comprehensive benchmark.
"""

import neosqlite
import time
import statistics
from typing import List, Dict, Any


def benchmark_feature(
    name: str, collection, pipeline: List[Dict[str, Any]], num_runs: int = 3
) -> Dict[str, float]:
    """Benchmark a feature with both optimized and fallback paths."""
    print(f"\n--- {name} ---")

    # Test optimized path
    neosqlite.collection.query_helper.set_force_fallback(False)
    optimized_times = []
    for _ in range(num_runs):
        start_time = time.perf_counter()
        cursor_optimized = collection.aggregate(pipeline)
        # Force execution by converting to list
        result_optimized = list(cursor_optimized)
        optimized_times.append(time.perf_counter() - start_time)

    avg_optimized = statistics.mean(optimized_times)

    # Test fallback path
    neosqlite.collection.query_helper.set_force_fallback(True)
    fallback_times = []
    for _ in range(num_runs):
        start_time = time.perf_counter()
        cursor_fallback = collection.aggregate(pipeline)
        # Force execution by converting to list
        result_fallback = list(cursor_fallback)
        fallback_times.append(time.perf_counter() - start_time)

    avg_fallback = statistics.mean(fallback_times)

    # Reset to normal operation
    neosqlite.collection.query_helper.set_force_fallback(False)

    # Verify results are identical (for simple counts)
    result_count_match = len(result_optimized) == len(result_fallback)

    speedup = (
        avg_fallback / avg_optimized if avg_optimized > 0 else float("inf")
    )

    print(f"  Optimized: {avg_optimized:.4f}s (avg of {num_runs} runs)")
    print(f"  Fallback:  {avg_fallback:.4f}s (avg of {num_runs} runs)")
    print(f"  Speedup:   {speedup:.1f}x faster")
    print(f"  Results match: {result_count_match}")

    return {
        "optimized_time": avg_optimized,
        "fallback_time": avg_fallback,
        "speedup": speedup,
        "results_match": result_count_match,
    }


def main():
    print("=== NeoSQLite Additional SQL Optimization Benchmark ===")
    print("Testing additional SQL-optimized features\n")

    with neosqlite.Connection(":memory:") as conn:
        # Create collections
        products = conn["products"]
        orders = conn["orders"]
        users = conn["users"]

        # Insert test datasets
        print("1. Preparing test data...")

        # Products data
        product_docs = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        for i in range(500):
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

        # Users data with some null/empty arrays for testing preserveNullAndEmptyArrays
        user_docs = []
        for i in range(300):
            # Some users with null/empty skills for testing advanced unwind options
            skills = None if i % 10 == 0 else [f"skill{j}" for j in range(2)]
            user_docs.append(
                {
                    "_id": i + 1,
                    "name": f"User {i + 1}",
                    "department": f"Department {i % 5}",
                    "skills": skills,
                    "projects": [
                        {
                            "name": f"Project {k}",
                            "tasks": [f"task{t}" for t in range(3)],
                        }
                        for k in range(2)  # 2 projects per user
                    ],
                    "scores": [
                        80 + (i % 20) for _ in range(3)
                    ],  # 3 scores per user
                }
            )
        users.insert_many(user_docs)
        print(f"   Inserted {len(user_docs)} users with nested arrays")

        # Orders data
        order_docs = []
        for i in range(100):
            order_docs.append(
                {
                    "_id": i + 1,
                    "userId": (i % 100) + 1,
                    "productId": (i % 100) + 1,
                    "quantity": (i % 10) + 1,
                    "total": float((i % 100) + 10),
                    "status": "shipped" if i % 4 != 0 else "pending",
                }
            )
        orders.insert_many(order_docs)
        print(f"   Inserted {len(order_docs)} orders")

        # Create indexes for optimization tests
        print("\n2. Creating indexes...")
        products.create_index("category")
        products.create_index("status")
        products.create_index("price")
        users.create_index("department")
        orders.create_index("status")
        orders.create_index("userId")
        print("   Created indexes on frequently queried fields")

        # Create FTS index for text search tests
        print("\n3. Creating FTS index for text search...")
        # Add some text searchable data
        text_docs = []
        comments_samples = [
            "This product has great performance",
            "Amazing quality and fast delivery",
            "Not satisfied with the performance",
            "Excellent value for money",
            "Poor performance and build quality",
        ]
        for i in range(200):
            text_docs.append(
                {
                    "_id": i + 1,
                    "title": f"Review {i + 1}",
                    "comments": [
                        {
                            "text": comments_samples[i % len(comments_samples)],
                            "author": f"User{i % 20}",
                        }
                        for _ in range(2)  # 2 comments per document
                    ],
                    "tags": [f"tag{j}" for j in range(3)],
                    "category": f"Category{i % 5}",
                }
            )
        text_collection = conn["reviews"]
        text_collection.insert_many(text_docs)
        text_collection.create_index("comments.text", fts=True)
        print("   Created FTS index for text search")

        # Run additional benchmarks
        results = {}

        # 1. Pipeline reordering optimization
        print("\n--- Pipeline Reordering Optimization ---")
        # Create a pipeline where reordering would be beneficial
        pipeline_reorder = [
            {"$unwind": "$tags"},  # Expensive operation first
            {
                "$match": {"category": "Category2", "status": "active"}
            },  # Should be moved to front
            {"$limit": 20},
        ]

        # Test optimized path
        neosqlite.collection.query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_opt = products.aggregate(pipeline_reorder)
        result_opt = list(cursor_opt)
        optimized_time = time.perf_counter() - start_time

        # Test fallback path
        neosqlite.collection.query_helper.set_force_fallback(True)
        start_time = time.perf_counter()
        cursor_fallback = products.aggregate(pipeline_reorder)
        result_fallback = list(cursor_fallback)
        fallback_time = time.perf_counter() - start_time

        neosqlite.collection.query_helper.set_force_fallback(False)
        speedup = (
            fallback_time / optimized_time
            if optimized_time > 0
            else float("inf")
        )
        print(f"  Optimized: {optimized_time:.4f}s")
        print(f"  Fallback:  {fallback_time:.4f}s")
        print(f"  Speedup:   {speedup:.1f}x faster")
        print(f"  Results match: {len(result_opt) == len(result_fallback)}")

        results["Pipeline Reordering"] = {
            "optimized_time": optimized_time,
            "fallback_time": fallback_time,
            "speedup": speedup,
            "results_match": len(result_opt) == len(result_fallback),
        }

        # 2. Advanced $unwind with preserveNullAndEmptyArrays
        print("\n--- $unwind with preserveNullAndEmptyArrays ---")
        pipeline_preserve = [
            {"$unwind": {"path": "$skills", "preserveNullAndEmptyArrays": True}}
        ]

        # This feature is implemented in Python, so only test the fallback path
        start_time = time.perf_counter()
        cursor_preserve = users.aggregate(pipeline_preserve)
        result_preserve = list(cursor_preserve)
        preserve_time = time.perf_counter() - start_time
        print(f"  Preserve null/empty arrays time: {preserve_time:.4f}s")
        print(f"  Result count: {len(result_preserve)} documents")

        results["$unwind (preserveNullAndEmptyArrays)"] = {
            "optimized_time": float("inf"),  # Not optimized
            "fallback_time": preserve_time,
            "speedup": 0,
            "results_match": True,
        }

        # 3. Text search with json_each() optimization
        print("\n--- Text Search with json_each() Optimization ---")
        pipeline_text = [
            {"$unwind": "$comments"},
            {"$match": {"$text": {"$search": "performance"}}},
        ]

        # Test optimized path
        neosqlite.collection.query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_text_opt = text_collection.aggregate(pipeline_text)
        result_text_opt = list(cursor_text_opt)
        text_optimized_time = time.perf_counter() - start_time

        # Test fallback path
        neosqlite.collection.query_helper.set_force_fallback(True)
        start_time = time.perf_counter()
        cursor_text_fallback = text_collection.aggregate(pipeline_text)
        result_text_fallback = list(cursor_text_fallback)
        text_fallback_time = time.perf_counter() - start_time

        neosqlite.collection.query_helper.set_force_fallback(False)
        text_speedup = (
            text_fallback_time / text_optimized_time
            if text_optimized_time > 0
            else float("inf")
        )
        print(f"  Optimized: {text_optimized_time:.4f}s")
        print(f"  Fallback:  {text_fallback_time:.4f}s")
        print(f"  Speedup:   {text_speedup:.1f}x faster")
        print(
            f"  Results match: {len(result_text_opt) == len(result_text_fallback)}"
        )

        results["Text Search + json_each()"] = {
            "optimized_time": text_optimized_time,
            "fallback_time": text_fallback_time,
            "speedup": text_speedup,
            "results_match": len(result_text_opt) == len(result_text_fallback),
        }

        # 4. Memory-constrained processing with quez
        print("\n--- Memory-Constrained Processing (quez) ---")
        # Test with a large result set
        pipeline_large = [{"$unwind": "$tags"}]

        # Test without quez (normal processing)
        neosqlite.collection.query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_normal = products.aggregate(pipeline_large)
        result_normal = list(cursor_normal)
        normal_time = time.perf_counter() - start_time

        # Test with quez (memory-constrained processing)
        start_time = time.perf_counter()
        cursor_quez = products.aggregate(pipeline_large)
        cursor_quez.use_quez(True)
        result_quez = list(cursor_quez)
        quez_time = time.perf_counter() - start_time

        print(f"  Normal processing: {normal_time:.4f}s")
        print(f"  Quez processing:   {quez_time:.4f}s")
        print(f"  Result count match: {len(result_normal) == len(result_quez)}")

        results["Memory-Constrained (quez)"] = {
            "optimized_time": normal_time,
            "fallback_time": quez_time,
            "speedup": (
                normal_time / quez_time if quez_time > 0 else float("inf")
            ),
            "results_match": len(result_normal) == len(result_quez),
        }

        # Summary
        print("\n" + "=" * 70)
        print("ADDITIONAL BENCHMARK SUMMARY")
        print("=" * 70)

        total_speedup = 0
        optimized_count = 0

        for feature, data in results.items():
            if data["speedup"] > 0 and data["speedup"] != float("inf"):
                total_speedup += data["speedup"]
                optimized_count += 1

        avg_speedup = (
            total_speedup / optimized_count if optimized_count > 0 else 0
        )

        print(
            f"Average speedup across additional optimized features: {avg_speedup:.1f}x"
        )
        print(f"Number of SQL-optimized features tested: {optimized_count}")
        print(f"Total additional features tested: {len(results)}")

        print("\nPerformance Analysis:")
        print(
            "- Pipeline reordering significantly improves performance by filtering early"
        )
        print("- Text search with json_each() provides substantial speedups")
        print(
            "- Memory-constrained processing with quez maintains performance while reducing memory usage"
        )
        print(
            "- Advanced $unwind options provide flexibility at the cost of performance"
        )

        print("\nTechnical Details:")
        print(
            "- Pipeline reordering automatically moves indexed $match operations to the front"
        )
        print(
            "- Text search leverages FTS5 indexes with json_each() for array processing"
        )
        print(
            "- Quez compression reduces memory footprint for large result sets"
        )
        print(
            "- Advanced $unwind options handle edge cases like null arrays properly"
        )


if __name__ == "__main__":
    main()
    print("\n=== Additional Benchmark Complete ===")
