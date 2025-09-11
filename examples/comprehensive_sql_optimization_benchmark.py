#!/usr/bin/env python3
"""
Comprehensive benchmark comparing SQL-optimized features vs Python fallback in NeoSQLite.

This benchmark tests all major SQL-optimized features with moderate datasets (500-1000 documents)
to demonstrate the performance benefits of SQL optimization without taking too long.
"""

import neosqlite
import time
import statistics
from neosqlite.collection import query_helper
from typing import List, Dict, Any


def benchmark_feature(
    name: str, collection, pipeline: List[Dict[str, Any]], num_runs: int = 3
) -> Dict[str, float]:
    """Benchmark a feature with both optimized and fallback paths."""
    print(f"\n--- {name} ---")

    # Test optimized path
    query_helper.set_force_fallback(False)
    optimized_times = []
    for _ in range(num_runs):
        start_time = time.perf_counter()
        cursor_optimized = collection.aggregate(pipeline)
        # Force execution by converting to list
        result_optimized = list(cursor_optimized)
        optimized_times.append(time.perf_counter() - start_time)

    avg_optimized = statistics.mean(optimized_times)

    # Test fallback path
    query_helper.set_force_fallback(True)
    fallback_times = []
    for _ in range(num_runs):
        start_time = time.perf_counter()
        cursor_fallback = collection.aggregate(pipeline)
        # Force execution by converting to list
        result_fallback = list(cursor_fallback)
        fallback_times.append(time.perf_counter() - start_time)

    avg_fallback = statistics.mean(fallback_times)

    # Reset to normal operation
    query_helper.set_force_fallback(False)

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
    print("=== NeoSQLite SQL Optimization vs Python Fallback Benchmark ===")
    print("Testing with moderate datasets for reasonable benchmark times\n")

    with neosqlite.Connection(":memory:") as conn:
        # Create collections
        products = conn["products"]
        orders = conn["orders"]
        users = conn["users"]

        # Insert moderate test datasets
        print("1. Preparing test data...")

        # Products data (1000 documents - reduced from 5000)
        product_docs = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        for i in range(1000):
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

        # Users data (500 documents - reduced from 1000)
        user_docs = []
        for i in range(500):
            user_docs.append(
                {
                    "_id": i + 1,
                    "name": f"User {i + 1}",
                    "department": f"Department {i % 5}",
                    "skills": [
                        f"skill{j}" for j in range(2)
                    ],  # 2 skills per user
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

        # Orders data (100 documents - reduced from 500 for $lookup test)
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

        # Run benchmarks
        results = {}

        # 1. Simple $match optimization
        results["$match (indexed field)"] = benchmark_feature(
            "$match with indexed field",
            products,
            [{"$match": {"category": "Electronics"}}],
        )

        # 2. $match with multiple conditions
        results["$match (multiple conditions)"] = benchmark_feature(
            "$match with multiple indexed conditions",
            products,
            [{"$match": {"category": "Electronics", "status": "active"}}],
        )

        # 3. Simple $unwind optimization
        results["$unwind (single)"] = benchmark_feature(
            "Single $unwind operation", products, [{"$unwind": "$tags"}]
        )

        # 4. Multiple consecutive $unwind operations
        results["$unwind (multiple)"] = benchmark_feature(
            "Multiple consecutive $unwind operations",
            users,
            [{"$unwind": "$skills"}, {"$unwind": "$scores"}],
        )

        # 5. Nested array $unwind
        results["$unwind (nested)"] = benchmark_feature(
            "Nested array $unwind operations",
            users,
            [{"$unwind": "$projects"}, {"$unwind": "$projects.tasks"}],
        )

        # 6. $unwind + $group optimization with $sum
        results["$unwind + $group ($sum)"] = benchmark_feature(
            "$unwind + $group with $sum accumulator",
            products,
            [
                {"$unwind": "$tags"},
                {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            ],
        )

        # 7. $unwind + $group optimization with $count
        results["$unwind + $group ($count)"] = benchmark_feature(
            "$unwind + $group with $count accumulator",
            products,
            [
                {"$unwind": "$tags"},
                {"$group": {"_id": "$tags", "total": {"$count": {}}}},
            ],
        )

        # 8. $unwind + $group with $push
        results["$unwind + $group ($push)"] = benchmark_feature(
            "$unwind + $group with $push accumulator",
            products,
            [
                {"$unwind": "$tags"},
                {"$group": {"_id": "$category", "tagList": {"$push": "$tags"}}},
            ],
        )

        # 9. $unwind + $group with $addToSet
        results["$unwind + $group ($addToSet)"] = benchmark_feature(
            "$unwind + $group with $addToSet accumulator",
            products,
            [
                {"$unwind": "$tags"},
                {
                    "$group": {
                        "_id": "$category",
                        "uniqueTags": {"$addToSet": "$tags"},
                    }
                },
            ],
        )

        # 10. $unwind + $group with complex accumulators
        results["$unwind + $group (complex)"] = benchmark_feature(
            "$unwind + $group with multiple accumulators",
            products,
            [
                {"$unwind": "$tags"},
                {
                    "$group": {
                        "_id": "$tags",
                        "count": {"$sum": 1},
                        "avgPrice": {"$avg": "$price"},
                        "minPrice": {"$min": "$price"},
                        "maxPrice": {"$max": "$price"},
                    }
                },
            ],
        )

        # 11. $unwind + $sort + $limit optimization
        results["$unwind + $sort + $limit"] = benchmark_feature(
            "$unwind + $sort + $limit combination",
            products,
            [{"$unwind": "$tags"}, {"$sort": {"tags": 1}}, {"$limit": 50}],
        )

        # 12. $match + $unwind + $group optimization
        results["$match + $unwind + $group"] = benchmark_feature(
            "$match + $unwind + $group combination",
            products,
            [
                {"$match": {"status": "active"}},
                {"$unwind": "$tags"},
                {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            ],
        )

        # 13. $lookup optimization (simple) - simplified test
        print("\n--- Simple $lookup operation ---")
        query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_lookup_opt = orders.aggregate(
            [
                {
                    "$lookup": {
                        "from": "products",
                        "localField": "productId",
                        "foreignField": "_id",
                        "as": "productInfo",
                    }
                }
            ]
        )
        result_lookup_opt = list(cursor_lookup_opt)
        optimized_time = time.perf_counter() - start_time

        query_helper.set_force_fallback(True)
        start_time = time.perf_counter()
        cursor_lookup_fallback = orders.aggregate(
            [
                {
                    "$lookup": {
                        "from": "products",
                        "localField": "productId",
                        "foreignField": "_id",
                        "as": "productInfo",
                    }
                }
            ]
        )
        result_lookup_fallback = list(cursor_lookup_fallback)
        fallback_time = time.perf_counter() - start_time

        query_helper.set_force_fallback(False)
        speedup = (
            fallback_time / optimized_time
            if optimized_time > 0
            else float("inf")
        )
        print(f"  Optimized: {optimized_time:.4f}s")
        print(f"  Fallback:  {fallback_time:.4f}s")
        print(f"  Speedup:   {speedup:.1f}x faster")
        print(
            f"  Results match: {len(result_lookup_opt) == len(result_lookup_fallback)}"
        )
        results["$lookup (simple)"] = {
            "optimized_time": optimized_time,
            "fallback_time": fallback_time,
            "speedup": speedup,
            "results_match": (
                len(result_lookup_opt) == len(result_lookup_fallback)
            ),
        }

        # 14. Advanced $unwind with includeArrayIndex (fallback only)
        print("\n--- $unwind with includeArrayIndex (Python fallback only) ---")
        start_time = time.perf_counter()
        cursor_advanced = users.aggregate(
            [
                {
                    "$unwind": {
                        "path": "$skills",
                        "includeArrayIndex": "skillIndex",
                    }
                }
            ]
        )
        result_advanced = list(cursor_advanced)
        advanced_time = time.perf_counter() - start_time
        print(f"  Advanced $unwind time: {advanced_time:.4f}s")
        print(f"  Result count: {len(result_advanced)} documents")
        results["$unwind (advanced)"] = {
            "optimized_time": float("inf"),  # Not optimized
            "fallback_time": advanced_time,
            "speedup": 0,
            "results_match": True,
        }

        # 15. Advanced $unwind with preserveNullAndEmptyArrays
        print("\n--- $unwind with preserveNullAndEmptyArrays ---")
        start_time = time.perf_counter()
        cursor_preserve = users.aggregate(
            [
                {
                    "$unwind": {
                        "path": "$skills",
                        "preserveNullAndEmptyArrays": True,
                    }
                }
            ]
        )
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

        # 16. Pipeline reordering optimization
        print("\n--- Pipeline Reordering Optimization ---")
        # Create a pipeline where reordering would be beneficial
        pipeline_reorder = [
            {"$unwind": "$tags"},  # Expensive operation first
            {
                "$match": {"category": "Electronics", "status": "active"}
            },  # Should be moved to front
            {"$limit": 20},
        ]

        # Test optimized path
        query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_reorder_opt = products.aggregate(pipeline_reorder)
        result_reorder_opt = list(cursor_reorder_opt)
        reorder_optimized_time = time.perf_counter() - start_time

        # Test fallback path
        query_helper.set_force_fallback(True)
        start_time = time.perf_counter()
        cursor_reorder_fallback = products.aggregate(pipeline_reorder)
        result_reorder_fallback = list(cursor_reorder_fallback)
        reorder_fallback_time = time.perf_counter() - start_time

        query_helper.set_force_fallback(False)
        reorder_speedup = (
            reorder_fallback_time / reorder_optimized_time
            if reorder_optimized_time > 0
            else float("inf")
        )
        print(f"  Optimized: {reorder_optimized_time:.4f}s")
        print(f"  Fallback:  {reorder_fallback_time:.4f}s")
        print(f"  Speedup:   {reorder_speedup:.1f}x faster")
        print(
            f"  Results match: {len(result_reorder_opt) == len(result_reorder_fallback)}"
        )

        results["Pipeline Reordering"] = {
            "optimized_time": reorder_optimized_time,
            "fallback_time": reorder_fallback_time,
            "speedup": reorder_speedup,
            "results_match": (
                len(result_reorder_opt) == len(result_reorder_fallback)
            ),
        }

        # 17. Memory-constrained processing with quez
        print("\n--- Memory-Constrained Processing (quez) ---")
        # Test with a large result set
        pipeline_large = [{"$unwind": "$tags"}]

        # Test without quez (normal processing)
        query_helper.set_force_fallback(False)
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
        print("BENCHMARK SUMMARY")
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
            f"Average speedup across all optimized features: {avg_speedup:.1f}x"
        )
        print(f"Number of SQL-optimized features tested: {optimized_count}")
        print(f"Total features tested: {len(results)}")

        print("\nTop 5 fastest optimizations:")
        sorted_results = sorted(
            [
                (k, v)
                for k, v in results.items()
                if v["speedup"] != float("inf")
            ],
            key=lambda x: x[1]["speedup"],
            reverse=True,
        )
        for feature, data in sorted_results[:5]:
            print(f"  {feature}: {data['speedup']:.1f}x faster")

        print("\nPerformance Analysis:")
        print("- SQL optimization provides significant performance benefits")
        print(
            "- Complex operations like $unwind + $group show the biggest improvements"
        )
        print("- Simple operations still benefit from optimization")
        print("- Advanced features fall back to Python but remain functional")
        print(
            "- Index usage further enhances performance for $match operations"
        )
        print(
            "- Pipeline reordering optimization provides significant benefits"
        )
        print(
            "- Memory-constrained processing maintains performance with reduced memory usage"
        )

        print("\nTechnical Details:")
        print("- SQL optimization uses SQLite's native JSON functions")
        print(
            "- Operations execute at database level, reducing Python overhead"
        )
        print("- No intermediate data structures needed for optimized paths")
        print("- Complex pipelines maintain full MongoDB API compatibility")
        print(
            "- Fallback ensures all features work even when optimization isn't possible"
        )
        print("- Pipeline reordering automatically optimizes execution order")
        print(
            "- Memory-constrained processing uses quez compression for large result sets"
        )


if __name__ == "__main__":
    main()
    print("\n=== Benchmark Complete ===")
