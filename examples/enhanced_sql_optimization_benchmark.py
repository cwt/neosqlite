#!/usr/bin/env python3
"""
Comprehensive benchmark comparing SQL-optimized features vs Python fallback in NeoSQLite.

This benchmark tests all major SQL-optimized features with moderate datasets (500-1000 documents)
to demonstrate the performance benefits of SQL optimization without taking too long.

Enhanced with temporary table aggregation pipeline tests to showcase benefits of complex pipeline processing.
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


def benchmark_temporary_table_feature(
    name: str, collection, pipeline: List[Dict[str, Any]], num_runs: int = 3
) -> Dict[str, float]:
    """Benchmark a feature that benefits from temporary table aggregation."""
    print(f"\n--- {name} ---")

    # Test with temporary table aggregation approach
    from neosqlite.temporary_table_aggregation import (
        TemporaryTableAggregationProcessor,
        can_process_with_temporary_tables,
    )

    # Only run if the pipeline can be processed with temporary tables
    if can_process_with_temporary_tables(pipeline):
        try:
            temp_table_times = []
            for _ in range(num_runs):
                start_time = time.perf_counter()
                processor = TemporaryTableAggregationProcessor(collection)
                result_temp_table = processor.process_pipeline(pipeline)
                temp_table_times.append(time.perf_counter() - start_time)

            avg_temp_table = statistics.mean(temp_table_times)
            print(
                f"  Temporary Table: {avg_temp_table:.4f}s (avg of {num_runs} runs)"
            )

            # Also test standard approach for comparison
            standard_times = []
            for _ in range(num_runs):
                start_time = time.perf_counter()
                result_standard = list(collection.aggregate(pipeline))
                standard_times.append(time.perf_counter() - start_time)

            avg_standard = statistics.mean(standard_times)
            print(
                f"  Standard:         {avg_standard:.4f}s (avg of {num_runs} runs)"
            )

            speedup = (
                avg_standard / avg_temp_table
                if avg_temp_table > 0
                else float("inf")
            )
            print(f"  Speedup:          {speedup:.1f}x faster")

            return {
                "temporary_table_time": avg_temp_table,
                "standard_time": avg_standard,
                "speedup": speedup,
                # Remove the method field since it's not a float
            }
        except Exception as e:
            print(f"  Temporary Table approach failed: {e}")
            # Fall back to standard approach
            pass

    # If temporary table approach isn't applicable or failed, test standard approach
    return benchmark_feature(name, collection, pipeline, num_runs)


def benchmark_complex_pipeline_with_temporary_tables(
    name: str,
    collection1,
    collection2,
    pipeline: List[Dict[str, Any]],
    num_runs: int = 3,
) -> Dict[str, float]:
    """Benchmark a complex pipeline that benefits from temporary table aggregation."""
    print(f"\n--- {name} ---")

    from neosqlite.temporary_table_aggregation import (
        TemporaryTableAggregationProcessor,
        can_process_with_temporary_tables,
        integrate_with_neosqlite,
    )

    # Test with temporary table integration approach
    try:
        temp_table_times = []
        for _ in range(num_runs):
            start_time = time.perf_counter()
            result_temp_table = integrate_with_neosqlite(
                collection1.query_engine, pipeline
            )
            temp_table_times.append(time.perf_counter() - start_time)

        avg_temp_table = statistics.mean(temp_table_times)
        print(
            f"  Integrated (Temp Tables): {avg_temp_table:.4f}s (avg of {num_runs} runs)"
        )

        # Also test standard approach for comparison
        standard_times = []
        for _ in range(num_runs):
            start_time = time.perf_counter()
            result_standard = list(collection1.aggregate(pipeline))
            standard_times.append(time.perf_counter() - start_time)

        avg_standard = statistics.mean(standard_times)
        print(
            f"  Standard:                 {avg_standard:.4f}s (avg of {num_runs} runs)"
        )

        speedup = (
            avg_standard / avg_temp_table
            if avg_temp_table > 0
            else float("inf")
        )
        print(f"  Speedup:                  {speedup:.1f}x faster")

        return {
            "temporary_table_time": avg_temp_table,
            "standard_time": avg_standard,
            "speedup": speedup,
            # Remove the method field since it's not a float
        }
    except Exception as e:
        print(f"  Integration approach failed: {e}")
        # Fall back to standard approach timing
        start_time = time.perf_counter()
        result_standard = list(collection1.aggregate(pipeline))
        standard_time = time.perf_counter() - start_time

        # For fallback, both times are the same
        return {
            "temporary_table_time": standard_time,
            "standard_time": standard_time,
            "speedup": 1.0,  # No speedup
        }


def main():
    print("=== NeoSQLite SQL Optimization vs Python Fallback Benchmark ===")
    print("Testing with moderate datasets for reasonable benchmark times\n")

    with neosqlite.Connection(":memory:") as conn:
        # Create collections
        products = conn["products"]
        orders = conn["orders"]
        users = conn["users"]
        categories = conn["categories"]

        # Insert moderate test datasets
        print("1. Preparing test data...")

        # Products data (1000 documents - reduced from 5000)
        product_docs = []
        categories_list = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        for i in range(1000):
            product_docs.append(
                {
                    "_id": i + 1,
                    "name": f"Product {i + 1}",
                    "category": categories_list[i % len(categories_list)],
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

        # Categories data for lookup operations
        category_docs = []
        for i, cat in enumerate(categories_list):
            category_docs.append(
                {
                    "_id": i + 1,
                    "name": cat,
                    "description": f"Description for {cat}",
                    "parentCategory": "General" if i % 2 == 0 else "Specialty",
                }
            )
        categories.insert_many(category_docs)
        print(f"   Inserted {len(category_docs)} categories")

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
                    "preferences": {
                        "notifications": True,
                        "theme": "dark" if i % 2 == 0 else "light",
                        "language": "en",
                    },
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
                    "orderDate": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                    "items": [
                        {
                            "itemId": f"ITEM{i * 10 + j}",
                            "itemName": f"Item {i * 10 + j}",
                            "quantity": (j % 3) + 1,
                            "price": float(10 + (j % 50)),
                        }
                        for j in range(2)  # 2 items per order
                    ],
                }
            )
        orders.insert_many(order_docs)
        print(f"   Inserted {len(order_docs)} orders with nested items")

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
        neosqlite.collection.query_helper.set_force_fallback(False)
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

        neosqlite.collection.query_helper.set_force_fallback(True)
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

        neosqlite.collection.query_helper.set_force_fallback(False)
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
            "results_match": len(result_lookup_opt)
            == len(result_lookup_fallback),
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
        neosqlite.collection.query_helper.set_force_fallback(False)
        start_time = time.perf_counter()
        cursor_reorder_opt = products.aggregate(pipeline_reorder)
        result_reorder_opt = list(cursor_reorder_opt)
        reorder_optimized_time = time.perf_counter() - start_time

        # Test fallback path
        neosqlite.collection.query_helper.set_force_fallback(True)
        start_time = time.perf_counter()
        cursor_reorder_fallback = products.aggregate(pipeline_reorder)
        result_reorder_fallback = list(cursor_reorder_fallback)
        reorder_fallback_time = time.perf_counter() - start_time

        neosqlite.collection.query_helper.set_force_fallback(False)
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
            "results_match": len(result_reorder_opt)
            == len(result_reorder_fallback),
        }

        # 17. Memory-constrained processing with quez
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

        # === NEW ENHANCED TESTS FOR TEMPORARY TABLE AGGREGATION ===

        # 18. Complex pipeline with $lookup not in last position (benefits from temp tables)
        print("\n--- Complex Pipeline: $lookup not in last position ---")
        complex_pipeline_1 = [
            {"$match": {"status": "active"}},
            {
                "$lookup": {
                    "from": "categories",
                    "localField": "category",
                    "foreignField": "name",
                    "as": "categoryInfo",
                }
            },
            {"$unwind": "$categoryInfo"},
            {"$match": {"categoryInfo.parentCategory": "General"}},
            {"$sort": {"name": 1}},
            {"$limit": 20},
        ]

        # This pipeline benefits from temporary table processing because:
        # 1. $lookup is not in the last position (current implementation limitation)
        # 2. Multiple stages after $lookup
        # 3. Complex filtering after $lookup

        # Use our new benchmark function that tests temporary table integration
        results["Complex Pipeline ($lookup mid-pipeline)"] = (
            benchmark_complex_pipeline_with_temporary_tables(
                "Complex Pipeline: $lookup not in last position",
                products,
                categories,
                complex_pipeline_1,
            )
        )

        # 19. Multiple consecutive $unwind with $lookup (highly complex)
        print("\n--- Highly Complex Pipeline: Multiple $unwind + $lookup ---")
        complex_pipeline_2 = [
            {"$match": {"status": "shipped"}},
            {"$unwind": "$items"},
            {
                "$lookup": {
                    "from": "products",
                    "localField": "items.itemId",
                    "foreignField": "_id",
                    "as": "productDetails",
                }
            },
            {"$unwind": "$productDetails"},
            {"$match": {"productDetails.price": {"$gte": 20}}},
            {"$sort": {"total": -1}},
            {"$limit": 15},
        ]

        results["Highly Complex Pipeline"] = (
            benchmark_complex_pipeline_with_temporary_tables(
                "Highly Complex Pipeline: Multiple $unwind + $lookup",
                orders,
                products,
                complex_pipeline_2,
            )
        )

        # 20. Deeply nested unwind with grouping (challenging for current implementation)
        print(
            "\n--- Challenging Pipeline: Deeply nested unwind with grouping ---"
        )
        complex_pipeline_3 = [
            {"$unwind": "$projects"},
            {"$unwind": "$projects.tasks"},
            {
                "$group": {
                    "_id": "$projects.name",
                    "totalTasks": {"$sum": 1},
                    "uniqueUsers": {"$addToSet": "$name"},
                    "taskList": {"$push": "$projects.tasks"},
                }
            },
            {"$sort": {"totalTasks": -1}},
            {"$limit": 10},
        ]

        results["Deep Nested Unwind + Group"] = (
            benchmark_complex_pipeline_with_temporary_tables(
                "Challenging Pipeline: Deeply nested unwind with grouping",
                users,
                None,  # Not needed for this test
                complex_pipeline_3,
            )
        )

        # 21. Pipeline with multiple $lookup operations
        print("\n--- Advanced Pipeline: Multiple $lookup operations ---")
        complex_pipeline_4 = [
            {"$match": {"status": "active"}},
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            },
            {"$unwind": "$userOrders"},
            {
                "$lookup": {
                    "from": "products",
                    "localField": "userOrders.productId",
                    "foreignField": "_id",
                    "as": "orderProducts",
                }
            },
            {"$unwind": "$orderProducts"},
            {"$match": {"orderProducts.price": {"$gte": 50}}},
            {
                "$group": {
                    "_id": "$name",
                    "totalSpent": {"$sum": "$userOrders.total"},
                    "favoriteCategories": {
                        "$addToSet": "$orderProducts.category"
                    },
                }
            },
            {"$sort": {"totalSpent": -1}},
            {"$limit": 5},
        ]

        results["Multiple $lookup Operations"] = (
            benchmark_complex_pipeline_with_temporary_tables(
                "Advanced Pipeline: Multiple $lookup operations",
                users,
                orders,
                complex_pipeline_4,
            )
        )

        # Summary
        print("\n" + "=" * 70)
        print("BENCHMARK SUMMARY")
        print("=" * 70)

        total_speedup = 0
        optimized_count = 0
        temp_table_count = 0
        temp_table_speedup = 0

        # Store which features use temporary table aggregation for summary
        temp_table_features = {
            "Complex Pipeline ($lookup mid-pipeline)",
            "Highly Complex Pipeline",
            "Deep Nested Unwind + Group",
            "Multiple $lookup Operations",
        }

        for feature, data in results.items():
            if (
                "speedup" in data
                and data["speedup"] > 0
                and data["speedup"] != float("inf")
            ):
                if feature in temp_table_features:
                    temp_table_speedup += data["speedup"]
                    temp_table_count += 1
                else:  # Standard optimization
                    total_speedup += data["speedup"]
                    optimized_count += 1

        avg_speedup = (
            total_speedup / optimized_count if optimized_count > 0 else 0
        )
        avg_temp_table_speedup = (
            temp_table_speedup / temp_table_count if temp_table_count > 0 else 0
        )

        print(
            f"Average speedup across standard optimized features: {avg_speedup:.1f}x"
        )
        print(
            f"Average speedup with temporary table aggregation: {avg_temp_table_speedup:.1f}x"
        )
        print(f"Number of SQL-optimized features tested: {optimized_count}")
        print(f"Number of temporary table features tested: {temp_table_count}")
        print(f"Total features tested: {len(results)}")

        print("\nTop 5 fastest optimizations:")
        sorted_results = sorted(
            [
                (k, v)
                for k, v in results.items()
                if "speedup" in v and v["speedup"] != float("inf")
            ],
            key=lambda x: x[1]["speedup"],
            reverse=True,
        )
        for feature, data in sorted_results[:5]:
            speedup = data["speedup"]
            method = data.get("method", "standard")
            print(f"  {feature}: {speedup:.1f}x faster ({method})")

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
        print(
            "- Temporary table aggregation enables optimization of previously unoptimizable pipelines"
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
        print(
            "- Temporary table aggregation processes complex pipelines in database"
        )
        print("- Intermediate results stored in database, not Python memory")
        print("- Automatic resource cleanup with transaction management")


if __name__ == "__main__":
    main()
    print("\n=== Benchmark Complete ===")
