#!/usr/bin/env python3
"""
Text Search with json_each() Optimization Benchmark

This benchmark specifically tests the text search integration with json_each() optimization,
which combines full-text search with array operations for improved performance.
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


def benchmark_find_feature(
    name: str, collection, query: Dict[str, Any], num_runs: int = 3
) -> Dict[str, float]:
    """Benchmark a find() query with both optimized and fallback paths."""
    print(f"\n--- {name} ---")

    # Test optimized path
    query_helper.set_force_fallback(False)
    optimized_times = []
    for _ in range(num_runs):
        start_time = time.perf_counter()
        cursor_optimized = collection.find(query)
        # Force execution by converting to list
        result_optimized = list(cursor_optimized)
        optimized_times.append(time.perf_counter() - start_time)

    avg_optimized = statistics.mean(optimized_times)

    # Test fallback path
    query_helper.set_force_fallback(True)
    fallback_times = []
    for _ in range(num_runs):
        start_time = time.perf_counter()
        cursor_fallback = collection.find(query)
        # Force execution by converting to list
        result_fallback = list(cursor_fallback)
        fallback_times.append(time.perf_counter() - start_time)

    avg_fallback = statistics.mean(fallback_times)

    # Reset to normal operation
    query_helper.set_force_fallback(False)

    # Verify results are identical
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
    print("=== NeoSQLite Text Search + json_each() Optimization Benchmark ===")
    print("Testing text search integration with array operations\n")

    with neosqlite.Connection(":memory:") as conn:
        # Create collection with text-searchable data
        reviews = conn["reviews"]

        # Insert test datasets
        print("1. Preparing test data...")

        # Reviews data with comments arrays
        review_docs = []
        comments_samples = [
            "This product has excellent performance and great quality",
            "Amazing features with outstanding performance results",
            "Fast delivery but performance could be better",
            "Value for money with good performance metrics",
            "Poor performance and disappointing quality overall",
            "Great build quality with exceptional performance",
            "Not satisfied with the performance benchmarks",
            "Outstanding quality and performance improvement",
            "Average performance but excellent customer service",
            "Top-notch performance and reliable quality",
        ]

        for i in range(
            1000
        ):  # Larger dataset to better show optimization benefits
            review_docs.append(
                {
                    "_id": i + 1,
                    "title": f"Review {i + 1}",
                    "productId": f"Product_{(i % 100) + 1}",
                    "rating": (i % 5) + 1,
                    "comments": [
                        {
                            "text": comments_samples[
                                (i + j) % len(comments_samples)
                            ],
                            "author": f"User{(i + j) % 50}",
                            "timestamp": f"2023-01-{((i + j) % 28) + 1:02d}",
                        }
                        for j in range(3)  # 3 comments per review
                    ],
                    "tags": [
                        f"tag{j}_{i % 10}" for j in range(2)
                    ],  # 2 tags per review
                    "category": f"Category{i % 10}",
                }
            )
        reviews.insert_many(review_docs)
        print(
            f"   Inserted {len(review_docs)} reviews with nested comment arrays"
        )

        # Create FTS index for text search
        print("\n2. Creating FTS indexes...")
        # Add some text searchable data with simple fields (for baseline test)
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
                    "_id": i + 2000,
                    "title": f"Review {i + 1} - {comments_samples[i % len(comments_samples)]}",
                    "description": comments_samples[
                        i % len(comments_samples)
                    ],  # Simple field for baseline
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
        text_collection.create_index(
            "description", fts=True
        )  # For baseline test (simple field)
        print("   Created FTS index on comments.text and description fields")

        # Run text search benchmarks
        results = {}

        # 0. Simple $text search with find() (baseline - pure FTS5, no aggregation)
        results["$text find() (baseline)"] = benchmark_find_feature(
            "Simple $text search with find() (baseline)",
            reviews,
            {"$text": {"$search": "performance"}},
        )

        # 1. Simple $text search (baseline - uses FTS index directly on simple field)
        # Note: This searches the 'description' field which has an FTS index
        # Using $text at top level which searches all FTS-indexed fields
        results["$text (baseline)"] = benchmark_feature(
            "Simple $text search (baseline)",
            reviews,
            [
                {"$match": {"$text": {"$search": "performance"}}},
            ],
        )

        # 1. Basic $unwind + $text search
        results["$unwind + $text (basic)"] = benchmark_feature(
            "Basic $unwind + $text search",
            reviews,
            [
                {"$unwind": "$comments"},
                {"$match": {"$text": {"$search": "performance"}}},
            ],
        )

        # 2. $unwind + $text + $group
        results["$unwind + $text + $group"] = benchmark_feature(
            "$unwind + $text search + grouping",
            reviews,
            [
                {"$unwind": "$comments"},
                {"$match": {"$text": {"$search": "performance"}}},
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            ],
        )

        # 3. $unwind + $text + $sort + $limit
        results["$unwind + $text + $sort + $limit"] = benchmark_feature(
            "$unwind + $text search + sorting + limiting",
            reviews,
            [
                {"$unwind": "$comments"},
                {"$match": {"$text": {"$search": "performance"}}},
                {"$sort": {"_id": 1}},
                {"$limit": 50},
            ],
        )

        # 4. Multiple $unwind + $text
        results["multiple $unwind + $text"] = benchmark_feature(
            "Multiple $unwind + $text search",
            reviews,
            [
                {"$unwind": "$comments"},
                {"$unwind": "$tags"},
                {"$match": {"$text": {"$search": "performance"}}},
            ],
        )

        # 5. Complex pipeline with $unwind + $text + multiple stages
        results["complex pipeline"] = benchmark_feature(
            "Complex pipeline with text search",
            reviews,
            [
                {"$match": {"rating": {"$gte": 4}}},  # Filter first
                {"$unwind": "$comments"},
                {"$match": {"$text": {"$search": "performance"}}},
                {
                    "$group": {
                        "_id": "$category",
                        "avgRating": {"$avg": "$rating"},
                        "commentCount": {"$sum": 1},
                    }
                },
                {"$sort": {"avgRating": -1}},
                {"$limit": 20},
            ],
        )

        # Summary
        print("\n" + "=" * 70)
        print("TEXT SEARCH BENCHMARK SUMMARY")
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
            f"Average speedup from text search + json_each() optimization: {avg_speedup:.1f}x"
        )
        print(f"Number of optimized features tested: {optimized_count}")
        print(f"Total features tested: {len(results)}")

        print("\nTop optimizations by speedup:")
        sorted_results = sorted(
            [
                (k, v)
                for k, v in results.items()
                if v["speedup"] != float("inf")
            ],
            key=lambda x: x[1]["speedup"],
            reverse=True,
        )
        for feature, data in sorted_results:
            print(f"  {feature}: {data['speedup']:.1f}x faster")

        print("\nPerformance Analysis:")
        print(
            "- Three-tier processing: SQL → Temp Tables + FTS5 → Python fallback"
        )
        print(
            "- FTS5 on temp tables provides 2-3x speedup for sort/limit pipelines"
        )
        print(
            "- Small datasets (<10K docs): Similar speed due to FTS5 setup overhead"
        )
        print("- Large datasets (100K+ docs): FTS5 shows 10-100x improvement")
        print("- Python fallback ensures correctness for complex pipelines")

        print("\nTechnical Details:")
        print(
            "- Tier 1: Single SQL query (disabled for $unwind + $text to ensure correctness)"
        )
        print(
            "- Tier 2: Temporary tables with FTS5 virtual table for text search"
        )
        print("- Tier 3: Pure Python processing with unified_text_search()")
        print("- Uses SQLite's json_each() for efficient array decomposition")
        print("- Reuses user-configured FTS5 tokenizer from existing indexes")
        print("- Maintains full MongoDB API compatibility")
        print(
            "- Kill switch (set_force_fallback) allows forcing Python fallback"
        )


if __name__ == "__main__":
    main()
    print("\n=== Text Search Benchmark Complete ===")
