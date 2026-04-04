#!/usr/bin/env python3
"""
Demonstration of NeoSQLite's Three-Tier Performance Architecture

Shows how the same query performs across different execution tiers:
- Tier 1: SQL (direct SQL processing)
- Tier 2: Python fallback (pure Python processing)
"""

import time

from neosqlite import Connection
from neosqlite.collection.query_helper import (
    set_force_fallback,
)


def run_timed_query(collection, query, num_runs=5):
    """Run a query multiple times and return average time and results."""
    # Warmup run
    list(collection.find(query))

    times = []
    results = None
    for _ in range(num_runs):
        start = time.perf_counter()
        results = list(collection.find(query))
        times.append(time.perf_counter() - start)

    avg_time = sum(times) / len(times)
    return avg_time, len(results)


def demonstrate_performance_tiers():
    """Demonstrate the three-tier performance architecture."""

    print("NeoSQLite Three-Tier Performance Architecture Demonstration")
    print("=" * 59)
    print()

    with Connection(":memory:") as db:
        collection = db["performance_test"]

        print("Setting up test data (10000 documents)...")

        test_docs = []
        for i in range(10000):
            timestamp = f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}"
            test_docs.append(
                {
                    "id": i,
                    "timestamp": timestamp,
                    "value": i * 1.5,
                    "category": f"category_{i % 5}",
                    "metadata": {
                        "created_by": f"user_{i % 10}",
                        "priority": i % 5,
                    },
                }
            )

        for doc in test_docs:
            collection.insert_one(doc)

        print(f"Inserted {len(test_docs)} documents into collection")
        print()

        # Create compound index using PyMongo-compatible tuple format
        print("Creating compound index on ('timestamp', 'category')...")
        collection.create_index([("timestamp", 1), ("category", 1)])
        print()

        # Use a query that benefits from the compound index
        query = {
            "timestamp": {
                "$gte": "2023-12-01T00:00:00",
                "$lt": "2023-12-31T00:00:00",
            },
            "category": "category_0",
        }

        # Tier 1: SQL (default, optimized path)
        print("Tier 1: SQL Tier (Direct SQL processing)")
        print("-" * 45)
        sql_time, sql_count = run_timed_query(collection, query)
        print(f"Query: {query}")
        print(f"Results: {sql_count} documents")
        print(f"Average Time: {sql_time * 1000:.2f}ms")
        print()

        # Tier 3: Python fallback (forced)
        print("Tier 3: Python Tier (Fallback mode)")
        print("-" * 42)
        set_force_fallback(True)
        try:
            python_time, python_count = run_timed_query(collection, query)
        finally:
            set_force_fallback(False)
        print(f"Query: {query}")
        print(f"Results: {python_count} documents")
        print(f"Average Time: {python_time * 1000:.2f}ms")
        print()

        # Performance comparison
        print("Performance Comparison:")
        print("=" * 30)
        print(f"SQL Tier:    {sql_time * 1000:.2f}ms (avg of 5 runs)")
        print(f"Python Tier: {python_time * 1000:.2f}ms (avg of 5 runs)")
        print()

        if sql_time > 0 and python_time > 0:
            speedup = python_time / sql_time
            print("Performance Ratios:")
            print(f"  SQL is {speedup:.1f}x faster than Python fallback")
            if speedup > 1:
                improvement = (1 - 1 / speedup) * 100
                print(f"  ({improvement:.0f}% faster)")
        else:
            print("Performance ratios: Unable to calculate (zero division)")

        print()
        print("Key Takeaways:")
        print("- SQL tier processes queries directly in SQLite")
        print("- Python fallback ensures correctness for complex queries")
        print(
            "- For in-memory databases with moderate data, both tiers perform similarly"
        )
        print(
            "- SQL tier advantage grows with larger datasets and disk-based storage"
        )
        print("- The library automatically selects the optimal tier")
        print()
        print("Performance tier demonstration completed!")


if __name__ == "__main__":
    demonstrate_performance_tiers()
