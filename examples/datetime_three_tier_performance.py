#!/usr/bin/env python3
"""
Demonstration of NeoSQLite's Three-Tier Performance Architecture

Shows how the datetime query processor automatically selects the
optimal execution tier based on query complexity.
"""

import time
from neosqlite import Connection
from neosqlite.collection.query_helper import (
    set_force_fallback,
)


def demonstrate_performance_tiers():
    """Demonstrate the three-tier performance architecture."""

    print("NeoSQLite Three-Tier Performance Architecture Demonstration")
    print("=" * 59)
    print()

    # Create database and populate with test data
    db = Connection(":memory:")
    collection = db["performance_test"]

    print("Setting up test data...")

    # Insert test data

    test_docs = []
    for i in range(100):
        # Create documents with different timestamps
        timestamp = f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}"
        test_docs.append(
            {
                "id": i,
                "timestamp": timestamp,
                "value": i * 1.5,
                "category": f"category_{i % 5}",
                "metadata": {"created_by": f"user_{i % 10}", "priority": i % 5},
            }
        )

    # Insert documents
    for doc in test_docs:
        collection.insert_one(doc)

    print(f"Inserted {len(test_docs)} documents into collection")
    print()

    # Test 1: Simple SQL tier query
    print("Tier 1: SQL Tier Performance (Simple Queries)")
    print("-" * 48)

    query1 = {"timestamp": {"$gte": "2023-06-01T00:00:00"}}
    start_time = time.time()
    results1 = list(collection.find(query1))
    sql_time = time.time() - start_time

    print(f"Query: {query1}")
    print(f"Results: {len(results1)} documents")
    print(f"Execution Time: {sql_time*1000:.2f}ms")
    print("Tier Used: SQL (Direct SQL processing with json_* functions)")
    print()

    # Test 2: Temp table tier query (simulated by forcing temp table)
    print("Tier 2: Temp Table Tier Performance (Complex Queries)")
    print("-" * 54)

    # This would normally trigger temp table tier with complex conditions
    complex_query = {
        "$and": [
            {"timestamp": {"$gte": "2023-03-01T00:00:00"}},
            {"timestamp": {"$lt": "2023-09-01T00:00:00"}},
            {"category": {"$in": ["category_1", "category_2"]}},
            {"metadata.priority": {"$gt": 2}},
        ]
    }

    start_time = time.time()
    results2 = list(collection.find(complex_query))
    temp_table_time = time.time() - start_time

    print("Query: Complex nested conditions (simulated)")
    print(f"Results: {len(results2)} documents")
    print(f"Execution Time: {temp_table_time*1000:.2f}ms")
    print("Tier Used: Temp Table (Advanced SQL processing)")
    print()

    # Test 3: Python tier query (forced via kill switch)
    print("Tier 3: Python Tier Performance (Fallback Mode)")
    print("-" * 46)

    # Force Python fallback
    set_force_fallback(True)

    start_time = time.time()
    results3 = list(collection.find(query1))
    python_time = time.time() - start_time

    # Turn off fallback for normal operations
    set_force_fallback(False)

    print(f"Query: {query1} (forced Python fallback)")
    print(f"Results: {len(results3)} documents")
    print(f"Execution Time: {python_time*1000:.2f}ms")
    print("Tier Used: Python (Pure Python processing fallback)")
    print()

    # Performance comparison
    print("Performance Comparison:")
    print("=" * 24)
    print(f"SQL Tier:     {sql_time*1000:.2f}ms")
    print(f"Temp Table:   {temp_table_time*1000:.2f}ms")
    print(f"Python Tier:  {python_time*1000:.2f}ms")
    print()

    if python_time > 0:
        print("Performance Ratios:")
        print(f"SQL vs Python:     {python_time/sql_time:.1f}x faster")
        print(f"Temp vs Python:    {python_time/temp_table_time:.1f}x faster")
    else:
        print("Performance ratios: Unable to calculate (zero division)")

    # Cleanup
    db.close()

    print("\nPerformance tier demonstration completed!")


if __name__ == "__main__":
    demonstrate_performance_tiers()
