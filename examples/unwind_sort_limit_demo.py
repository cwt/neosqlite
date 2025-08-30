#!/usr/bin/env python3
"""
Demonstration of the $unwind + $sort + $limit optimization in NeoSQLite.

This example shows how NeoSQLite optimizes aggregation pipelines that combine
$unwind, $sort, and $limit operations by pushing them down to SQLite's native
JSON processing capabilities.
"""

import neosqlite
import time


def demonstrate_unwind_sort_limit():
    """Demonstrate the $unwind + $sort + $limit optimization."""
    print("=== NeoSQLite $unwind + $sort + $limit Optimization ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data with arrays
        print("1. Inserting sample data...")
        sample_docs = [
            {
                "name": "Alice",
                "tags": ["python", "javascript", "go", "rust"],
                "score": 95,
                "department": "Engineering",
            },
            {
                "name": "Bob",
                "tags": ["java", "python", "c++", "rust"],
                "score": 87,
                "department": "Engineering",
            },
            {
                "name": "Charlie",
                "tags": ["javascript", "go", "rust", "swift"],
                "score": 92,
                "department": "Product",
            },
            {
                "name": "Diana",
                "tags": ["python", "java", "go", "javascript"],
                "score": 88,
                "department": "Engineering",
            },
            {
                "name": "Eve",
                "tags": ["c++", "rust", "go", "swift"],
                "score": 90,
                "department": "Product",
            },
        ]

        result = users.insert_many(sample_docs)
        print(f"   Inserted {len(result.inserted_ids)} documents\n")

        # Example 1: Basic $unwind + $sort + $limit
        print("2. Example: Basic $unwind + $sort + $limit")
        print("   Pipeline: $unwind tags, sort alphabetically, limit to 5")
        pipeline1 = [
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},  # 1 = ascending
            {"$limit": 5},
        ]

        start_time = time.time()
        result1 = list(users.aggregate(pipeline1))
        elapsed_time1 = time.time() - start_time

        print("   Results:")
        for doc in result1:
            print(f"     {doc['name']}: {doc['tags']}")
        print(f"   Processed in {elapsed_time1:.6f} seconds\n")

        # Example 2: $unwind + $sort by original field + $limit
        print("3. Example: $unwind + $sort by original field + $limit")
        print(
            "   Pipeline: $unwind tags, sort by user score (desc), limit to 5"
        )
        pipeline2 = [
            {"$unwind": "$tags"},
            {"$sort": {"score": -1}},  # -1 = descending
            {"$limit": 5},
        ]

        start_time = time.time()
        result2 = list(users.aggregate(pipeline2))
        elapsed_time2 = time.time() - start_time

        print("   Results:")
        for doc in result2:
            print(f"     {doc['name']} (score: {doc['score']}): {doc['tags']}")
        print(f"   Processed in {elapsed_time2:.6f} seconds\n")

        # Example 3: $match + $unwind + $sort + $limit
        print("4. Example: $match + $unwind + $sort + $limit")
        print(
            "   Pipeline: filter Engineering dept, $unwind tags, sort tags, limit to 5"
        )
        pipeline3 = [
            {"$match": {"department": "Engineering"}},
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$limit": 5},
        ]

        start_time = time.time()
        result3 = list(users.aggregate(pipeline3))
        elapsed_time3 = time.time() - start_time

        print("   Results:")
        for doc in result3:
            print(f"     {doc['name']} ({doc['department']}): {doc['tags']}")
        print(f"   Processed in {elapsed_time3:.6f} seconds\n")

        # Example 4: $unwind + $sort + $skip + $limit
        print("5. Example: $unwind + $sort + $skip + $limit")
        print("   Pipeline: $unwind tags, sort tags, skip 3, limit to 5")
        pipeline4 = [
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$skip": 3},
            {"$limit": 5},
        ]

        start_time = time.time()
        result4 = list(users.aggregate(pipeline4))
        elapsed_time4 = time.time() - start_time

        print("   Results:")
        for doc in result4:
            print(f"     {doc['name']}: {doc['tags']}")
        print(f"   Processed in {elapsed_time4:.6f} seconds\n")

        # Example 5: $unwind + $sort + $skip + $limit (already shown above)
        # Let's show another example with different sorting

        print("6. Example: $unwind + $sort by multiple fields + $limit")
        print("   Pipeline: $unwind tags, sort by name then tags, limit to 5")
        pipeline6 = [
            {"$unwind": "$tags"},
            {"$sort": {"name": 1, "tags": 1}},
            {"$limit": 5},
        ]

        start_time = time.time()
        result6 = list(users.aggregate(pipeline6))
        elapsed_time6 = time.time() - start_time

        print("   Results:")
        for doc in result6:
            print(f"     {doc['name']}: {doc['tags']}")
        print(f"   Processed in {elapsed_time6:.6f} seconds\n")

        print("=== Performance Benefits ===")
        print("• All operations are performed at the database level")
        print("• No intermediate Python data structures needed")
        print("• Native SQLite sorting is faster than Python-based sorting")
        print("• Memory usage is reduced by limiting results at database level")
        print("• Works with complex pipelines combining multiple operations")

        print("\n=== Supported Patterns ===")
        print("✓ $unwind + $sort + $limit")
        print("✓ $match + $unwind + $sort + $limit")
        print("✓ $unwind + $sort + $skip + $limit")
        print("✓ Multiple consecutive $unwind stages with sort/limit")
        print("✓ Sorting by unwound fields or original document fields")
        print("✓ Sorting by multiple fields")

        print("\n=== How It Works ===")
        print("1. NeoSQLite detects supported pipeline patterns")
        print("2. Generates optimized SQL with json_each() for unwinding")
        print("3. Uses ORDER BY for sorting and LIMIT/OFFSET for limiting")
        print("4. Executes everything at the database level")
        print("5. Falls back to Python for unsupported patterns")


if __name__ == "__main__":
    demonstrate_unwind_sort_limit()
    print("\n=== Demonstration Complete ===")
