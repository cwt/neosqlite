#!/usr/bin/env python3
"""
Focused test to verify actual memory savings with quez
"""

import neosqlite
import gc
import sys


def get_memory_usage():
    """Get current memory usage in bytes."""
    objects = gc.get_objects()
    total_size = 0
    for obj in objects:
        try:
            total_size += sys.getsizeof(obj)
        except Exception:
            pass
    return total_size


def test_actual_memory_savings():
    """Test actual memory savings with quez."""
    print("=== Testing Actual Memory Savings with Quez ===\n")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Create test data
        print("Creating test data...")
        test_docs = []
        for i in range(1000):
            doc = {
                "name": f"User {i:04d}",
                "email": f"user{i:04d}@example.com",
                "age": 20 + (i % 50),
                "department": f"Department {i % 10}",
                "salary": 30000 + (i * 100),
                "tags": [f"tag{j}_{i}" for j in range(5)],
                "bio": f"This is a detailed biography for user {i:04d}. " * 10,
            }
            test_docs.append(doc)

        collection.insert_many(test_docs)
        print(f"Inserted {len(test_docs):,} documents\n")

        # Pipeline
        pipeline = [
            {"$match": {"age": {"$gte": 25}}},
            {"$unwind": "$tags"},
            {"$limit": 500},
        ]

        print("Testing memory usage...")

        # Test normal processing
        print("\n1. Normal processing:")
        gc.collect()
        memory_before_normal = get_memory_usage()
        objects_before_normal = len(gc.get_objects())

        cursor_normal = collection.aggregate(pipeline)
        results_normal = list(cursor_normal)

        gc.collect()
        memory_after_normal = get_memory_usage()
        objects_after_normal = len(gc.get_objects())

        normal_memory_increase = memory_after_normal - memory_before_normal
        normal_objects_increase = objects_after_normal - objects_before_normal

        print(f"   Documents processed: {len(results_normal):,}")
        print(
            f"   Memory increase: {normal_memory_increase / (1024*1024):.2f} MB"
        )
        print(f"   Object increase: {normal_objects_increase:,}")

        # Test quez processing
        print("\n2. Quez processing:")
        gc.collect()
        memory_before_quez = get_memory_usage()
        objects_before_quez = len(gc.get_objects())

        cursor_quez = collection.aggregate(pipeline)
        cursor_quez.use_quez(True)
        cursor_quez._memory_threshold = 1  # Force quez usage

        # Process all documents
        results_quez = list(cursor_quez)

        gc.collect()
        memory_after_quez = get_memory_usage()
        objects_after_quez = len(gc.get_objects())

        quez_memory_increase = memory_after_quez - memory_before_quez
        quez_objects_increase = objects_after_quez - objects_before_quez

        print(f"   Documents processed: {len(results_quez):,}")
        print(
            f"   Memory increase: {quez_memory_increase / (1024*1024):.2f} MB"
        )
        print(f"   Object increase: {quez_objects_increase:,}")

        # Compare results
        print("\n3. Comparison:")
        if normal_memory_increase > 0:
            memory_savings = (
                1 - (quez_memory_increase / normal_memory_increase)
            ) * 100
            print(f"   Memory savings: {memory_savings:.1f}%")

        if normal_objects_increase > 0:
            object_savings = (
                1 - (quez_objects_increase / normal_objects_increase)
            ) * 100
            print(f"   Object savings: {object_savings:.1f}%")

        # Verify results are the same
        print("\n4. Result verification:")
        print(
            f"   Both methods processed same number of documents: {len(results_normal) == len(results_quez)}"
        )

        print("\n=== Test completed ===")


if __name__ == "__main__":
    test_actual_memory_savings()
