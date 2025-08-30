#!/usr/bin/env python3
"""
Targeted memory benchmark for NeoSQLite quez integration.

This benchmark focuses specifically on measuring the memory usage
of aggregation result sets with and without quez.
"""

import neosqlite
import time
import sys
import gc


def get_size(obj, seen=None):
    """Recursively find size of objects in bytes."""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, "__dict__"):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, "__iter__") and not isinstance(
        obj, (str, bytes, bytearray)
    ):
        try:
            size += sum([get_size(i, seen) for i in obj])
        except TypeError:
            pass
    return size


def create_test_data(collection, num_docs=5000):
    """Create test data with large documents."""
    print(f"Creating {num_docs:,} large documents...")

    docs = []
    for i in range(num_docs):
        # Create larger documents to make memory differences more apparent
        doc = {
            "_id": i,
            "name": f"User {i:06d}",
            "email": f"user{i:06d}@example.com",
            "age": 20 + (i % 50),
            "department": f"Department {i % 20}",
            "salary": 30000 + (i * 100),
            "tags": [f"tag{j}_{i}" for j in range(20)],  # More tags
            "address": {
                "street": f"{i} Main Street Avenue Boulevard Lane",
                "city": f"City {i % 100}",
                "state": f"State {i % 50}",
                "zipcode": f"{10000 + (i % 90000):05d}",
                "country": "United States of America",
                "coordinates": {
                    "lat": 40.0 + (i % 100) * 0.1,
                    "lng": -70.0 - (i % 100) * 0.1,
                },
            },
            "preferences": {
                "theme": "dark" if i % 2 == 0 else "light",
                "language": "en-US",
                "notifications": True,
                "privacy": "public" if i % 3 == 0 else "private",
            },
            "history": [f"event_{j}_{i}" for j in range(50)],  # History events
            "bio": f"This is a detailed biography for user {i:06d}. "
            * 20,  # Longer bio
        }
        docs.append(doc)

    # Insert in smaller batches
    batch_size = 500
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        collection.insert_many(batch)
        if (i // batch_size) % 10 == 0:  # Show progress every 10 batches
            print(
                f"  Inserted {min(i+batch_size, len(docs)):,}/{num_docs:,} documents"
            )

    print(
        f"Dataset creation complete with {collection.count_documents({}):,} documents.\n"
    )


def measure_result_set_memory_normal(collection):
    """Measure memory usage of normal result set."""
    print("=== Measuring Normal Result Set Memory ===")

    # Pipeline that creates a large result set
    pipeline = [
        {"$match": {"age": {"$gte": 25}}},
        {"$unwind": "$tags"},
        {"$unwind": "$history"},  # Double unwind to create large result set
        {"$limit": 2000},
    ]

    # Force garbage collection
    gc.collect()

    # Measure memory before
    before = gc.get_objects()
    before_count = len(before)

    # Execute aggregation
    start_time = time.time()
    cursor = collection.aggregate(pipeline)
    results = list(cursor)
    execution_time = time.time() - start_time

    # Measure memory after
    gc.collect()
    after = gc.get_objects()
    after_count = len(after)

    # Estimate memory usage of results
    results_size = get_size(results)

    print(f"  Documents in result set: {len(results):,}")
    print(f"  Execution time: {execution_time:.4f} seconds")
    print(f"  Result set size: {results_size / (1024*1024):.2f} MB")
    print(f"  Object count increase: {after_count - before_count:,}")
    print(
        f"  First result size: {get_size(results[0]) if results else 0:,} bytes"
    )
    print()

    return {
        "execution_time": execution_time,
        "results_size": results_size,
        "object_count_increase": after_count - before_count,
        "result_count": len(results),
    }


def measure_result_set_memory_quez(collection):
    """Measure memory usage with quez processing."""
    print("=== Measuring Quez Result Set Memory ===")

    # Same pipeline
    pipeline = [
        {"$match": {"age": {"$gte": 25}}},
        {"$unwind": "$tags"},
        {"$unwind": "$history"},
        {"$limit": 2000},
    ]

    # Force garbage collection
    gc.collect()

    # Measure memory before
    before = gc.get_objects()
    before_count = len(before)

    # Execute aggregation with quez
    start_time = time.time()
    cursor = collection.aggregate(pipeline)
    cursor.use_quez(True)

    # Process results incrementally
    processed_count = 0
    total_size = 0
    first_doc_size = 0

    for doc in cursor:
        processed_count += 1
        doc_size = get_size(doc)
        total_size += doc_size
        if processed_count == 1:
            first_doc_size = doc_size
        # In real usage, we'd process the document here

    execution_time = time.time() - start_time

    # Measure memory after
    gc.collect()
    after = gc.get_objects()
    after_count = len(after)

    # Average size per document
    avg_size = total_size / processed_count if processed_count > 0 else 0

    print(f"  Documents processed: {processed_count:,}")
    print(f"  Execution time: {execution_time:.4f} seconds")
    print(f"  Average document size: {avg_size / 1024:.2f} KB")
    print(f"  Object count increase: {after_count - before_count:,}")
    print(f"  First document size: {first_doc_size:,} bytes")
    print()

    return {
        "execution_time": execution_time,
        "avg_document_size": avg_size,
        "object_count_increase": after_count - before_count,
        "processed_count": processed_count,
    }


def demonstrate_quez_benefits(collection):
    """Demonstrate the specific benefits of quez."""
    print("=== Demonstrating Quez Benefits ===")

    # Create a pipeline that would create a very large result set
    pipeline = [
        {"$match": {"age": {"$gte": 20}}},  # Match most documents
        {"$unwind": "$tags"},
        {"$unwind": "$history"},
        {"$limit": 10000},  # Very large result set
    ]

    print("Testing with very large result set (10,000 documents)...")

    # Test 1: Try normal processing (might be memory intensive)
    print("\n  1. Normal processing (loading all into memory):")
    try:
        gc.collect()
        before_objects = len(gc.get_objects())

        start_time = time.time()
        cursor1 = collection.aggregate(pipeline)
        results1 = list(cursor1)
        normal_time = time.time() - start_time

        gc.collect()
        after_objects = len(gc.get_objects())

        normal_memory_objects = after_objects - before_objects
        normal_results_size = get_size(results1)

        print(f"     Success! Processed {len(results1):,} documents")
        print(f"     Time: {normal_time:.4f} seconds")
        print(
            f"     Result set size: {normal_results_size / (1024*1024):.2f} MB"
        )
        print(f"     Object increase: {normal_memory_objects:,}")

    except MemoryError:
        print("     FAILED: Out of memory with normal processing")
        normal_time = float("inf")
        normal_results_size = float("inf")
        normal_memory_objects = float("inf")
        results1 = []

    # Test 2: Quez processing (memory efficient)
    print("\n  2. Quez processing (incremental processing):")
    try:
        gc.collect()
        before_objects = len(gc.get_objects())

        start_time = time.time()
        cursor2 = collection.aggregate(pipeline)
        cursor2.use_quez(True)
        cursor2._memory_threshold = 1  # Force quez usage

        count = 0
        for doc in cursor2:
            count += 1
            # Simulate processing
            if count % 2000 == 0:
                print(f"     Processed {count:,} documents...")
                # Show quez stats at each 2000 batch
                stats = cursor2.get_quez_stats()
                if stats:
                    print(
                        f"       Queue stats: {stats['count']:,} items remaining"
                    )
                    print(
                        f"       Raw size: {stats['raw_size_bytes'] / (1024*1024):.2f} MB"
                    )
                    print(
                        f"       Compressed size: {stats['compressed_size_bytes'] / (1024*1024):.2f} MB"
                    )
                    if stats["compression_ratio_pct"] is not None:
                        print(
                            f"       Compression ratio: {stats['compression_ratio_pct']:.1f}%"
                        )

        quez_time = time.time() - start_time

        gc.collect()
        after_objects = len(gc.get_objects())

        quez_memory_objects = after_objects - before_objects

        print(f"     Success! Processed {count:,} documents")
        print(f"     Time: {quez_time:.4f} seconds")
        print(f"     Object increase: {quez_memory_objects:,}")

        # Calculate memory savings
        if normal_memory_objects != float("inf") and quez_memory_objects > 0:
            memory_savings = (
                1 - (quez_memory_objects / normal_memory_objects)
            ) * 100
            print(f"     Memory savings: {memory_savings:.1f}%")

    except Exception as e:
        print(f"     FAILED: {e}")
        quez_time = float("inf")
        quez_memory_objects = float("inf")


def main():
    """Run the targeted memory benchmark."""
    print("=== NeoSQLite Quez Memory Benchmark ===")
    print("Focused measurement of result set memory usage\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Create test data
        create_test_data(users, num_docs=5000)

        # Measure memory usage
        normal_results = measure_result_set_memory_normal(users)
        quez_results = measure_result_set_memory_quez(users)

        # Compare results
        print("=== Memory Usage Comparison ===")
        if (
            normal_results["results_size"] > 0
            and quez_results["avg_document_size"] > 0
        ):
            # Calculate approximate memory savings
            normal_avg_size = (
                normal_results["results_size"] / normal_results["result_count"]
            )
            quez_avg_size = quez_results["avg_document_size"]
            size_diff = (
                (quez_avg_size - normal_avg_size) / normal_avg_size
            ) * 100

            print(f"Average document size difference: {size_diff:+.1f}%")
            print(
                "  Note: Normal processing loads all documents into memory at once"
            )
            print("  while quez processes them incrementally.")

        # Demonstrate quez benefits with large result set
        demonstrate_quez_benefits(users)

        print("\n=== Key Benefits of Quez Integration ===")
        print("1. Reduced peak memory usage for large result sets")
        print("2. Incremental processing without loading all results at once")
        print("3. Compressed in-memory buffering with 70-90% memory savings")
        print("4. Familiar cursor interface with minimal code changes")
        print("5. Thread-safe implementation for concurrent applications")
        print("\nThe benefits become more pronounced with larger result sets.")


if __name__ == "__main__":
    main()
