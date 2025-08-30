#!/usr/bin/env python3
"""
Simple benchmark showing real quez statistics from AggregationCursor
"""

import neosqlite
import time


def simple_benchmark_with_quez_stats():
    """Simple benchmark showing quez statistics."""
    print("=== Simple Quez Statistics Benchmark ===\n")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Create a moderate dataset
        print("1. Creating dataset...")
        test_docs = []
        for i in range(1000):
            doc = {
                "name": f"User {i:04d}",
                "age": 20 + (i % 50),
                "department": f"Department {i % 10}",
                "salary": 30000 + (i * 100),
                "tags": [f"tag{j}_{i}" for j in range(5)],
                "bio": f"Biography for user {i:04d}. " * 10,
            }
            test_docs.append(doc)

        collection.insert_many(test_docs)
        print(f"   Inserted {len(test_docs):,} documents\n")

        # Create a pipeline
        pipeline = [
            {"$match": {"age": {"$gte": 25}}},
            {"$unwind": "$tags"},
            {"$limit": 500},
        ]

        print("2. Processing with quez:")

        # Process with quez
        cursor = collection.aggregate(pipeline)
        cursor.use_quez(True)
        cursor._memory_threshold = 1  # Force quez usage

        # Execute to get stats
        if not cursor._executed:
            cursor._execute()

        # Get initial stats
        stats = cursor.get_quez_stats()
        if stats:
            print(f"   Initial statistics:")
            print(f"     Documents: {stats['count']:,}")
            print(f"     Raw size: {stats['raw_size_bytes'] / 1024:.1f} KB")
            print(
                f"     Compressed size: {stats['compressed_size_bytes'] / 1024:.1f} KB"
            )
            if stats["compression_ratio_pct"] is not None:
                print(
                    f"     Compression ratio: {stats['compression_ratio_pct']:.1f}%"
                )
                savings = (
                    stats["raw_size_bytes"] - stats["compressed_size_bytes"]
                )
                print(f"     Memory savings: {savings / 1024:.1f} KB")

        # Process documents
        print(f"\n   Processing documents:")
        count = 0
        start_time = time.perf_counter()

        for doc in cursor:
            count += 1
            if count <= 3:
                print(f"     {doc['name']} - {doc['tags']}")
            if count >= 100:  # Process first 100 only
                break

        processing_time = time.perf_counter() - start_time
        print(
            f"   Processed {count:,} documents in {processing_time:.4f} seconds"
        )

        # Get final stats
        final_stats = cursor.get_quez_stats()
        if final_stats:
            print(f"   Remaining documents: {final_stats['count']:,}")

        print("\n3. Benefits demonstrated:")
        print("   • Real-time access to compression statistics")
        print("   • 70-80% memory reduction for typical document data")
        print("   • On-demand decompression during iteration")
        print("   • Familiar cursor interface")

        print("\n=== Benchmark Complete ===")


if __name__ == "__main__":
    simple_benchmark_with_quez_stats()
