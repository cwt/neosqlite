#!/usr/bin/env python3
"""
Example demonstrating access to quez compression statistics from AggregationCursor
"""

import neosqlite


def demonstrate_quez_stats():
    """Demonstrate accessing quez statistics from AggregationCursor."""
    print("=== Accessing Quez Statistics from AggregationCursor ===\n")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert some test data with larger documents to see compression benefits
        print("1. Creating test data...")
        test_docs = []
        for i in range(1000):
            doc = {
                "name": f"User {i:04d}",
                "email": f"user{i:04d}@example.com",
                "age": 20 + (i % 50),
                "department": f"Department {i % 10}",
                "salary": 30000 + (i * 100),
                "tags": [f"tag{j}_{i}" for j in range(5)],
                "bio": f"This is a detailed biography for user {i:04d}. " * 20,
            }
            test_docs.append(doc)

        collection.insert_many(test_docs)
        print(f"   Inserted {len(test_docs):,} documents\n")

        # Create a pipeline that generates a large result set
        pipeline = [
            {"$match": {"age": {"$gte": 25}}},
            {"$unwind": "$tags"},
            {"$limit": 500},
        ]

        # Test with normal processing
        print("2. Normal processing:")
        cursor_normal = collection.aggregate(pipeline)
        results_normal = list(cursor_normal)
        print(f"   Processed {len(results_normal):,} documents")
        print(
            f"   First document size: {len(str(results_normal[0])):,} bytes\n"
        )

        # Test with quez processing
        print("3. Quez memory-constrained processing:")
        cursor_quez = collection.aggregate(pipeline)
        cursor_quez.use_quez(True)
        cursor_quez._memory_threshold = 1  # Force quez usage

        # Execute to get results
        if not cursor_quez._executed:
            cursor_quez._execute()

        # Get quez stats before processing
        stats_before = cursor_quez.get_quez_stats()
        if stats_before:
            print("   Before processing:")
            print(f"     Documents in queue: {stats_before['count']:,}")
            print(
                f"     Raw size: {stats_before['raw_size_bytes'] / 1024:.1f} KB"
            )
            print(
                f"     Compressed size: {stats_before['compressed_size_bytes'] / 1024:.1f} KB"
            )
            if stats_before["compression_ratio_pct"] is not None:
                print(
                    f"     Compression ratio: {stats_before['compression_ratio_pct']:.1f}%"
                )
                savings = (
                    stats_before["raw_size_bytes"]
                    - stats_before["compressed_size_bytes"]
                )
                print(
                    f"     Memory savings: {savings:,} bytes ({savings / 1024:.1f} KB)"
                )

        # Process some documents
        print("\n   Processing first 10 documents:")
        count = 0
        for doc in cursor_quez:
            count += 1
            if count <= 3:
                print(f"     {doc['name']} - {doc['tags']}")
            if count >= 10:
                break

        # Get quez stats after partial processing
        stats_after = cursor_quez.get_quez_stats()
        if stats_after:
            print(f"\n   After processing {count} documents:")
            print(f"     Remaining documents: {stats_after['count']:,}")
            print(
                f"     Raw size: {stats_after['raw_size_bytes'] / 1024:.1f} KB"
            )
            print(
                f"     Compressed size: {stats_after['compressed_size_bytes'] / 1024:.1f} KB"
            )
            if stats_after["compression_ratio_pct"] is not None:
                print(
                    f"     Compression ratio: {stats_after['compression_ratio_pct']:.1f}%"
                )

        # Convert remaining to list
        remaining_docs = cursor_quez.to_list()
        print(
            f"\n   Converted remaining {len(remaining_docs):,} documents to list"
        )

        # Final stats
        stats_final = cursor_quez.get_quez_stats()
        if stats_final:
            print(f"   Final queue stats: {stats_final['count']} items")

        print("\n4. Benefits of quez integration:")
        print(
            "   • Real-time compression statistics via cursor.get_quez_stats()"
        )
        print("   • 60-80% memory reduction for large result sets")
        print("   • Familiar cursor interface with minimal code changes")
        print("   • Automatic compression/decompression on-demand")
        print("   • Thread-safe implementation")

        print("\n=== Quez Statistics Demonstration Complete ===")


if __name__ == "__main__":
    demonstrate_quez_stats()
