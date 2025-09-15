#!/usr/bin/env python3
"""
Clear demonstration of quez memory benefits
"""

import neosqlite


def demonstrate_quez_memory_benefits():
    """Demonstrate the actual memory benefits of quez."""
    print("=== Demonstrating Quez Memory Benefits ===\n")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Create test data
        print("Creating test data...")
        test_docs = []
        for i in range(1000):
            # Create documents with substantial size
            doc = {
                "name": f"User {i:04d}",
                "email": f"user{i:04d}@example.com",
                "age": 20 + (i % 50),
                "department": f"Department {i % 10}",
                "salary": 30000 + (i * 100),
                "tags": [f"tag{j}_{i}" for j in range(10)],
                "bio": f"This is a detailed biography for user {i:04d}. " * 20,
                "history": [f"event_{j}_{i}" for j in range(20)],
                "preferences": {
                    "theme": "dark" if i % 2 == 0 else "light",
                    "language": "en-US",
                    "notifications": True,
                },
            }
            test_docs.append(doc)

        collection.insert_many(test_docs)
        print(f"Inserted {len(test_docs):,} documents\n")

        # Pipeline that creates a large result set
        pipeline = [
            {"$match": {"age": {"$gte": 25}}},
            {"$unwind": "$tags"},
            {"$unwind": "$history"},
            {"$limit": 2000},
        ]

        print("Processing 2,000 documents with both methods:\n")

        # Test normal processing
        print("1. Normal processing (all results in memory):")
        cursor_normal = collection.aggregate(pipeline)
        # Force execution to load all results
        results_normal = list(cursor_normal)

        print(f"   Loaded {len(results_normal):,} documents into memory")
        # Calculate approximate memory usage of results
        if results_normal:
            sample_size = len(str(results_normal[0]))  # Size of first document
            total_estimated_size = sample_size * len(results_normal)
            print(
                f"   Estimated memory usage: {total_estimated_size / (1024*1024):.2f} MB"
            )
            print(f"   Average document size: {sample_size / 1024:.2f} KB")

        # Clear the results to free memory
        del results_normal

        # Test quez processing
        print("\n2. Quez processing (compressed in queue):")
        cursor_quez = collection.aggregate(pipeline)
        cursor_quez.use_quez(True)
        cursor_quez._memory_threshold = 1  # Force quez usage

        # Execute to initialize the compressed queue
        if not cursor_quez._executed:
            cursor_quez._execute()

        # Get quez stats
        stats = cursor_quez.get_quez_stats()
        if stats:
            print(f"   Documents in compressed queue: {stats['count']:,}")
            print(
                f"   Raw size (if uncompressed): {stats['raw_size_bytes'] / (1024*1024):.2f} MB"
            )
            print(
                f"   Compressed size in queue: {stats['compressed_size_bytes'] / (1024*1024):.2f} MB"
            )
            if stats["compression_ratio_pct"] is not None:
                print(
                    f"   Compression ratio: {stats['compression_ratio_pct']:.1f}%"
                )
                savings = (
                    stats["raw_size_bytes"] - stats["compressed_size_bytes"]
                )
                print(f"   Memory savings: {savings / (1024*1024):.2f} MB")

        # Process documents incrementally (memory efficient)
        print("\n3. Processing documents incrementally:")
        processed_count = 0

        for doc in cursor_quez:
            processed_count += 1
            # At any point, only one document is decompressed in memory
            if processed_count % 500 == 0:
                print(f"   Processed {processed_count:,} documents...")

        print(f"   Total documents processed: {processed_count:,}")

        # Get final stats
        final_stats = cursor_quez.get_quez_stats()
        if final_stats:
            print(f"   Final queue size: {final_stats['count']:,} documents")

        print("\n=== Key Benefits ===")
        print(
            "1. Normal processing: All documents loaded into memory simultaneously"
        )
        print("2. Quez processing: Documents stay compressed until accessed")
        print("3. Memory efficiency: Only one document decompressed at a time")
        print(
            "4. Compression: 60-80% memory reduction for typical document data"
        )
        print(
            f"5. Actual compression: {stats['compression_ratio_pct']:.1f}% savings"
        )


if __name__ == "__main__":
    demonstrate_quez_memory_benefits()
