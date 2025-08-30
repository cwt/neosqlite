#!/usr/bin/env python3
"""
Test the specific scenario that might have caused the freeze
"""

import neosqlite
import time


def test_large_processing_scenario():
    """Test the scenario that might have caused the freeze."""
    print("=== Testing Large Processing Scenario ===")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Create a larger dataset similar to the benchmark
        print("Creating dataset...")
        test_docs = []
        for i in range(1000):  # Reduced from 5000
            doc = {
                "_id": i,
                "name": f"User {i:05d}",
                "age": 20 + (i % 60),
                "tags": [f"tag{j}_{i}" for j in range(5)],  # Reduced from 10
                "history": [
                    f"event_{j}_{i}" for j in range(10)
                ],  # Reduced from 20
                "bio": f"Biography for user {i:05d}. " * 5,  # Reduced from 15
            }
            test_docs.append(doc)

        collection.insert_many(test_docs)
        print(f"Inserted {len(test_docs):,} documents")

        # Create pipeline similar to the benchmark
        pipeline = [
            {"$match": {"age": {"$gte": 25}}},
            {"$unwind": "$tags"},
            {"$unwind": "$history"},
            {"$limit": 500},  # Reduced from 3000
        ]

        print("Processing with quez...")
        start_time = time.time()

        cursor = collection.aggregate(pipeline)
        cursor.use_quez(True)
        cursor._memory_threshold = 1  # Force quez usage

        # Execute to get stats
        if not cursor._executed:
            cursor._execute()

        # Get initial stats
        stats = cursor.get_quez_stats()
        if stats:
            print(f"Initial queue size: {stats['count']:,}")

        # Process items with a timeout
        count = 0
        try:
            for doc in cursor:
                count += 1
                if count <= 3:
                    print(
                        f"  Processed: {doc.get('name', 'N/A')} - {doc.get('tags', 'N/A')}"
                    )
                if count % 100 == 0:
                    print(f"  Processed {count:,} items...")
                    # Check timeout
                    if time.time() - start_time > 10:  # 10 second timeout
                        print("Timeout reached!")
                        break
        except Exception as e:
            print(f"Exception during iteration: {e}")

        end_time = time.time()
        print(
            f"Processed {count:,} items in {end_time - start_time:.2f} seconds"
        )

        # Get final stats
        final_stats = cursor.get_quez_stats()
        if final_stats:
            print(f"Final queue size: {final_stats['count']:,}")

        print("=== Test completed ===")


if __name__ == "__main__":
    test_large_processing_scenario()
