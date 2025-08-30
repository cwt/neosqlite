#!/usr/bin/env python3
"""
Simple test to check for potential deadlocks in quez integration
"""

import neosqlite
import time


def test_for_deadlock():
    """Test to check if there's a deadlock or infinite loop."""
    print("=== Testing for Deadlocks ===")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a few test documents
        test_docs = [{"name": f"User {i}", "value": i} for i in range(10)]
        collection.insert_many(test_docs)

        # Create a cursor with quez enabled
        cursor = collection.aggregate([{"$match": {"value": {"$lt": 5}}}])
        cursor.use_quez(True)
        cursor._memory_threshold = 1  # Force quez usage

        print("Starting iteration...")
        start_time = time.time()

        # Try to iterate through all items
        count = 0
        try:
            for doc in cursor:
                count += 1
                print(f"  Processed item {count}: {doc['name']}")

                # Check if we're taking too long
                if time.time() - start_time > 5:  # 5 second timeout
                    print("Timeout - possible deadlock!")
                    break

        except Exception as e:
            print(f"Exception occurred: {e}")

        print(f"Total items processed: {count}")
        print(f"Time taken: {time.time() - start_time:.2f} seconds")

        # Try to get final stats
        stats = cursor.get_quez_stats()
        if stats:
            print(f"Final queue size: {stats['count']}")

        print("=== Test completed ===")


if __name__ == "__main__":
    test_for_deadlock()
