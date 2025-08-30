#!/usr/bin/env python3
"""
Comprehensive test to verify quez integration is working correctly without deadlocks
"""

import neosqlite
import time


def comprehensive_quez_test():
    """Comprehensive test of quez integration."""
    print("=== Comprehensive Quez Integration Test ===")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Test 1: Basic functionality
        print("\n1. Basic functionality test:")
        test_docs = [{"name": f"User {i}", "value": i} for i in range(100)]
        collection.insert_many(test_docs)

        cursor = collection.aggregate([{"$match": {"value": {"$lt": 50}}}])
        cursor.use_quez(True)
        cursor._memory_threshold = 1  # Force quez usage

        # Test len()
        length = len(cursor)
        print(f"   len(cursor): {length}")

        # Test get_quez_stats()
        stats = cursor.get_quez_stats()
        if stats:
            print(
                f"   Queue stats - count: {stats['count']}, ratio: {stats['compression_ratio_pct']:.1f}%"
            )

        # Test iteration
        count = 0
        for doc in cursor:
            count += 1
            if count >= 10:  # Only process first 10
                break
        print(f"   Iterated through {count} items")

        # Test final stats
        final_stats = cursor.get_quez_stats()
        if final_stats:
            print(f"   Final queue count: {final_stats['count']}")

        # Test 2: to_list() functionality
        print("\n2. to_list() functionality test:")
        cursor2 = collection.aggregate(
            [{"$match": {"value": {"$gte": 45, "$lt": 55}}}]
        )
        cursor2.use_quez(True)
        cursor2._memory_threshold = 1  # Force quez usage

        # Execute to initialize
        if not cursor2._executed:
            cursor2._execute()

        initial_stats = cursor2.get_quez_stats()
        print(
            f"   Initial queue size: {initial_stats['count'] if initial_stats else 'N/A'}"
        )

        # Convert to list
        result_list = cursor2.to_list()
        print(f"   to_list() returned {len(result_list)} items")

        final_stats2 = cursor2.get_quez_stats()
        print(
            f"   Final queue size: {final_stats2['count'] if final_stats2 else 'N/A'}"
        )

        # Test 3: Multiple operations on same cursor
        print("\n3. Multiple operations test:")
        cursor3 = collection.aggregate([{"$limit": 20}])
        cursor3.use_quez(True)
        cursor3._memory_threshold = 1  # Force quez usage

        # Get initial stats
        stats3 = cursor3.get_quez_stats()
        initial_count = stats3["count"] if stats3 else 0
        print(f"   Initial count: {initial_count}")

        # Process a few items
        processed = []
        for i, doc in enumerate(cursor3):
            processed.append(doc)
            if i >= 5:
                break
        print(f"   Processed {len(processed)} items")

        # Check stats after processing
        stats3_after = cursor3.get_quez_stats()
        remaining_count = stats3_after["count"] if stats3_after else 0
        print(f"   Remaining count: {remaining_count}")

        # Convert rest to list
        rest = cursor3.to_list()
        print(f"   Converted remaining {len(rest)} items to list")

        # Final check
        stats3_final = cursor3.get_quez_stats()
        final_count = stats3_final["count"] if stats3_final else 0
        print(f"   Final count: {final_count}")

        # Test 4: Error conditions
        print("\n4. Error conditions test:")
        cursor4 = collection.aggregate([{"$match": {"nonexistent": "value"}}])
        cursor4.use_quez(True)
        cursor4._memory_threshold = 1  # Force quez usage

        empty_length = len(cursor4)
        print(f"   len() on empty cursor: {empty_length}")

        empty_stats = cursor4.get_quez_stats()
        print(
            f"   Stats on empty cursor: {empty_stats['count'] if empty_stats else 'N/A'}"
        )

        # Try to iterate (should be empty)
        empty_count = 0
        for doc in cursor4:
            empty_count += 1
        print(f"   Iteration on empty cursor processed: {empty_count} items")

        print("\n=== All tests completed successfully ===")


if __name__ == "__main__":
    comprehensive_quez_test()
