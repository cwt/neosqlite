#!/usr/bin/env python3
"""
Test to verify that len() works correctly with CompressedQueue without consuming items
"""

import neosqlite


def test_len_and_iteration():
    """Test that len() works correctly and we can iterate without consuming everything upfront."""
    print("=== Testing len() and iteration with CompressedQueue ===")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert some test data
        test_docs = [{"name": f"User {i}", "value": i} for i in range(100)]
        collection.insert_many(test_docs)

        # Create a cursor with quez enabled
        cursor = collection.aggregate([{"$match": {"value": {"$lt": 50}}}])
        cursor.use_quez(True)

        # Test len() without consuming items
        print(f"Length before iteration: {len(cursor)}")

        # Iterate through some items
        print("Iterating through first 5 items:")
        count = 0
        for doc in cursor:
            print(f"  {doc['name']}: {doc['value']}")
            count += 1
            if count >= 5:
                break

        # Test len() again - should still work
        print(f"Length after partial iteration: {len(cursor)}")

        # Convert to list (this will consume remaining items)
        print("Converting to list...")
        all_results = cursor.to_list()
        print(f"Total items in list: {len(all_results)}")

        print("=== Test completed successfully ===")


if __name__ == "__main__":
    test_len_and_iteration()
