#!/usr/bin/env python3
"""
Test to understand CompressedQueue behavior with cursor iteration
"""

import neosqlite


def test_cursor_iteration_consumption():
    """Test to see how cursor iteration consumes CompressedQueue."""
    print("=== Testing cursor iteration and consumption ===")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert some test data
        test_docs = [{"name": f"User {i}", "value": i} for i in range(10)]
        collection.insert_many(test_docs)

        # Create a cursor with quez enabled
        cursor = collection.aggregate([{"$match": {"value": {"$lt": 5}}}])
        cursor.use_quez(True)

        # Get the internal results to check type
        if not cursor._executed:
            cursor._execute()

        print(f"Results type: {type(cursor._results)}")

        if hasattr(cursor._results, "qsize"):
            print(f"Initial queue size: {cursor._results.qsize()}")

            # Test len() - should not consume
            length = len(cursor)
            print(f"Length from len(): {length}")
            print(f"Queue size after len(): {cursor._results.qsize()}")

            # Iterate manually through a few items
            print("Manually iterating through items:")
            try:
                for i in range(3):
                    item = next(cursor)
                    print(f"  Item {i}: {item['name']} - {item['value']}")
                    print(f"  Queue size after get: {cursor._results.qsize()}")
            except StopIteration:
                print("  No more items")

            # Test len() again
            length = len(cursor)
            print(f"Length from len() after iteration: {length}")
            print(f"Queue size after len(): {cursor._results.qsize()}")

        print("=== Test completed ===")


if __name__ == "__main__":
    test_cursor_iteration_consumption()
