#!/usr/bin/env python3
"""
Test to verify that len() works correctly without consuming items - forcing quez usage
"""

import neosqlite


def test_len_without_consumption_forced_quez():
    """Test that len() doesn't consume items from CompressedQueue - forced quez usage."""
    print("=== Testing len() without consumption (forced quez) ===")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert some test data
        test_docs = [
            {"name": f"User {i}", "value": i, "data": "x" * 1000}
            for i in range(5)
        ]
        collection.insert_many(test_docs)

        # Create a cursor with quez enabled
        cursor = collection.aggregate([{"$match": {"value": {"$lt": 5}}}])
        cursor.use_quez(True)

        # Force a very low memory threshold to ensure quez is used
        cursor._memory_threshold = 1  # 1 byte threshold to force quez usage

        # Execute to get the results
        if not cursor._executed:
            cursor._execute()

        print(f"Results type: {type(cursor._results)}")

        if hasattr(cursor._results, "qsize"):
            initial_size = cursor._results.qsize()
            print(f"Initial queue size: {initial_size}")

            # Call len() multiple times - should not consume anything
            for i in range(3):
                length = len(cursor)
                current_size = cursor._results.qsize()
                print(f"len() call {i+1}: {length}, queue size: {current_size}")
                assert (
                    length == initial_size
                ), f"Length changed after len() call {i+1}"
                assert (
                    current_size == initial_size
                ), f"Queue size changed after len() call {i+1}"

            print("✓ len() calls do not consume items from CompressedQueue")

            # Now test actual iteration - this should consume items
            print("\nTesting iteration (this should consume items):")
            count = 0
            for doc in cursor:
                count += 1
                current_size = cursor._results.qsize()
                print(
                    f"  After consuming {count} items, queue size: {current_size}"
                )
                if count >= 2:  # Only consume first 2 items
                    break

            # Test len() again after iteration
            length = len(cursor)
            current_size = cursor._results.qsize()
            print(
                f"After partial iteration - len(): {length}, queue size: {current_size}"
            )

            # Convert to list to consume remaining items
            remaining_items = cursor.to_list()
            print(f"Remaining items converted to list: {len(remaining_items)}")

            # Final len() call
            final_length = len(cursor)
            final_size = cursor._results.qsize()
            print(f"Final - len(): {final_length}, queue size: {final_size}")
        else:
            print("Results are not a CompressedQueue - likely a list")
            length = len(cursor)
            print(f"Length of list results: {length}")

        print("=== Test completed successfully ===")


if __name__ == "__main__":
    test_len_without_consumption_forced_quez()
