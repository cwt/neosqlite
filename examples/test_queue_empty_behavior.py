#!/usr/bin/env python3
"""
Test to understand CompressedQueue empty() and get() behavior
"""

try:
    from quez import CompressedQueue

    print("=== Testing CompressedQueue empty() and get() behavior ===")

    # Create a queue
    queue = CompressedQueue()

    # Test empty queue
    print(f"Empty queue - empty(): {queue.empty()}")

    # Add some items
    for i in range(5):
        queue.put({"value": i})

    print(f"Queue with items - empty(): {queue.empty()}")
    print(f"Queue size: {queue.qsize()}")

    # Consume all items
    items_consumed = 0
    while not queue.empty():
        try:
            item = queue.get(block=False)
            items_consumed += 1
            print(f"Consumed item {items_consumed}: {item}")
            print(f"  After consumption - empty(): {queue.empty()}")
            print(f"  Queue size: {queue.qsize()}")
        except Exception as e:
            print(f"Exception during get(): {e}")
            break

    print(f"Final state - empty(): {queue.empty()}")
    print(f"Final queue size: {queue.qsize()}")

    # Try to get from empty queue
    try:
        item = queue.get(block=False)
        print(f"Got item from empty queue: {item}")
    except Exception as e:
        print(f"Exception when getting from empty queue: {e}")

    print("=== Test completed ===")

except ImportError:
    print("Quez not available")
