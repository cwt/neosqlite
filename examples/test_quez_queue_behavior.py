#!/usr/bin/env python3
"""
Test to understand CompressedQueue behavior
"""

try:
    from quez import CompressedQueue

    print("=== Testing CompressedQueue behavior ===")

    # Create a queue
    queue = CompressedQueue()

    # Add some items
    for i in range(10):
        queue.put({"value": i})

    print(f"Initial queue size: {queue.qsize()}")

    # Get one item
    item = queue.get()
    print(f"Got item: {item}")
    print(f"Queue size after get(): {queue.qsize()}")

    # Get another item
    item = queue.get()
    print(f"Got item: {item}")
    print(f"Queue size after second get(): {queue.qsize()}")

    print("=== Test completed ===")

except ImportError:
    print("Quez not available")
