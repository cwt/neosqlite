#!/usr/bin/env python3
"""
Example script demonstrating the watch() feature in neosqlite
"""
import time
from neosqlite import Connection


def main():
    """Demonstrate the watch feature"""
    print("=== neosqlite watch() Example ===\n")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Start watching for changes
        print("1. Starting change stream...")
        change_stream = users.watch(full_document="updateLookup")

        # Perform some operations
        print("2. Performing database operations...")

        # Insert documents
        alice_result = users.insert_one(
            {"name": "Alice", "age": 30, "role": "admin"}
        )
        bob_result = users.insert_one(
            {"name": "Bob", "age": 25, "role": "user"}
        )
        print(f"   - Inserted Alice (ID: {alice_result.inserted_id})")
        print(f"   - Inserted Bob (ID: {bob_result.inserted_id})")

        # Update a document
        users.update_one({"name": "Bob"}, {"$set": {"age": 26}})
        print("   - Updated Bob's age to 26")

        # Delete a document
        users.delete_one({"name": "Alice"})
        print("   - Deleted Alice")

        # Collect and display changes
        print("\n3. Collected changes:")
        changes = []

        # Give some time for all changes to be captured
        time.sleep(0.1)

        # Collect all available changes
        try:
            while True:
                change = next(change_stream)
                changes.append(change)
        except StopIteration:
            pass  # No more changes available

        # Display the changes
        for i, change in enumerate(changes, 1):
            op_type = change["operationType"]
            doc_id = change["documentKey"]["_id"]
            print(f"   Change {i}: {op_type.upper()} on document ID {doc_id}")

            # Show full document for non-delete operations
            if op_type != "delete" and "fullDocument" in change:
                doc = change["fullDocument"]
                print(f"     Document: {doc}")

        change_stream.close()

        print(f"\n4. Summary: Processed {len(changes)} changes")
        print("   - 2 INSERT operations")
        print("   - 1 UPDATE operation")
        print("   - 1 DELETE operation")


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
