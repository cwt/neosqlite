#!/usr/bin/env python3
"""
Simple demonstration of the watch() feature in neosqlite
"""
from neosqlite import Connection
import time


def demonstrate_watch_feature():
    """Demonstrate the watch feature with a practical example"""
    print("=== neosqlite watch() Feature Demonstration ===\n")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        print("1. Starting change stream to watch for user changes...")
        # Start watching for changes with full document lookup
        change_stream = users.watch(full_document="updateLookup")

        print("\n2. Performing user operations...")

        # Insert a new user
        print("   - Inserting user: Alice")
        result = users.insert_one({"name": "Alice", "age": 30, "role": "admin"})
        alice_id = result.inserted_id
        time.sleep(0.1)

        # Insert another user
        print("   - Inserting user: Bob")
        result = users.insert_one({"name": "Bob", "age": 25, "role": "user"})
        bob_id = result.inserted_id
        time.sleep(0.1)

        # Update a user
        print("   - Updating Alice's age")
        users.update_one({"_id": alice_id}, {"$set": {"age": 31}})
        time.sleep(0.1)

        # Delete a user
        print("   - Deleting Bob")
        users.delete_one({"_id": bob_id})
        time.sleep(0.1)

        print("   - User operations completed")

        # Watch for changes (collect all changes that happened)
        print("\n3. Collecting change notifications...")
        changes = []
        start_time = time.time()

        # Collect changes for a short period
        while (time.time() - start_time) < 1:  # 1 second timeout
            try:
                # Try to get a change
                change = next(change_stream)
                changes.append(change)
                operation = change["operationType"]
                doc_id = change["documentKey"]["_id"]

                print(f"   Change: {operation.upper()} on document ID {doc_id}")

                # Show full document if available
                if "fullDocument" in change:
                    doc = change["fullDocument"]
                    if operation != "delete":
                        print(f"     Full document: {doc}")

            except StopIteration:
                # No more changes right now
                time.sleep(0.1)
                continue
            except Exception:
                # Continue watching
                time.sleep(0.1)
                continue

        change_stream.close()

        print(f"\n4. Summary:")
        print(f"   - Collected {len(changes)} change notifications")
        print(f"   - Demonstrated INSERT, UPDATE, and DELETE operations")

        # Show current users in the collection
        print(f"\n5. Current users in collection:")
        for user in users.find():
            print(f"   - {user}")


if __name__ == "__main__":
    demonstrate_watch_feature()
    print("\n=== Demonstration Complete ===")
