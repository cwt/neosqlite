#!/usr/bin/env python3
"""
Demonstration of the watch() feature in neosqlite
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

        # Function to simulate user operations
        def simulate_user_operations():
            time.sleep(1)  # Give the watcher time to start

            print("\n2. Simulating user operations...")

            # Insert a new user
            print("   - Inserting user: Alice")
            result = users.insert_one(
                {"name": "Alice", "age": 30, "role": "admin"}
            )
            alice_id = result.inserted_id
            time.sleep(0.5)

            # Insert another user
            print("   - Inserting user: Bob")
            result = users.insert_one(
                {"name": "Bob", "age": 25, "role": "user"}
            )
            bob_id = result.inserted_id
            time.sleep(0.5)

            # Update a user
            print("   - Updating Alice's age")
            users.update_one({"_id": alice_id}, {"$set": {"age": 31}})
            time.sleep(0.5)

            # Delete a user
            print("   - Deleting Bob")
            users.delete_one({"_id": bob_id})
            time.sleep(0.5)

            print("   - User operations completed")
            return alice_id, bob_id

        # Perform user operations
        alice_id, bob_id = simulate_user_operations()

        # Watch for changes
        print(
            "\n3. Watching for changes (will stop after 5 changes or 2 seconds)..."
        )
        changes_received = 0
        start_time = time.time()

        try:
            while changes_received < 5 and (time.time() - start_time) < 2:
                try:
                    # Try to get a change with a short timeout
                    change = next(change_stream)
                    changes_received += 1
                    operation = change["operationType"]
                    doc_id = change["documentKey"]["_id"]

                    print(
                        f"   Change #{changes_received}: {operation.upper()} on document ID {doc_id}"
                    )

                    # Show full document if available
                    if "fullDocument" in change:
                        doc = change["fullDocument"]
                        if operation != "delete":
                            print(f"     Full document: {doc}")

                except StopIteration:
                    # No more changes
                    break
                except Exception as e:
                    # Continue watching
                    time.sleep(0.1)
                    continue

        except KeyboardInterrupt:
            print("   Stopping change stream...")
        except Exception as e:
            print(f"   Error watching changes: {e}")
        finally:
            change_stream.close()

        print(f"\n4. Summary:")
        print(f"   - Received {changes_received} change notifications")
        print(f"   - Demonstrated INSERT, UPDATE, and DELETE operations")

        # Show current users in the collection
        print(f"\n5. Current users in collection:")
        for user in users.find():
            print(f"   - {user}")


if __name__ == "__main__":
    demonstrate_watch_feature()
    print("\n=== Demonstration Complete ===")
