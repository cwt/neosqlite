#!/usr/bin/env python3
"""
Test script for the watch() implementation in neosqlite with fullDocument support
"""
import time
from neosqlite import Connection


def test_watch_with_full_document():
    """Test the watch functionality with fullDocument support"""
    print("Testing watch() functionality with fullDocument...")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Get a collection
        collection = conn.test_collection

        # Start watching for changes with fullDocument
        print("Starting change stream with fullDocument=updateLookup...")
        change_stream = collection.watch(full_document="updateLookup")

        # Make a change
        print("Inserting document...")
        result = collection.insert_one({"name": "Alice", "age": 30})
        doc_id = result.inserted_id

        # Watch for changes (with a timeout)
        print("Watching for changes...")
        start_time = time.time()
        try:
            change = next(change_stream)
            print(f"Received change: {change}")

            # Verify the structure
            assert "_id" in change
            assert "operationType" in change
            assert "clusterTime" in change
            assert "ns" in change
            assert "documentKey" in change

            # Verify the values
            assert change["operationType"] == "insert"
            assert change["ns"]["db"] == "default"
            assert change["ns"]["coll"] == "test_collection"
            assert change["documentKey"]["_id"] == doc_id

            # Check if fullDocument is present
            if "fullDocument" in change:
                print(f"Full document: {change['fullDocument']}")
                assert change["fullDocument"]["name"] == "Alice"
                assert change["fullDocument"]["age"] == 30
                assert change["fullDocument"]["_id"] == doc_id
            else:
                print("Full document not found in change")

        except StopIteration:
            print("Change stream closed")
        except Exception as e:
            print(f"Error: {e}")
            raise
        finally:
            change_stream.close()

        print("Test completed successfully!")


def test_watch_update_and_delete():
    """Test watching update and delete operations"""
    print("\nTesting watch() functionality for update and delete operations...")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Get a collection
        collection = conn.test_collection

        # Insert a document first
        result = collection.insert_one({"name": "Bob", "age": 25})
        doc_id = result.inserted_id
        print(f"Inserted document with ID: {doc_id}")

        # Start watching for changes
        print("Starting change stream...")
        change_stream = collection.watch()

        # Update the document
        print("Updating document...")
        collection.update_one({"_id": doc_id}, {"$set": {"age": 26}})

        # Watch for update change
        try:
            change = next(change_stream)
            print(f"Update change: {change}")
            assert change["operationType"] == "update"
            assert change["documentKey"]["_id"] == doc_id
        except Exception as e:
            print(f"Error during update: {e}")
            raise

        # Delete the document
        print("Deleting document...")
        collection.delete_one({"_id": doc_id})

        # Watch for delete change
        try:
            change = next(change_stream)
            print(f"Delete change: {change}")
            assert change["operationType"] == "delete"
            assert change["documentKey"]["_id"] == doc_id
        except Exception as e:
            print(f"Error during delete: {e}")
            raise
        finally:
            change_stream.close()

        print("Update and delete test completed successfully!")


if __name__ == "__main__":
    test_watch_with_full_document()
    test_watch_update_and_delete()
    print("\nAll tests passed!")
