# coding: utf-8
"""
Tests for the watch() method and ChangeStream functionality
"""
import pytest
import time


def test_watch_basic_functionality(collection):
    """Test basic watch functionality"""
    # Start watching for changes
    change_stream = collection.watch()

    # Insert a document
    result = collection.insert_one({"name": "Alice", "age": 30})
    doc_id = result.inserted_id

    # Get the change
    change = next(change_stream)

    # Verify change structure
    assert "_id" in change
    assert "operationType" in change
    assert change["operationType"] == "insert"
    assert "clusterTime" in change
    assert "ns" in change
    assert change["ns"]["db"] == "default"
    assert change["ns"]["coll"] == collection.name
    assert "documentKey" in change
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_with_full_document(collection):
    """Test watch with fullDocument lookup"""
    # Start watching with fullDocument lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Insert a document
    result = collection.insert_one({"name": "Bob", "age": 25})
    doc_id = result.inserted_id

    # Get the change
    change = next(change_stream)

    # Verify full document is included
    assert "fullDocument" in change
    full_doc = change["fullDocument"]
    assert full_doc["name"] == "Bob"
    assert full_doc["age"] == 25
    assert full_doc["_id"] == doc_id

    change_stream.close()


def test_watch_update_operations(collection):
    """Test watching update operations"""
    # Insert a document first
    result = collection.insert_one({"name": "Charlie", "age": 35})
    doc_id = result.inserted_id

    # Start watching for changes
    change_stream = collection.watch()

    # Update the document
    collection.update_one({"_id": doc_id}, {"$set": {"age": 36}})

    # Get the change
    change = next(change_stream)

    # Verify update change
    assert change["operationType"] == "update"
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_delete_operations(collection):
    """Test watching delete operations"""
    # Insert a document first
    result = collection.insert_one({"name": "David", "age": 40})
    doc_id = result.inserted_id

    # Start watching for changes
    change_stream = collection.watch()

    # Delete the document
    collection.delete_one({"_id": doc_id})

    # Get the change
    change = next(change_stream)

    # Verify delete change
    assert change["operationType"] == "delete"
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_multiple_operations(collection):
    """Test watching multiple operations in sequence"""
    # Start watching for changes
    change_stream = collection.watch()

    # Insert documents
    result1 = collection.insert_one({"name": "Eve", "age": 28})
    result2 = collection.insert_one({"name": "Frank", "age": 32})

    # Update a document
    collection.update_one({"_id": result1.inserted_id}, {"$set": {"age": 29}})

    # Delete a document
    collection.delete_one({"_id": result2.inserted_id})

    # Collect all changes
    changes = []
    for i in range(4):  # Expecting 4 changes
        try:
            change = next(change_stream)
            changes.append(change)
        except StopIteration:
            break

    # Verify we got all changes
    assert len(changes) == 4

    # Verify operation types
    operation_types = [change["operationType"] for change in changes]
    assert "insert" in operation_types
    assert "update" in operation_types
    assert "delete" in operation_types

    change_stream.close()


def test_watch_context_manager(collection):
    """Test ChangeStream as a context manager"""
    with collection.watch() as change_stream:
        # Insert a document
        collection.insert_one({"name": "Grace", "age": 33})

        # Get the change
        change = next(change_stream)

        # Verify change
        assert change["operationType"] == "insert"

    # The change stream should be closed after exiting the context


def test_watch_timeout_mechanism(collection):
    """Test the timeout mechanism"""
    # Start watching with a short timeout
    change_stream = collection.watch(max_await_time_ms=100)

    # Try to get a change when there are none
    start_time = time.time()
    with pytest.raises(StopIteration):
        next(change_stream)

    elapsed = time.time() - start_time
    # Should timeout in about 0.1 seconds
    assert elapsed < 1.0

    change_stream.close()


def test_watch_close_functionality(collection):
    """Test that closing the change stream works"""
    # Start watching
    change_stream = collection.watch()

    # Close the stream
    change_stream.close()

    # Trying to get a change should raise StopIteration
    with pytest.raises(StopIteration):
        next(change_stream)


def test_watch_batch_size(collection):
    """Test the batch_size parameter"""
    # Start watching with batch size of 1
    change_stream = collection.watch(batch_size=1)

    # Insert documents
    collection.insert_one({"name": "Henry", "age": 45})
    collection.insert_one({"name": "Ivy", "age": 27})

    # Get changes one by one (due to batch_size=1)
    change1 = next(change_stream)
    change2 = next(change_stream)

    # Verify both changes
    assert change1["operationType"] == "insert"
    assert change2["operationType"] == "insert"

    change_stream.close()


def test_watch_with_no_changes(collection):
    """Test watch when there are no changes"""
    # Start watching
    change_stream = collection.watch(max_await_time_ms=100)

    # Immediately try to get a change (should timeout)
    with pytest.raises(StopIteration):
        next(change_stream)

    change_stream.close()


def test_watch_different_collections(connection):
    """Test that watch only captures changes from the specific collection"""
    # Get two different collections
    collection1 = connection["collection1"]
    collection2 = connection["collection2"]

    # Start watching collection1
    change_stream = collection1.watch()

    # Insert into collection2 (should not trigger change in collection1 stream)
    collection2.insert_one({"name": "Jack", "age": 50})

    # Insert into collection1 (should trigger change)
    result = collection1.insert_one({"name": "Kate", "age": 29})
    doc_id = result.inserted_id

    # Get the change (should be from collection1 only)
    change = next(change_stream)

    # Verify it's from collection1
    assert change["operationType"] == "insert"
    assert change["ns"]["coll"] == "collection1"
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()
