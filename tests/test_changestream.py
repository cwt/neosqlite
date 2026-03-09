# coding: utf-8
"""
Tests for the watch() method and ChangeStream functionality
"""
import pytest
import json
import time
from unittest.mock import MagicMock
from neosqlite.changestream import ChangeStream
from neosqlite.objectid import ObjectId


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


def test_watch_with_binary_data(collection):
    """Test watch with binary data that may contain non-UTF-8 bytes"""
    from neosqlite.binary import Binary

    # Start watching with fullDocument lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Insert a document with binary data containing non-UTF-8 bytes
    # This simulates the bug where bytes like 0xcc can't be decoded as UTF-8
    binary_data = Binary(b"\x00\x01\x02\xcc\xfe\xff\x00\x01\x02\x03")
    result = collection.insert_one({"name": "BinaryTest", "data": binary_data})
    doc_id = result.inserted_id

    # Get the change - should not raise UnicodeDecodeError
    change = next(change_stream)

    # Verify basic change structure
    assert change["operationType"] == "insert"
    assert change["documentKey"]["_id"] == doc_id

    # Full document should be included
    assert "fullDocument" in change
    assert change["fullDocument"]["name"] == "BinaryTest"

    change_stream.close()


def test_watch_with_various_binary_subtypes(collection):
    """Test watch with different binary subtypes"""
    from neosqlite.binary import Binary

    # Start watching
    change_stream = collection.watch(full_document="updateLookup")

    # Test with different binary subtypes
    test_cases = [
        ("generic", Binary(b"\x00\xcc\xfe\xff", subtype=0)),
        ("function", Binary(b"\x00\xcc\xfe\xff", subtype=1)),
        ("uuid", Binary(b"\x00\xcc\xfe\xff" * 4, subtype=3)),
        ("md5", Binary(b"\x00\xcc\xfe\xff" * 4, subtype=5)),
    ]

    for name, binary_data in test_cases:
        result = collection.insert_one({"type": name, "binary": binary_data})
        doc_id = result.inserted_id

        # Should not raise UnicodeDecodeError
        change = next(change_stream)
        assert change["operationType"] == "insert"
        assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_update_with_binary_data(collection):
    """Test watching update operations with binary data"""
    from neosqlite.binary import Binary

    # Insert a document first
    result = collection.insert_one({"name": "UpdateTest", "value": 1})
    doc_id = result.inserted_id

    # Start watching
    change_stream = collection.watch(full_document="updateLookup")

    # Update with binary data containing non-UTF-8 bytes
    binary_data = Binary(b"\xcc\xdd\xee\xff\x00\x11\x22\x33")
    collection.update_one(
        {"_id": doc_id}, {"$set": {"data": binary_data, "value": 2}}
    )

    # Get the change - should not raise UnicodeDecodeError
    change = next(change_stream)

    # Verify update change
    assert change["operationType"] == "update"
    assert change["documentKey"]["_id"] == doc_id
    assert "fullDocument" in change
    assert change["fullDocument"]["value"] == 2

    change_stream.close()


def test_changestream_iter(collection):
    """Test __iter__ returns the stream itself."""
    with collection.watch() as stream:
        assert iter(stream) is stream

def test_changestream_closed_next(collection):
    """Test __next__ raises StopIteration when closed."""
    stream = collection.watch()
    stream.close()
    with pytest.raises(StopIteration, match="Change stream is closed"):
        next(stream)

def test_next_document_id_value_not_objectid(collection):
    """Test __next__ when document_id_value is not a valid ObjectId hex."""
    stream = collection.watch()
    
    # Manually insert a record with a non-ObjectId string as document_id_value
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_id_value) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, "not-an-objectid")
    )
    
    change = next(stream)
    assert change["documentKey"]["_id"] == "not-an-objectid"
    stream.close()

def test_next_document_data_bytes_decode_fail(collection):
    """Test __next__ when document_data bytes fail to decode as UTF-8."""
    stream = collection.watch()
    
    # \xcc is invalid UTF-8
    invalid_utf8 = b"\xcc\xdd\xee"
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, invalid_utf8)
    )
    
    change = next(stream)
    # Should fall back to document_id (1) because decoding failed
    assert change["documentKey"]["_id"] == 1
    stream.close()

def test_next_document_data_json_decode_fail(collection):
    """Test __next__ when document_data is invalid JSON."""
    stream = collection.watch()
    
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, "{invalid-json}")
    )
    
    change = next(stream)
    # Should fall back to database lookup or document_id
    assert change["documentKey"]["_id"] == 1
    stream.close()

def test_next_document_data_no_id_in_json(collection):
    """Test __next__ when document_data is JSON but missing _id."""
    stream = collection.watch()
    
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, json.dumps({"name": "test"}))
    )
    
    change = next(stream)
    # Should fall back to database lookup or document_id
    assert change["documentKey"]["_id"] == 1
    stream.close()

def test_next_no_json_data_fallback(collection):
    """Test fallback when document_data is None."""
    stream = collection.watch()
    
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 999, None)
    )
    
    change = next(stream)
    # Should fall back to document_id 999
    assert change["documentKey"]["_id"] == 999
    stream.close()

def test_updatelookup_decode_fail(collection):
    """Test updateLookup when document_data bytes fail to decode."""
    stream = collection.watch(full_document="updateLookup", max_await_time_ms=500)
    
    # Insert invalid record that should be skipped (causing a continue in the loop)
    invalid_utf8 = b"\xcc\xdd\xee"
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "update", 1, invalid_utf8)
    )
    
    # After one invalid record, it should keep polling until timeout
    with pytest.raises(StopIteration, match="Change stream timeout exceeded"):
        next(stream)
    
    stream.close()

def test_updatelookup_json_fail(collection):
    """Test updateLookup when JSON parsing fails (should just pass through)."""
    stream = collection.watch(full_document="updateLookup")
    
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "update", 1, "{invalid-json}")
    )
    
    change = next(stream)
    assert "fullDocument" not in change
    stream.close()

def test_updatelookup_non_objectid_value(collection):
    """Test updateLookup with non-ObjectId document_id_value."""
    stream = collection.watch(full_document="updateLookup")
    
    doc_data = json.dumps({"_id": 1, "name": "test"})
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data, document_id_value) VALUES (?, ?, ?, ?, ?)",
        (collection.name, "update", 1, doc_data, "string-id")
    )
    
    change = next(stream)
    assert change["fullDocument"]["_id"] == "string-id"
    stream.close()

def test_updatelookup_no_id_in_json_fallback(collection):
    """Test updateLookup fallback when _id is missing in JSON and no document_id_value."""
    stream = collection.watch(full_document="updateLookup")
    
    doc_data = json.dumps({"name": "test"})
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "update", 1, doc_data)
    )
    
    change = next(stream)
    assert change["fullDocument"]["_id"] == 1
    stream.close()

def test_next_document_data_has_id_in_json(collection):
    """Test __next__ when document_data is JSON and has _id."""
    stream = collection.watch()
    
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, json.dumps({"_id": "custom-id", "name": "test"}))
    )
    
    change = next(stream)
    assert change["documentKey"]["_id"] == "custom-id"
    stream.close()

def test_close_idempotent(collection):
    """Test that close() can be called multiple times."""
    stream = collection.watch()
    stream.close()
    assert stream._closed is True
    # Should not raise any error
    stream.close()
    assert stream._closed is True
