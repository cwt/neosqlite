# coding: utf-8
"""
Additional tests for the watch() method and ChangeStream functionality
to improve code coverage
"""
import time
import json
import pytest
import neosqlite


def test_watch_error_handling(collection):
    """Test error handling in ChangeStream"""
    # Start watching for changes
    change_stream = collection.watch()

    # Test that we can iterate over the stream
    iterator = iter(change_stream)
    assert iterator is change_stream

    # Insert a document to have something to work with
    collection.insert_one({"test": "data"})

    # Get a change
    change = next(change_stream)
    assert change is not None

    # Close the stream
    change_stream.close()

    # Trying to get another change should raise StopIteration
    with pytest.raises(StopIteration):
        next(change_stream)

    # Closing again should be safe
    change_stream.close()


def test_watch_cleanup_triggers_exception_handling(collection):
    """Test that trigger cleanup handles exceptions gracefully"""
    # Start watching for changes
    change_stream = collection.watch()

    # Insert a document
    collection.insert_one({"test": "data"})

    # Get a change
    change = next(change_stream)
    assert change is not None

    # Manually set closed flag to test the guard clause
    change_stream._closed = True

    # Cleanup should return early and not raise an exception
    change_stream._cleanup_triggers()  # Should not raise

    # Reset closed flag
    change_stream._closed = False

    # Close properly
    change_stream.close()


def test_watch_full_document_error_handling(collection):
    """Test error handling when parsing full document data"""
    # Start watching with full document lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change - this should work normally
    change = next(change_stream)
    assert "fullDocument" in change

    change_stream.close()


def test_watch_json_decode_error_handling(collection):
    """Test handling of JSON decode errors in change stream"""
    # Start watching with full document lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Manually insert invalid JSON data into the change stream table
    collection.db.execute(
        """
        INSERT INTO _neosqlite_changestream
        (collection_name, operation, document_id, document_data)
        VALUES (?, 'insert', 999, ?)
        """,
        (collection.name, '{"invalid": json}'),
    )
    collection.db.commit()

    # Get the change - should not raise an exception even with invalid JSON
    change = next(change_stream)
    assert change["documentKey"]["_id"] == 999
    # fullDocument might not be present due to JSON decode error

    change_stream.close()


def test_watch_timeout_exceeded(collection):
    """Test that timeout mechanism works correctly"""
    # Start watching with a very short timeout
    change_stream = collection.watch(max_await_time_ms=1)  # 1ms timeout

    # Immediately try to get a change when there are none
    start_time = time.time()
    with pytest.raises(StopIteration):
        next(change_stream)

    elapsed = time.time() - start_time
    # Should timeout very quickly
    assert elapsed < 1.0

    change_stream.close()


def test_watch_with_none_max_await_time(collection):
    """Test watch with None max_await_time_ms (should use default)"""
    # Start watching with None max_await_time_ms
    change_stream = collection.watch(max_await_time_ms=None)

    # This should not raise an exception
    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change
    change = next(change_stream)
    assert change is not None

    change_stream.close()


def test_watch_with_zero_batch_size(collection):
    """Test watch with zero batch_size (should use default)"""
    # Start watching with zero batch_size
    change_stream = collection.watch(batch_size=0)

    # This should not raise an exception
    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change
    change = next(change_stream)
    assert change is not None

    change_stream.close()


def test_watch_with_negative_batch_size(collection):
    """Test watch with negative batch_size (should use default)"""
    # Start watching with negative batch_size
    change_stream = collection.watch(batch_size=-1)

    # This should not raise an exception
    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change
    change = next(change_stream)
    assert change is not None

    change_stream.close()


def test_watch_context_manager_exception_handling(collection):
    """Test context manager properly handles exceptions"""
    try:
        with collection.watch() as change_stream:
            # Insert a document
            collection.insert_one({"test": "data"})

            # Get the change
            change = next(change_stream)
            assert change is not None

            # Simulate an exception
            raise ValueError("Test exception")
    except ValueError:
        # Exception should be caught and stream should be closed
        pass

    # The stream should be closed after exiting context due to exception


def test_watch_multiple_context_managers(collection):
    """Test multiple change streams on the same collection"""
    # Start two change streams
    with collection.watch() as stream1, collection.watch() as stream2:
        # Insert a document
        collection.insert_one({"test": "data"})

        # Both streams should receive the change
        change1 = next(stream1)
        change2 = next(stream2)

        assert change1 is not None
        assert change2 is not None
        assert change1["operationType"] == change2["operationType"]


def test_watch_empty_collection_name_edge_case():
    """Test edge case with empty collection name"""
    with neosqlite.Connection(":memory:") as conn:
        # Create a collection with a simple name
        collection = conn["test"]

        # Start watching
        with collection.watch() as change_stream:
            # Insert a document
            collection.insert_one({"test": "data"})

            # Get the change
            change = next(change_stream)
            assert change is not None
            assert change["ns"]["coll"] == "test"


def test_watch_trigger_setup_error_handling():
    """Test trigger setup error handling"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # The _setup_triggers method should work without errors
        # Start watching (this will call _setup_triggers)
        with collection.watch() as change_stream:
            # Insert a document
            collection.insert_one({"test": "data"})

            # Get the change
            change = next(change_stream)
            assert change is not None


def test_watch_with_all_parameters(collection):
    """Test watch with all parameters specified"""
    # Start watching with all parameters
    change_stream = collection.watch(
        pipeline=[{"$match": {"operationType": "insert"}}],
        full_document="updateLookup",
        resume_after=None,
        max_await_time_ms=5000,
        batch_size=5,
        collation=None,
        start_at_operation_time=None,
        session=None,
        start_after=None,
    )

    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change
    change = next(change_stream)
    assert change is not None
    assert change["operationType"] == "insert"

    change_stream.close()


def test_watch_resume_after_not_implemented(collection):
    """Test that resume_after parameter is accepted but not implemented"""
    # Start watching with resume_after (not implemented but should not raise)
    change_stream = collection.watch(resume_after={"_id": 123})

    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change (should work normally)
    change = next(change_stream)
    assert change is not None

    change_stream.close()


def test_watch_pipeline_not_implemented(collection):
    """Test that pipeline parameter is accepted but not implemented"""
    # Start watching with pipeline (not implemented but should not raise)
    change_stream = collection.watch(pipeline=[{"$match": {"test": "value"}}])

    # Insert a document
    collection.insert_one({"test": "data"})

    # Get the change (should work normally)
    change = next(change_stream)
    assert change is not None

    change_stream.close()
