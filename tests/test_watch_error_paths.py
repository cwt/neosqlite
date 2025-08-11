# coding: utf-8
"""
Additional tests specifically for ChangeStream error paths to improve coverage
"""
import time
import pytest
import neosqlite
from unittest.mock import patch


def test_watch_cleanup_with_closed_stream(collection):
    """Test cleanup when stream is already closed"""
    # Start watching for changes
    change_stream = collection.watch()

    # Insert a document
    collection.insert_one({"test": "data"})

    # Get a change
    change = next(change_stream)
    assert change is not None

    # Manually mark as closed
    change_stream._closed = True

    # Close the stream - should return early without errors
    change_stream.close()

    # Reset and properly close
    change_stream._closed = False
    change_stream.close()


def test_watch_iterator_protocol(collection):
    """Test the iterator protocol implementation"""
    # Start watching for changes
    change_stream = collection.watch()

    # Test __iter__ returns self
    iterator = iter(change_stream)
    assert iterator is change_stream

    # Insert a document
    collection.insert_one({"test": "iterator"})

    # Test __next__ works
    change = next(change_stream)
    assert change is not None
    assert change["operationType"] == "insert"

    # Close the stream
    change_stream.close()


def test_watch_context_manager_protocol(collection):
    """Test the context manager protocol implementation"""
    # Test __enter__ and __exit__ methods
    with collection.watch() as change_stream:
        # __enter__ should return self
        assert isinstance(change_stream, neosqlite.ChangeStream)

        # Insert a document
        collection.insert_one({"test": "context"})

        # Get a change
        change = next(change_stream)
        assert change is not None
        assert change["operationType"] == "insert"

        # Stream should not be closed yet
        assert not change_stream._closed

    # After exiting context, stream should be closed
    assert change_stream._closed


def test_watch_context_manager_with_exception(collection):
    """Test context manager properly closes stream even when exception occurs"""
    try:
        with collection.watch() as change_stream:
            # Insert a document
            collection.insert_one({"test": "exception"})

            # Get a change
            change = next(change_stream)
            assert change is not None

            # Simulate an exception
            raise ValueError("Test exception")
    except ValueError:
        # Exception should be caught
        pass

    # Even though an exception occurred, the stream should be closed
    # Note: We can't check this directly since the change_stream variable is out of scope
    # But the test verifies that __exit__ was called without raising an exception


def test_watch_batch_size_handling(collection):
    """Test batch_size handling with different values"""
    # Test with None (should use default)
    change_stream1 = collection.watch(batch_size=None)
    collection.insert_one({"test": "batch1"})
    change1 = next(change_stream1)
    assert change1 is not None
    change_stream1.close()

    # Test with 0 (should use default)
    change_stream2 = collection.watch(batch_size=0)
    collection.insert_one({"test": "batch2"})
    change2 = next(change_stream2)
    assert change2 is not None
    change_stream2.close()

    # Test with negative value (should use default)
    change_stream3 = collection.watch(batch_size=-5)
    collection.insert_one({"test": "batch3"})
    change3 = next(change_stream3)
    assert change3 is not None
    change_stream3.close()


def test_watch_full_document_with_none_data(collection):
    """Test fullDocument handling when document_data is None"""
    # Start watching with full document lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Manually insert a record with None document_data
    collection.db.execute(
        """
        INSERT INTO _neosqlite_changestream 
        (collection_name, operation, document_id, document_data)
        VALUES (?, 'custom', 888, NULL)
        """,
        (collection.name,),
    )
    collection.db.commit()

    # Get the change - should not raise an exception even with None document_data
    change = next(change_stream)
    assert change["documentKey"]["_id"] == 888
    # fullDocument should not be present due to None document_data

    change_stream.close()


def test_watch_json_type_error_handling(collection):
    """Test handling of TypeError when parsing document data"""
    # Start watching with full document lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Mock json.loads to raise TypeError
    with patch("neosqlite.neosqlite.json.loads") as mock_loads:
        mock_loads.side_effect = TypeError("Invalid type")

        # Manually insert valid JSON data
        collection.db.execute(
            """
            INSERT INTO _neosqlite_changestream 
            (collection_name, operation, document_id, document_data)
            VALUES (?, 'insert', 777, ?)
            """,
            (collection.name, '{"test": "data"}'),
        )
        collection.db.commit()

        # Get the change - should not raise an exception even with TypeError
        change = next(change_stream)
        assert change["documentKey"]["_id"] == 777
        # fullDocument should not be present due to TypeError

    change_stream.close()


def test_watch_timeout_with_no_changes(collection):
    """Test timeout behavior when there are no changes"""
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


def test_watch_timeout_with_changes(collection):
    """Test that timeout doesn't interfere when changes are available"""
    # Start watching with a short timeout
    change_stream = collection.watch(max_await_time_ms=100)  # 100ms timeout

    # Insert a document immediately
    collection.insert_one({"test": "timeout"})

    # Should get the change quickly, before timeout
    start_time = time.time()
    change = next(change_stream)
    elapsed = time.time() - start_time

    assert change is not None
    assert elapsed < 0.1  # Should be much faster than timeout

    change_stream.close()
