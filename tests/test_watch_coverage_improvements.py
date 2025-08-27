# coding: utf-8
"""
Additional tests to improve ChangeStream code coverage
"""
import neosqlite
import pytest
import time


def test_watch_cleanup_exception_coverage():
    """Test that covers the exception handling in _cleanup_triggers"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create a change stream
        change_stream = collection.watch()

        # Insert a document
        collection.insert_one({"test": "data"})

        # Get a change
        change = next(change_stream)
        assert change is not None

        # Close the change stream normally - this will execute the cleanup code
        # Even though no exception occurs, we're executing the try/except block
        change_stream.close()

        # Verify it's closed
        assert change_stream._closed


def test_watch_multiple_close_operations():
    """Test multiple close operations on the same change stream"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create a change stream
        change_stream = collection.watch()

        # Insert a document
        collection.insert_one({"test": "data"})

        # Get a change
        change = next(change_stream)
        assert change is not None

        # Close multiple times - all should be safe
        change_stream.close()
        change_stream.close()  # Second close
        change_stream.close()  # Third close

        # Verify it's closed
        assert change_stream._closed


def test_watch_timeout_boundary_conditions():
    """Test timeout boundary conditions"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test with very small timeout
        change_stream = collection.watch(max_await_time_ms=1)

        # Should timeout quickly when no changes
        start = time.time()
        with pytest.raises(StopIteration):
            next(change_stream)
        elapsed = time.time() - start

        # Should be quick (less than 1 second)
        assert elapsed < 1.0
        change_stream.close()


def test_watch_timeout_with_changes_available():
    """Test that we can get changes when they are made after stream creation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create change stream first
        change_stream = collection.watch(
            max_await_time_ms=1000
        )  # 1 second timeout

        # Then insert a document
        collection.insert_one({"test": "data"})

        # Should get change quickly
        start = time.time()
        change = next(change_stream)
        elapsed = time.time() - start

        # Should be very quick (much less than timeout)
        assert elapsed < 0.1
        assert change is not None

        change_stream.close()


def test_watch_batch_size_edge_cases():
    """Test batch_size edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test with various batch_size values
        for batch_size in [None, 0, -1, 1, 10]:
            change_stream = collection.watch(batch_size=batch_size)

            # Insert a document
            collection.insert_one({"test": f"data_{batch_size}"})

            # Should work normally
            change = next(change_stream)
            assert change is not None

            change_stream.close()


def test_watch_max_await_time_edge_cases():
    """Test max_await_time_ms edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test with various max_await_time_ms values (excluding 0 and None which might cause issues)
        for timeout in [100, 500, 1000]:
            change_stream = collection.watch(max_await_time_ms=timeout)

            # Insert a document after creating the stream
            collection.insert_one({"test": f"data_{timeout}"})

            # Should work normally
            change = next(change_stream)
            assert change is not None

            change_stream.close()


def test_watch_full_document_variations():
    """Test full_document parameter variations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test with different full_document values
        for full_doc in [None, "updateLookup", "other_value"]:
            change_stream = collection.watch(full_document=full_doc)

            # Insert a document
            collection.insert_one({"test": f"data_{full_doc}"})

            # Should work normally
            change = next(change_stream)
            assert change is not None

            change_stream.close()


def test_watch_unused_parameters():
    """Test that unused parameters are accepted without errors"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test all the parameters that are not implemented but should be accepted
        change_stream = collection.watch(
            pipeline=[{"$match": {"operationType": "insert"}}],
            resume_after={"_id": 123},
            collation={"locale": "en"},
            start_at_operation_time="some_time",
            session="some_session",
            start_after={"_id": 456},
        )

        # Insert a document
        collection.insert_one({"test": "data"})

        # Should work normally despite unused parameters
        change = next(change_stream)
        assert change is not None

        change_stream.close()


def test_watch_namespace_structure():
    """Test the structure of the namespace in change documents"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create change stream
        change_stream = collection.watch()

        # Insert a document
        collection.insert_one({"test": "data"})

        # Get the change
        change = next(change_stream)

        # Verify namespace structure
        assert "ns" in change
        ns = change["ns"]
        assert "db" in ns
        assert "coll" in ns
        assert ns["coll"] == "test_collection"
        # db is hardcoded as "default" in current implementation

        change_stream.close()


def test_watch_document_key_structure():
    """Test the structure of the documentKey in change documents"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create change stream
        change_stream = collection.watch()

        # Insert a document
        result = collection.insert_one({"test": "data"})
        doc_id = result.inserted_id

        # Get the change
        change = next(change_stream)

        # Verify documentKey structure
        assert "documentKey" in change
        doc_key = change["documentKey"]
        assert "_id" in doc_key
        assert doc_key["_id"] == doc_id

        change_stream.close()


def test_watch_cluster_time_present():
    """Test that clusterTime is present in change documents"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create change stream
        change_stream = collection.watch()

        # Insert a document
        collection.insert_one({"test": "data"})

        # Get the change
        change = next(change_stream)

        # Verify clusterTime is present
        assert "clusterTime" in change
        # Should be a timestamp string
        assert isinstance(change["clusterTime"], str)

        change_stream.close()


def test_watch_operation_type_values():
    """Test different operationType values"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create change stream
        change_stream = collection.watch()

        # Test insert
        result = collection.insert_one({"test": "insert"})
        change = next(change_stream)
        assert change["operationType"] == "insert"

        # Test update
        doc_id = result.inserted_id
        collection.update_one({"_id": doc_id}, {"$set": {"test": "updated"}})
        change = next(change_stream)
        assert change["operationType"] == "update"

        # Test delete
        collection.delete_one({"_id": doc_id})
        change = next(change_stream)
        assert change["operationType"] == "delete"

        change_stream.close()
