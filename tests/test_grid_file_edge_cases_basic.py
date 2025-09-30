"""
Test cases to improve coverage for grid_file.py - basic version to avoid hangs
"""

import pytest
from unittest.mock import Mock, MagicMock
from neosqlite.gridfs.grid_file import GridIn, GridOut


def test_gridin_serialize_deserialize_metadata_edge_cases():
    """Test GridIn metadata serialization/deserialization edge cases"""
    db = MagicMock()

    grid_in = GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
    )

    # Test serialize with None
    result = grid_in._serialize_metadata(None)
    assert result is None

    # Test serialize with normal dict
    metadata = {"key": "value"}
    result = grid_in._serialize_metadata(metadata)
    assert result is not None
    assert "key" in result

    # Test deserialize with None
    result = grid_in._deserialize_metadata(None)
    assert result is None

    # Test deserialize with valid JSON
    json_str = '{"key": "value"}'
    result = grid_in._deserialize_metadata(json_str)
    assert result is not None
    assert result["key"] == "value"

    # Test deserialize with invalid JSON (should use ast.literal_eval fallback)
    invalid_json = "{'key': 'value'}"  # Single quotes, invalid JSON
    result = grid_in._deserialize_metadata(invalid_json)
    assert result is not None
    assert result["key"] == "value"

    # Test deserialize with completely invalid data (should return as _metadata)
    completely_invalid = "{invalid: data"
    result = grid_in._deserialize_metadata(completely_invalid)
    assert result is not None
    assert "_metadata" in result
    assert result["_metadata"] == completely_invalid


def test_gridin_write_type_validation():
    """Test GridIn write with type validation"""
    db = MagicMock()

    # Mock the execute method to simulate database operations
    # Set up proper mock for fetchone calls
    def mock_execute(query, params=None):
        mock_cursor = Mock()
        if "SELECT id FROM" in query:
            mock_cursor.fetchone.return_value = [1]  # Return integer ID
        else:
            mock_cursor.lastrowid = 1
            mock_cursor.rowcount = 1
        return mock_cursor

    db.execute = mock_execute

    with GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
    ) as grid_in:
        # Test writing bytes
        bytes_written = grid_in.write(b"test data")
        assert bytes_written == len(b"test data")

        # Test writing bytearray
        bytes_written = grid_in.write(bytearray(b"more test data"))
        assert bytes_written == len(b"more test data")

        # Test writing invalid type should raise TypeError
        with pytest.raises(TypeError):
            grid_in.write("string data")

        with pytest.raises(TypeError):
            grid_in.write(123)


def test_gridin_force_sync_if_needed():
    """Test GridIn _force_sync_if_needed method"""
    db = MagicMock()
    mock_cursor = Mock()
    mock_cursor.lastrowid = 1
    db.execute.return_value = mock_cursor

    # Test with write concern requiring sync
    grid_in_sync = GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
        write_concern={"j": True},
    )
    grid_in_sync._force_sync_if_needed()
    # Check that execute was called with WAL checkpoint
    db.execute.assert_called_with("PRAGMA wal_checkpoint(PASSIVE)")

    # Test with write concern requiring sync via "majority"
    db.reset_mock()
    grid_in_majority = GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
        write_concern={"w": "majority"},
    )
    grid_in_majority._force_sync_if_needed()
    # Should call WAL checkpoint again
    db.execute.assert_called_with("PRAGMA wal_checkpoint(PASSIVE)")

    # Test with write concern not requiring sync
    db.reset_mock()
    grid_in_no_sync = GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
        write_concern={"w": 1},
    )
    grid_in_no_sync._force_sync_if_needed()
    # Should not call WAL checkpoint
    db.execute.assert_not_called()


def test_gridin_context_manager():
    """Test GridIn context manager functionality"""
    db = MagicMock()

    # Mock the execute method to simulate database operations
    def mock_execute(query, params=None):
        mock_cursor = Mock()
        if "SELECT id FROM" in query:
            mock_cursor.fetchone.return_value = [1]  # Return integer ID
        else:
            mock_cursor.lastrowid = 1
            mock_cursor.rowcount = 1
        return mock_cursor

    db.execute = mock_execute

    with GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
    ) as grid_in:
        grid_in.write(b"test data")
        assert grid_in._closed is False  # Should not be closed yet

    # After context manager exits, should be closed
    assert grid_in._closed is True


def test_gridin_create_file_document_with_integer_id():
    """Test GridIn._create_file_document with integer ID"""
    db = MagicMock()
    mock_cursor = Mock()
    db.execute.return_value = mock_cursor

    # Test with integer file_id
    grid_in_int = GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test_int.txt",
        file_id=456,
    )

    # Call _create_file_document to test the integer ID path
    grid_in_int._create_file_document()


def test_gridin_disable_md5():
    """Test GridIn with MD5 disabled"""
    db = MagicMock()

    # Mock the execute method to simulate database operations
    def mock_execute(query, params=None):
        mock_cursor = Mock()
        if "SELECT id FROM" in query:
            mock_cursor.fetchone.return_value = [1]  # Return integer ID
        else:
            mock_cursor.lastrowid = 1
            mock_cursor.rowcount = 1
        return mock_cursor

    db.execute = mock_execute

    with GridIn(
        db=db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
        disable_md5=True,
    ) as grid_in:
        # Write some data
        grid_in.write(b"test data for md5 disabled")

        # Verify that no MD5 hasher was created
        assert grid_in._md5_hasher is None


def test_gridout_deserialize_metadata_edge_cases():
    """Test GridOut._deserialize_metadata with edge cases"""
    db = MagicMock()

    # Mock the database query for file metadata - return minimal data
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = [
        "test.txt",
        100,
        255 * 1024,
        "2023-01-01",
        "md5hash",
        None,
        "123",
    ]
    db.execute.return_value = mock_cursor

    grid_out = GridOut(db=db, bucket_name="fs", file_id=123)

    # Test with None
    result = grid_out._deserialize_metadata(None)
    assert result is None

    # Test with valid JSON
    result = grid_out._deserialize_metadata('{"key": "value"}')
    assert result is not None
    assert result["key"] == "value"

    # Test with invalid JSON (should use ast.literal_eval fallback)
    result = grid_out._deserialize_metadata("{'key': 'value'}")
    assert result is not None
    assert result["key"] == "value"

    # Test with completely invalid data
    result = grid_out._deserialize_metadata("{invalid: data")
    assert result is not None
    assert "_metadata" in result


def test_gridout_context_manager():
    """Test GridOut context manager functionality"""
    db = MagicMock()

    # Mock the database query for file metadata
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = [
        "test.txt",
        100,
        255 * 1024,
        "2023-01-01",
        "md5hash",
        None,
        "123",
    ]
    db.execute.return_value = mock_cursor

    with GridOut(db=db, bucket_name="fs", file_id=123) as grid_out:
        assert grid_out._closed is False  # Should not be closed yet

    # After context manager exits, should be closed
    assert grid_out._closed is True
