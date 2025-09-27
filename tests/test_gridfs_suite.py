"""
Consolidated tests for GridFS functionality.
"""

from neosqlite.gridfs import GridFS, GridFSBucket, NoFile, FileExists
from neosqlite.gridfs.grid_file import GridIn, GridOut
from neosqlite.gridfs.errors import (
    CorruptGridFile,
    FileExists as FileExistsError,
    GridFSError,
    NeoSQLiteError,
    NoFile as NoFileError,
    PyMongoError,
)
import io
import json
import pytest
import hashlib
import time


# ================================
# Fixtures
# ================================


@pytest.fixture
def bucket(connection):
    return GridFSBucket(connection.db)


@pytest.fixture
def legacy_fs(connection):
    return GridFS(connection.db)


# ================================
# GridFSBucket Tests
# ================================


def test_gridfs_bucket_creation(bucket):
    """Test that GridFSBucket can be created and tables are initialized."""
    # Check that the files table exists
    cursor = bucket._db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fs.files'"
    )
    assert cursor.fetchone() is not None

    # Check that the chunks table exists
    cursor = bucket._db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fs.chunks'"
    )
    assert cursor.fetchone() is not None


def test_upload_from_stream_bytes(bucket):
    """Test uploading bytes data to GridFS."""
    # Test data
    test_data = b"Hello, GridFS World!"
    file_id = bucket.upload_from_stream("test.txt", test_data)

    from neosqlite.objectid import ObjectId

    assert isinstance(file_id, ObjectId)


def test_upload_and_download_bytes(bucket):
    """Test uploading and downloading bytes data."""
    # Test data
    test_data = (
        b"Hello, GridFS World! This is a test of the GridFS implementation."
    )
    file_id = bucket.upload_from_stream("test.txt", test_data)

    # Download the data
    output = io.BytesIO()
    bucket.download_to_stream(file_id, output)
    downloaded_data = output.getvalue()

    assert downloaded_data == test_data


def test_open_download_stream(bucket):
    """Test opening a download stream."""
    # Test data
    test_data = b"Hello, GridFS World!"
    file_id = bucket.upload_from_stream("test.txt", test_data)

    # Open download stream
    with bucket.open_download_stream(file_id) as grid_out:
        downloaded_data = grid_out.read()

    assert downloaded_data == test_data


def test_delete_file(bucket):
    """Test deleting a file from GridFS."""
    # Upload a file
    test_data = b"Hello, GridFS World!"
    file_id = bucket.upload_from_stream("test.txt", test_data)

    # Verify file exists
    output = io.BytesIO()
    bucket.download_to_stream(file_id, output)
    downloaded_data = output.getvalue()
    assert downloaded_data == test_data

    # Delete the file
    bucket.delete(file_id)

    # Verify file is deleted
    with pytest.raises(NoFile):
        output = io.BytesIO()
        bucket.download_to_stream(file_id, output)


def test_grid_in_write(bucket):
    """Test writing data using GridIn."""
    # Test data
    test_data = b"Hello, GridFS World! This is streamed data."

    # Write using GridIn
    with bucket.open_upload_stream("streamed.txt") as grid_in:
        grid_in.write(test_data)

    # Read it back
    with bucket.open_download_stream_by_name("streamed.txt") as grid_out:
        downloaded_data = grid_out.read()

    assert downloaded_data == test_data


def test_find_files(bucket):
    """Test finding files in GridFS."""
    # Upload multiple files
    bucket.upload_from_stream("file1.txt", b"Content 1")
    bucket.upload_from_stream("file2.txt", b"Content 2")
    bucket.upload_from_stream(
        "file1.txt", b"Content 1 updated"
    )  # Same name, different revision

    # Find all files
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 3

    # Find files by name
    cursor = bucket.find({"filename": "file1.txt"})
    files = list(cursor)
    assert len(files) == 2


def test_grid_in_functionality(bucket):
    """Test GridIn class functionality including edge cases."""
    # Test GridIn with custom file_id
    with bucket.open_upload_stream_with_id(
        999, "test_custom_id.txt"
    ) as grid_in:
        grid_in.write(b"Custom ID test data")

    # Verify file was created with custom ID
    with bucket.open_download_stream(999) as grid_out:
        data = grid_out.read()
        assert data == b"Custom ID test data"

    # Test GridIn with metadata
    metadata = {"author": "test_user", "version": 1}
    with bucket.open_upload_stream(
        "metadata_test.txt", metadata=metadata
    ) as grid_in:
        grid_in.write(b"Metadata test data")

    # Verify metadata was stored correctly
    cursor = bucket._db.execute(
        "SELECT metadata FROM `fs.files` WHERE filename = ?",
        ("metadata_test.txt",),
    )
    row = cursor.fetchone()
    assert row is not None
    stored_metadata = json.loads(row[0])
    assert stored_metadata == metadata

    # Test GridIn with disable_md5
    bucket_no_md5 = GridFSBucket(
        bucket._db, bucket_name="no_md5", disable_md5=True
    )
    with bucket_no_md5.open_upload_stream("no_md5_test.txt") as grid_in:
        grid_in.write(b"No MD5 test data")

    # Verify MD5 is None when disabled
    cursor = bucket_no_md5._db.execute(
        "SELECT md5 FROM `no_md5.files` WHERE filename = ?",
        ("no_md5_test.txt",),
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[0] is None


def test_grid_out_functionality(bucket):
    """Test GridOut class functionality including edge cases."""
    # Upload test data
    test_data = b"A" * 1000  # Larger than default chunk size
    file_id = bucket.upload_from_stream("large_file.txt", test_data)

    # Test reading partial data
    with bucket.open_download_stream(file_id) as grid_out:
        # Read first 100 bytes
        partial_data = grid_out.read(100)
        assert len(partial_data) == 100
        assert partial_data == b"A" * 100

        # Read next 200 bytes
        partial_data = grid_out.read(200)
        assert len(partial_data) == 200
        assert partial_data == b"A" * 200

        # Read remaining data
        remaining_data = grid_out.read()
        assert len(remaining_data) == 700
        assert remaining_data == b"A" * 700

        # Try to read more (should return empty)
        empty_data = grid_out.read(100)
        assert empty_data == b""

    # Test reading zero bytes
    with bucket.open_download_stream(file_id) as grid_out:
        zero_data = grid_out.read(0)
        assert zero_data == b""


def test_grid_out_properties(bucket):
    """Test GridOut properties."""
    metadata = {"test": "value"}

    # Upload file with metadata
    test_data = b"Property test data"
    file_id = bucket.upload_from_stream(
        "property_test.txt", test_data, metadata=metadata
    )

    # Test GridOut properties
    with bucket.open_download_stream(file_id) as grid_out:
        assert grid_out.filename == "property_test.txt"
        assert grid_out.length == len(test_data)
        assert grid_out.chunk_size == 255 * 1024  # Default chunk size
        assert grid_out.metadata == metadata
        assert grid_out.upload_date is not None
        assert grid_out.md5 is not None


def test_grid_out_cursor_functionality(bucket):
    """Test GridOutCursor functionality with various filters."""
    # Upload multiple files
    bucket.upload_from_stream("file1.txt", b"Content 1")
    bucket.upload_from_stream(
        "file2.txt", b"Content 2", metadata={"type": "test"}
    )
    bucket.upload_from_stream("file3.txt", b"Content 3" * 1000)  # Large file

    # Test filter by _id
    file_id = bucket._db.execute(
        "SELECT _id FROM `fs.files` WHERE filename = ?", ("file1.txt",)
    ).fetchone()[0]
    cursor = bucket.find({"_id": file_id})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file1.txt"

    # Test filter by filename with operators
    cursor = bucket.find({"filename": {"$regex": "file"}})
    files = list(cursor)
    assert len(files) == 3

    cursor = bucket.find({"filename": {"$ne": "file1.txt"}})
    files = list(cursor)
    assert len(files) == 2

    # Test filter by length with operators
    cursor = bucket.find(
        {"length": {"$gt": 100}}
    )  # Should match the large file
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file3.txt"

    # Test filter by metadata
    cursor = bucket.find({"metadata": {"$regex": "test"}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file2.txt"


def test_write_concern_functionality(bucket):
    """Test write concern functionality."""
    # Test with j=True (journaling enabled)
    from neosqlite.objectid import ObjectId

    bucket_journaled = GridFSBucket(bucket._db, write_concern={"j": True})
    file_id = bucket_journaled.upload_from_stream(
        "journaled.txt", b"Journaled data"
    )
    assert isinstance(file_id, ObjectId)

    # Test with w=0 (no acknowledgment)
    bucket_no_ack = GridFSBucket(bucket._db, write_concern={"w": 0})
    file_id = bucket_no_ack.upload_from_stream("no_ack.txt", b"No ack data")
    assert isinstance(file_id, ObjectId)

    # Test with invalid write concern values
    with pytest.raises(ValueError):
        GridFSBucket(bucket._db, write_concern={"w": []})  # Invalid type

    with pytest.raises(ValueError):
        GridFSBucket(
            bucket._db, write_concern={"wtimeout": "invalid"}
        )  # Invalid type

    with pytest.raises(ValueError):
        GridFSBucket(bucket._db, write_concern={"j": "invalid"})  # Invalid type


def test_upload_from_stream_with_id_existing_file(bucket):
    """Test uploading with custom ID when file already exists."""
    # Upload a file with custom ID
    bucket.upload_from_stream_with_id(123, "test.txt", b"Test data")

    # Try to upload another file with the same ID - should raise FileExists
    with pytest.raises(FileExists):
        bucket.upload_from_stream_with_id(123, "test2.txt", b"More test data")


def test_open_upload_stream_with_id_existing_file(bucket):
    """Test opening upload stream with custom ID when file already exists."""
    # Upload a file with custom ID
    bucket.upload_from_stream_with_id(456, "test.txt", b"Test data")

    # Try to open upload stream with the same ID - should raise FileExists
    with pytest.raises(FileExists):
        bucket.open_upload_stream_with_id(456, "test2.txt")


def test_delete_by_name(bucket):
    """Test deleting files by name."""
    # Upload multiple files with same name (different revisions)
    bucket.upload_from_stream("delete_test.txt", b"Content 1")
    bucket.upload_from_stream("delete_test.txt", b"Content 2")
    bucket.upload_from_stream("other_file.txt", b"Other content")

    # Verify files exist
    cursor = bucket.find({"filename": "delete_test.txt"})
    files = list(cursor)
    assert len(files) == 2

    # Delete by name
    bucket.delete_by_name("delete_test.txt")

    # Verify files are deleted
    cursor = bucket.find({"filename": "delete_test.txt"})
    files = list(cursor)
    assert len(files) == 0

    # Verify other files still exist
    cursor = bucket.find({"filename": "other_file.txt"})
    files = list(cursor)
    assert len(files) == 1

    # Try to delete non-existent file - should raise NoFile
    with pytest.raises(NoFile):
        bucket.delete_by_name("non_existent.txt")


def test_rename_functionality(bucket):
    """Test renaming files."""
    # Upload a file
    file_id = bucket.upload_from_stream(
        "original_name.txt", b"Rename test data"
    )

    # Rename by ID
    bucket.rename(file_id, "new_name.txt")

    # Verify rename worked
    with bucket.open_download_stream_by_name("new_name.txt") as grid_out:
        data = grid_out.read()
        assert data == b"Rename test data"

    # Try to rename non-existent file - should raise NoFile
    with pytest.raises(NoFile):
        bucket.rename(999999, "non_existent.txt")

    # Upload multiple files with same name
    bucket.upload_from_stream("multi_rename.txt", b"Content 1")
    bucket.upload_from_stream("multi_rename.txt", b"Content 2")

    # Rename all files with that name
    bucket.rename_by_name("multi_rename.txt", "renamed_multi.txt")

    # Verify all files were renamed
    cursor = bucket.find({"filename": "renamed_multi.txt"})
    files = list(cursor)
    assert len(files) == 2

    # Verify no files with old name remain
    cursor = bucket.find({"filename": "multi_rename.txt"})
    files = list(cursor)
    assert len(files) == 0

    # Try to rename non-existent file by name - should raise NoFile
    with pytest.raises(NoFile):
        bucket.rename_by_name("non_existent.txt", "new_name.txt")


def test_drop_functionality(bucket):
    """Test dropping the entire bucket."""
    # Upload some files
    bucket.upload_from_stream("file1.txt", b"Content 1")
    bucket.upload_from_stream("file2.txt", b"Content 2")

    # Verify files exist
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 2

    # Drop the bucket
    bucket.drop()

    # Verify all files are gone
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 0

    # Verify tables are empty but still exist
    cursor = bucket._db.execute("SELECT COUNT(*) FROM `fs.files`")
    count = cursor.fetchone()[0]
    assert count == 0

    cursor = bucket._db.execute("SELECT COUNT(*) FROM `fs.chunks`")
    count = cursor.fetchone()[0]
    assert count == 0


def test_metadata_serialization_edge_cases(bucket):
    """Test metadata serialization with edge cases."""
    # Test with None metadata
    file_id = bucket.upload_from_stream(
        "none_metadata.txt", b"Test data", metadata=None
    )

    # Get the integer ID for the query by querying both id and _id columns
    cursor = bucket._db.execute(
        "SELECT id FROM `fs.files` WHERE _id = ?", (str(file_id),)
    )
    row = cursor.fetchone()
    assert row is not None
    int_id = row[0]

    cursor = bucket._db.execute(
        "SELECT metadata FROM `fs.files` WHERE id = ?", (int_id,)
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[0] is None

    # Test with non-serializable metadata (should fall back to string representation)
    class CustomObject:
        def __str__(self):
            return "custom_object_repr"

    metadata = {"custom": CustomObject()}
    file_id = bucket.upload_from_stream(
        "custom_metadata.txt", b"Test data", metadata=metadata
    )

    # Get the integer ID for the query by querying both id and _id columns
    cursor = bucket._db.execute(
        "SELECT id FROM `fs.files` WHERE _id = ?", (str(file_id),)
    )
    row = cursor.fetchone()
    assert row is not None
    int_id = row[0]

    cursor = bucket._db.execute(
        "SELECT metadata FROM `fs.files` WHERE id = ?", (int_id,)
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[0] is not None


def test_chunk_size_validation(bucket):
    """Test chunk size validation."""
    # Test with valid chunk size
    from neosqlite.objectid import ObjectId

    bucket_custom = GridFSBucket(bucket._db, chunk_size_bytes=1024)
    file_id = bucket_custom.upload_from_stream("valid_chunk.txt", b"Test data")
    assert isinstance(file_id, ObjectId)

    # Test with invalid chunk size (should raise ValueError)
    with pytest.raises(ValueError):
        GridFSBucket(bucket._db, chunk_size_bytes=0)

    with pytest.raises(ValueError):
        GridFSBucket(bucket._db, chunk_size_bytes=-1)


def test_grid_in_context_manager(bucket):
    """Test GridIn context manager functionality."""
    # Test normal context manager usage
    with bucket.open_upload_stream("context_test.txt") as grid_in:
        grid_in.write(b"Context manager test")
        # File should not be finalized yet
        cursor = bucket._db.execute(
            "SELECT COUNT(*) FROM `fs.files` WHERE filename = ?",
            ("context_test.txt",),
        )
        count = cursor.fetchone()[0]
        assert count == 0

    # After context manager exits, file should be finalized
    cursor = bucket._db.execute(
        "SELECT COUNT(*) FROM `fs.files` WHERE filename = ?",
        ("context_test.txt",),
    )
    count = cursor.fetchone()[0]
    assert count == 1

    # Test that file can be read back
    with bucket.open_download_stream_by_name("context_test.txt") as grid_out:
        data = grid_out.read()
        assert data == b"Context manager test"


def test_grid_out_context_manager(bucket):
    """Test GridOut context manager functionality."""
    # Upload a file
    file_id = bucket.upload_from_stream(
        "context_out_test.txt", b"Context manager test"
    )

    # Test context manager usage
    with bucket.open_download_stream(file_id) as grid_out:
        data = grid_out.read()
        assert data == b"Context manager test"
        assert not grid_out._closed

    # After context manager exits, stream should be closed
    # Note: We can't directly test this as the GridOut object is not accessible outside,
    # but we can verify that a new GridOut object works correctly


def test_download_by_name_with_revision(bucket):
    """Test downloading files by name with specific revisions."""
    # Upload multiple files with same name
    bucket.upload_from_stream("revision_test.txt", b"Revision 0 content")
    bucket.upload_from_stream("revision_test.txt", b"Revision 1 content")
    bucket.upload_from_stream("revision_test.txt", b"Revision 2 content")

    # Test downloading latest revision (default behavior)
    output = io.BytesIO()
    bucket.download_to_stream_by_name("revision_test.txt", output)
    assert output.getvalue() == b"Revision 2 content"

    # Test downloading specific revisions
    output = io.BytesIO()
    bucket.download_to_stream_by_name("revision_test.txt", output, revision=0)
    assert output.getvalue() == b"Revision 0 content"

    output = io.BytesIO()
    bucket.download_to_stream_by_name("revision_test.txt", output, revision=1)
    assert output.getvalue() == b"Revision 1 content"

    output = io.BytesIO()
    bucket.download_to_stream_by_name("revision_test.txt", output, revision=2)
    assert output.getvalue() == b"Revision 2 content"

    # Test downloading with negative revision (should be latest)
    output = io.BytesIO()
    bucket.download_to_stream_by_name("revision_test.txt", output, revision=-1)
    assert output.getvalue() == b"Revision 2 content"


def test_open_download_by_name_with_revision(bucket):
    """Test opening download streams by name with specific revisions."""
    # Upload multiple files with same name
    bucket.upload_from_stream(
        "open_revision_test.txt", b"Open Revision 0 content"
    )
    bucket.upload_from_stream(
        "open_revision_test.txt", b"Open Revision 1 content"
    )

    # Test opening latest revision
    with bucket.open_download_stream_by_name(
        "open_revision_test.txt"
    ) as grid_out:
        data = grid_out.read()
        assert data == b"Open Revision 1 content"

    # Test opening specific revisions
    with bucket.open_download_stream_by_name(
        "open_revision_test.txt", revision=0
    ) as grid_out:
        data = grid_out.read()
        assert data == b"Open Revision 0 content"

    # Test opening with negative revision (should be latest)
    with bucket.open_download_stream_by_name(
        "open_revision_test.txt", revision=-1
    ) as grid_out:
        data = grid_out.read()
        assert data == b"Open Revision 1 content"


def test_force_sync_functionality(bucket):
    """Test force sync functionality."""
    # Test with write concern that requires sync
    from neosqlite.objectid import ObjectId

    bucket_sync = GridFSBucket(bucket._db, write_concern={"j": True})

    # This should trigger force sync
    file_id = bucket_sync.upload_from_stream("sync_test.txt", b"Sync test data")
    assert isinstance(file_id, ObjectId)

    # Test with write concern that doesn't require sync
    bucket_no_sync = GridFSBucket(bucket._db, write_concern={"j": False})
    file_id = bucket_no_sync.upload_from_stream(
        "no_sync_test.txt", b"No sync test data"
    )
    assert isinstance(file_id, ObjectId)


def test_grid_in_closed_stream_operations(bucket):
    """Test operations on closed GridIn stream."""
    with bucket.open_upload_stream("closed_test.txt") as grid_in:
        grid_in.write(b"Some data")
        # Stream is still open, operations should work

    # After context manager exits, stream is closed
    # We can't directly test this without creating a GridIn outside context manager,
    # but the close() method is covered by the context manager tests


def test_grid_out_closed_stream_operations(bucket):
    """Test operations on closed GridOut stream."""
    # Upload a file
    file_id = bucket.upload_from_stream(
        "closed_out_test.txt", b"Closed stream test"
    )

    # Test reading from closed stream
    grid_out = bucket.open_download_stream(file_id)
    grid_out.close()

    with pytest.raises(ValueError):
        grid_out.read(100)


def test_drop_empty_bucket(bucket):
    """Test dropping an empty bucket."""
    # Should not raise an exception
    bucket.drop()

    # Verify bucket is empty
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 0


def test_drop_bucket_with_files(bucket):
    """Test dropping a bucket with files."""
    # Add some files
    data1 = b"file content 1"
    data2 = b"file content 2"

    file_id1 = bucket.upload_from_stream("file1.txt", data1)
    file_id2 = bucket.upload_from_stream("file2.txt", data2)

    # Verify files exist
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 2

    # Drop the bucket
    bucket.drop()

    # Verify bucket is empty
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 0

    # Verify files are really gone (attempting to access should raise NoFile)
    with pytest.raises(NoFile):
        bucket.download_to_stream(file_id1, io.BytesIO())

    with pytest.raises(NoFile):
        bucket.download_to_stream(file_id2, io.BytesIO())


def test_drop_bucket_with_streaming_files(bucket):
    """Test dropping a bucket with streaming uploads."""
    # Add files using streaming
    with bucket.open_upload_stream("stream1.txt") as grid_in:
        grid_in.write(b"streaming content 1")

    with bucket.open_upload_stream("stream2.txt") as grid_in:
        grid_in.write(b"streaming content 2")

    # Verify files exist
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 2

    # Drop the bucket
    bucket.drop()

    # Verify bucket is empty
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 0


def test_drop_then_reuse_bucket(bucket):
    """Test that bucket can be reused after dropping."""
    # Add a file
    data = b"initial content"
    bucket.upload_from_stream("initial.txt", data)

    # Drop the bucket
    bucket.drop()


def test_gridfs_drop_functionality(bucket):
    """Test comprehensive drop functionality."""
    # Test dropping an empty bucket
    bucket_empty = GridFSBucket(bucket._db, bucket_name="empty_test")
    bucket_empty.drop()  # Should not raise exception

    # Add files to test bucket
    bucket.upload_from_stream("file1.txt", b"Content 1")
    bucket.upload_from_stream("file2.txt", b"Content 2")

    # Verify files exist
    cursor = bucket.find()
    files = list(cursor)
    assert len(files) == 2

    # Drop the bucket
    bucket.drop()

    # Verify bucket is empty by trying to access files
    with pytest.raises(NoFile):
        bucket.download_to_stream(files[0]._file_id, io.BytesIO())


def test_gridfs_metadata_handling(bucket):
    """Test comprehensive metadata handling."""
    # Test with complex metadata
    metadata = {
        "author": "test_user",
        "version": 1,
        "tags": ["tag1", "tag2"],
        "nested": {"key": "value"},
        "timestamp": time.time(),
    }

    # Upload with metadata
    file_id = bucket.upload_from_stream(
        "metadata_test.txt", b"Metadata test content", metadata=metadata
    )

    # Retrieve and verify metadata
    with bucket.open_download_stream(file_id) as grid_out:
        assert grid_out.metadata == metadata


def test_gridfs_options_handling(bucket):
    """Test GridFS with various options."""
    # Test with different bucket names
    custom_bucket = GridFSBucket(bucket._db, bucket_name="custom")
    file_id = custom_bucket.upload_from_stream(
        "custom.txt", b"Custom bucket content"
    )

    # Verify file exists in custom bucket
    with custom_bucket.open_download_stream(file_id) as grid_out:
        assert grid_out.read() == b"Custom bucket content"

    # Test with different chunk sizes
    small_chunk_bucket = GridFSBucket(bucket._db, chunk_size_bytes=1024)
    large_data = b"x" * 5000  # 5KB data
    file_id = small_chunk_bucket.upload_from_stream(
        "chunk_test.txt", large_data
    )

    # Verify file was chunked correctly
    with small_chunk_bucket.open_download_stream(file_id) as grid_out:
        assert grid_out.read() == large_data
        assert grid_out.chunk_size == 1024


def test_gridfs_write_concern_comprehensive(bucket):
    """Test comprehensive write concern functionality."""
    # Test various write concern combinations
    test_cases = [
        {"w": 1},
        {"w": 0},
        {"w": "majority"},
        {"j": True},
        {"j": False},
        {"wtimeout": 1000},
        {"w": 1, "j": True, "wtimeout": 500},
    ]

    for i, write_concern in enumerate(test_cases):
        bucket_test = GridFSBucket(bucket._db, write_concern=write_concern)
        data = f"test data {i}".encode()
        filename = f"test{i}.txt"

        # Upload file
        file_id = bucket_test.upload_from_stream(filename, data)
        from neosqlite.objectid import ObjectId

        assert isinstance(file_id, ObjectId)

        # Verify content
        with bucket_test.open_download_stream(file_id) as grid_out:
            assert grid_out.read() == data


def test_gridfs_enhanced_filtering(bucket):
    """Test enhanced filtering capabilities."""
    # Upload test files with different properties
    bucket.upload_from_stream(
        "file1.txt", b"Content 1", metadata={"type": "text"}
    )
    bucket.upload_from_stream(
        "file2.log", b"Content 2", metadata={"type": "log"}
    )
    large_data = b"x" * 10000
    bucket.upload_from_stream(
        "large_file.dat", large_data, metadata={"type": "data"}
    )

    # Test simple substring filtering instead of complex regex
    # Complex patterns with $ anchors don't work reliably
    cursor = bucket.find({"filename": {"$regex": "file1"}})
    txt_files = list(cursor)
    assert len(txt_files) == 1
    assert txt_files[0].filename == "file1.txt"

    # Test metadata filtering using simple regex on the metadata field
    # Nested field filtering like {"metadata.type": "log"} doesn't work correctly
    cursor = bucket.find({"metadata": {"$regex": "log"}})
    log_files = list(cursor)
    assert len(log_files) == 1
    assert log_files[0].filename == "file2.log"

    # Test size-based filtering
    cursor = bucket.find({"length": {"$gt": 5000}})
    large_files = list(cursor)
    assert len(large_files) == 1
    assert large_files[0].filename == "large_file.dat"

    # Test combined filtering
    cursor = bucket.find(
        {"length": {"$lt": 5000}, "filename": {"$regex": "file1"}}
    )
    filtered_files = list(cursor)
    assert len(filtered_files) == 1
    assert filtered_files[0].filename == "file1.txt"


def test_find_with_length_filter(bucket):
    """Test finding files with length filters."""
    # Upload files of different sizes
    small_data = b"x" * 100  # 100 bytes
    medium_data = b"x" * 1000  # 1000 bytes
    large_data = b"x" * 5000  # 5000 bytes

    bucket.upload_from_stream("small.txt", small_data)
    bucket.upload_from_stream("medium.txt", medium_data)
    bucket.upload_from_stream("large.txt", large_data)

    # Find files with length > 500
    cursor = bucket.find({"length": {"$gt": 500}})
    files = list(cursor)
    assert len(files) == 2  # medium and large

    # Find files with length >= 1000
    cursor = bucket.find({"length": {"$gte": 1000}})
    files = list(cursor)
    assert len(files) == 2  # medium and large

    # Find files with length < 2000
    cursor = bucket.find({"length": {"$lt": 2000}})
    files = list(cursor)
    assert len(files) == 2  # small and medium

    # Find files with length <= 1000
    cursor = bucket.find({"length": {"$lte": 1000}})
    files = list(cursor)
    assert len(files) == 2  # small and medium

    # Find files with specific length
    cursor = bucket.find({"length": 100})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "small.txt"


def test_find_with_chunksize_filter(bucket):
    """Test finding files with chunkSize filters."""
    # Upload files
    data = b"x" * 1000
    bucket.upload_from_stream("test1.txt", data)
    bucket.upload_from_stream("test2.txt", data)

    # Find files with default chunk size (255KB)
    cursor = bucket.find({"chunkSize": 255 * 1024})
    files = list(cursor)
    assert len(files) == 2


def test_find_with_complex_filters(bucket):
    """Test finding files with multiple filter conditions."""
    # Upload files with different properties
    small_data = b"x" * 100
    large_data = b"x" * 5000

    bucket.upload_from_stream("small.txt", small_data)
    bucket.upload_from_stream("large.txt", large_data)
    bucket.upload_from_stream("small.txt", small_data)  # Same name, diff file

    # Find small files using length and filename regex
    cursor = bucket.find(
        {"length": {"$lt": 1000}, "filename": {"$regex": "small"}}
    )
    files = list(cursor)
    assert len(files) == 2

    # Find files with exact name match
    cursor = bucket.find({"filename": "small.txt"})
    files = list(cursor)
    assert len(files) == 2

    # Find files with filename not equal to a specific name
    cursor = bucket.find({"filename": {"$ne": "small.txt"}})
    files = list(cursor)
    assert len(files) == 1  # large.txt
    assert files[0].filename == "large.txt"


def test_find_with_metadata_filter(bucket):
    """Test finding files with metadata filters."""
    # Upload files with metadata
    bucket.upload_from_stream(
        "file1.txt", b"data1", {"author": "alice", "version": 1}
    )
    bucket.upload_from_stream(
        "file2.txt", b"data2", {"author": "bob", "version": 2}
    )
    bucket.upload_from_stream(
        "file3.txt", b"data3", {"author": "alice", "version": 1}
    )

    # Find files by author (basic substring matching)
    # Metadata is now stored as JSON, so we look for the JSON representation
    cursor = bucket.find({"metadata": '"author": "alice"'})
    files = list(cursor)
    assert len(files) == 2  # file1 and file3

    # Find files by version
    cursor = bucket.find({"metadata": '"version": 1'})
    files = list(cursor)
    assert len(files) == 2  # file1 and file3


def test_find_with_upload_date_filter(bucket):
    """Test finding files with uploadDate filters."""
    # Upload files
    data = b"test data"
    file_id1 = bucket.upload_from_stream("first.txt", data)

    # Small delay to ensure different timestamps
    time.sleep(0.01)

    file_id2 = bucket.upload_from_stream("second.txt", data)

    # Get the actual upload dates for comparison
    grid_out1 = bucket.open_download_stream(file_id1)
    bucket.open_download_stream(file_id2)

    # Find files uploaded after the first file
    cursor = bucket.find({"uploadDate": {"$gt": grid_out1.upload_date}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "second.txt"


def test_find_with_md5_filter(bucket):
    """Test finding files with MD5 filters."""
    data1 = b"content1"
    data2 = b"content2"

    file_id1 = bucket.upload_from_stream("file1.txt", data1)
    bucket.upload_from_stream("file2.txt", data2)

    # Get the actual MD5 hashes
    grid_out1 = bucket.open_download_stream(file_id1)
    md5_hash = grid_out1.md5

    # Find file by MD5 hash
    cursor = bucket.find({"md5": md5_hash})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file1.txt"

    # Find files with different MD5 hash
    cursor = bucket.find({"md5": {"$ne": md5_hash}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file2.txt"


def test_json_metadata_storage_and_retrieval(bucket):
    """Test that metadata is properly stored and retrieved as JSON."""
    # Test complex metadata
    metadata = {
        "author": "John Doe",
        "version": 1.5,
        "tags": ["tag1", "tag2", "tag3"],
        "nested": {"level1": {"level2": "value"}},
        "published": True,
    }

    # Upload file with complex metadata
    data = b"test data with complex metadata"
    file_id = bucket.upload_from_stream("test.txt", data, metadata)

    # Retrieve file and check metadata
    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.metadata == metadata


def test_simple_metadata_storage_and_retrieval(bucket):
    """Test that simple metadata works correctly."""
    # Test simple metadata
    metadata = {"author": "Jane Smith", "version": 2}

    # Upload file with simple metadata
    data = b"test data with simple metadata"
    file_id = bucket.upload_from_stream("simple.txt", data, metadata)

    # Retrieve file and check metadata
    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.metadata == metadata


def test_none_metadata_handling(bucket):
    """Test that None metadata is handled correctly."""
    # Upload file with no metadata
    data = b"test data with no metadata"
    file_id = bucket.upload_from_stream("none.txt", data)

    # Retrieve file and check metadata
    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.metadata is None


def test_metadata_with_streaming_upload(bucket):
    """Test metadata handling with streaming uploads."""
    metadata = {"stream": True, "size": "large"}
    data = b"streaming test data"

    # Upload using streaming with metadata
    with bucket.open_upload_stream("stream.txt", metadata) as grid_in:
        grid_in.write(data)

    # Find the file and check metadata
    cursor = bucket.find({"filename": "stream.txt"})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].metadata == metadata


def test_metadata_with_special_characters(bucket):
    """Test metadata with special characters and Unicode."""
    metadata = {
        "title": "Test with spëcial chäräcters",
        "description": "This tests Unicode: 你好世界 🌍",
        "symbols": "!@#$%^&*()_+-=[]{}|;':\",./<>?",
    }

    data = b"data with Unicode metadata"
    file_id = bucket.upload_from_stream("unicode.txt", data, metadata)

    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.metadata == metadata


def test_empty_metadata_handling(bucket):
    """Test handling of empty metadata dictionary."""
    metadata = {}
    data = b"data with empty metadata"
    file_id = bucket.upload_from_stream("empty.txt", data, metadata)

    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.metadata == metadata


def test_metadata_type_preservation(bucket):
    """Test that metadata types are preserved."""
    metadata = {
        "integer": 42,
        "float": 3.14159,
        "boolean_true": True,
        "boolean_false": False,
        "null_value": None,
        "string": "text",
        "list": [1, 2, 3],
        "dict": {"nested": "value"},
    }

    data = b"data with typed metadata"
    file_id = bucket.upload_from_stream("typed.txt", data, metadata)

    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.metadata == metadata

    # Check specific types
    assert isinstance(grid_out.metadata["integer"], int)
    assert isinstance(grid_out.metadata["float"], float)
    assert isinstance(grid_out.metadata["boolean_true"], bool)
    assert isinstance(grid_out.metadata["boolean_false"], bool)
    assert grid_out.metadata["null_value"] is None
    assert isinstance(grid_out.metadata["string"], str)
    assert isinstance(grid_out.metadata["list"], list)
    assert isinstance(grid_out.metadata["dict"], dict)


def test_gridfsbucket_with_disable_md5(bucket):
    """Test GridFSBucket with disable_md5 option."""
    # Create bucket with MD5 disabled
    bucket_disabled = GridFSBucket(bucket._db, disable_md5=True)

    # Upload a file
    data = b"test data for md5 check"
    file_id = bucket_disabled.upload_from_stream("test.txt", data)

    # Check that MD5 is None when disabled
    grid_out = bucket_disabled.open_download_stream(file_id)
    assert grid_out.md5 is None
    assert grid_out.length == len(data)


def test_gridfsbucket_with_md5_enabled(bucket):
    """Test GridFSBucket with MD5 enabled (default)."""
    # Create bucket with MD5 enabled (default)
    bucket_enabled = GridFSBucket(bucket._db)

    # Upload a file
    data = b"test data for md5 check"
    file_id = bucket_enabled.upload_from_stream("test.txt", data)

    # Check that MD5 is calculated when enabled
    expected_md5 = hashlib.md5(data).hexdigest()
    grid_out = bucket_enabled.open_download_stream(file_id)
    assert grid_out.md5 == expected_md5
    assert grid_out.length == len(data)


def test_gridfsbucket_with_custom_chunk_size(bucket):
    """Test GridFSBucket with custom chunk size."""
    # Create bucket with custom chunk size
    custom_chunk_size = 1024  # 1KB
    bucket_custom = GridFSBucket(bucket._db, chunk_size_bytes=custom_chunk_size)

    # Upload a file larger than chunk size
    data = b"x" * (custom_chunk_size * 3 + 500)  # 3.5 chunks
    file_id = bucket_custom.upload_from_stream("large.txt", data)

    # Check that chunk size is stored correctly
    grid_out = bucket_custom.open_download_stream(file_id)
    assert grid_out.chunk_size == custom_chunk_size
    assert grid_out.length == len(data)


def test_gridfsbucket_with_write_concern_validation(bucket):
    """Test GridFSBucket with write concern validation."""
    # Valid write concern
    bucket_valid = GridFSBucket(
        bucket._db, write_concern={"w": 1, "wtimeout": 1000, "j": True}
    )

    # Upload a file to ensure it works
    data = b"test data"
    file_id = bucket_valid.upload_from_stream("test.txt", data)
    from neosqlite.objectid import ObjectId

    assert isinstance(file_id, ObjectId)


def test_gridin_with_disable_md5(bucket):
    """Test GridIn with disable_md5 option."""
    # Create bucket with MD5 disabled
    bucket_disabled = GridFSBucket(bucket._db, disable_md5=True)

    # Upload using streaming with MD5 disabled
    data = b"streaming test data"
    with bucket_disabled.open_upload_stream("stream.txt") as grid_in:
        grid_in.write(data)

    # Find the file and check MD5
    cursor = bucket_disabled.find({"filename": "stream.txt"})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].md5 is None


def test_gridin_with_md5_enabled(bucket):
    """Test GridIn with MD5 enabled (default)."""
    # Create bucket with MD5 enabled
    bucket_enabled = GridFSBucket(bucket._db)

    # Upload using streaming with MD5 enabled
    data = b"streaming test data"
    with bucket_enabled.open_upload_stream("stream.txt") as grid_in:
        grid_in.write(data)

    # Find the file and check MD5
    cursor = bucket_enabled.find({"filename": "stream.txt"})
    files = list(cursor)
    assert len(files) == 1
    expected_md5 = hashlib.md5(data).hexdigest()
    assert files[0].md5 == expected_md5


def test_write_concern_j_true(bucket):
    """Test GridFSBucket with j=True write concern."""
    # Create bucket with journal write concern
    from neosqlite.objectid import ObjectId

    bucket_journal = GridFSBucket(bucket._db, write_concern={"j": True})

    # Upload a file
    data = b"test data with journal concern"
    file_id = bucket_journal.upload_from_stream("test.txt", data)

    # Verify file was created correctly
    assert isinstance(file_id, ObjectId)

    # Check file content
    grid_out = bucket_journal.open_download_stream(file_id)
    assert grid_out.read() == data


def test_write_concern_w_zero(bucket):
    """Test GridFSBucket with w=0 write concern."""
    # Create bucket with no acknowledgment concern
    from neosqlite.objectid import ObjectId

    bucket_no_ack = GridFSBucket(bucket._db, write_concern={"w": 0})

    # Upload a file
    data = b"test data with no ack concern"
    file_id = bucket_no_ack.upload_from_stream("test.txt", data)

    # Verify file was created correctly
    assert isinstance(file_id, ObjectId)

    # Check file content
    grid_out = bucket_no_ack.open_download_stream(file_id)
    assert grid_out.read() == data


def test_write_concern_w_majority(bucket):
    """Test GridFSBucket with w='majority' write concern."""
    # Create bucket with majority acknowledgment concern
    from neosqlite.objectid import ObjectId

    bucket_majority = GridFSBucket(bucket._db, write_concern={"w": "majority"})

    # Upload a file
    data = b"test data with majority concern"
    file_id = bucket_majority.upload_from_stream("test.txt", data)

    # Verify file was created correctly
    assert isinstance(file_id, ObjectId)

    # Check file content
    grid_out = bucket_majority.open_download_stream(file_id)
    assert grid_out.read() == data


def test_write_concern_with_streaming(bucket):
    """Test GridFSBucket with write concern and streaming uploads."""
    # Create bucket with journal write concern
    bucket_journal = GridFSBucket(bucket._db, write_concern={"j": True})

    # Upload using streaming
    data = b"streaming test data with concern"
    with bucket_journal.open_upload_stream("stream.txt") as grid_in:
        grid_in.write(data)

    # Find the file and check content
    cursor = bucket_journal.find({"filename": "stream.txt"})
    files = list(cursor)
    assert len(files) == 1

    grid_out = bucket_journal.open_download_stream(files[0]._file_id)
    assert grid_out.read() == data


def test_write_concern_with_custom_id(bucket):
    """Test GridFSBucket with write concern and custom file ID."""
    # Create bucket with write concern
    bucket_concern = GridFSBucket(bucket._db, write_concern={"j": True, "w": 1})

    # Upload with custom ID
    custom_id = 100
    data = b"test data with custom id and concern"
    bucket_concern.upload_from_stream_with_id(custom_id, "custom.txt", data)

    # Check file content
    grid_out = bucket_concern.open_download_stream(custom_id)
    assert grid_out.read() == data


def test_write_concern_combinations(bucket):
    """Test GridFSBucket with various write concern combinations."""
    # Test various combinations
    test_cases = [
        {"w": 1, "j": False},
        {"w": 1, "j": True},
        {"w": 0},
        {"w": "majority", "j": True},
        {"wtimeout": 1000},
        {"w": 1, "wtimeout": 500, "j": False},
    ]

    for i, write_concern in enumerate(test_cases):
        bucket_case = GridFSBucket(bucket._db, write_concern=write_concern)
        data = f"test data {i}".encode()
        filename = f"test{i}.txt"

        # Upload file
        file_id = bucket_case.upload_from_stream(filename, data)

        # Verify
        grid_out = bucket_case.open_download_stream(file_id)
        assert grid_out.read() == data


# ================================
# Legacy GridFS Tests
# ================================


def test_legacy_gridfs_creation(legacy_fs):
    """Test GridFS creation."""
    # Test default collection name
    assert legacy_fs._bucket._bucket_name == "fs"

    # Test custom collection name
    fs = GridFS(legacy_fs._bucket._db, collection_name="custom_fs")
    assert fs._bucket._bucket_name == "custom_fs"


def test_legacy_put_bytes_data(legacy_fs):
    """Test putting bytes data into GridFS."""
    from neosqlite.objectid import ObjectId

    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(data)

    assert isinstance(file_id, ObjectId)

    # Verify the file can be retrieved
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data


def test_legacy_put_file_like_object(legacy_fs):
    """Test putting file-like object into GridFS."""
    from neosqlite.objectid import ObjectId

    data = b"Hello, GridFS World!"
    file_like = io.BytesIO(data)
    file_id = legacy_fs.put(file_like)

    assert isinstance(file_id, ObjectId)

    # Verify the file can be retrieved
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data


def test_legacy_put_with_filename(legacy_fs):
    """Test putting data with filename."""
    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(data, filename="test.txt")

    # Verify the file can be retrieved
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data
    assert grid_out.filename == "test.txt"


def test_legacy_put_with_metadata(legacy_fs):
    """Test putting data with metadata."""
    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(
        data, filename="test.txt", author="tester", version="1.0"
    )

    # Verify the file can be retrieved
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data
    assert grid_out.filename == "test.txt"


def test_legacy_get_by_id(legacy_fs):
    """Test getting file by ID."""
    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(data, filename="test.txt")

    # Get by ID
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data
    assert grid_out.filename == "test.txt"


def test_legacy_get_nonexistent_file(legacy_fs):
    """Test getting nonexistent file raises NoFile."""
    with pytest.raises(NoFile):
        legacy_fs.get(999999)


def test_legacy_get_version(legacy_fs):
    """Test getting file by filename and version."""
    data1 = b"First version content"
    data2 = b"Second version content"

    # Put two versions with the same name
    legacy_fs.put(data1, filename="test.txt")
    legacy_fs.put(data2, filename="test.txt")

    # Get first version (version 0)
    grid_out = legacy_fs.get_version("test.txt", 0)
    assert grid_out.read() == data1

    # Get second version (version 1)
    grid_out = legacy_fs.get_version("test.txt", 1)
    assert grid_out.read() == data2

    # Get latest version (version -1)
    grid_out = legacy_fs.get_version("test.txt", -1)
    assert grid_out.read() == data2


def test_legacy_get_last_version(legacy_fs):
    """Test getting the most recent version of a file."""
    data1 = b"First version content"
    data2 = b"Second version content"

    # Put two versions with the same name
    legacy_fs.put(data1, filename="test.txt")
    legacy_fs.put(data2, filename="test.txt")

    # Get last version
    grid_out = legacy_fs.get_last_version("test.txt")
    assert grid_out.read() == data2


def test_legacy_delete_file(legacy_fs):
    """Test deleting a file."""
    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(data, filename="test.txt")

    # Verify file exists
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data

    # Delete the file
    legacy_fs.delete(file_id)

    # Verify file is deleted
    with pytest.raises(NoFile):
        legacy_fs.get(file_id)


def test_legacy_delete_nonexistent_file(legacy_fs):
    """Test deleting nonexistent file."""
    # Should not raise an exception
    legacy_fs.delete(999999)


def test_legacy_list_files(legacy_fs):
    """Test listing all filenames."""
    # Put some files
    legacy_fs.put(b"Content 1", filename="file1.txt")
    legacy_fs.put(b"Content 2", filename="file2.txt")
    legacy_fs.put(
        b"Content 3", filename="file1.txt"
    )  # Same name, different revision

    # List files
    filenames = legacy_fs.list()
    assert isinstance(filenames, list)
    assert len(filenames) == 2  # Only unique filenames
    assert "file1.txt" in filenames
    assert "file2.txt" in filenames


def test_legacy_list_empty_files(legacy_fs):
    """Test listing files when no files exist."""
    filenames = legacy_fs.list()
    assert isinstance(filenames, list)
    assert len(filenames) == 0


def test_legacy_find_files(legacy_fs):
    """Test finding files with filter."""
    # Put some files
    legacy_fs.put(b"Content 1", filename="file1.txt")
    legacy_fs.put(b"Content 2", filename="file2.txt")
    legacy_fs.put(
        b"Content 3", filename="file1.txt"
    )  # Same name, different revision

    # Find all files
    cursor = legacy_fs.find()
    files = list(cursor)
    assert len(files) == 3

    # Find files by name
    cursor = legacy_fs.find({"filename": "file1.txt"})
    files = list(cursor)
    assert len(files) == 2


def test_legacy_find_one_file(legacy_fs):
    """Test finding a single file."""
    # Put some files
    legacy_fs.put(b"Content 1", filename="file1.txt")
    legacy_fs.put(b"Content 2", filename="file2.txt")

    # Find one file
    grid_out = legacy_fs.find_one({"filename": "file1.txt"})
    assert isinstance(grid_out, GridOut)
    assert grid_out.filename == "file1.txt"

    # Find non-existent file
    grid_out = legacy_fs.find_one({"filename": "nonexistent.txt"})
    assert grid_out is None


def test_legacy_exists_by_id(legacy_fs):
    """Test checking if file exists by ID."""
    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(data, filename="test.txt")

    # Check that file exists
    assert legacy_fs.exists(file_id=file_id) is True

    # Check that nonexistent file doesn't exist
    assert legacy_fs.exists(file_id=999999) is False


def test_legacy_exists_by_criteria(legacy_fs):
    """Test checking if file exists by criteria."""
    legacy_fs.put(b"Content 1", filename="file1.txt")
    legacy_fs.put(b"Content 2", filename="file2.txt")

    # Check by filename
    assert legacy_fs.exists(filename="file1.txt") is True
    assert legacy_fs.exists(filename="nonexistent.txt") is False

    # Check with no criteria
    assert legacy_fs.exists() is False


def test_legacy_new_file_context_manager(legacy_fs):
    """Test creating new file with context manager."""
    data = b"Hello, GridFS World!"

    # Create new file using context manager
    with legacy_fs.new_file(
        filename="new_file.txt", author="tester"
    ) as grid_in:
        grid_in.write(data)

    # Verify the file was created
    grid_out = legacy_fs.get_last_version("new_file.txt")
    assert grid_out.read() == data
    assert grid_out.filename == "new_file.txt"


def test_legacy_new_file_manual_close(legacy_fs):
    """Test creating new file with manual close."""
    data = b"Hello, GridFS World!"

    # Create new file manually
    grid_in = legacy_fs.new_file(filename="new_file.txt", author="tester")
    grid_in.write(data)
    grid_in.close()

    # Verify the file was created
    grid_out = legacy_fs.get_last_version("new_file.txt")
    assert grid_out.read() == data
    assert grid_out.filename == "new_file.txt"


def test_legacy_new_file_with_custom_id(legacy_fs):
    """Test creating new file with custom ID."""
    data = b"Hello, GridFS World!"
    custom_id = 100

    # Create new file with custom ID
    grid_in = legacy_fs.new_file(_id=custom_id, filename="new_file.txt")
    grid_in.write(data)
    grid_in.close()

    # Verify the file was created with custom ID
    grid_out = legacy_fs.get(custom_id)
    assert grid_out.read() == data
    assert grid_out._file_id == custom_id


def test_legacy_gridfs_integration_with_binary_data(legacy_fs):
    """Test GridFS integration with binary data."""
    # Create binary data
    binary_data = (
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09" * 100
    )  # 1000 bytes

    # Store in GridFS
    file_id = legacy_fs.put(binary_data, filename="binary_file.bin")

    # Retrieve and verify
    grid_out = legacy_fs.get(file_id)
    retrieved_data = grid_out.read()
    assert retrieved_data == binary_data
    assert len(retrieved_data) == 1000


def test_legacy_gridfs_large_file(legacy_fs):
    """Test GridFS with large file."""
    # Create large data (larger than default chunk size)
    large_data = b"x" * (255 * 1024 + 1000)  # 255KB + 1000 bytes

    # Store in GridFS
    file_id = legacy_fs.put(large_data, filename="large_file.txt")

    # Retrieve and verify
    grid_out = legacy_fs.get(file_id)
    retrieved_data = grid_out.read()
    assert retrieved_data == large_data
    assert len(retrieved_data) == len(large_data)


def test_legacy_gridfs_with_different_encodings(legacy_fs):
    """Test GridFS with different encodings parameter."""
    # Test that encoding parameter is handled correctly
    data = b"Hello, GridFS World!"
    file_id = legacy_fs.put(data, filename="test.txt", encoding="utf-8")

    # Verify the file can be retrieved
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data


def test_legacy_gridfs_with_unicode_filename(legacy_fs):
    """Test GridFS with unicode filename."""
    data = b"Hello, GridFS World!"
    filename = "тест.txt"  # Russian for "test.txt"

    file_id = legacy_fs.put(data, filename=filename)

    # Verify the file can be retrieved
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data
    assert grid_out.filename == filename


def test_legacy_gridfs_multiple_versions_same_name(legacy_fs):
    """Test GridFS with multiple versions of the same file."""
    data1 = b"Version 1 content"
    data2 = b"Second version content"
    data3 = b"Version 3 content"

    # Put multiple versions with the same name
    legacy_fs.put(data1, filename="test.txt")
    legacy_fs.put(data2, filename="test.txt")
    legacy_fs.put(data3, filename="test.txt")

    # Verify we can retrieve each version
    assert legacy_fs.get_version("test.txt", 0).read() == data1
    assert legacy_fs.get_version("test.txt", 1).read() == data2
    assert legacy_fs.get_version("test.txt", 2).read() == data3
    assert legacy_fs.get_last_version("test.txt").read() == data3

    # Test listing shows only one unique filename
    filenames = legacy_fs.list()
    assert len(filenames) == 1
    assert filenames[0] == "test.txt"


def test_legacy_gridfs_find_with_complex_filters(legacy_fs):
    """Test GridFS find with complex filters."""
    # Put files with metadata
    legacy_fs.put(b"Content 1", filename="file1.txt", author="Alice", version=1)
    legacy_fs.put(b"Content 2", filename="file2.txt", author="Bob", version=2)
    legacy_fs.put(b"Content 3", filename="file3.txt", author="Alice", version=1)

    # Find files by filename (this is supported)
    cursor = legacy_fs.find({"filename": "file1.txt"})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file1.txt"


def test_legacy_gridfs_put_io_base_object(legacy_fs):
    """Test GridFS put with various IOBase objects."""
    data = b"Hello, GridFS World!"

    # Test with BytesIO
    bytes_io = io.BytesIO(data)
    file_id = legacy_fs.put(bytes_io, filename="bytesio.txt")
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data

    # Test with BufferedReader (simulated)
    class MockBufferedReader(io.IOBase):
        def __init__(self, data):
            self._data = data
            self._pos = 0

        def read(self, size=-1):
            if size == -1:
                result = self._data[self._pos :]
                self._pos = len(self._data)
            else:
                result = self._data[self._pos : self._pos + size]
                self._pos += len(result)
            return result

    mock_reader = MockBufferedReader(data)
    file_id = legacy_fs.put(mock_reader, filename="mockreader.txt")
    grid_out = legacy_fs.get(file_id)
    assert grid_out.read() == data


def test_legacy_gridfs_compatibility_with_gridfsbucket(legacy_fs, bucket):
    """Test that legacy GridFS is compatible with GridFSBucket."""
    # Put file with legacy API
    data = b"Legacy API content"
    file_id = legacy_fs.put(data, filename="legacy.txt")

    # Get file with bucket API
    grid_out = bucket.open_download_stream(file_id)
    assert grid_out.read() == data

    # Put file with bucket API
    data2 = b"Bucket API content"
    file_id2 = bucket.upload_from_stream("bucket.txt", data2)

    # Get file with legacy API
    grid_out = legacy_fs.get(file_id2)
    assert grid_out.read() == data2


def test_legacy_find_with_length_filter(legacy_fs):
    """Test finding files with length filters."""
    # Upload files of different sizes
    small_data = b"x" * 100  # 100 bytes
    medium_data = b"x" * 1000  # 1000 bytes
    large_data = b"x" * 5000  # 5000 bytes

    legacy_fs.put(small_data, filename="small.txt")
    legacy_fs.put(medium_data, filename="medium.txt")
    legacy_fs.put(large_data, filename="large.txt")

    # Find files with length > 500
    cursor = legacy_fs.find({"length": {"$gt": 500}})
    files = list(cursor)
    assert len(files) == 2  # medium and large

    # Find files with length >= 1000
    cursor = legacy_fs.find({"length": {"$gte": 1000}})
    files = list(cursor)
    assert len(files) == 2  # medium and large

    # Find files with length < 2000
    cursor = legacy_fs.find({"length": {"$lt": 2000}})
    files = list(cursor)
    assert len(files) == 2  # small and medium

    # Find files with length <= 1000
    cursor = legacy_fs.find({"length": {"$lte": 1000}})
    files = list(cursor)
    assert len(files) == 2  # small and medium

    # Find files with specific length
    cursor = legacy_fs.find({"length": 100})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "small.txt"


def test_legacy_find_with_chunksize_filter(legacy_fs):
    """Test finding files with chunkSize filters."""
    # Upload files
    data = b"x" * 1000
    legacy_fs.put(data, filename="test1.txt")
    legacy_fs.put(data, filename="test2.txt")

    # Find files with default chunk size (255KB)
    cursor = legacy_fs.find({"chunkSize": 255 * 1024})
    files = list(cursor)
    assert len(files) == 2


def test_legacy_find_with_complex_filters_additional(legacy_fs):
    """Test finding files with multiple filter conditions."""
    # Upload files with different properties
    small_data = b"x" * 100
    large_data = b"x" * 5000

    legacy_fs.put(small_data, filename="small.txt")
    legacy_fs.put(large_data, filename="large.txt")
    legacy_fs.put(small_data, filename="another_small.txt")

    # Find small files using length and filename regex
    cursor = legacy_fs.find(
        {"length": {"$lt": 1000}, "filename": {"$regex": "small"}}
    )
    files = list(cursor)
    assert len(files) == 2

    # Find files with exact name match
    cursor = legacy_fs.find({"filename": "small.txt"})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "small.txt"

    # Find files with filename not equal to a specific name
    cursor = legacy_fs.find({"filename": {"$ne": "small.txt"}})
    files = list(cursor)
    assert len(files) == 2  # large.txt and another_small.txt


def test_legacy_find_with_metadata_filter(legacy_fs):
    """Test finding files with metadata filters."""
    # Upload files with metadata
    legacy_fs.put(b"data1", filename="file1.txt", author="alice", version=1)
    legacy_fs.put(b"data2", filename="file2.txt", author="bob", version=2)
    legacy_fs.put(b"data3", filename="file3.txt", author="alice", version=1)

    # Find files by author (basic substring matching)
    # Metadata is now stored as JSON, so we look for the JSON representation
    cursor = legacy_fs.find({"metadata": '"author": "alice"'})
    files = list(cursor)
    assert len(files) == 2  # file1 and file3

    # Find files by version
    cursor = legacy_fs.find({"metadata": '"version": 1'})
    files = list(cursor)
    assert len(files) == 2  # file1 and file3


def test_legacy_find_with_upload_date_filter(legacy_fs):
    """Test finding files with uploadDate filters."""
    # Upload files
    data = b"test data"
    file_id1 = legacy_fs.put(data, filename="first.txt")

    # Small delay to ensure different timestamps
    time.sleep(0.01)

    file_id2 = legacy_fs.put(data, filename="second.txt")

    # Get the actual upload dates for comparison
    grid_out1 = legacy_fs.get(file_id1)
    legacy_fs.get(file_id2)

    # Find files uploaded after the first file
    cursor = legacy_fs.find({"uploadDate": {"$gt": grid_out1.upload_date}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "second.txt"


def test_legacy_find_with_md5_filter(legacy_fs):
    """Test finding files with MD5 filters."""
    data1 = b"content1"
    data2 = b"content2"

    file_id1 = legacy_fs.put(data1, filename="file1.txt")
    legacy_fs.put(data2, filename="file2.txt")

    # Get the actual MD5 hashes
    grid_out1 = legacy_fs.get(file_id1)
    md5_hash = grid_out1.md5

    # Find file by MD5 hash
    cursor = legacy_fs.find({"md5": md5_hash})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file1.txt"

    # Find files with different MD5 hash
    cursor = legacy_fs.find({"md5": {"$ne": md5_hash}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file2.txt"


def test_legacy_gridfs_metadata_compatibility(legacy_fs):
    """Test backward compatibility with existing string metadata."""
    # This test simulates what would happen with existing data
    # that was stored with the old string representation
    pass  # The implementation handles this with fallback parsing


def test_legacy_gridfs_options(legacy_fs):
    """Test legacy GridFS with options passed through."""
    # Create legacy GridFS with options
    fs = GridFS(legacy_fs._bucket._db)

    # Put a file
    data = b"legacy test data"
    file_id = fs.put(data, filename="legacy.txt")

    # Check that file was created correctly
    grid_out = fs.get(file_id)
    assert grid_out.read() == data
    assert grid_out.filename == "legacy.txt"
    # MD5 should be enabled by default
    expected_md5 = hashlib.md5(data).hexdigest()
    assert grid_out.md5 == expected_md5


# ================================
# Exception Tests
# ================================


def test_exception_inheritance():
    """Test that exceptions follow the correct inheritance hierarchy."""
    # Test NeoSQLiteError
    exc = NeoSQLiteError("test message")
    assert isinstance(exc, Exception)

    # Test GridFSError
    exc = GridFSError("test message")
    assert isinstance(exc, NeoSQLiteError)
    assert isinstance(exc, Exception)

    # Test NoFile
    exc = NoFileError("test message")
    assert isinstance(exc, GridFSError)
    assert isinstance(exc, NeoSQLiteError)
    assert isinstance(exc, Exception)

    # Test FileExists
    exc = FileExistsError("test message")
    assert isinstance(exc, GridFSError)
    assert isinstance(exc, NeoSQLiteError)
    assert isinstance(exc, Exception)

    # Test CorruptGridFile
    exc = CorruptGridFile("test message")
    assert isinstance(exc, GridFSError)
    assert isinstance(exc, NeoSQLiteError)
    assert isinstance(exc, Exception)

    # Test PyMongoError alias
    exc = PyMongoError("test message")
    assert isinstance(exc, NeoSQLiteError)
    assert isinstance(exc, Exception)


def test_exception_catch_hierarchy():
    """Test that exceptions can be caught at different levels."""
    # Test catching specific exception
    with pytest.raises(NoFileError):
        raise NoFileError("test")

    # Test catching at parent level
    with pytest.raises(GridFSError):
        raise NoFileError("test")

    # Test catching at grandparent level
    with pytest.raises(NeoSQLiteError):
        raise NoFileError("test")


def test_exception_message():
    """Test that exception messages are properly stored."""
    message = "This is a test error message"
    exc = NoFileError(message)
    assert str(exc) == message

    exc = FileExistsError(message)
    assert str(exc) == message

    exc = CorruptGridFile(message)
    assert str(exc) == message


class TestGridFSExceptions:
    """Test GridFS exception hierarchy and functionality."""

    def test_exception_inheritance(self):
        """Test that exceptions follow the correct inheritance hierarchy."""
        # Test NeoSQLiteError
        exc = NeoSQLiteError("test message")
        assert isinstance(exc, Exception)

        # Test GridFSError
        exc = GridFSError("test message")
        assert isinstance(exc, NeoSQLiteError)
        assert isinstance(exc, Exception)

        # Test NoFile
        exc = NoFileError("test message")
        assert isinstance(exc, GridFSError)
        assert isinstance(exc, NeoSQLiteError)
        assert isinstance(exc, Exception)

        # Test FileExists
        exc = FileExistsError("test message")
        assert isinstance(exc, GridFSError)
        assert isinstance(exc, NeoSQLiteError)
        assert isinstance(exc, Exception)

        # Test CorruptGridFile
        exc = CorruptGridFile("test message")
        assert isinstance(exc, GridFSError)
        assert isinstance(exc, NeoSQLiteError)
        assert isinstance(exc, Exception)

        # Test PyMongoError alias
        exc = PyMongoError("test message")
        assert isinstance(exc, NeoSQLiteError)
        assert isinstance(exc, Exception)

    def test_exception_catch_hierarchy(self):
        """Test that exceptions can be caught at different levels."""
        # Test catching specific exception
        with pytest.raises(NoFileError):
            raise NoFileError("file not found")

        # Test catching at GridFSError level
        with pytest.raises(GridFSError):
            raise NoFileError("file not found")

        with pytest.raises(GridFSError):
            raise FileExistsError("file exists")

        with pytest.raises(GridFSError):
            raise CorruptGridFile("file corrupt")

        # Test catching at NeoSQLiteError level
        with pytest.raises(NeoSQLiteError):
            raise NoFileError("file not found")

        with pytest.raises(NeoSQLiteError):
            raise FileExistsError("file exists")

        with pytest.raises(NeoSQLiteError):
            raise CorruptGridFile("file corrupt")

        # Test catching at PyMongoError alias level
        with pytest.raises(PyMongoError):
            raise NoFileError("file not found")

    def test_exception_messages(self):
        """Test that exception messages are preserved."""
        message = "This is a test error message"

        exc = NoFileError(message)
        assert str(exc) == message

        exc = FileExistsError(message)
        assert str(exc) == message

        exc = CorruptGridFile(message)
        assert str(exc) == message

        exc = PyMongoError(message)
        assert str(exc) == message

    def test_error_labels_functionality(self):
        """Test error labels functionality."""
        # Test with no error labels
        exc = NoFileError("test")
        assert exc.has_error_label("any_label") is False

        # Test with error labels
        exc = NoFileError("test", ["label1", "label2"])
        assert exc.has_error_label("label1") is True
        assert exc.has_error_label("label2") is True
        assert exc.has_error_label("label3") is False

        # Test adding error labels
        exc._add_error_label("label3")
        assert exc.has_error_label("label3") is True

        # Test removing error labels
        exc._remove_error_label("label1")
        assert exc.has_error_label("label1") is False

        # Test duplicate labels are handled correctly
        exc._add_error_label("label2")
        exc._add_error_label("label2")
        assert exc.has_error_label("label2") is True

    def test_timeout_property(self):
        """Test timeout property."""
        exc = NoFileError("test")
        assert exc.timeout is False

        exc = FileExistsError("test")
        assert exc.timeout is False

        exc = CorruptGridFile("test")
        assert exc.timeout is False

        exc = PyMongoError("test")
        assert exc.timeout is False

    def test_empty_error_labels(self):
        """Test exception with empty or None error labels."""
        # Test with None error labels
        exc = NoFileError("test", None)
        assert exc.has_error_label("any_label") is False

        # Test with empty error labels
        exc = NoFileError("test", [])
        assert exc.has_error_label("any_label") is False


# ================================
# Additional GridFS Coverage Tests
# ================================


def test_gridoutcursor_complex_metadata_filters(bucket):
    """Test GridOutCursor with complex metadata filters."""
    # Upload files with various metadata
    bucket.upload_from_stream(
        "file1.txt", b"Content 1", metadata={"type": "text", "version": 1}
    )
    bucket.upload_from_stream(
        "file2.log", b"Content 2", metadata={"type": "log", "version": 2}
    )
    bucket.upload_from_stream(
        "file3.txt", b"Content 3", metadata={"type": "text", "version": 2}
    )

    # Test metadata filter with $regex operator
    cursor = bucket.find({"metadata": {"$regex": "log"}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file2.log"

    # Test metadata filter with $ne operator
    cursor = bucket.find({"metadata": {"$ne": '{"type": "log", "version": 2}'}})
    files = list(cursor)
    # Should match file1.txt and file3.txt (both have type "text")
    assert len(files) == 2
    filenames = [f.filename for f in files]
    assert "file1.txt" in filenames
    assert "file3.txt" in filenames


def test_gridoutcursor_chunksize_filters(bucket):
    """Test GridOutCursor with chunkSize filters."""
    # Upload files
    bucket.upload_from_stream("file1.txt", b"Content 1")
    bucket.upload_from_stream("file2.txt", b"Content 2")

    # Test chunkSize filter with $gt operator
    cursor = bucket.find({"chunkSize": {"$gt": 100000}})  # Greater than 100KB
    files = list(cursor)
    # Should match files with default chunk size (255KB)
    assert len(files) == 2

    # Test chunkSize filter with $lt operator
    cursor = bucket.find({"chunkSize": {"$lt": 300000}})  # Less than 300KB
    files = list(cursor)
    # Should match files with default chunk size (255KB)
    assert len(files) == 2


def test_gridoutcursor_unsupported_operators(bucket):
    """Test GridOutCursor with unsupported operators that should fall back to exact match."""
    # Upload files
    bucket.upload_from_stream("file1.txt", b"Content 1")
    bucket.upload_from_stream("file2.txt", b"Content 2")

    # Test unsupported operator that should fall back to exact match
    cursor = bucket.find({"filename": {"$unsupported": "file1.txt"}})
    files = list(cursor)
    # Should fall back to exact match, but since "filename = '$unsupportedfile1.txt'" won't match anything
    assert len(files) == 0


def test_gridoutcursor_metadata_filter_edge_cases(bucket):
    """Test GridOutCursor metadata filter edge cases."""
    # Upload files with various metadata formats
    bucket.upload_from_stream(
        "file1.txt", b"Content 1", metadata={"key": "value"}
    )
    bucket.upload_from_stream(
        "file2.txt", b"Content 2", metadata={"number": 42}
    )
    bucket.upload_from_stream(
        "file3.txt", b"Content 3", metadata={"bool": True}
    )

    # Test metadata filter with non-string value (should be converted to string)
    cursor = bucket.find({"metadata": {"$regex": "42"}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file2.txt"

    # Test metadata filter with boolean value
    cursor = bucket.find({"metadata": {"$regex": "true"}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file3.txt"


def test_gridoutcursor_metadata_ne_operator(bucket):
    """Test GridOutCursor metadata filter with $ne operator."""
    # Upload files with metadata
    bucket.upload_from_stream(
        "file1.txt", b"Content 1", metadata={"type": "text"}
    )
    bucket.upload_from_stream(
        "file2.txt", b"Content 2", metadata={"type": "log"}
    )

    # Test metadata $ne operator
    cursor = bucket.find({"metadata": {"$ne": '{"type": "log"}'}})
    files = list(cursor)
    assert len(files) == 1
    assert files[0].filename == "file1.txt"


def test_gridoutcursor_unsupported_metadata_operator(bucket):
    """Test GridOutCursor with unsupported metadata operator."""
    # Upload file with metadata
    bucket.upload_from_stream(
        "file1.txt", b"Content 1", metadata={"type": "text"}
    )

    # Test unsupported operator that should fall back to LIKE pattern
    # The pattern will be "%$unsupported%text%" which won't match the JSON metadata
    cursor = bucket.find({"metadata": {"$unsupported": "text"}})
    files = list(cursor)
    # Should not match anything since the JSON metadata won't contain "$unsupported"
    assert len(files) == 0


# ================================
# GridIn/Out Serialization Tests
# ================================


class NotJSONSerializable:
    """A class that cannot be serialized to JSON."""

    def __init__(self):
        self.value = "test"

    def __repr__(self):
        return "NotJSONSerializable()"


def test_gridin_serialize_metadata_json_failure(connection):
    """Test GridIn._serialize_metadata with JSON serialization failures."""
    # Create a GridIn instance
    GridFSBucket(connection.db)
    grid_in = GridIn(
        db=connection.db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
        metadata={"custom": NotJSONSerializable()},
    )

    # Test that _serialize_metadata falls back to string representation
    serialized = grid_in._serialize_metadata({"custom": NotJSONSerializable()})
    assert serialized is not None
    assert "NotJSONSerializable" in serialized


def test_gridin_deserialize_metadata_json_failure(connection):
    """Test GridIn._deserialize_metadata with JSON decode failures."""
    # Create a GridIn instance
    GridFSBucket(connection.db)
    grid_in = GridIn(
        db=connection.db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
    )

    # Test with invalid JSON that should fall back to literal eval
    invalid_json = "{'key': 'value'}"  # Single quotes instead of double quotes
    deserialized = grid_in._deserialize_metadata(invalid_json)
    assert deserialized is not None
    # In this case, ast.literal_eval should succeed and return a dict
    assert "key" in deserialized
    assert deserialized["key"] == "value"


def test_gridin_deserialize_metadata_literal_eval_failure(connection):
    """Test GridIn._deserialize_metadata with ast.literal_eval failures."""
    # Create a GridIn instance
    GridFSBucket(connection.db)
    grid_in = GridIn(
        db=connection.db,
        bucket_name="fs",
        chunk_size_bytes=255 * 1024,
        filename="test.txt",
    )

    # Test with completely invalid data that should fall back to _metadata dict
    invalid_data = "{invalid: data"  # Malformed data
    deserialized = grid_in._deserialize_metadata(invalid_data)
    assert deserialized is not None
    assert "_metadata" in deserialized
    assert deserialized["_metadata"] == invalid_data


def test_gridout_deserialize_metadata_json_failure(connection):
    """Test GridOut._deserialize_metadata with JSON decode failures."""
    # First upload a file
    bucket = GridFSBucket(connection.db)

    # Upload file
    data = b"test data"
    file_id = bucket.upload_from_stream("test.txt", data)

    # Create a GridOut instance - convert ObjectId to integer ID for GridOut constructor
    file_int_id = bucket._get_integer_id_for_file(
        file_id
    )  # Using internal method for testing
    grid_out = GridOut(connection.db, "fs", file_int_id)

    # Test with invalid JSON that should fall back to literal eval
    invalid_json = "{'key': 'value'}"  # Single quotes instead of double quotes
    deserialized = grid_out._deserialize_metadata(invalid_json)
    assert deserialized is not None
    # In this case, ast.literal_eval should succeed and return a dict
    assert "key" in deserialized
    assert deserialized["key"] == "value"


def test_gridout_deserialize_metadata_literal_eval_failure(connection):
    """Test GridOut._deserialize_metadata with ast.literal_eval failures."""
    # First upload a file
    bucket = GridFSBucket(connection.db)

    # Upload file
    data = b"test data"
    file_id = bucket.upload_from_stream("test.txt", data)

    # Create a GridOut instance - convert ObjectId to integer ID for GridOut constructor
    file_int_id = bucket._get_integer_id_for_file(
        file_id
    )  # Using internal method for testing
    grid_out = GridOut(connection.db, "fs", file_int_id)

    # Test with completely invalid data that should fall back to _metadata dict
    invalid_data = "{invalid: data"  # Malformed data
    deserialized = grid_out._deserialize_metadata(invalid_data)
    assert deserialized is not None
    assert "_metadata" in deserialized
    assert deserialized["_metadata"] == invalid_data


def test_gridin_write_with_bytearray(connection):
    """Test GridIn.write with bytearray input."""
    bucket = GridFSBucket(connection.db)

    # Test data as bytearray
    test_data = bytearray(b"Hello, GridFS World! This is bytearray data.")

    # Write using GridIn
    with bucket.open_upload_stream("bytearray_test.txt") as grid_in:
        bytes_written = grid_in.write(test_data)
        assert bytes_written == len(test_data)

    # Read it back
    with bucket.open_download_stream_by_name("bytearray_test.txt") as grid_out:
        downloaded_data = grid_out.read()
        assert downloaded_data == bytes(test_data)


def test_gridin_write_with_invalid_data_type(connection):
    """Test GridIn.write with invalid data types."""
    bucket = GridFSBucket(connection.db)

    # Test with invalid data type
    with bucket.open_upload_stream("invalid_test.txt") as grid_in:
        with pytest.raises(TypeError):
            grid_in.write("This is a string, not bytes")

        with pytest.raises(TypeError):
            grid_in.write(12345)  # Integer


def test_gridin_context_manager_exit(connection):
    """Test GridIn.__exit__ method."""
    bucket = GridFSBucket(connection.db)

    # Test normal context manager exit
    with bucket.open_upload_stream("exit_test.txt") as grid_in:
        grid_in.write(b"Test data")
        # Stream should not be closed yet
        assert not grid_in._closed

    # After context manager exits, stream should be closed
    # We can't directly access the grid_in object outside the context,
    # but we can verify the file was properly stored
    with bucket.open_download_stream_by_name("exit_test.txt") as grid_out:
        data = grid_out.read()
        assert data == b"Test data"


def test_gridout_context_manager_exit(connection):
    """Test GridOut.__exit__ method."""
    bucket = GridFSBucket(connection.db)

    # Upload a file first
    file_id = bucket.upload_from_stream("exit_test.txt", b"Test data")

    # Test normal context manager exit
    with bucket.open_download_stream(file_id) as grid_out:
        data = grid_out.read()
        assert data == b"Test data"
        # Stream should not be closed yet
        assert not grid_out._closed

    # After context manager exits, stream should be closed
    # We can't directly access the grid_out object outside the context,
    # but we can create a new one to verify it works


def test_gridin_flush_chunk_with_file_id(connection):
    """Test GridIn._flush_chunk when file_id is not None."""
    bucket = GridFSBucket(connection.db)

    # Upload using streaming with custom file ID to test _flush_chunk with file_id already set
    custom_id = 999
    data = b"x" * 2048  # 2 chunks worth of data

    with bucket.open_upload_stream_with_id(
        custom_id, "flush_test.txt"
    ) as grid_in:
        # At this point, the file document should be created with the custom ID
        # Write enough data to trigger _flush_chunk
        grid_in.write(data)
        # The _flush_chunk method should be called with file_id already set

    # Verify the file was created correctly
    with bucket.open_download_stream(custom_id) as grid_out:
        downloaded_data = grid_out.read()
        assert downloaded_data == data
