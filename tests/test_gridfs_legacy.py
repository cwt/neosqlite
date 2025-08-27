# coding: utf-8
"""
Tests for the legacy GridFS implementation in neosqlite.
"""
from neosqlite import Connection
from neosqlite.gridfs import GridFS, NoFile
from neosqlite.gridfs.grid_file import GridOut
import io
import pytest


class TestLegacyGridFS:
    """Test the legacy GridFS interface."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db
        self.fs = GridFS(self.db)

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_gridfs_creation(self):
        """Test GridFS creation."""
        # Test default collection name
        fs = GridFS(self.db)
        assert fs._bucket._bucket_name == "fs"

        # Test custom collection name
        fs = GridFS(self.db, collection_name="custom_fs")
        assert fs._bucket._bucket_name == "custom_fs"

    def test_put_bytes_data(self):
        """Test putting bytes data into GridFS."""
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(data)

        assert isinstance(file_id, int)
        assert file_id > 0

        # Verify the file can be retrieved
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data

    def test_put_file_like_object(self):
        """Test putting file-like object into GridFS."""
        data = b"Hello, GridFS World!"
        file_like = io.BytesIO(data)
        file_id = self.fs.put(file_like)

        assert isinstance(file_id, int)
        assert file_id > 0

        # Verify the file can be retrieved
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data

    def test_put_with_filename(self):
        """Test putting data with filename."""
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(data, filename="test.txt")

        # Verify the file can be retrieved
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data
        assert grid_out.filename == "test.txt"

    def test_put_with_metadata(self):
        """Test putting data with metadata."""
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(
            data, filename="test.txt", author="tester", version="1.0"
        )

        # Verify the file can be retrieved
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data
        assert grid_out.filename == "test.txt"

    def test_get_by_id(self):
        """Test getting file by ID."""
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(data, filename="test.txt")

        # Get by ID
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data
        assert grid_out.filename == "test.txt"

    def test_get_nonexistent_file(self):
        """Test getting nonexistent file raises NoFile."""
        with pytest.raises(NoFile):
            self.fs.get(999999)

    def test_get_version(self):
        """Test getting file by filename and version."""
        data1 = b"First version content"
        data2 = b"Second version content"

        # Put two versions with the same name
        file_id1 = self.fs.put(data1, filename="test.txt")
        file_id2 = self.fs.put(data2, filename="test.txt")

        # Get first version (version 0)
        grid_out = self.fs.get_version("test.txt", 0)
        assert grid_out.read() == data1

        # Get second version (version 1)
        grid_out = self.fs.get_version("test.txt", 1)
        assert grid_out.read() == data2

        # Get latest version (version -1)
        grid_out = self.fs.get_version("test.txt", -1)
        assert grid_out.read() == data2

    def test_get_last_version(self):
        """Test getting the most recent version of a file."""
        data1 = b"First version content"
        data2 = b"Second version content"

        # Put two versions with the same name
        self.fs.put(data1, filename="test.txt")
        self.fs.put(data2, filename="test.txt")

        # Get last version
        grid_out = self.fs.get_last_version("test.txt")
        assert grid_out.read() == data2

    def test_delete_file(self):
        """Test deleting a file."""
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(data, filename="test.txt")

        # Verify file exists
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data

        # Delete the file
        self.fs.delete(file_id)

        # Verify file is deleted
        with pytest.raises(NoFile):
            self.fs.get(file_id)

    def test_delete_nonexistent_file(self):
        """Test deleting nonexistent file."""
        # Should not raise an exception
        self.fs.delete(999999)

    def test_list_files(self):
        """Test listing all filenames."""
        # Put some files
        self.fs.put(b"Content 1", filename="file1.txt")
        self.fs.put(b"Content 2", filename="file2.txt")
        self.fs.put(
            b"Content 3", filename="file1.txt"
        )  # Same name, different version

        # List files
        filenames = self.fs.list()
        assert isinstance(filenames, list)
        assert len(filenames) == 2  # Only unique filenames
        assert "file1.txt" in filenames
        assert "file2.txt" in filenames

    def test_list_empty_files(self):
        """Test listing files when no files exist."""
        filenames = self.fs.list()
        assert isinstance(filenames, list)
        assert len(filenames) == 0

    def test_find_files(self):
        """Test finding files with filter."""
        # Put some files
        self.fs.put(b"Content 1", filename="file1.txt")
        self.fs.put(b"Content 2", filename="file2.txt")
        self.fs.put(
            b"Content 3", filename="file1.txt"
        )  # Same name, different version

        # Find all files
        cursor = self.fs.find()
        files = list(cursor)
        assert len(files) == 3

        # Find files by name
        cursor = self.fs.find({"filename": "file1.txt"})
        files = list(cursor)
        assert len(files) == 2

    def test_find_one_file(self):
        """Test finding a single file."""
        # Put some files
        self.fs.put(b"Content 1", filename="file1.txt")
        self.fs.put(b"Content 2", filename="file2.txt")

        # Find one file
        grid_out = self.fs.find_one({"filename": "file1.txt"})
        assert isinstance(grid_out, GridOut)
        assert grid_out.filename == "file1.txt"

        # Find non-existent file
        grid_out = self.fs.find_one({"filename": "nonexistent.txt"})
        assert grid_out is None

    def test_exists_by_id(self):
        """Test checking if file exists by ID."""
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(data, filename="test.txt")

        # Check that file exists
        assert self.fs.exists(file_id=file_id) is True

        # Check that nonexistent file doesn't exist
        assert self.fs.exists(file_id=999999) is False

    def test_exists_by_criteria(self):
        """Test checking if file exists by criteria."""
        self.fs.put(b"Content 1", filename="file1.txt")
        self.fs.put(b"Content 2", filename="file2.txt")

        # Check by filename
        assert self.fs.exists(filename="file1.txt") is True
        assert self.fs.exists(filename="nonexistent.txt") is False

        # Check with no criteria
        assert self.fs.exists() is False

    def test_new_file_context_manager(self):
        """Test creating new file with context manager."""
        data = b"Hello, GridFS World!"

        # Create new file using context manager
        with self.fs.new_file(
            filename="new_file.txt", author="tester"
        ) as grid_in:
            grid_in.write(data)

        # Verify the file was created
        grid_out = self.fs.get_last_version("new_file.txt")
        assert grid_out.read() == data
        assert grid_out.filename == "new_file.txt"

    def test_new_file_manual_close(self):
        """Test creating new file with manual close."""
        data = b"Hello, GridFS World!"

        # Create new file manually
        grid_in = self.fs.new_file(filename="new_file.txt", author="tester")
        grid_in.write(data)
        grid_in.close()

        # Verify the file was created
        grid_out = self.fs.get_last_version("new_file.txt")
        assert grid_out.read() == data
        assert grid_out.filename == "new_file.txt"

    def test_new_file_with_custom_id(self):
        """Test creating new file with custom ID."""
        data = b"Hello, GridFS World!"
        custom_id = 100

        # Create new file with custom ID
        grid_in = self.fs.new_file(_id=custom_id, filename="new_file.txt")
        grid_in.write(data)
        grid_in.close()

        # Verify the file was created with custom ID
        grid_out = self.fs.get(custom_id)
        assert grid_out.read() == data
        assert grid_out._file_id == custom_id

    def test_gridfs_integration_with_binary_data(self):
        """Test GridFS integration with binary data."""
        # Create binary data
        binary_data = (
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09" * 100
        )  # 1000 bytes

        # Store in GridFS
        file_id = self.fs.put(binary_data, filename="binary_file.bin")

        # Retrieve and verify
        grid_out = self.fs.get(file_id)
        retrieved_data = grid_out.read()
        assert retrieved_data == binary_data
        assert len(retrieved_data) == 1000

    def test_gridfs_large_file(self):
        """Test GridFS with large file."""
        # Create large data (larger than default chunk size)
        large_data = b"x" * (255 * 1024 + 1000)  # 255KB + 1000 bytes

        # Store in GridFS
        file_id = self.fs.put(large_data, filename="large_file.txt")

        # Retrieve and verify
        grid_out = self.fs.get(file_id)
        retrieved_data = grid_out.read()
        assert retrieved_data == large_data
        assert len(retrieved_data) == len(large_data)


class TestLegacyGridFSAdvanced:
    """Advanced tests for legacy GridFS."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db
        self.fs = GridFS(self.db)

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_gridfs_with_different_encodings(self):
        """Test GridFS with different encodings parameter."""
        # Test that encoding parameter is handled correctly
        data = b"Hello, GridFS World!"
        file_id = self.fs.put(data, filename="test.txt", encoding="utf-8")

        # Verify the file can be retrieved
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data

    def test_gridfs_with_unicode_filename(self):
        """Test GridFS with unicode filename."""
        data = b"Hello, GridFS World!"
        filename = "тест.txt"  # Russian for "test.txt"

        file_id = self.fs.put(data, filename=filename)

        # Verify the file can be retrieved
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data
        assert grid_out.filename == filename

    def test_gridfs_multiple_versions_same_name(self):
        """Test GridFS with multiple versions of the same file."""
        data1 = b"Version 1 content"
        data2 = b"Version 2 content"
        data3 = b"Version 3 content"

        # Put multiple versions with the same name
        file_id1 = self.fs.put(data1, filename="test.txt")
        file_id2 = self.fs.put(data2, filename="test.txt")
        file_id3 = self.fs.put(data3, filename="test.txt")

        # Verify we can retrieve each version
        assert self.fs.get_version("test.txt", 0).read() == data1
        assert self.fs.get_version("test.txt", 1).read() == data2
        assert self.fs.get_version("test.txt", 2).read() == data3
        assert self.fs.get_last_version("test.txt").read() == data3

        # Test listing shows only one unique filename
        filenames = self.fs.list()
        assert len(filenames) == 1
        assert filenames[0] == "test.txt"

    def test_gridfs_find_with_complex_filters(self):
        """Test GridFS find with complex filters."""
        # Put files with metadata
        self.fs.put(
            b"Content 1", filename="file1.txt", author="Alice", version=1
        )
        self.fs.put(b"Content 2", filename="file2.txt", author="Bob", version=2)
        self.fs.put(
            b"Content 3", filename="file3.txt", author="Alice", version=1
        )

        # Find files by filename (this is supported)
        cursor = self.fs.find({"filename": "file1.txt"})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "file1.txt"

        # Note: Filtering by metadata fields like 'author' is not yet implemented
        # but filtering by filename works correctly

    def test_gridfs_put_io_base_object(self):
        """Test GridFS put with various IOBase objects."""
        data = b"Hello, GridFS World!"

        # Test with BytesIO
        bytes_io = io.BytesIO(data)
        file_id = self.fs.put(bytes_io, filename="bytesio.txt")
        grid_out = self.fs.get(file_id)
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
        file_id = self.fs.put(mock_reader, filename="mockreader.txt")
        grid_out = self.fs.get(file_id)
        assert grid_out.read() == data

    def test_gridfs_compatibility_with_gridfsbucket(self):
        """Test that legacy GridFS is compatible with GridFSBucket."""
        from neosqlite.gridfs import GridFSBucket

        # Create both interfaces
        legacy_fs = GridFS(self.db)
        bucket = GridFSBucket(self.db)

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
