import io
import hashlib
import pytest
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket, GridFS


class TestGridFSOptions:
    """Test GridFS options and parameters."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_gridfsbucket_with_disable_md5(self):
        """Test GridFSBucket with disable_md5 option."""
        # Create bucket with MD5 disabled
        bucket = GridFSBucket(self.db, disable_md5=True)

        # Upload a file
        data = b"test data for md5 check"
        file_id = bucket.upload_from_stream("test.txt", data)

        # Check that MD5 is None when disabled
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.md5 is None
        assert grid_out.length == len(data)

    def test_gridfsbucket_with_md5_enabled(self):
        """Test GridFSBucket with MD5 enabled (default)."""
        # Create bucket with MD5 enabled (default)
        bucket = GridFSBucket(self.db)

        # Upload a file
        data = b"test data for md5 check"
        file_id = bucket.upload_from_stream("test.txt", data)

        # Check that MD5 is calculated when enabled
        expected_md5 = hashlib.md5(data).hexdigest()
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.md5 == expected_md5
        assert grid_out.length == len(data)

    def test_gridfsbucket_with_custom_chunk_size(self):
        """Test GridFSBucket with custom chunk size."""
        # Create bucket with custom chunk size
        custom_chunk_size = 1024  # 1KB
        bucket = GridFSBucket(self.db, chunk_size_bytes=custom_chunk_size)

        # Upload a file larger than chunk size
        data = b"x" * (custom_chunk_size * 3 + 500)  # 3.5 chunks
        file_id = bucket.upload_from_stream("large.txt", data)

        # Check that chunk size is stored correctly
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.chunk_size == custom_chunk_size
        assert grid_out.length == len(data)

    def test_gridfsbucket_with_invalid_chunk_size(self):
        """Test GridFSBucket with invalid chunk size."""
        # Test negative chunk size
        with pytest.raises(ValueError):
            GridFSBucket(self.db, chunk_size_bytes=-1)

        # Test zero chunk size
        with pytest.raises(ValueError):
            GridFSBucket(self.db, chunk_size_bytes=0)

    def test_gridfsbucket_with_write_concern_validation(self):
        """Test GridFSBucket with write concern validation."""
        # Valid write concern
        bucket = GridFSBucket(
            self.db, write_concern={"w": 1, "wtimeout": 1000, "j": True}
        )

        # Upload a file to ensure it works
        data = b"test data"
        file_id = bucket.upload_from_stream("test.txt", data)
        assert isinstance(file_id, int)

    def test_gridfsbucket_with_invalid_write_concern(self):
        """Test GridFSBucket with invalid write concern parameters."""
        # Invalid 'w' parameter
        with pytest.raises(ValueError):
            GridFSBucket(self.db, write_concern={"w": 1.5})

        # Invalid 'wtimeout' parameter
        with pytest.raises(ValueError):
            GridFSBucket(self.db, write_concern={"wtimeout": "invalid"})

        # Invalid 'j' parameter
        with pytest.raises(ValueError):
            GridFSBucket(self.db, write_concern={"j": "invalid"})

    def test_gridin_with_disable_md5(self):
        """Test GridIn with disable_md5 option."""
        # Create bucket with MD5 disabled
        bucket = GridFSBucket(self.db, disable_md5=True)

        # Upload using streaming with MD5 disabled
        data = b"streaming test data"
        with bucket.open_upload_stream("stream.txt") as grid_in:
            grid_in.write(data)

        # Find the file and check MD5
        cursor = bucket.find({"filename": "stream.txt"})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].md5 is None

    def test_gridin_with_md5_enabled(self):
        """Test GridIn with MD5 enabled (default)."""
        # Create bucket with MD5 enabled
        bucket = GridFSBucket(self.db)

        # Upload using streaming with MD5 enabled
        data = b"streaming test data"
        with bucket.open_upload_stream("stream.txt") as grid_in:
            grid_in.write(data)

        # Find the file and check MD5
        cursor = bucket.find({"filename": "stream.txt"})
        files = list(cursor)
        assert len(files) == 1
        expected_md5 = hashlib.md5(data).hexdigest()
        assert files[0].md5 == expected_md5

    def test_legacy_gridfs_with_options(self):
        """Test legacy GridFS with options passed through."""
        # Create legacy GridFS with options
        fs = GridFS(self.db)

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
