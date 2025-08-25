import io
import pytest
from neosqlite import Connection
from neosqlite.gridfs import GridFS, GridFSBucket, NoFile


class TestGridFSFiltering:
    """Test enhanced filtering capabilities in GridFS."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db
        self.fs = GridFS(self.db)
        self.bucket = GridFSBucket(self.db)

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_find_with_length_filter(self):
        """Test finding files with length filters."""
        # Upload files of different sizes
        small_data = b"x" * 100  # 100 bytes
        medium_data = b"x" * 1000  # 1000 bytes
        large_data = b"x" * 5000  # 5000 bytes

        self.fs.put(small_data, filename="small.txt")
        self.fs.put(medium_data, filename="medium.txt")
        self.fs.put(large_data, filename="large.txt")

        # Find files with length > 500
        cursor = self.fs.find({"length": {"$gt": 500}})
        files = list(cursor)
        assert len(files) == 2  # medium and large

        # Find files with length >= 1000
        cursor = self.fs.find({"length": {"$gte": 1000}})
        files = list(cursor)
        assert len(files) == 2  # medium and large

        # Find files with length < 2000
        cursor = self.fs.find({"length": {"$lt": 2000}})
        files = list(cursor)
        assert len(files) == 2  # small and medium

        # Find files with length <= 1000
        cursor = self.fs.find({"length": {"$lte": 1000}})
        files = list(cursor)
        assert len(files) == 2  # small and medium

        # Find files with specific length
        cursor = self.fs.find({"length": 100})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "small.txt"

    def test_find_with_chunksize_filter(self):
        """Test finding files with chunkSize filters."""
        # Upload files
        data = b"x" * 1000
        self.fs.put(data, filename="test1.txt")
        self.fs.put(data, filename="test2.txt")

        # Find files with default chunk size (255KB)
        cursor = self.fs.find({"chunkSize": 255 * 1024})
        files = list(cursor)
        assert len(files) == 2

    def test_find_with_complex_filters(self):
        """Test finding files with multiple filter conditions."""
        # Upload files with different properties
        small_data = b"x" * 100
        large_data = b"x" * 5000

        self.fs.put(small_data, filename="small.txt")
        self.fs.put(large_data, filename="large.txt")
        self.fs.put(small_data, filename="another_small.txt")

        # Find small files using length and filename regex
        cursor = self.fs.find(
            {"length": {"$lt": 1000}, "filename": {"$regex": "small"}}
        )
        files = list(cursor)
        assert len(files) == 2

        # Find files with exact name match
        cursor = self.fs.find({"filename": "small.txt"})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "small.txt"

        # Find files with filename not equal to a specific name
        cursor = self.fs.find({"filename": {"$ne": "small.txt"}})
        files = list(cursor)
        assert len(files) == 2  # large.txt and another_small.txt

        # Find files with exact name match
        cursor = self.fs.find({"filename": "small.txt"})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "small.txt"

    def test_find_with_metadata_filter(self):
        """Test finding files with metadata filters."""
        # Upload files with metadata
        self.fs.put(b"data1", filename="file1.txt", author="alice", version=1)
        self.fs.put(b"data2", filename="file2.txt", author="bob", version=2)
        self.fs.put(b"data3", filename="file3.txt", author="alice", version=1)

        # Find files by author (basic substring matching)
        # Metadata is now stored as JSON, so we look for the JSON representation
        cursor = self.fs.find({"metadata": '"author": "alice"'})
        files = list(cursor)
        assert len(files) == 2  # file1 and file3

        # Find files by version
        cursor = self.fs.find({"metadata": '"version": 1'})
        files = list(cursor)
        assert len(files) == 2  # file1 and file3

    def test_gridfsbucket_find_with_filters(self):
        """Test that GridFSBucket also supports enhanced filtering."""
        # Upload files of different sizes
        small_data = b"x" * 100
        large_data = b"x" * 5000

        self.bucket.upload_from_stream("small.txt", small_data)
        self.bucket.upload_from_stream("large.txt", large_data)

        # Test with GridFSBucket API
        cursor = self.bucket.find({"length": {"$gt": 1000}})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "large.txt"

    def test_find_with_upload_date_filter(self):
        """Test finding files with uploadDate filters."""
        import time

        # Upload files
        data = b"test data"
        file_id1 = self.fs.put(data, filename="first.txt")

        # Small delay to ensure different timestamps
        time.sleep(0.01)

        file_id2 = self.fs.put(data, filename="second.txt")

        # Get the actual upload dates for comparison
        grid_out1 = self.fs.get(file_id1)
        grid_out2 = self.fs.get(file_id2)

        # Find files uploaded after the first file
        cursor = self.fs.find({"uploadDate": {"$gt": grid_out1.upload_date}})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "second.txt"

    def test_find_with_md5_filter(self):
        """Test finding files with MD5 filters."""
        data1 = b"content1"
        data2 = b"content2"

        file_id1 = self.fs.put(data1, filename="file1.txt")
        file_id2 = self.fs.put(data2, filename="file2.txt")

        # Get the actual MD5 hashes
        grid_out1 = self.fs.get(file_id1)
        md5_hash = grid_out1.md5

        # Find file by MD5 hash
        cursor = self.fs.find({"md5": md5_hash})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "file1.txt"

        # Find files with different MD5 hash
        cursor = self.fs.find({"md5": {"$ne": md5_hash}})
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "file2.txt"
