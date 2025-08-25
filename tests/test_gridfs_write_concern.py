import io
import pytest
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket


class TestGridFSWriteConcern:
    """Test GridFS write concern simulation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_write_concern_j_true(self):
        """Test GridFSBucket with j=True write concern."""
        # Create bucket with journal write concern
        bucket = GridFSBucket(self.db, write_concern={"j": True})

        # Upload a file
        data = b"test data with journal concern"
        file_id = bucket.upload_from_stream("test.txt", data)

        # Verify file was created correctly
        assert isinstance(file_id, int)

        # Check file content
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.read() == data

    def test_write_concern_w_zero(self):
        """Test GridFSBucket with w=0 write concern."""
        # Create bucket with no acknowledgment concern
        bucket = GridFSBucket(self.db, write_concern={"w": 0})

        # Upload a file
        data = b"test data with no ack concern"
        file_id = bucket.upload_from_stream("test.txt", data)

        # Verify file was created correctly
        assert isinstance(file_id, int)

        # Check file content
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.read() == data

    def test_write_concern_w_majority(self):
        """Test GridFSBucket with w='majority' write concern."""
        # Create bucket with majority acknowledgment concern
        bucket = GridFSBucket(self.db, write_concern={"w": "majority"})

        # Upload a file
        data = b"test data with majority concern"
        file_id = bucket.upload_from_stream("test.txt", data)

        # Verify file was created correctly
        assert isinstance(file_id, int)

        # Check file content
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.read() == data

    def test_write_concern_with_streaming(self):
        """Test GridFSBucket with write concern and streaming uploads."""
        # Create bucket with journal write concern
        bucket = GridFSBucket(self.db, write_concern={"j": True})

        # Upload using streaming
        data = b"streaming test data with concern"
        with bucket.open_upload_stream("stream.txt") as grid_in:
            grid_in.write(data)

        # Find the file and check content
        cursor = bucket.find({"filename": "stream.txt"})
        files = list(cursor)
        assert len(files) == 1

        grid_out = bucket.open_download_stream(files[0]._file_id)
        assert grid_out.read() == data

    def test_write_concern_with_custom_id(self):
        """Test GridFSBucket with write concern and custom file ID."""
        # Create bucket with write concern
        bucket = GridFSBucket(self.db, write_concern={"j": True, "w": 1})

        # Upload with custom ID
        custom_id = 100
        data = b"test data with custom id and concern"
        bucket.upload_from_stream_with_id(custom_id, "custom.txt", data)

        # Check file content
        grid_out = bucket.open_download_stream(custom_id)
        assert grid_out.read() == data

    def test_write_concern_combinations(self):
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
            bucket = GridFSBucket(self.db, write_concern=write_concern)
            data = f"test data {i}".encode()
            filename = f"test{i}.txt"

            # Upload file
            file_id = bucket.upload_from_stream(filename, data)

            # Verify
            grid_out = bucket.open_download_stream(file_id)
            assert grid_out.read() == data
