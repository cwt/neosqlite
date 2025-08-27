from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket, NoFile
import io
import pytest


class TestGridFSDrop:
    """Test GridFS drop functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db
        self.bucket = GridFSBucket(self.db)

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_drop_empty_bucket(self):
        """Test dropping an empty bucket."""
        # Should not raise an exception
        self.bucket.drop()

        # Verify bucket is empty
        cursor = self.bucket.find()
        files = list(cursor)
        assert len(files) == 0

    def test_drop_bucket_with_files(self):
        """Test dropping a bucket with files."""
        # Add some files
        data1 = b"file content 1"
        data2 = b"file content 2"

        file_id1 = self.bucket.upload_from_stream("file1.txt", data1)
        file_id2 = self.bucket.upload_from_stream("file2.txt", data2)

        # Verify files exist
        cursor = self.bucket.find()
        files = list(cursor)
        assert len(files) == 2

        # Drop the bucket
        self.bucket.drop()

        # Verify bucket is empty
        cursor = self.bucket.find()
        files = list(cursor)
        assert len(files) == 0

        # Verify files are really gone (attempting to access should raise NoFile)
        with pytest.raises(NoFile):
            self.bucket.download_to_stream(file_id1, io.BytesIO())

        with pytest.raises(NoFile):
            self.bucket.download_to_stream(file_id2, io.BytesIO())

    def test_drop_bucket_with_streaming_files(self):
        """Test dropping a bucket with streaming uploads."""
        # Add files using streaming
        with self.bucket.open_upload_stream("stream1.txt") as grid_in:
            grid_in.write(b"streaming content 1")

        with self.bucket.open_upload_stream("stream2.txt") as grid_in:
            grid_in.write(b"streaming content 2")

        # Verify files exist
        cursor = self.bucket.find()
        files = list(cursor)
        assert len(files) == 2

        # Drop the bucket
        self.bucket.drop()

        # Verify bucket is empty
        cursor = self.bucket.find()
        files = list(cursor)
        assert len(files) == 0

    def test_drop_then_reuse_bucket(self):
        """Test that bucket can be reused after dropping."""
        # Add a file
        data = b"initial content"
        self.bucket.upload_from_stream("initial.txt", data)

        # Drop the bucket
        self.bucket.drop()

        # Add new files
        new_data = b"new content"
        new_file_id = self.bucket.upload_from_stream("new.txt", new_data)

        # Verify only new file exists
        cursor = self.bucket.find()
        files = list(cursor)
        assert len(files) == 1
        assert files[0].filename == "new.txt"

        # Verify content
        output = io.BytesIO()
        self.bucket.download_to_stream(new_file_id, output)
        assert output.getvalue() == new_data
