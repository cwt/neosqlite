import io
import sqlite3
import unittest
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket, NoFile


class TestGridFSBucket(unittest.TestCase):
    def setUp(self):
        # Create an in-memory database for testing
        self.conn = Connection(":memory:")
        self.db = self.conn.db

    def tearDown(self):
        self.conn.close()

    def test_gridfs_bucket_creation(self):
        """Test that GridFSBucket can be created and tables are initialized."""
        bucket = GridFSBucket(self.db)

        # Check that the files table exists
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fs.files'"
        )
        self.assertIsNotNone(cursor.fetchone())

        # Check that the chunks table exists
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fs.chunks'"
        )
        self.assertIsNotNone(cursor.fetchone())

    def test_upload_from_stream_bytes(self):
        """Test uploading bytes data to GridFS."""
        bucket = GridFSBucket(self.db)

        # Test data
        test_data = b"Hello, GridFS World!"
        file_id = bucket.upload_from_stream("test.txt", test_data)

        self.assertIsInstance(file_id, int)
        self.assertGreater(file_id, 0)

    def test_upload_and_download_bytes(self):
        """Test uploading and downloading bytes data."""
        bucket = GridFSBucket(self.db)

        # Test data
        test_data = (
            b"Hello, GridFS World! This is a test of the GridFS implementation."
        )
        file_id = bucket.upload_from_stream("test.txt", test_data)

        # Download the data
        output = io.BytesIO()
        bucket.download_to_stream(file_id, output)
        downloaded_data = output.getvalue()

        self.assertEqual(downloaded_data, test_data)

    def test_open_download_stream(self):
        """Test opening a download stream."""
        bucket = GridFSBucket(self.db)

        # Test data
        test_data = b"Hello, GridFS World!"
        file_id = bucket.upload_from_stream("test.txt", test_data)

        # Open download stream
        with bucket.open_download_stream(file_id) as grid_out:
            downloaded_data = grid_out.read()

        self.assertEqual(downloaded_data, test_data)

    def test_delete_file(self):
        """Test deleting a file from GridFS."""
        bucket = GridFSBucket(self.db)

        # Upload a file
        test_data = b"Hello, GridFS World!"
        file_id = bucket.upload_from_stream("test.txt", test_data)

        # Verify file exists
        output = io.BytesIO()
        bucket.download_to_stream(file_id, output)
        downloaded_data = output.getvalue()
        self.assertEqual(downloaded_data, test_data)

        # Delete the file
        bucket.delete(file_id)

        # Verify file is deleted
        with self.assertRaises(NoFile):
            output = io.BytesIO()
            bucket.download_to_stream(file_id, output)

    def test_grid_in_write(self):
        """Test writing data using GridIn."""
        bucket = GridFSBucket(self.db)

        # Test data
        test_data = b"Hello, GridFS World! This is streamed data."

        # Write using GridIn
        with bucket.open_upload_stream("streamed.txt") as grid_in:
            grid_in.write(test_data)

        # Read it back
        with bucket.open_download_stream_by_name("streamed.txt") as grid_out:
            downloaded_data = grid_out.read()

        self.assertEqual(downloaded_data, test_data)

    def test_find_files(self):
        """Test finding files in GridFS."""
        bucket = GridFSBucket(self.db)

        # Upload multiple files
        bucket.upload_from_stream("file1.txt", b"Content 1")
        bucket.upload_from_stream("file2.txt", b"Content 2")
        bucket.upload_from_stream(
            "file1.txt", b"Content 1 updated"
        )  # Same name, different revision

        # Find all files
        cursor = bucket.find()
        files = list(cursor)
        self.assertEqual(len(files), 3)

        # Find files by name
        cursor = bucket.find({"filename": "file1.txt"})
        files = list(cursor)
        self.assertEqual(len(files), 2)


if __name__ == "__main__":
    unittest.main()
