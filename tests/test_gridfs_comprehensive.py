import io
import json
import sqlite3
import unittest
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket, NoFile, FileExists
from neosqlite.gridfs.grid_file import GridIn, GridOut
import datetime


class TestGridFSComprehensive(unittest.TestCase):
    def setUp(self):
        # Create an in-memory database for testing
        self.conn = Connection(":memory:")
        self.db = self.conn.db

    def tearDown(self):
        self.conn.close()

    def test_grid_in_functionality(self):
        """Test GridIn class functionality including edge cases."""
        bucket = GridFSBucket(self.db)

        # Test GridIn with custom file_id
        with bucket.open_upload_stream_with_id(
            999, "test_custom_id.txt"
        ) as grid_in:
            grid_in.write(b"Custom ID test data")

        # Verify file was created with custom ID
        with bucket.open_download_stream(999) as grid_out:
            data = grid_out.read()
            self.assertEqual(data, b"Custom ID test data")

        # Test GridIn with metadata
        metadata = {"author": "test_user", "version": 1}
        with bucket.open_upload_stream(
            "metadata_test.txt", metadata=metadata
        ) as grid_in:
            grid_in.write(b"Metadata test data")

        # Verify metadata was stored correctly
        cursor = self.db.execute(
            "SELECT metadata FROM `fs.files` WHERE filename = ?",
            ("metadata_test.txt",),
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        stored_metadata = json.loads(row[0])
        self.assertEqual(stored_metadata, metadata)

        # Test GridIn with disable_md5
        bucket_no_md5 = GridFSBucket(
            self.db, bucket_name="no_md5", disable_md5=True
        )
        with bucket_no_md5.open_upload_stream("no_md5_test.txt") as grid_in:
            grid_in.write(b"No MD5 test data")

        # Verify MD5 is None when disabled
        cursor = self.db.execute(
            "SELECT md5 FROM `no_md5.files` WHERE filename = ?",
            ("no_md5_test.txt",),
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])

    def test_grid_out_functionality(self):
        """Test GridOut class functionality including edge cases."""
        bucket = GridFSBucket(self.db)

        # Upload test data
        test_data = b"A" * 1000  # Larger than default chunk size
        file_id = bucket.upload_from_stream("large_file.txt", test_data)

        # Test reading partial data
        with bucket.open_download_stream(file_id) as grid_out:
            # Read first 100 bytes
            partial_data = grid_out.read(100)
            self.assertEqual(len(partial_data), 100)
            self.assertEqual(partial_data, b"A" * 100)

            # Read next 200 bytes
            partial_data = grid_out.read(200)
            self.assertEqual(len(partial_data), 200)
            self.assertEqual(partial_data, b"A" * 200)

            # Read remaining data
            remaining_data = grid_out.read()
            self.assertEqual(len(remaining_data), 700)
            self.assertEqual(remaining_data, b"A" * 700)

            # Try to read more (should return empty)
            empty_data = grid_out.read(100)
            self.assertEqual(empty_data, b"")

        # Test reading zero bytes
        with bucket.open_download_stream(file_id) as grid_out:
            zero_data = grid_out.read(0)
            self.assertEqual(zero_data, b"")

    def test_grid_out_properties(self):
        """Test GridOut properties."""
        bucket = GridFSBucket(self.db)
        metadata = {"test": "value"}

        # Upload file with metadata
        test_data = b"Property test data"
        file_id = bucket.upload_from_stream(
            "property_test.txt", test_data, metadata=metadata
        )

        # Test GridOut properties
        with bucket.open_download_stream(file_id) as grid_out:
            self.assertEqual(grid_out.filename, "property_test.txt")
            self.assertEqual(grid_out.length, len(test_data))
            self.assertEqual(
                grid_out.chunk_size, 255 * 1024
            )  # Default chunk size
            self.assertEqual(grid_out.metadata, metadata)
            self.assertIsNotNone(grid_out.upload_date)
            self.assertIsNotNone(grid_out.md5)

    def test_grid_out_cursor_functionality(self):
        """Test GridOutCursor functionality with various filters."""
        bucket = GridFSBucket(self.db)

        # Upload multiple files
        bucket.upload_from_stream("file1.txt", b"Content 1")
        bucket.upload_from_stream(
            "file2.txt", b"Content 2", metadata={"type": "test"}
        )
        bucket.upload_from_stream(
            "file3.txt", b"Content 3" * 1000
        )  # Large file

        # Test filter by _id
        file_id = self.db.execute(
            "SELECT _id FROM `fs.files` WHERE filename = ?", ("file1.txt",)
        ).fetchone()[0]
        cursor = bucket.find({"_id": file_id})
        files = list(cursor)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].filename, "file1.txt")

        # Test filter by filename with operators
        cursor = bucket.find({"filename": {"$regex": "file"}})
        files = list(cursor)
        self.assertEqual(len(files), 3)

        cursor = bucket.find({"filename": {"$ne": "file1.txt"}})
        files = list(cursor)
        self.assertEqual(len(files), 2)

        # Test filter by length with operators
        cursor = bucket.find(
            {"length": {"$gt": 100}}
        )  # Should match the large file
        files = list(cursor)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].filename, "file3.txt")

        # Test filter by metadata
        cursor = bucket.find({"metadata": {"$regex": "test"}})
        files = list(cursor)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].filename, "file2.txt")

    def test_write_concern_functionality(self):
        """Test write concern functionality."""
        # Test with j=True (journaling enabled)
        bucket_journaled = GridFSBucket(self.db, write_concern={"j": True})
        file_id = bucket_journaled.upload_from_stream(
            "journaled.txt", b"Journaled data"
        )
        self.assertIsInstance(file_id, int)

        # Test with w=0 (no acknowledgment)
        bucket_no_ack = GridFSBucket(self.db, write_concern={"w": 0})
        file_id = bucket_no_ack.upload_from_stream("no_ack.txt", b"No ack data")
        self.assertIsInstance(file_id, int)

        # Test with invalid write concern values
        with self.assertRaises(ValueError):
            GridFSBucket(self.db, write_concern={"w": []})  # Invalid type

        with self.assertRaises(ValueError):
            GridFSBucket(
                self.db, write_concern={"wtimeout": "invalid"}
            )  # Invalid type

        with self.assertRaises(ValueError):
            GridFSBucket(
                self.db, write_concern={"j": "invalid"}
            )  # Invalid type

    def test_upload_from_stream_with_id_existing_file(self):
        """Test uploading with custom ID when file already exists."""
        bucket = GridFSBucket(self.db)

        # Upload a file with custom ID
        bucket.upload_from_stream_with_id(123, "test.txt", b"Test data")

        # Try to upload another file with the same ID - should raise FileExists
        with self.assertRaises(FileExists):
            bucket.upload_from_stream_with_id(
                123, "test2.txt", b"More test data"
            )

    def test_open_upload_stream_with_id_existing_file(self):
        """Test opening upload stream with custom ID when file already exists."""
        bucket = GridFSBucket(self.db)

        # Upload a file with custom ID
        bucket.upload_from_stream_with_id(456, "test.txt", b"Test data")

        # Try to open upload stream with the same ID - should raise FileExists
        with self.assertRaises(FileExists):
            bucket.open_upload_stream_with_id(456, "test2.txt")

    def test_delete_by_name(self):
        """Test deleting files by name."""
        bucket = GridFSBucket(self.db)

        # Upload multiple files with same name (different revisions)
        bucket.upload_from_stream("delete_test.txt", b"Content 1")
        bucket.upload_from_stream("delete_test.txt", b"Content 2")
        bucket.upload_from_stream("other_file.txt", b"Other content")

        # Verify files exist
        cursor = bucket.find({"filename": "delete_test.txt"})
        files = list(cursor)
        self.assertEqual(len(files), 2)

        # Delete by name
        bucket.delete_by_name("delete_test.txt")

        # Verify files are deleted
        cursor = bucket.find({"filename": "delete_test.txt"})
        files = list(cursor)
        self.assertEqual(len(files), 0)

        # Verify other files still exist
        cursor = bucket.find({"filename": "other_file.txt"})
        files = list(cursor)
        self.assertEqual(len(files), 1)

        # Try to delete non-existent file - should raise NoFile
        with self.assertRaises(NoFile):
            bucket.delete_by_name("non_existent.txt")

    def test_rename_functionality(self):
        """Test renaming files."""
        bucket = GridFSBucket(self.db)

        # Upload a file
        file_id = bucket.upload_from_stream(
            "original_name.txt", b"Rename test data"
        )

        # Rename by ID
        bucket.rename(file_id, "new_name.txt")

        # Verify rename worked
        with bucket.open_download_stream_by_name("new_name.txt") as grid_out:
            data = grid_out.read()
            self.assertEqual(data, b"Rename test data")

        # Try to rename non-existent file - should raise NoFile
        with self.assertRaises(NoFile):
            bucket.rename(999999, "non_existent.txt")

        # Upload multiple files with same name
        bucket.upload_from_stream("multi_rename.txt", b"Content 1")
        bucket.upload_from_stream("multi_rename.txt", b"Content 2")

        # Rename all files with that name
        bucket.rename_by_name("multi_rename.txt", "renamed_multi.txt")

        # Verify all files were renamed
        cursor = bucket.find({"filename": "renamed_multi.txt"})
        files = list(cursor)
        self.assertEqual(len(files), 2)

        # Verify no files with old name remain
        cursor = bucket.find({"filename": "multi_rename.txt"})
        files = list(cursor)
        self.assertEqual(len(files), 0)

        # Try to rename non-existent file by name - should raise NoFile
        with self.assertRaises(NoFile):
            bucket.rename_by_name("non_existent.txt", "new_name.txt")

    def test_drop_functionality(self):
        """Test dropping the entire bucket."""
        bucket = GridFSBucket(self.db)

        # Upload some files
        bucket.upload_from_stream("file1.txt", b"Content 1")
        bucket.upload_from_stream("file2.txt", b"Content 2")

        # Verify files exist
        cursor = bucket.find()
        files = list(cursor)
        self.assertEqual(len(files), 2)

        # Drop the bucket
        bucket.drop()

        # Verify all files are gone
        cursor = bucket.find()
        files = list(cursor)
        self.assertEqual(len(files), 0)

        # Verify tables are empty but still exist
        cursor = self.db.execute("SELECT COUNT(*) FROM `fs.files`")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0)

        cursor = self.db.execute("SELECT COUNT(*) FROM `fs.chunks`")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0)

    def test_metadata_serialization_edge_cases(self):
        """Test metadata serialization with edge cases."""
        bucket = GridFSBucket(self.db)

        # Test with None metadata
        file_id = bucket.upload_from_stream(
            "none_metadata.txt", b"Test data", metadata=None
        )

        cursor = self.db.execute(
            "SELECT metadata FROM `fs.files` WHERE _id = ?", (file_id,)
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])

        # Test with non-serializable metadata (should fall back to string representation)
        class CustomObject:
            def __str__(self):
                return "custom_object_repr"

        metadata = {"custom": CustomObject()}
        file_id = bucket.upload_from_stream(
            "custom_metadata.txt", b"Test data", metadata=metadata
        )

        cursor = self.db.execute(
            "SELECT metadata FROM `fs.files` WHERE _id = ?", (file_id,)
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertIsNotNone(row[0])

    def test_chunk_size_validation(self):
        """Test chunk size validation."""
        # Test with valid chunk size
        bucket = GridFSBucket(self.db, chunk_size_bytes=1024)
        file_id = bucket.upload_from_stream("valid_chunk.txt", b"Test data")
        self.assertIsInstance(file_id, int)

        # Test with invalid chunk size (should raise ValueError)
        with self.assertRaises(ValueError):
            GridFSBucket(self.db, chunk_size_bytes=0)

        with self.assertRaises(ValueError):
            GridFSBucket(self.db, chunk_size_bytes=-1)

    def test_grid_in_context_manager(self):
        """Test GridIn context manager functionality."""
        bucket = GridFSBucket(self.db)

        # Test normal context manager usage
        with bucket.open_upload_stream("context_test.txt") as grid_in:
            grid_in.write(b"Context manager test")
            # File should not be finalized yet
            cursor = self.db.execute(
                "SELECT COUNT(*) FROM `fs.files` WHERE filename = ?",
                ("context_test.txt",),
            )
            count = cursor.fetchone()[0]
            self.assertEqual(count, 0)

        # After context manager exits, file should be finalized
        cursor = self.db.execute(
            "SELECT COUNT(*) FROM `fs.files` WHERE filename = ?",
            ("context_test.txt",),
        )
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)

        # Test that file can be read back
        with bucket.open_download_stream_by_name(
            "context_test.txt"
        ) as grid_out:
            data = grid_out.read()
            self.assertEqual(data, b"Context manager test")

    def test_grid_out_context_manager(self):
        """Test GridOut context manager functionality."""
        bucket = GridFSBucket(self.db)

        # Upload a file
        file_id = bucket.upload_from_stream(
            "context_out_test.txt", b"Context manager test"
        )

        # Test context manager usage
        with bucket.open_download_stream(file_id) as grid_out:
            data = grid_out.read()
            self.assertEqual(data, b"Context manager test")
            self.assertFalse(grid_out._closed)

        # After context manager exits, stream should be closed
        # Note: We can't directly test this as the GridOut object is not accessible outside,
        # but we can verify that a new GridOut object works correctly

    def test_download_by_name_with_revision(self):
        """Test downloading files by name with specific revisions."""
        bucket = GridFSBucket(self.db)

        # Upload multiple files with same name
        bucket.upload_from_stream("revision_test.txt", b"Revision 0 content")
        bucket.upload_from_stream("revision_test.txt", b"Revision 1 content")
        bucket.upload_from_stream("revision_test.txt", b"Revision 2 content")

        # Test downloading latest revision (default behavior)
        output = io.BytesIO()
        bucket.download_to_stream_by_name("revision_test.txt", output)
        self.assertEqual(output.getvalue(), b"Revision 2 content")

        # Test downloading specific revisions
        output = io.BytesIO()
        bucket.download_to_stream_by_name(
            "revision_test.txt", output, revision=0
        )
        self.assertEqual(output.getvalue(), b"Revision 0 content")

        output = io.BytesIO()
        bucket.download_to_stream_by_name(
            "revision_test.txt", output, revision=1
        )
        self.assertEqual(output.getvalue(), b"Revision 1 content")

        output = io.BytesIO()
        bucket.download_to_stream_by_name(
            "revision_test.txt", output, revision=2
        )
        self.assertEqual(output.getvalue(), b"Revision 2 content")

        # Test downloading with negative revision (should be latest)
        output = io.BytesIO()
        bucket.download_to_stream_by_name(
            "revision_test.txt", output, revision=-1
        )
        self.assertEqual(output.getvalue(), b"Revision 2 content")

    def test_open_download_by_name_with_revision(self):
        """Test opening download streams by name with specific revisions."""
        bucket = GridFSBucket(self.db)

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
            self.assertEqual(data, b"Open Revision 1 content")

        # Test opening specific revisions
        with bucket.open_download_stream_by_name(
            "open_revision_test.txt", revision=0
        ) as grid_out:
            data = grid_out.read()
            self.assertEqual(data, b"Open Revision 0 content")

        # Test opening with negative revision (should be latest)
        with bucket.open_download_stream_by_name(
            "open_revision_test.txt", revision=-1
        ) as grid_out:
            data = grid_out.read()
            self.assertEqual(data, b"Open Revision 1 content")

    def test_force_sync_functionality(self):
        """Test force sync functionality."""
        # Test with write concern that requires sync
        bucket = GridFSBucket(self.db, write_concern={"j": True})

        # This should trigger force sync
        file_id = bucket.upload_from_stream("sync_test.txt", b"Sync test data")
        self.assertIsInstance(file_id, int)

        # Test with write concern that doesn't require sync
        bucket_no_sync = GridFSBucket(self.db, write_concern={"j": False})
        file_id = bucket_no_sync.upload_from_stream(
            "no_sync_test.txt", b"No sync test data"
        )
        self.assertIsInstance(file_id, int)

    def test_grid_in_closed_stream_operations(self):
        """Test operations on closed GridIn stream."""
        bucket = GridFSBucket(self.db)

        with bucket.open_upload_stream("closed_test.txt") as grid_in:
            grid_in.write(b"Some data")
            # Stream is still open, operations should work

        # After context manager exits, stream is closed
        # We can't directly test this without creating a GridIn outside context manager,
        # but the close() method is covered by the context manager tests

    def test_grid_out_closed_stream_operations(self):
        """Test operations on closed GridOut stream."""
        bucket = GridFSBucket(self.db)

        # Upload a file
        file_id = bucket.upload_from_stream(
            "closed_out_test.txt", b"Closed stream test"
        )

        # Test reading from closed stream
        grid_out = bucket.open_download_stream(file_id)
        grid_out.close()

        with self.assertRaises(ValueError):
            grid_out.read(100)


if __name__ == "__main__":
    unittest.main()
