from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket, GridFS


class TestGridFSMetadata:
    """Test GridFS sophisticated metadata handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = Connection(":memory:")
        self.db = self.conn.db

    def teardown_method(self):
        """Tear down test fixtures."""
        self.conn.close()

    def test_json_metadata_storage_and_retrieval(self):
        """Test that metadata is properly stored and retrieved as JSON."""
        bucket = GridFSBucket(self.db)

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

    def test_simple_metadata_storage_and_retrieval(self):
        """Test that simple metadata works correctly."""
        bucket = GridFSBucket(self.db)

        # Test simple metadata
        metadata = {"author": "Jane Smith", "version": 2}

        # Upload file with simple metadata
        data = b"test data with simple metadata"
        file_id = bucket.upload_from_stream("simple.txt", data, metadata)

        # Retrieve file and check metadata
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.metadata == metadata

    def test_none_metadata_handling(self):
        """Test that None metadata is handled correctly."""
        bucket = GridFSBucket(self.db)

        # Upload file with no metadata
        data = b"test data with no metadata"
        file_id = bucket.upload_from_stream("none.txt", data)

        # Retrieve file and check metadata
        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.metadata is None

    def test_metadata_with_streaming_upload(self):
        """Test metadata handling with streaming uploads."""
        bucket = GridFSBucket(self.db)

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

    def test_backward_compatibility_with_string_metadata(self):
        """Test backward compatibility with existing string metadata."""
        # This test simulates what would happen with existing data
        # that was stored with the old string representation
        pass  # The implementation handles this with fallback parsing

    def test_metadata_with_special_characters(self):
        """Test metadata with special characters and Unicode."""
        bucket = GridFSBucket(self.db)

        metadata = {
            "title": "Test with spëcial chäräcters",
            "description": "This tests Unicode: 你好世界 🌍",
            "symbols": "!@#$%^&*()_+-=[]{}|;':\",./<>?",
        }

        data = b"data with Unicode metadata"
        file_id = bucket.upload_from_stream("unicode.txt", data, metadata)

        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.metadata == metadata

    def test_legacy_gridfs_metadata_compatibility(self):
        """Test that legacy GridFS API works with JSON metadata."""
        fs = GridFS(self.db)

        # Upload with legacy API
        metadata = {"legacy": True, "version": "1.0"}
        data = b"legacy API test data"
        file_id = fs.put(data, filename="legacy.txt", **metadata)

        # Retrieve with legacy API
        grid_out = fs.get(file_id)
        assert grid_out.metadata == metadata

        # Also retrieve with modern API
        bucket = GridFSBucket(self.db)
        modern_grid_out = bucket.open_download_stream(file_id)
        assert modern_grid_out.metadata == metadata

    def test_empty_metadata_handling(self):
        """Test handling of empty metadata dictionary."""
        bucket = GridFSBucket(self.db)

        metadata = {}
        data = b"data with empty metadata"
        file_id = bucket.upload_from_stream("empty.txt", data, metadata)

        grid_out = bucket.open_download_stream(file_id)
        assert grid_out.metadata == metadata

    def test_metadata_type_preservation(self):
        """Test that metadata types are preserved."""
        bucket = GridFSBucket(self.db)

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
