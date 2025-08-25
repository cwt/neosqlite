import pytest
from neosqlite.gridfs.errors import (
    NeoSQLiteError,
    GridFSError,
    NoFile,
    FileExists,
    CorruptGridFile,
    PyMongoError,
)


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
        exc = NoFile("test message")
        assert isinstance(exc, GridFSError)
        assert isinstance(exc, NeoSQLiteError)
        assert isinstance(exc, Exception)

        # Test FileExists
        exc = FileExists("test message")
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
        with pytest.raises(NoFile):
            raise NoFile("file not found")

        # Test catching at GridFSError level
        with pytest.raises(GridFSError):
            raise NoFile("file not found")

        with pytest.raises(GridFSError):
            raise FileExists("file exists")

        with pytest.raises(GridFSError):
            raise CorruptGridFile("file corrupt")

        # Test catching at NeoSQLiteError level
        with pytest.raises(NeoSQLiteError):
            raise NoFile("file not found")

        with pytest.raises(NeoSQLiteError):
            raise FileExists("file exists")

        with pytest.raises(NeoSQLiteError):
            raise CorruptGridFile("file corrupt")

        # Test catching at PyMongoError alias level
        with pytest.raises(PyMongoError):
            raise NoFile("file not found")

    def test_exception_messages(self):
        """Test that exception messages are preserved."""
        message = "This is a test error message"

        exc = NoFile(message)
        assert str(exc) == message

        exc = FileExists(message)
        assert str(exc) == message

        exc = CorruptGridFile(message)
        assert str(exc) == message

        exc = PyMongoError(message)
        assert str(exc) == message

    def test_error_labels_functionality(self):
        """Test error labels functionality."""
        # Test with no error labels
        exc = NoFile("test")
        assert exc.has_error_label("any_label") is False

        # Test with error labels
        exc = NoFile("test", ["label1", "label2"])
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
        exc = NoFile("test")
        assert exc.timeout is False

        exc = FileExists("test")
        assert exc.timeout is False

        exc = CorruptGridFile("test")
        assert exc.timeout is False

        exc = PyMongoError("test")
        assert exc.timeout is False

    def test_empty_error_labels(self):
        """Test exception with empty or None error labels."""
        # Test with None error labels
        exc = NoFile("test", None)
        assert exc.has_error_label("any_label") is False

        # Test with empty error labels
        exc = NoFile("test", [])
        assert exc.has_error_label("any_label") is False
