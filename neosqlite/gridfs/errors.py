"""GridFS and NeoSQLite exception classes and PyMongo compatibility aliases."""


class NeoSQLiteError(Exception):
    """Base class for all NeoSQLite exceptions.

    Tracks an error message alongside an optional set of PyMongo-compatible
    error labels and exposes a ``timeout`` status property.
    """

    def __init__(self, message: str = "", error_labels=None):
        """Initialize a NeoSQLiteError instance.

        Args:
            message: Human-readable error message. Defaults to ``""``.
            error_labels: Optional iterable of error label strings to
                associate with this exception. Defaults to ``None``.
        """
        super().__init__(message)
        self._message = message
        self._error_labels = set(error_labels or [])

    def has_error_label(self, label: str) -> bool:
        """Return whether the exception carries the given error label.

        Args:
            label: Error label name to check.

        Returns:
            bool: ``True`` if ``label`` is set on this exception, else
                ``False``.
        """
        return label in self._error_labels

    def _add_error_label(self, label: str) -> None:
        """Add an error label to this exception instance.

        Args:
            label: Error label name to add.
        """
        self._error_labels.add(label)

    def _remove_error_label(self, label: str) -> None:
        """Remove an error label from this exception instance if present.

        Args:
            label: Error label name to discard.
        """
        self._error_labels.discard(label)

    @property
    def timeout(self) -> bool:
        """Return whether this exception represents a timeout error.

        Returns:
            bool: Always ``False`` for base ``NeoSQLiteError`` instances.
        """
        return False


class GridFSError(NeoSQLiteError):
    """Base class for all GridFS exceptions.

    Subclasses ``NeoSQLiteError`` to group errors raised by GridFS file,
    bucket, and stream operations.
    """


class NoFile(GridFSError):
    """Raised when trying to access a non-existent file in GridFS.

    Triggered by download, lookup, or deletion operations targeting a file ID
    or filename that does not exist in the GridFS bucket.
    """


class FileExists(GridFSError):
    """Raised when trying to create a file that already exists in GridFS.

    Triggered when uploading or creating a file whose identifier conflicts with
    an existing entry in the GridFS files collection.
    """


class CorruptGridFile(GridFSError):
    """Raised when a file in GridFS is corrupt or incomplete.

    Triggered when chunk counts, chunk sizes, or stored chunk payloads do not
    match the expected GridFS file metadata.
    """


# PyMongo compatibility aliases
PyMongoError = NeoSQLiteError
