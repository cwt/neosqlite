class NeoSQLiteError(Exception):
    """Base class for all NeoSQLite exceptions."""

    def __init__(self, message: str = "", error_labels=None):
        super().__init__(message)
        self._message = message
        self._error_labels = set(error_labels or [])

    def has_error_label(self, label: str) -> bool:
        return label in self._error_labels

    def _add_error_label(self, label: str) -> None:
        self._error_labels.add(label)

    def _remove_error_label(self, label: str) -> None:
        self._error_labels.discard(label)

    @property
    def timeout(self) -> bool:
        return False


class GridFSError(NeoSQLiteError):
    """Base class for all GridFS exceptions."""


class NoFile(GridFSError):
    """Raised when trying to access a non-existent file in GridFS."""


class FileExists(GridFSError):
    """Raised when trying to create a file that already exists in GridFS."""


class CorruptGridFile(GridFSError):
    """Raised when a file in GridFS is corrupt or incomplete."""


# PyMongo compatibility aliases
PyMongoError = NeoSQLiteError