class NoFile(Exception):
    """Raised when trying to access a non-existent file in GridFS."""
    pass


class FileExists(Exception):
    """Raised when trying to create a file that already exists in GridFS."""
    pass


class CorruptGridFile(Exception):
    """Raised when a file in GridFS is corrupt or incomplete."""
    pass