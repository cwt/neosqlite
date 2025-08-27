from .errors import NoFile
from .grid_file import GridOut, GridOutCursor, GridIn
from .gridfs_bucket import GridFSBucket
from typing import Any, Dict, Optional, Union
import io

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


class GridFS:
    """
    A legacy GridFS interface for storing and retrieving files in SQLite.

    This class provides the legacy PyMongo-compatible GridFS interface,
    which is simpler to use than the GridFSBucket API but less flexible.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        collection_name: str = "fs",
    ):
        """
        Initialize a new GridFS instance.

        Args:
            db: SQLite database connection
            collection_name: The collection name for the GridFS files (default: "fs")
        """
        self._db = db
        self._bucket = GridFSBucket(db, bucket_name=collection_name)

    def put(
        self,
        data: Union[bytes, io.IOBase],
        filename: Optional[str] = None,
        **kwargs: Any,
    ) -> int:
        """
        Put data into GridFS.

        Args:
            data: The data to store (bytes or file-like object)
            filename: The filename to use (optional)
            **kwargs: Additional metadata fields

        Returns:
            The _id of the stored file document
        """
        # Extract metadata from kwargs
        metadata = {}
        for key, value in kwargs.items():
            # In legacy GridFS, all extra parameters become metadata except encoding
            if key != "encoding":
                metadata[key] = value

        # If filename is provided in kwargs, use it
        if filename is None and "filename" in kwargs:
            filename = kwargs["filename"]

        # Default filename if none provided
        if filename is None:
            filename = "file"

        # Upload the data
        return self._bucket.upload_from_stream(filename, data, metadata or None)

    def get(self, file_id: int) -> GridOut:
        """
        Get a file from GridFS by its _id.

        Args:
            file_id: The _id of the file to retrieve

        Returns:
            A GridOut instance for reading the file
        """
        return self._bucket.open_download_stream(file_id)

    def get_version(self, filename: str, version: int = -1) -> GridOut:
        """
        Get a file from GridFS by filename and version.

        Args:
            filename: The name of the file to retrieve
            version: The version number (-1 for latest, 0 for first, etc.)

        Returns:
            A GridOut instance for reading the file
        """
        return self._bucket.open_download_stream_by_name(filename, version)

    def get_last_version(self, filename: str) -> GridOut:
        """
        Get the most recent version of a file from GridFS by filename.

        Args:
            filename: The name of the file to retrieve

        Returns:
            A GridOut instance for reading the file
        """
        return self._bucket.open_download_stream_by_name(filename, -1)

    def delete(self, file_id: int) -> None:
        """
        Delete a file from GridFS by its _id.

        Args:
            file_id: The _id of the file to delete
        """
        try:
            self._bucket.delete(file_id)
        except NoFile:
            # Legacy GridFS API is more lenient - doesn't raise exception for nonexistent files
            pass

    def list(self) -> list:
        """
        List all filenames in GridFS.

        Returns:
            A list of filenames
        """
        cursor = self._bucket.find({})
        filenames = []
        for grid_out in cursor:
            if grid_out.filename not in filenames:
                filenames.append(grid_out.filename)
        return filenames

    def find(self, filter: Optional[Dict[str, Any]] = None) -> "GridOutCursor":
        """
        Find files in GridFS that match the filter.

        Args:
            filter: The filter to apply when searching for files

        Returns:
            A GridOutCursor instance
        """
        return self._bucket.find(filter or {})

    def find_one(
        self, filter: Optional[Dict[str, Any]] = None
    ) -> Optional[GridOut]:
        """
        Find a single file in GridFS that matches the filter.

        Args:
            filter: The filter to apply when searching for files

        Returns:
            A GridOut instance for reading the file, or None if not found
        """
        cursor = self._bucket.find(filter or {})
        try:
            return next(iter(cursor))
        except StopIteration:
            return None

    def exists(self, file_id: Optional[int] = None, **kwargs: Any) -> bool:
        """
        Check if a file exists in GridFS.

        Args:
            file_id: The _id of the file to check
            **kwargs: Additional filter criteria (e.g., filename="test.txt")

        Returns:
            True if the file exists, False otherwise
        """
        if file_id is not None:
            try:
                self._bucket.open_download_stream(file_id)
                return True
            except NoFile:
                return False
        elif kwargs:
            # Check by other criteria
            cursor = self._bucket.find(kwargs)
            try:
                next(iter(cursor))
                return True
            except StopIteration:
                return False
        else:
            # No criteria provided
            return False

    def new_file(self, **kwargs: Any) -> GridIn:
        """
        Create a new file in GridFS and return a GridIn instance to which data can be written.

        Args:
            **kwargs: Arguments to pass to the GridIn constructor (e.g., filename, metadata)

        Returns:
            A GridIn instance for writing the file contents
        """
        # Extract parameters from kwargs
        filename = kwargs.pop("filename", "file")
        metadata = kwargs.pop("metadata", None) or kwargs or None

        # Handle the case where _id is specified
        file_id = kwargs.pop("_id", None)

        if file_id is not None:
            return self._bucket.open_upload_stream_with_id(
                file_id, filename, metadata
            )
        else:
            return self._bucket.open_upload_stream(filename, metadata)


# Backward compatibility alias
GridFS.__name__ = "GridFS"
