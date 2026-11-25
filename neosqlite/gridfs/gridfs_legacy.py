"""Legacy PyMongo-compatible GridFS interface backed by SQLite."""

import io
import logging
from typing import Any

from .._sqlite import sqlite3

# Import ObjectId for GridFS
from ..objectid import ObjectId
from .errors import NoFile
from .grid_file import GridIn, GridOut, GridOutCursor
from .gridfs_bucket import GridFSBucket

logger = logging.getLogger(__name__)


class GridFS:
    """Legacy GridFS interface for storing and retrieving files in SQLite.

    Provides the legacy PyMongo-compatible ``GridFS`` wrapper over
    ``GridFSBucket``, offering simpler one-shot file helpers than the bucket
    stream API.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        collection_name: str = "fs",
    ):
        """Initialize a new GridFS instance.

        Args:
            db: SQLite database connection.
            collection_name: Base collection name for the GridFS files and
                chunks. Defaults to ``"fs"``.
        """
        self._db = db
        self._bucket = GridFSBucket(db, bucket_name=collection_name)

    def put(
        self,
        data: bytes | io.IOBase,
        filename: str | None = None,
        **kwargs: Any,
    ) -> ObjectId:
        """Put data into GridFS.

        Extracts optional metadata fields from ``kwargs`` (excluding
        ``"encoding"``), resolves a filename fallback (from ``kwargs`` or
        ``"file"``), and uploads the payload via ``upload_from_stream``.

        Args:
            data: File payload as ``bytes`` or a readable file-like object.
            filename: Optional filename to store on the file document.
                Defaults to ``None``.
            **kwargs: Additional metadata fields (and optional ``filename``).

        Returns:
            ObjectId: The ``ObjectId`` of the stored file document.
        """
        # Extract metadata from kwargs
        metadata = {
            key: value for key, value in kwargs.items() if key != "encoding"
        }

        # If filename is provided in kwargs, use it
        if filename is None and "filename" in kwargs:
            filename = kwargs["filename"]

        # Default filename if none provided
        if filename is None:
            filename = "file"

        # Upload the data - this will return an ObjectId
        return self._bucket.upload_from_stream(
            filename, data, metadata=metadata or None
        )

    def get(self, file_id: ObjectId | str | int) -> GridOut:
        """Get a file from GridFS by its ``_id``.

        Args:
            file_id: The ``_id`` of the file to retrieve (``ObjectId``, hex
                string, or integer ID).

        Returns:
            GridOut: A stream instance for reading the file.
        """
        return self._bucket.open_download_stream(file_id)

    def get_version(self, filename: str, version: int = -1) -> GridOut:
        """Get a file from GridFS by filename and revision number.

        Args:
            filename: Name of the file to retrieve.
            version: Revision index (``-1`` for latest, ``0`` for first, etc.).
                Defaults to ``-1``.

        Returns:
            GridOut: A stream instance for reading the file.
        """
        return self._bucket.open_download_stream_by_name(filename, version)

    def get_last_version(self, filename: str) -> GridOut:
        """Get the most recent version of a file from GridFS by filename.

        Args:
            filename: Name of the file to retrieve.

        Returns:
            GridOut: A stream instance for reading the latest revision.
        """
        return self._bucket.open_download_stream_by_name(filename, -1)

    def delete(self, file_id: ObjectId | str | int) -> None:
        """Delete a file from GridFS by its ``_id``.

        Matches legacy PyMongo ``GridFS.delete`` semantics by ignoring
        ``NoFile`` when the target file does not exist.

        Args:
            file_id: The ``_id`` of the file to delete (``ObjectId``, hex
                string, or integer ID).
        """
        try:
            self._bucket.delete(file_id)
        except NoFile as e:
            # Legacy GridFS API is more lenient - doesn't raise exception for nonexistent files
            logger.debug(
                f"Legacy GridFS delete: file '{file_id}' not found: {e}"
            )
            pass

    def list(self) -> list:
        """List all unique filenames in GridFS.

        Returns:
            list: Distinct filenames stored in GridFS.
        """
        return self._bucket.list()

    def find(self, filter: dict[str, Any] | None = None) -> GridOutCursor:
        """Find files in GridFS that match the filter.

        Args:
            filter: Optional query filter dictionary applied to the files
                collection. Defaults to ``None``.

        Returns:
            GridOutCursor: Cursor over matching ``GridOut`` file streams.
        """
        return self._bucket.find(filter or {})

    def find_one(self, filter: dict[str, Any] | None = None) -> GridOut | None:
        """Find a single file in GridFS that matches the filter.

        Args:
            filter: Optional query filter dictionary applied to the files
                collection. Defaults to ``None``.

        Returns:
            GridOut | None: The first matching ``GridOut`` stream, or ``None``
                if no file matches.
        """
        cursor = self._bucket.find(filter or {})
        try:
            return next(iter(cursor))
        except StopIteration:
            return None

    def exists(
        self, file_id: ObjectId | str | int | None = None, **kwargs: Any
    ) -> bool:
        """Check if a file exists in GridFS by ID or filter criteria.

        Args:
            file_id: Optional ``_id`` of the file to check (``ObjectId``, hex
                string, or integer ID). Defaults to ``None``.
            **kwargs: Additional filter criteria (for example,
                ``filename="test.txt"``) used when ``file_id`` is ``None``.

        Returns:
            bool: ``True`` if a matching file exists, ``False`` otherwise.
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
        """Create a new file in GridFS and return a writable GridIn stream.

        Uses ``filename`` (defaulting to ``"file"``), ``metadata`` (or any
        remaining keyword arguments), and optional ``_id`` to open either a
        custom-ID or auto-ID upload stream.

        Args:
            **kwargs: File attributes and upload options such as
                ``filename``, ``metadata``, and ``_id``.

        Returns:
            GridIn: Writable stream for writing the new file's contents.
        """
        # Extract parameters from kwargs
        filename = kwargs.pop("filename", "file")
        metadata = kwargs.pop("metadata", None) or kwargs or None

        # Handle the case where _id is specified
        file_id = kwargs.pop("_id", None)

        if file_id is not None:
            return self._bucket.open_upload_stream_with_id(
                file_id, filename, metadata=metadata
            )
        else:
            return self._bucket.open_upload_stream(filename, metadata=metadata)


# Backward compatibility alias
GridFS.__name__ = "GridFS"
