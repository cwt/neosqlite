from .errors import NoFile
from typing import Any, Dict, Optional, Union
import datetime
import hashlib
import json

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


class GridIn:
    """
    A file-like object for writing data to GridFS.

    This class provides a writable interface for storing files in GridFS.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str,
        chunk_size_bytes: int,
        filename: str,
        metadata: Optional[Dict[str, Any]] = None,
        file_id: Optional[int] = None,
        disable_md5: bool = False,
        write_concern: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a new GridIn instance.

        Args:
            db: SQLite database connection
            bucket_name: The bucket name for the GridFS files
            chunk_size_bytes: The chunk size in bytes
            filename: The name of the file
            metadata: Optional metadata for the file
            file_id: Optional custom file ID
            disable_md5: Disable MD5 checksum calculation for performance
            write_concern: Write concern settings (simulated for compatibility)
        """
        self._db = db
        self._bucket_name = bucket_name
        self._chunk_size_bytes = chunk_size_bytes
        self._filename = filename
        self._metadata = metadata
        self._file_id = file_id
        self._files_collection = f"{bucket_name}.files"
        self._chunks_collection = f"{bucket_name}.chunks"
        self._disable_md5 = disable_md5
        self._write_concern = write_concern or {}

        # Stream state
        self._buffer = bytearray()
        self._chunk_number = 0
        self._position = 0
        self._closed = False
        self._md5_hasher = None if disable_md5 else hashlib.md5()

    def _serialize_metadata(
        self, metadata: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Serialize metadata to JSON string.

        Args:
            metadata: Metadata dictionary to serialize

        Returns:
            JSON string representation or None
        """
        if metadata is None:
            return None
        try:
            return json.dumps(metadata)
        except (TypeError, ValueError):
            # Fallback to string representation if JSON serialization fails
            return str(metadata)

    def _deserialize_metadata(
        self, metadata_str: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Deserialize metadata from JSON string.

        Args:
            metadata_str: JSON string representation of metadata

        Returns:
            Metadata dictionary or None
        """
        if metadata_str is None:
            return None
        try:
            return json.loads(metadata_str)
        except (TypeError, ValueError, json.JSONDecodeError):
            # Fallback to parsing as Python literal (for backward compatibility)
            try:
                # Try to evaluate as Python literal (for backward compatibility)
                import ast

                result = ast.literal_eval(metadata_str)
                if isinstance(result, dict):
                    return result
            except (ValueError, SyntaxError):
                pass
            # Return as simple string in a dict for backward compatibility
            return {"_metadata": metadata_str}

    def _force_sync_if_needed(self):
        """Force database synchronization if write concern requires it."""
        if (
            self._write_concern.get("j") is True
            or self._write_concern.get("w") == "majority"
        ):
            # Force sync to disk for maximum durability
            self._db.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def write(self, data: Union[bytes, bytearray]) -> int:
        """
        Write data to the GridIn stream.

        Args:
            data: The data to write

        Returns:
            The number of bytes written
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes or bytearray")

        # Add data to buffer
        self._buffer.extend(data)
        self._position += len(data)
        if self._md5_hasher:
            self._md5_hasher.update(data)

        # Flush chunks if buffer is full
        while len(self._buffer) >= self._chunk_size_bytes:
            self._flush_chunk()

        return len(data)

    def _flush_chunk(self) -> None:
        """Flush a chunk from the buffer to the database."""
        if len(self._buffer) >= self._chunk_size_bytes:
            # Extract a chunk from the buffer
            chunk_data = bytes(self._buffer[: self._chunk_size_bytes])
            del self._buffer[: self._chunk_size_bytes]

            # If this is the first chunk, create the file document
            if self._chunk_number == 0 and self._file_id is None:
                self._create_file_document()

            # Insert the chunk
            self._db.execute(
                f"""
                INSERT INTO `{self._chunks_collection}`
                (files_id, n, data)
                VALUES (?, ?, ?)
            """,
                (self._get_file_id(), self._chunk_number, chunk_data),
            )

            self._chunk_number += 1

    def _create_file_document(self) -> None:
        """Create the file document in the files collection."""
        upload_date = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if self._file_id is None:
            cursor = self._db.execute(
                f"""
                INSERT INTO `{self._files_collection}`
                (filename, chunkSize, uploadDate, metadata)
                VALUES (?, ?, ?, ?)
            """,
                (
                    self._filename,
                    self._chunk_size_bytes,
                    upload_date,
                    self._serialize_metadata(self._metadata),
                ),
            )
            self._file_id = cursor.lastrowid
        else:
            self._db.execute(
                f"""
                INSERT INTO `{self._files_collection}`
                (_id, filename, chunkSize, uploadDate, metadata)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    self._file_id,
                    self._filename,
                    self._chunk_size_bytes,
                    upload_date,
                    self._serialize_metadata(self._metadata),
                ),
            )

    def _get_file_id(self) -> int:
        """Get the file ID, creating the file document if necessary."""
        if self._file_id is None:
            self._create_file_document()
        return self._file_id  # type: ignore

    def close(self) -> None:
        """Close the GridIn stream and finalize the file storage."""
        if self._closed:
            return

        # Flush any remaining data in the buffer
        if self._buffer or self._chunk_number == 0:
            # If no chunks have been written yet, we still need to create the file
            if self._chunk_number == 0:
                self._create_file_document()

            # Write the final chunk (which may be smaller than chunk_size_bytes)
            if self._buffer:
                self._db.execute(
                    f"""
                    INSERT INTO `{self._chunks_collection}`
                    (files_id, n, data)
                    VALUES (?, ?, ?)
                """,
                    (
                        self._get_file_id(),
                        self._chunk_number,
                        bytes(self._buffer),
                    ),
                )
                self._chunk_number += 1

            # Update the file document with final metadata
            md5_hash = None
            if self._md5_hasher:
                md5_hash = self._md5_hasher.hexdigest()
            self._db.execute(
                f"""
                UPDATE `{self._files_collection}`
                SET length = ?, md5 = ?
                WHERE _id = ?
            """,
                (self._position, md5_hash, self._get_file_id()),
            )

        # Force sync if write concern requires it
        self._force_sync_if_needed()

        self._closed = True

    def __enter__(self) -> "GridIn":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


class GridOut:
    """
    A file-like object for reading data from GridFS.

    This class provides a readable interface for retrieving files from GridFS.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str,
        file_id: int,
    ):
        """
        Initialize a new GridOut instance.

        Args:
            db: SQLite database connection
            bucket_name: The bucket name for the GridFS files
            file_id: The ID of the file to read
        """
        self._db = db
        self._bucket_name = bucket_name
        self._file_id = file_id
        self._files_collection = f"{bucket_name}.files"
        self._chunks_collection = f"{bucket_name}.chunks"

        # Get file metadata
        row = self._db.execute(
            f"""
            SELECT filename, length, chunkSize, uploadDate, md5, metadata
            FROM `{self._files_collection}`
            WHERE _id = ?
        """,
            (file_id,),
        ).fetchone()

        if row is None:
            raise NoFile(f"File with id {file_id} not found")

        (
            self._filename,
            self._length,
            self._chunk_size,
            self._upload_date,
            self._md5,
            metadata_str,
        ) = row
        self._metadata = self._deserialize_metadata(metadata_str)
        self._position = 0
        self._current_chunk_data = b""
        self._current_chunk_index = -1
        self._closed = False

    def _deserialize_metadata(
        self, metadata_str: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Deserialize metadata from JSON string.

        Args:
            metadata_str: JSON string representation of metadata

        Returns:
            Metadata dictionary or None
        """
        if metadata_str is None:
            return None
        try:
            return json.loads(metadata_str)
        except (TypeError, ValueError, json.JSONDecodeError):
            # Fallback to parsing as Python literal (for backward compatibility)
            try:
                # Try to evaluate as Python literal (for backward compatibility)
                import ast

                result = ast.literal_eval(metadata_str)
                if isinstance(result, dict):
                    return result
            except (ValueError, SyntaxError):
                pass
            # Return as simple string in a dict for backward compatibility
            return {"_metadata": metadata_str}

    def read(self, size: int = -1) -> bytes:
        """
        Read data from the GridOut stream.

        Args:
            size: The number of bytes to read (-1 for all remaining data)

        Returns:
            The data read from the stream
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            # Read all remaining data
            size = self._length - self._position

        if size <= 0:
            return b""

        # Calculate which chunks we need
        result = bytearray()
        bytes_read = 0

        while bytes_read < size and self._position < self._length:
            # Load the current chunk if needed
            self._load_chunk()

            # Calculate how much we can read from the current chunk
            chunk_offset = self._position % self._chunk_size
            bytes_available_in_chunk = (
                len(self._current_chunk_data) - chunk_offset
            )
            bytes_to_read = min(size - bytes_read, bytes_available_in_chunk)

            # Read from the current chunk
            result.extend(
                self._current_chunk_data[
                    chunk_offset : chunk_offset + bytes_to_read
                ]
            )

            # Update position
            self._position += bytes_to_read
            bytes_read += bytes_to_read

            # Move to next chunk if we've exhausted the current one
            if self._position % self._chunk_size == 0:
                self._current_chunk_index += 1
                self._current_chunk_data = b""

        return bytes(result)

    def _load_chunk(self) -> None:
        """Load the chunk containing the current position."""
        chunk_index = self._position // self._chunk_size

        # If we already have the right chunk, we're done
        if chunk_index == self._current_chunk_index:
            return

        # Load the required chunk
        row = self._db.execute(
            f"""
            SELECT data FROM `{self._chunks_collection}`
            WHERE files_id = ? AND n = ?
        """,
            (self._file_id, chunk_index),
        ).fetchone()

        if row is None:
            raise NoFile(
                f"Chunk {chunk_index} for file id {self._file_id} not found"
            )

        self._current_chunk_data = row[0]
        self._current_chunk_index = chunk_index

    @property
    def filename(self) -> str:
        """Get the filename."""
        return self._filename

    @property
    def length(self) -> int:
        """Get the length of the file in bytes."""
        return self._length

    @property
    def chunk_size(self) -> int:
        """Get the chunk size in bytes."""
        return self._chunk_size

    @property
    def upload_date(self) -> str:
        """Get the upload date."""
        return self._upload_date

    @property
    def md5(self) -> str:
        """Get the MD5 hash of the file."""
        return self._md5

    @property
    def metadata(self) -> Optional[Dict[str, Any]]:
        """Get the metadata of the file."""
        return self._metadata

    def close(self) -> None:
        """Close the GridOut stream."""
        self._closed = True

    def __enter__(self) -> "GridOut":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


class GridOutCursor:
    """
    A cursor for iterating over GridFS files.

    This class provides an iterator interface for retrieving file documents from GridFS.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str,
        filter: Dict[str, Any],
    ):
        """
        Initialize a new GridOutCursor instance.

        Args:
            db: SQLite database connection
            bucket_name: The bucket name for the GridFS files
            filter: The filter to apply when searching for files
        """
        self._db = db
        self._bucket_name = bucket_name
        self._filter = filter
        self._files_collection = f"{bucket_name}.files"

        # Build query based on filter
        where_clause = ""
        params = []

        if filter:
            where_conditions = []
            for key, value in filter.items():
                if key == "_id":
                    where_conditions.append("_id = ?")
                    params.append(value)
                elif key == "filename":
                    if isinstance(value, dict):
                        # Handle operators like {"$regex": "pattern"}, {"$ne": "name"}, etc.
                        for op, val in value.items():
                            if op == "$regex":
                                where_conditions.append("filename LIKE ?")
                                params.append(f"%{val}%")
                            elif op == "$ne":
                                where_conditions.append("filename != ?")
                                params.append(val)
                            elif op == "$eq":
                                where_conditions.append("filename = ?")
                                params.append(val)
                            else:
                                # For unsupported operators, fall back to exact match
                                where_conditions.append("filename = ?")
                                params.append(str(value))
                    else:
                        # Direct value comparison
                        where_conditions.append("filename = ?")
                        params.append(value)
                elif key == "length":
                    if isinstance(value, dict):
                        # Handle operators like {"$gt": 1000}, {"$lt": 5000}, etc.
                        for op, val in value.items():
                            if op == "$gt":
                                where_conditions.append("length > ?")
                                params.append(val)
                            elif op == "$gte":
                                where_conditions.append("length >= ?")
                                params.append(val)
                            elif op == "$lt":
                                where_conditions.append("length < ?")
                                params.append(val)
                            elif op == "$lte":
                                where_conditions.append("length <= ?")
                                params.append(val)
                            elif op == "$eq":
                                where_conditions.append("length = ?")
                                params.append(val)
                            elif op == "$ne":
                                where_conditions.append("length != ?")
                                params.append(val)
                    else:
                        # Direct value comparison
                        where_conditions.append("length = ?")
                        params.append(value)
                elif key == "chunkSize":
                    if isinstance(value, dict):
                        # Handle operators like {"$gt": 1000}, {"$lt": 5000}, etc.
                        for op, val in value.items():
                            if op == "$gt":
                                where_conditions.append("chunkSize > ?")
                                params.append(val)
                            elif op == "$gte":
                                where_conditions.append("chunkSize >= ?")
                                params.append(val)
                            elif op == "$lt":
                                where_conditions.append("chunkSize < ?")
                                params.append(val)
                            elif op == "$lte":
                                where_conditions.append("chunkSize <= ?")
                                params.append(val)
                            elif op == "$eq":
                                where_conditions.append("chunkSize = ?")
                                params.append(val)
                            elif op == "$ne":
                                where_conditions.append("chunkSize != ?")
                                params.append(val)
                    else:
                        # Direct value comparison
                        where_conditions.append("chunkSize = ?")
                        params.append(value)
                elif key == "uploadDate":
                    if isinstance(value, dict):
                        # Handle operators like {"$gt": date}, {"$lt": date}, etc.
                        for op, val in value.items():
                            if op == "$gt":
                                where_conditions.append("uploadDate > ?")
                                params.append(val)
                            elif op == "$gte":
                                where_conditions.append("uploadDate >= ?")
                                params.append(val)
                            elif op == "$lt":
                                where_conditions.append("uploadDate < ?")
                                params.append(val)
                            elif op == "$lte":
                                where_conditions.append("uploadDate <= ?")
                                params.append(val)
                            elif op == "$eq":
                                where_conditions.append("uploadDate = ?")
                                params.append(val)
                            elif op == "$ne":
                                where_conditions.append("uploadDate != ?")
                                params.append(val)
                    else:
                        # Direct value comparison
                        where_conditions.append("uploadDate = ?")
                        params.append(value)
                elif key == "md5":
                    if isinstance(value, dict) and "$ne" in value:
                        where_conditions.append("md5 != ?")
                        params.append(value["$ne"])
                    else:
                        # Direct value comparison
                        where_conditions.append("md5 = ?")
                        params.append(value)
                # For metadata, we do a simple string match (basic implementation)
                # In a full implementation, we'd parse the JSON, but for now we'll do substring matching
                elif key == "metadata":
                    if isinstance(value, dict):
                        # Handle metadata queries with operators
                        for op, val in value.items():
                            if op == "$regex":
                                where_conditions.append("metadata LIKE ?")
                                params.append(f"%{val}%")
                            elif op == "$ne":
                                where_conditions.append("metadata != ?")
                                params.append(
                                    str(val)
                                    if not isinstance(val, str)
                                    else val
                                )
                            else:
                                # For other operators, convert to string and match
                                where_conditions.append("metadata LIKE ?")
                                params.append(f"%{op}%{val}%")
                    else:
                        # Direct metadata string matching
                        where_conditions.append("metadata LIKE ?")
                        params.append(f"%{value}%")

            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

        # Execute query to get file IDs
        query = f"SELECT _id FROM `{self._files_collection}` {where_clause}"
        cursor = self._db.execute(query, params)
        self._file_ids = [row[0] for row in cursor.fetchall()]
        self._index = 0

    def __iter__(self) -> "GridOutCursor":
        """Return the iterator object."""
        return self

    def __next__(self) -> GridOut:
        """Get the next GridOut object."""
        if self._index >= len(self._file_ids):
            raise StopIteration

        file_id = self._file_ids[self._index]
        self._index += 1
        return GridOut(self._db, self._bucket_name, file_id)
