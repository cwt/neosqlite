from .errors import NoFile, FileExists
from .grid_file import GridIn, GridOut, GridOutCursor
from typing import Any, Dict
import datetime
import hashlib
import io
import json

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore

# Import ObjectId for MongoDB-compatible ID support
from ..objectid import ObjectId

# Import JSONB support utilities to reuse existing implementation
from ..collection.jsonb_support import supports_jsonb

# Import _normalize_id_query to reuse existing ID normalization logic
from ..collection.query_helper import QueryHelper


class GridFSBucket:
    """
    A GridFSBucket-like class for storing large files in SQLite.

    This implementation provides a PyMongo-compatible interface for GridFS
    functionality using SQLite as the backend storage.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str = "fs",
        chunk_size_bytes: int = 255 * 1024,  # 255KB default chunk size
        write_concern: Dict[str, Any] | None = None,
        read_preference: Any | None = None,
        disable_md5: bool = False,
    ):
        """
        Initialize a new GridFSBucket instance.

        Args:
            db: SQLite database connection
            bucket_name: The bucket name for the GridFS files (default: "fs")
            chunk_size_bytes: The chunk size in bytes (default: 255KB)
            write_concern: Write concern settings (simulated for compatibility)
            read_preference: Read preference settings (not applicable to SQLite)
            disable_md5: Disable MD5 checksum calculation for performance
        """
        self._db = db
        self._bucket_name = bucket_name
        self._chunk_size_bytes = chunk_size_bytes
        self._files_collection = f"{bucket_name}.files"
        self._chunks_collection = f"{bucket_name}.chunks"

        # Process write concern settings
        self._write_concern = write_concern or {}
        self._read_preference = read_preference
        self._disable_md5 = disable_md5

        # Apply write concern settings to SQLite
        self._apply_write_concern()

        # Validate chunk size
        if chunk_size_bytes <= 0:
            raise ValueError("chunk_size_bytes must be a positive integer")

        # Validate write concern settings (basic validation for compatibility)
        if write_concern:
            # Basic validation - in a real implementation, you might want to do more
            if "w" in write_concern and not isinstance(
                write_concern["w"], (int, str)
            ):
                raise ValueError(
                    "write_concern 'w' must be an integer or string"
                )
            if "wtimeout" in write_concern and not isinstance(
                write_concern["wtimeout"], int
            ):
                raise ValueError("write_concern 'wtimeout' must be an integer")
            if "j" in write_concern and not isinstance(
                write_concern["j"], bool
            ):
                raise ValueError("write_concern 'j' must be a boolean")

        # Create the necessary tables if they don't exist
        self._create_collections()

    def _apply_write_concern(self):
        """Apply write concern settings to SQLite connection."""
        # Handle journal concern (j=True)
        if self._write_concern.get("j") is True:
            # Set synchronous to FULL for maximum durability
            self._db.execute("PRAGMA synchronous = FULL")
        else:
            # Default behavior (NORMAL is a good balance)
            self._db.execute("PRAGMA synchronous = NORMAL")

        # Handle write acknowledgment level
        w_level = self._write_concern.get("w", 1)
        if w_level == 0:
            # No acknowledgment - set to OFF for maximum performance
            self._db.execute("PRAGMA synchronous = OFF")

    def _create_collections(self):
        """Create the files and chunks collections (tables) if they don't exist."""
        # Check if JSONB is supported using the established utility
        jsonb_supported = supports_jsonb(self._db)

        # Create files collection (table)
        if jsonb_supported:
            self._db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self._files_collection}` (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    _id JSONB,
                    filename TEXT,
                    length INTEGER,
                    chunkSize INTEGER,
                    uploadDate TEXT,
                    md5 TEXT,
                    metadata TEXT
                )
            """
            )
        else:
            self._db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self._files_collection}` (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    _id TEXT,
                    filename TEXT,
                    length INTEGER,
                    chunkSize INTEGER,
                    uploadDate TEXT,
                    md5 TEXT,
                    metadata TEXT
                )
            """
            )

        # Create chunks collection (table) - no change needed
        self._db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{self._chunks_collection}` (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                files_id INTEGER,
                n INTEGER,
                data BLOB,
                FOREIGN KEY (files_id) REFERENCES `{self._files_collection}` (id)
            )
        """
        )

        # Create indexes for better performance
        self._db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS `idx_{self._files_collection}_filename`
            ON `{self._files_collection}` (filename)
        """
        )

        self._db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS `idx_{self._chunks_collection}_files_id`
            ON `{self._chunks_collection}` (files_id)
        """
        )

        # Create unique index on _id column for faster lookups
        try:
            self._db.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS `idx_{self._files_collection}_id` ON `{self._files_collection}`(_id)"
            )
        except Exception:
            # If we can't create the index (e.g., due to duplicate values), continue without it
            pass

        # Create indexes for better performance
        self._db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS `idx_{self._files_collection}_filename`
            ON `{self._files_collection}` (filename)
        """
        )

        self._db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS `idx_{self._chunks_collection}_files_id`
            ON `{self._chunks_collection}` (files_id)
        """
        )

    def _serialize_metadata(
        self, metadata: Dict[str, Any] | None
    ) -> str | None:
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
        self, metadata_str: str | None
    ) -> Dict[str, Any] | None:
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
            # Fallback to parsing as dictionary if possible, otherwise return as-is
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
            # Note: In pysqlite, we can't directly call fsync on the file,
            # but we can force SQLite to flush its buffers

    def upload_from_stream(
        self,
        filename: str,
        source: bytes | io.IOBase,
        metadata: Dict[str, Any] | None = None,
    ) -> ObjectId:
        """
        Uploads a user file to a GridFS bucket.

        Reads the contents of the user file from source and uploads it
        as chunks in the chunks collection. After all the chunks have
        been uploaded, it creates a file document in the files collection.

        Args:
            filename: The name of the file to upload
            source: The source data (bytes or file-like object)
            metadata: Optional metadata for the file

        Returns:
            The ObjectId of the uploaded file document
        """
        # Get the data from the source
        if isinstance(source, bytes):
            data = source
        elif hasattr(source, "read"):
            data = source.read()
        else:
            raise TypeError("source must be bytes or a file-like object")

        # Calculate MD5 hash of the data (unless disabled)
        md5_hash = None
        if not self._disable_md5:
            md5_hash = hashlib.md5(data).hexdigest()

        # Generate ObjectId for the file
        file_oid = ObjectId()

        # Insert file metadata first
        upload_date = datetime.datetime.now(datetime.timezone.utc).isoformat()

        cursor = self._db.execute(
            f"""
            INSERT INTO `{self._files_collection}`
            (id, _id, filename, length, chunkSize, uploadDate, md5, metadata)
            VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                str(file_oid),  # Store ObjectId as hex string
                filename,
                len(data),
                self._chunk_size_bytes,
                upload_date,
                md5_hash,
                self._serialize_metadata(metadata),
            ),
        )

        file_id = cursor.lastrowid
        if file_id is None:
            raise RuntimeError("Failed to get file ID")

        # Split data into chunks and insert them
        self._insert_chunks(file_id, data)

        # Force sync if write concern requires it
        self._force_sync_if_needed()

        return file_oid

    def _insert_chunks(self, file_id: int, data: bytes):
        """
        Split data into chunks and insert them into the chunks collection.

        Args:
            file_id: The ID of the file document
            data: The data to be chunked
        """
        # Split data into chunks
        for i in range(0, len(data), self._chunk_size_bytes):
            chunk_data = data[i : i + self._chunk_size_bytes]

            self._db.execute(
                f"""
                INSERT INTO `{self._chunks_collection}`
                (files_id, n, data)
                VALUES (?, ?, ?)
            """,
                (file_id, i // self._chunk_size_bytes, chunk_data),
            )

    def download_to_stream(
        self, file_id: ObjectId | str | int, destination: io.IOBase
    ) -> None:
        """
        Downloads the contents of the stored file specified by file_id
        and writes the contents to destination.

        Args:
            file_id: The _id of the file document (ObjectId, hex string, or integer ID)
            destination: A file-like object to which the file contents will be written
        """
        # Convert file_id to appropriate format for lookup
        file_int_id = self._get_integer_id_for_file(file_id)
        if file_int_id is None:
            raise NoFile(f"File with id {file_id} not found")

        # Get file metadata using integer ID
        row = self._db.execute(
            f"""
            SELECT length, chunkSize FROM `{self._files_collection}`
            WHERE id = ?
        """,
            (file_int_id,),
        ).fetchone()

        if row is None:
            raise NoFile(f"File with id {file_id} not found")

        # Get all chunks in order
        cursor = self._db.execute(
            f"""
            SELECT data FROM `{self._chunks_collection}`
            WHERE files_id = ?
            ORDER BY n ASC
        """,
            (file_int_id,),
        )

        # Write chunks to destination
        for chunk_row in cursor:
            destination.write(chunk_row[0])

    def _get_integer_id_for_file(
        self, file_id: ObjectId | str | int
    ) -> int | None:
        """
        Convert a file identifier (ObjectId, hex string, or integer) to an integer ID.

        Args:
            file_id: The file identifier (ObjectId, hex string, or integer ID)

        Returns:
            The integer ID corresponding to the file, or None if not found
        """
        if isinstance(file_id, ObjectId):
            # Look up by _id column containing ObjectId hex string
            cursor = self._db.execute(
                f"SELECT id FROM `{self._files_collection}` WHERE _id = ?",
                (str(file_id),),
            )
        elif isinstance(file_id, str) and len(file_id) == 24:
            # Check if it's a valid ObjectId hex string
            try:
                ObjectId(file_id)  # Validate the hex string
                cursor = self._db.execute(
                    f"SELECT id FROM `{self._files_collection}` WHERE _id = ?",
                    (file_id,),
                )
            except ValueError:
                # Not a valid ObjectId hex string, treat as integer string
                try:
                    int_file_id = int(file_id)
                    cursor = self._db.execute(
                        f"SELECT id FROM `{self._files_collection}` WHERE id = ?",
                        (int_file_id,),
                    )
                except ValueError:
                    # Not an integer string either
                    return None
        elif isinstance(file_id, int):
            cursor = self._db.execute(
                f"SELECT id FROM `{self._files_collection}` WHERE id = ?",
                (file_id,),
            )
        else:
            return None

        row = cursor.fetchone()
        return row[0] if row else None

    def download_to_stream_by_name(
        self, filename: str, destination: io.IOBase, revision: int = -1
    ) -> None:
        """
        Downloads the contents of the stored file specified by filename
        and writes the contents to destination.

        Args:
            filename: The name of the file to download
            destination: A file-like object to which the file contents will be written
            revision: The revision of the file to download (default: -1 for latest)
        """
        file_id = self._get_file_id_by_name(filename, revision)
        self.download_to_stream(file_id, destination)

    def _get_file_id_by_name(self, filename: str, revision: int = -1) -> int:
        """
        Get the file ID for a given filename and revision.

        Args:
            filename: The name of the file
            revision: The revision number (-1 for latest, 0 for first, etc.)

        Returns:
            The integer _id of the file document
        """
        if revision == -1:
            # Get the latest revision
            row = self._db.execute(
                f"""
                SELECT id FROM `{self._files_collection}`
                WHERE filename = ?
                ORDER BY uploadDate DESC
                LIMIT 1
            """,
                (filename,),
            ).fetchone()
        else:
            # Get specific revision (0-indexed)
            row = self._db.execute(
                f"""
                SELECT id FROM `{self._files_collection}`
                WHERE filename = ?
                ORDER BY uploadDate ASC
                LIMIT 1 OFFSET ?
            """,
                (filename, revision),
            ).fetchone()

        if row is None:
            raise NoFile(f"File with name {filename} not found")

        return row[0]

    def open_download_stream(self, file_id: ObjectId | str | int) -> GridOut:
        """
        Opens a stream to read the contents of the stored file specified by file_id.

        Args:
            file_id: The _id of the file document (ObjectId, hex string, or integer ID)

        Returns:
            A GridOut instance to read the file contents
        """
        # Convert to integer ID for internal use
        file_int_id = self._get_integer_id_for_file(file_id)
        if file_int_id is None:
            raise NoFile(f"File with id {file_id} not found")
        return GridOut(self._db, self._bucket_name, file_int_id)

    def open_download_stream_by_name(
        self, filename: str, revision: int = -1
    ) -> GridOut:
        """
        Opens a stream to read the contents of the stored file specified by filename.

        Args:
            filename: The name of the file to read
            revision: The revision of the file to read (default: -1 for latest)

        Returns:
            A GridOut instance to read the file contents
        """
        file_id = self._get_file_id_by_name(filename, revision)
        return GridOut(self._db, self._bucket_name, file_id)

    def delete(self, file_id: ObjectId | str | int) -> None:
        """
        Given a file_id, delete the stored file's files collection document
        and associated chunks from a GridFS bucket.

        Args:
            file_id: The _id of the file document (ObjectId, hex string, or integer ID)
        """
        # Convert to integer ID for internal use
        file_int_id = self._get_integer_id_for_file(file_id)
        if file_int_id is None:
            raise NoFile(f"File with id {file_id} not found")

        # Delete chunks first
        self._db.execute(
            f"""
            DELETE FROM `{self._chunks_collection}`
            WHERE files_id = ?
        """,
            (file_int_id,),
        )

        # Delete file document
        cursor = self._db.execute(
            f"""
            DELETE FROM `{self._files_collection}`
            WHERE id = ?
        """,
            (file_int_id,),
        )

        if cursor.rowcount == 0:
            raise NoFile(f"File with id {file_id} not found")

    def find(self, filter: Dict[str, Any] | None = None) -> GridOutCursor:
        """
        Find and return the files collection documents that match filter.

        Args:
            filter: The filter to apply when searching for files

        Returns:
            A GridOutCursor instance
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        # or other common type mismatches, reusing the collection's implementation
        if filter is not None:
            # Create a temporary QueryHelper instance to use its _normalize_id_query method
            # This is how the collection system does it - by creating a temporary helper
            temp_helper = QueryHelper(self._get_mock_collection())
            filter = temp_helper._normalize_id_query(filter)
        return GridOutCursor(self._db, self._bucket_name, filter or {})

    def _get_mock_collection(self):
        """Create a minimal mock collection object to use QueryHelper's methods."""

        class MockCollection:
            def __init__(self, db, name):
                self.db = db
                self.name = name

        return MockCollection(self._db, self._bucket_name + ".files")

    def _normalize_id_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize ID types in a query dictionary to correct common mismatches.
        This reuses the exact same logic as the collection's implementation.

        Args:
            query: The query dictionary to process

        Returns:
            A new query dictionary with corrected ID types
        """
        # Create a temporary QueryHelper instance to reuse the exact same logic
        temp_helper = QueryHelper(self._get_mock_collection())
        return temp_helper._normalize_id_query(query)

    def open_upload_stream(
        self,
        filename: str,
        metadata: Dict[str, Any] | None = None,
    ) -> GridIn:
        """
        Opens a stream for writing a file to a GridFS bucket.

        Args:
            filename: The name of the file to upload
            metadata: Optional metadata for the file

        Returns:
            A GridIn instance to write the file contents
        """
        return GridIn(
            self._db,
            self._bucket_name,
            self._chunk_size_bytes,
            filename,
            metadata,
            disable_md5=self._disable_md5,
            write_concern=self._write_concern,
        )

    def upload_from_stream_with_id(
        self,
        file_id: ObjectId | int,
        filename: str,
        source: bytes | io.IOBase,
        metadata: Dict[str, Any] | None = None,
    ):
        """
        Uploads a user file to a GridFS bucket with a custom file id.

        Args:
            file_id: The custom _id for the file document (ObjectId or integer ID)
            filename: The name of the file to upload
            source: The source data (bytes or file-like object)
            metadata: Optional metadata for the file
        """
        # Convert file_id to appropriate format for storage
        if isinstance(file_id, ObjectId):
            id_for_lookup = str(
                file_id
            )  # Look up by the string representation of ObjectId
        else:
            id_for_lookup = str(
                file_id
            )  # Look up by string representation of integer

        # Check if file with this ID already exists
        row = self._db.execute(
            f"""
            SELECT id FROM `{self._files_collection}`
            WHERE _id = ?
        """,
            (id_for_lookup,),
        ).fetchone()

        if row is not None:
            raise FileExists(f"File with id {file_id} already exists")

        # Get the data from the source
        if isinstance(source, bytes):
            data = source
        elif hasattr(source, "read"):
            data = source.read()
        else:
            raise TypeError("source must be bytes or a file-like object")

        # Calculate MD5 hash of the data (unless disabled)
        md5_hash = None
        if not self._disable_md5:
            md5_hash = hashlib.md5(data).hexdigest()

        # Insert file metadata
        upload_date = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # When providing a custom ID, we need to handle it properly
        if isinstance(file_id, ObjectId):
            # Store the ObjectId in the _id column, let SQLite auto-generate the integer id
            self._db.execute(
                f"""
                INSERT INTO `{self._files_collection}`
                (id, _id, filename, length, chunkSize, uploadDate, md5, metadata)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    str(file_id),
                    filename,
                    len(data),
                    self._chunk_size_bytes,
                    upload_date,
                    md5_hash,
                    self._serialize_metadata(metadata),
                ),
            )
        else:
            # When using integer ID as custom ID, store it in both places but in appropriate formats
            # The integer in the 'id' column as the primary key, and string representation in '_id' column
            self._db.execute(
                f"""
                INSERT INTO `{self._files_collection}`
                (id, _id, filename, length, chunkSize, uploadDate, md5, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    file_id,  # Use integer ID for the auto-increment column (for compatibility)
                    str(
                        file_id
                    ),  # Store as string in _id column for general searchability
                    filename,
                    len(data),
                    self._chunk_size_bytes,
                    upload_date,
                    md5_hash,
                    self._serialize_metadata(metadata),
                ),
            )

        # Get the integer ID that was used/created
        if isinstance(file_id, ObjectId):
            # If it was an ObjectId, get the auto-generated integer ID
            file_int_id = self._db.execute(
                f"SELECT id FROM `{self._files_collection}` WHERE _id = ?",
                (str(file_id),),
            ).fetchone()[0]
        else:
            # If it was an integer ID, that's the integer ID
            file_int_id = file_id

        # Split data into chunks and insert them
        self._insert_chunks(file_int_id, data)

        # Force sync if write concern requires it
        self._force_sync_if_needed()

    def open_upload_stream_with_id(
        self,
        file_id: ObjectId | int,
        filename: str,
        metadata: Dict[str, Any] | None = None,
    ) -> GridIn:
        """
        Opens a stream for writing a file to a GridFS bucket with a custom file id.

        Args:
            file_id: The custom _id for the file document (ObjectId or integer ID)
            filename: The name of the file to upload
            metadata: Optional metadata for the file

        Returns:
            A GridIn instance to write the file contents
        """
        # Check if file with this ID already exists
        if isinstance(file_id, ObjectId):
            id_for_lookup = str(file_id)
        else:
            id_for_lookup = str(
                file_id
            )  # Use string representation for consistency

        row = self._db.execute(
            f"""
            SELECT id FROM `{self._files_collection}`
            WHERE _id = ?
        """,
            (id_for_lookup,),
        ).fetchone()

        if row is not None:
            raise FileExists(f"File with id {file_id} already exists")

        return GridIn(
            self._db,
            self._bucket_name,
            self._chunk_size_bytes,
            filename,
            metadata,
            file_id,  # Can be ObjectId or int
            disable_md5=self._disable_md5,
            write_concern=self._write_concern,
        )

    def delete_by_name(self, filename: str) -> None:
        """
        Delete all stored file documents and associated chunks with the given filename.

        Args:
            filename: The name of the file to delete
        """
        # Get all file IDs with this filename
        cursor = self._db.execute(
            f"""
            SELECT _id FROM `{self._files_collection}`
            WHERE filename = ?
        """,
            (filename,),
        )

        file_ids = [row[0] for row in cursor.fetchall()]

        if not file_ids:
            raise NoFile(f"File with name {filename} not found")

        # Delete all chunks for these files
        placeholders = ",".join("?" * len(file_ids))
        self._db.execute(
            f"""
            DELETE FROM `{self._chunks_collection}`
            WHERE files_id IN ({placeholders})
        """,
            file_ids,
        )

        # Delete all file documents
        self._db.execute(
            f"""
            DELETE FROM `{self._files_collection}`
            WHERE filename = ?
        """,
            (filename,),
        )

    def rename(self, file_id: ObjectId | str | int, new_filename: str) -> None:
        """
        Rename a stored file with the specified file_id to a new filename.

        Args:
            file_id: The _id of the file to rename (ObjectId, hex string, or integer ID)
            new_filename: The new name for the file
        """
        file_int_id = self._get_integer_id_for_file(file_id)
        if file_int_id is None:
            raise NoFile(f"File with id {file_id} not found")

        cursor = self._db.execute(
            f"""
            UPDATE `{self._files_collection}`
            SET filename = ?
            WHERE id = ?
        """,
            (new_filename, file_int_id),
        )

        if cursor.rowcount == 0:
            raise NoFile(f"File with id {file_id} not found")

    def rename_by_name(self, filename: str, new_filename: str) -> None:
        """
        Rename all stored files with the specified filename to a new filename.

        Args:
            filename: The current name of the files to rename
            new_filename: The new name for the files
        """
        cursor = self._db.execute(
            f"""
            UPDATE `{self._files_collection}`
            SET filename = ?
            WHERE filename = ?
        """,
            (new_filename, filename),
        )

        if cursor.rowcount == 0:
            raise NoFile(f"File with name {filename} not found")

    def drop(self) -> None:
        """
        Remove all files and chunks from the bucket.

        This method deletes all data in the GridFS bucket, including
        all files and their associated chunks.
        """
        # Delete all chunks first (foreign key constraint)
        self._db.execute(f"DELETE FROM `{self._chunks_collection}`")

        # Delete all files
        self._db.execute(f"DELETE FROM `{self._files_collection}`")
