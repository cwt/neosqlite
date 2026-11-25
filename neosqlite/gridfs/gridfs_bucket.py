import datetime
import hashlib
import io
import logging
from typing import Any

# Import JSONB support utilities to reuse existing implementation
from ..collection.jsonb_support import JSONBContext
from ..collection.schema_utils import column_exists

# Import the centralized ID normalization and lookup functions
from ..collection.type_correction import (
    get_integer_id_for_table,
    normalize_id_query_for_db,
)
from ..collection.type_utils import validate_session

# Import ObjectId for MongoDB-compatible ID support
from ..objectid import ObjectId

logger = logging.getLogger(__name__)
from ..sql_utils import quote_table_name
from .errors import FileExists, NoFile
from .grid_file import GridIn, GridOut, GridOutCursor
from .utils import (
    deserialize_metadata,
    force_sync_if_needed,
    serialize_metadata,
)


class GridFSBucket:
    """PyMongo-compatible GridFS bucket backed by SQLite tables.

    Stores file metadata documents in ``<bucket_name>_files`` and binary data
    chunks in ``<bucket_name>_chunks``. Supports streaming and buffer-based
    uploads/downloads, custom file ids, revisions, metadata/aliases, legacy
    dot-named table migration, and SQLite write-concern pragmas.
    """

    def __init__(
        self,
        db: Any,
        bucket_name: str = "fs",
        chunk_size_bytes: int = 255 * 1024,  # 255KB default chunk size
        write_concern: dict[str, Any] | None = None,
        read_preference: Any | None = None,
        disable_md5: bool = False,
    ):
        """Initialize a ``GridFSBucket`` and ensure backing tables exist.

        Validates ``chunk_size_bytes`` and ``write_concern``, applies SQLite
        ``PRAGMA synchronous`` settings, migrates any legacy dot-named GridFS
        tables, and creates the files/chunks tables and indexes.

        Args:
            db: SQLite database connection or ``neosqlite.Connection`` wrapper.
            bucket_name: Base bucket prefix used for ``<bucket>_files`` and
                ``<bucket>_chunks`` tables. Defaults to ``"fs"``.
            chunk_size_bytes: Default chunk size in bytes. Defaults to
                ``255 * 1024`` (255 KiB).
            write_concern: Optional write concern dict supporting ``"w"``,
                ``"wtimeout"``, and ``"j"`` keys. Defaults to ``None``.
            read_preference: Accepted for PyMongo compatibility and stored on
                the bucket instance. Defaults to ``None``.
            disable_md5: If ``True``, skip MD5 checksum computation on uploads.
                Defaults to ``False``.

        Raises:
            ValueError: If ``chunk_size_bytes <= 0`` or any ``write_concern``
                option has an invalid type.
        """
        raw_db = getattr(db, "db", db)
        self._db = raw_db
        self._connection = db
        self._bucket_name = bucket_name
        self._chunk_size_bytes = chunk_size_bytes
        self._files_collection = f"{bucket_name}_files"
        self._chunks_collection = f"{bucket_name}_chunks"

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

        # Check for and migrate old GridFS table names (dot-based to underscore-based)
        self._migrate_legacy_tables_if_needed()

        # Create the necessary tables if they don't exist
        self._create_collections()

    def _migrate_legacy_tables_if_needed(self):
        """Migrate legacy dot-named GridFS tables to underscore-named tables.

        Renames ``<bucket>.files`` and ``<bucket>.chunks`` when present,
        falling back to the legacy names if renaming the files table fails.
        """
        old_files = f"{self._bucket_name}.files"
        old_chunks = f"{self._bucket_name}.chunks"
        new_files = quote_table_name(self._files_collection)
        new_chunks = quote_table_name(self._chunks_collection)

        cursor = self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (old_files,),
        )
        if cursor.fetchone():
            try:
                self._db.execute(
                    f'ALTER TABLE "{old_files}" RENAME TO {new_files}'
                )
            except Exception as e:
                logger.warning(
                    f"Failed to rename legacy files table '{old_files}' to '{new_files}': {e}. "
                    f"Using legacy table names."
                )
                self._files_collection = old_files
                self._chunks_collection = old_chunks
                return

        cursor = self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (old_chunks,),
        )
        if cursor.fetchone():
            try:
                self._db.execute(
                    f'ALTER TABLE "{old_chunks}" RENAME TO {new_chunks}'
                )
            except Exception as e:
                logger.warning(
                    f"Failed to rename legacy chunks table '{old_chunks}' to '{new_chunks}': {e}. "
                    f"GridFS bucket may be in inconsistent state."
                )

    def _apply_write_concern(self):
        """Apply write concern settings to the SQLite connection.

        Configures ``PRAGMA synchronous`` to ``FULL`` when journaling (``j``)
        is enabled, ``NORMAL`` otherwise, and overrides to ``OFF`` when
        ``w == 0``.
        """
        # Handle journal concern (j=True)
        if self._write_concern.get("j"):
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
        """Create the files and chunks SQLite tables and indexes if absent.

        Also ensures missing schema columns (``content_type``, ``aliases``)
        and the unique ``_id`` index exist on pre-existing tables.
        """
        json_type = JSONBContext.from_db(self._db).storage_type
        files_coll = quote_table_name(self._files_collection)
        chunks_coll = quote_table_name(self._chunks_collection)

        self._db.execute(f"""
            CREATE TABLE IF NOT EXISTS {files_coll} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                _id {json_type},
                filename TEXT,
                length INTEGER,
                chunkSize INTEGER,
                uploadDate TEXT,
                md5 TEXT,
                metadata {json_type},
                content_type TEXT,
                aliases {json_type}
            )
        """)

        self._db.execute(f"""
            CREATE TABLE IF NOT EXISTS {chunks_coll} (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                files_id INTEGER,
                n INTEGER,
                data BLOB,
                FOREIGN KEY (files_id) REFERENCES {files_coll} (id)
            )
        """)

        self._db.execute(f"""
            CREATE INDEX IF NOT EXISTS {quote_table_name(f"idx_{self._files_collection}_filename")}
            ON {files_coll} (filename)
        """)

        self._db.execute(f"""
            CREATE INDEX IF NOT EXISTS {quote_table_name(f"idx_{self._chunks_collection}_files_id")}
            ON {chunks_coll} (files_id)
        """)

        # UNIQUE(files_id, n): racing writers must not interleave duplicate
        # chunk indices — reads pick an arbitrary row via fetchone, silently
        # garbling file content (#128).
        self._db.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {quote_table_name(f"idx_{self._chunks_collection}_files_id_n")}
            ON {chunks_coll} (files_id, n)
        """)

        # Migrate existing tables to add new columns
        self._migrate_table_schema()

        # Create unique index on _id column for faster lookups
        files_coll = quote_table_name(self._files_collection)
        try:
            self._db.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {quote_table_name(f"idx_{self._files_collection}_id")} ON {files_coll}(_id)"
            )
        except Exception as e:
            logger.debug(f"{e=}")
            pass

    def _migrate_table_schema(self):
        """Add ``content_type`` and ``aliases`` columns when missing.

        Checks the existing files table schema and runs ``ALTER TABLE``
        for each missing column, logging debug details on failure.
        """
        files_coll = quote_table_name(self._files_collection)
        try:
            if not column_exists(
                self._db, self._files_collection, "content_type"
            ):
                self._db.execute(
                    f"ALTER TABLE {files_coll} ADD COLUMN content_type TEXT"
                )

            if not column_exists(self._db, self._files_collection, "aliases"):
                column_type = JSONBContext.from_db(self._db).storage_type
                self._db.execute(
                    f"ALTER TABLE {files_coll} ADD COLUMN aliases {column_type}"
                )
        except Exception as e:
            logger.debug(f"{e=}")
            pass

    def _serialize_metadata(
        self, metadata: dict[str, Any] | None
    ) -> str | None:
        """Serialize a metadata dictionary into a JSON string.

        Args:
            metadata: Optional metadata dictionary to serialize.

        Returns:
            str | None: Serialized JSON string, or ``None`` if ``metadata`` is
                ``None``.
        """
        return serialize_metadata(metadata)

    def _deserialize_metadata(
        self, metadata_str: str | None
    ) -> dict[str, Any] | None:
        """Deserialize a JSON string back into a metadata dictionary.

        Args:
            metadata_str: Serialized metadata string or ``None``.

        Returns:
            dict[str, Any] | None: Deserialized metadata dictionary, or
                ``None`` if ``metadata_str`` is ``None``.
        """
        return deserialize_metadata(metadata_str)

    def _force_sync_if_needed(self):
        """Checkpoint the WAL when write concern requires durability."""
        force_sync_if_needed(self._db, self._write_concern)

    def upload_from_stream(
        self,
        filename: str,
        source: bytes | io.IOBase,
        chunk_size_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
        session: Any | None = None,
    ) -> ObjectId:
        """Upload a file from bytes or a readable stream into GridFS.

        Reads the full payload from ``source``, optionally computes its MD5
        digest, inserts a file document into ``_files_collection``, stores all
        chunks via ``_insert_chunks``, and forces sync if configured.

        Args:
            filename: Stored filename for the GridFS file document.
            source: Raw ``bytes`` payload or file-like object with ``read()``.
            chunk_size_bytes: Bytes per chunk. Defaults to the bucket's
                ``chunk_size_bytes`` when ``None`` or ``0``.
            metadata: Optional metadata dictionary for the file document.
            session: Optional ``ClientSession`` validated before writing.

        Returns:
            ObjectId: Newly generated ``ObjectId`` assigned to the file's
                ``_id`` field.

        Raises:
            TypeError: If ``source`` is neither ``bytes`` nor a readable
                file-like object.
            RuntimeError: If SQLite does not return a ``lastrowid`` for the
                inserted file record.
        """
        if session:
            validate_session(session, self._db)

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
        upload_date = datetime.datetime.now(datetime.UTC).isoformat()

        chunk_size = chunk_size_bytes or self._chunk_size_bytes

        cursor = self._db.execute(
            f"""
            INSERT INTO {quote_table_name(self._files_collection)}
            (id, _id, filename, length, chunkSize, uploadDate, md5, metadata)
            VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                str(file_oid),  # Store ObjectId as hex string
                filename,
                len(data),
                chunk_size,
                upload_date,
                md5_hash,
                serialize_metadata(metadata),
            ),
        )

        file_id = cursor.lastrowid
        if file_id is None:
            raise RuntimeError("Failed to get file ID")

        # Split data into chunks and insert them
        self._insert_chunks(file_id, data, chunk_size_bytes=chunk_size)

        # Force sync if write concern requires it
        self._force_sync_if_needed()

        return file_oid

    def _insert_chunks(
        self, file_id: int, data: bytes, chunk_size_bytes: int | None = None
    ):
        """Split ``data`` into chunks and insert them in a single transaction.

        Inserts all chunks with ``executemany`` inside an explicit transaction
        to avoid one implicit fsync per chunk (#152). Does nothing if ``data``
        is empty.

        Args:
            file_id: Integer primary key (``id``) of the parent file row.
            data: Raw file bytes to split into chunks.
            chunk_size_bytes: Bytes per chunk. Defaults to the bucket's
                ``chunk_size_bytes`` when ``None`` or ``0``.
        """
        chunk_size = chunk_size_bytes or self._chunk_size_bytes
        rows = [
            (
                file_id,
                i // chunk_size,
                data[i : i + chunk_size],
            )
            for i in range(0, len(data), chunk_size)
        ]
        if not rows:
            return
        self._db.execute("BEGIN")
        try:
            self._db.executemany(
                f"""
                INSERT INTO {quote_table_name(self._chunks_collection)}
                (files_id, n, data)
                VALUES (?, ?, ?)
            """,
                rows,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def download_to_stream(
        self, file_id: ObjectId | str | int, destination: io.IOBase
    ) -> None:
        """Write the contents of a stored GridFS file to ``destination``.

        Looks up the file by ``file_id`` and writes its chunks in ascending
        chunk index order using ``destination.write()``.

        Args:
            file_id: File identifier as an ``ObjectId``, 24-char hex string,
                or integer ``id``.
            destination: Writable binary file-like object receiving the bytes.

        Raises:
            NoFile: If no file matching ``file_id`` exists in the bucket.
        """
        # Convert file_id to appropriate format for lookup
        if (file_int_id := self._get_integer_id_for_file(file_id)) is None:
            raise NoFile(f"File with id {file_id} not found")

        # Get file metadata using integer ID
        row = self._db.execute(
            f"""
            SELECT length, chunkSize FROM {quote_table_name(self._files_collection)}
            WHERE id = ?
        """,
            (file_int_id,),
        ).fetchone()

        if row is None:
            raise NoFile(f"File with id {file_id} not found")

        # Get all chunks in order
        cursor = self._db.execute(
            f"""
            SELECT data FROM {quote_table_name(self._chunks_collection)}
            WHERE files_id = ?
            ORDER BY n ASC
        """,
            (file_int_id,),
        )

        # Write chunks to destination. Keep write() instead of writelines():
        # callers only need to provide a write-only binary stream, not IOBase.
        for chunk_row in cursor:  # noqa: FURB122
            destination.write(chunk_row[0])

    def _get_integer_id_for_file(
        self, file_id: ObjectId | str | int
    ) -> int | None:
        """Resolve an ``ObjectId``, hex string, or int id to row ``id``.

        Args:
            file_id: File identifier (``ObjectId``, 24-char hex string,
                integer string, or integer row ``id``).

        Returns:
            int | None: Integer primary key ``id`` in ``_files_collection``,
                or ``None`` if not found or invalid.
        """
        try:
            return get_integer_id_for_table(
                self._db, self._files_collection, file_id
            )
        except ValueError:
            # Handle string ID types that the centralized function doesn't handle
            if isinstance(file_id, str):
                # Check if it's a valid ObjectId hex string
                if len(file_id) == 24:
                    try:
                        ObjectId(file_id)  # Validate the hex string
                        cursor = self._db.execute(
                            f"SELECT id FROM {quote_table_name(self._files_collection)} WHERE _id = ?",
                            (file_id,),
                        )
                        return row[0] if (row := cursor.fetchone()) else None
                    except ValueError as e:
                        logger.debug(
                            f"Invalid ObjectId hex string '{file_id}': {e}"
                        )
                        pass
                # Try as integer string
                try:
                    int_file_id = int(file_id)
                    cursor = self._db.execute(
                        f"SELECT id FROM {quote_table_name(self._files_collection)} WHERE id = ?",
                        (int_file_id,),
                    )
                    return row[0] if (row := cursor.fetchone()) else None
                except ValueError as e:
                    logger.debug(
                        f"Invalid integer file ID string '{file_id}': {e}"
                    )
                    pass
            return None

    def download_to_stream_by_name(
        self, filename: str, destination: io.IOBase, revision: int = -1
    ) -> None:
        """Write a stored file selected by ``filename`` and ``revision``.

        Args:
            filename: Stored filename to look up.
            destination: Writable binary file-like object receiving the bytes.
            revision: Revision offset (``-1`` for newest, ``0`` for oldest).
                Defaults to ``-1``.

        Raises:
            NoFile: If no matching file revision exists.
        """
        file_id = self._get_file_id_by_name(filename, revision)
        self.download_to_stream(file_id, destination)

    def _get_file_id_by_name(self, filename: str, revision: int = -1) -> int:
        """Resolve ``filename`` and ``revision`` to its integer row ``id``.

        Non-negative revisions index forward from the oldest upload; negative
        revisions index backward from the newest (``-1`` is latest) (#109).

        Args:
            filename: Stored filename to query.
            revision: Revision index (``-1`` for latest, ``0`` for oldest).
                Defaults to ``-1``.

        Returns:
            int: Internal integer primary-key ``id`` of the file row.

        Raises:
            NoFile: If no file exists at the requested filename/revision.
        """
        if revision >= 0:
            # Forward revisions: 0-indexed from the oldest upload
            order = "ASC"
            offset = revision
        else:
            # Negative revisions count backwards from the newest:
            # -1 = latest, -2 = second-to-last, ... (#109)
            order = "DESC"
            offset = -revision - 1

        row = self._db.execute(
            f"""
            SELECT id FROM {quote_table_name(self._files_collection)}
            WHERE filename = ?
            ORDER BY uploadDate {order}, id {order}
            LIMIT 1 OFFSET ?
        """,
            (filename, offset),
        ).fetchone()

        if row is None:
            raise NoFile(f"File with name {filename} not found")

        return row[0]

    def open_download_stream(self, file_id: ObjectId | str | int) -> GridOut:
        """Open a ``GridOut`` stream to read the file matching ``file_id``.

        Args:
            file_id: File identifier as an ``ObjectId``, hex string, or int.

        Returns:
            GridOut: Readable GridFS download stream for the matched file.

        Raises:
            NoFile: If no file matching ``file_id`` exists in the bucket.
        """
        # Convert to integer ID for internal use
        if (file_int_id := self._get_integer_id_for_file(file_id)) is None:
            raise NoFile(f"File with id {file_id} not found")
        return GridOut(self._db, self._bucket_name, file_int_id)

    def open_download_stream_by_name(
        self, filename: str, revision: int = -1
    ) -> GridOut:
        """Open a ``GridOut`` stream for a file by name and revision.

        Args:
            filename: Stored filename to look up.
            revision: Revision index (``-1`` for latest, ``0`` for oldest).
                Defaults to ``-1``.

        Returns:
            GridOut: Readable GridFS download stream for the matched file.

        Raises:
            NoFile: If no matching file revision exists.
        """
        file_id = self._get_file_id_by_name(filename, revision)
        return GridOut(self._db, self._bucket_name, file_id)

    def delete(self, file_id: ObjectId | str | int) -> None:
        """Delete a stored file document and all of its chunks by id.

        Args:
            file_id: File identifier as an ``ObjectId``, hex string, or int.

        Raises:
            NoFile: If no file matching ``file_id`` exists in the bucket.
        """
        # Convert to integer ID for internal use
        if (file_int_id := self._get_integer_id_for_file(file_id)) is None:
            raise NoFile(f"File with id {file_id} not found")

        # Delete chunks first
        self._db.execute(
            f"""
            DELETE FROM {quote_table_name(self._chunks_collection)}
            WHERE files_id = ?
        """,
            (file_int_id,),
        )

        # Delete file document
        cursor = self._db.execute(
            f"""
            DELETE FROM {quote_table_name(self._files_collection)}
            WHERE id = ?
        """,
            (file_int_id,),
        )

        if cursor.rowcount == 0:
            raise NoFile(f"File with id {file_id} not found")

    def find(
        self, filter: dict[str, Any] | None = None, session: Any | None = None
    ) -> GridOutCursor:
        """Find file documents matching ``filter`` and return a cursor.

        Validates ``session`` and normalizes id queries via
        ``normalize_id_query_for_db`` before constructing ``GridOutCursor``.

        Args:
            filter: Optional MongoDB-style filter query dictionary. Defaults
                to ``None`` (matches all files).
            session: Optional ``ClientSession`` validated before querying.

        Returns:
            GridOutCursor: Cursor iterating matching ``GridOut`` instances.
        """
        validate_session(session, self._db)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        # or other common type mismatches, using the centralized function
        if filter is not None:
            filter = normalize_id_query_for_db(filter)
        return GridOutCursor(self._db, self._bucket_name, filter or {})

    def open_upload_stream(
        self,
        filename: str,
        chunk_size_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
        session: Any | None = None,
    ) -> GridIn:
        """Open a writable ``GridIn`` stream for uploading a new file.

        Args:
            filename: Filename to store on the created GridFS file document.
            chunk_size_bytes: Bytes per chunk. Defaults to the bucket's
                ``chunk_size_bytes`` when ``None`` or ``0``.
            metadata: Optional metadata dictionary for the file document.
            session: Optional ``ClientSession`` validated before opening.

        Returns:
            GridIn: Writable GridFS upload stream.
        """
        validate_session(session, self._db)

        return GridIn(
            self._db,
            self._bucket_name,
            chunk_size_bytes or self._chunk_size_bytes,
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
        chunk_size_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
        session: Any | None = None,
    ):
        """Upload a file with a caller-supplied ``_id`` from bytes or stream.

        Ensures no file with ``file_id`` already exists, stores ``ObjectId``
        or integer custom ids in ``_files_collection``, inserts chunks via
        ``_insert_chunks``, and forces sync if configured.

        Args:
            file_id: Custom ``ObjectId`` or integer identifier for the file.
            filename: Stored filename for the GridFS file document.
            source: Raw ``bytes`` payload or readable file-like object.
            chunk_size_bytes: Bytes per chunk. Defaults to the bucket's
                ``chunk_size_bytes`` when ``None``.
            metadata: Optional metadata dictionary for the file document.
            session: Optional ``ClientSession`` validated before writing.

        Raises:
            FileExists: If a file with ``file_id`` already exists.
            TypeError: If ``source`` is neither ``bytes`` nor a readable
                file-like object.
        """
        validate_session(session, self._db)

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
        files_coll = quote_table_name(self._files_collection)
        row = self._db.execute(
            f"""
            SELECT id FROM {files_coll}
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
        upload_date = datetime.datetime.now(datetime.UTC).isoformat()

        # When providing a custom ID, we need to handle it properly
        if isinstance(file_id, ObjectId):
            # Store the ObjectId in the _id column, let SQLite auto-generate the integer id
            self._db.execute(
                f"""
                INSERT INTO {files_coll}
                (id, _id, filename, length, chunkSize, uploadDate, md5, metadata)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    str(file_id),
                    filename,
                    len(data),
                    chunk_size_bytes or self._chunk_size_bytes,
                    upload_date,
                    md5_hash,
                    serialize_metadata(metadata),
                ),
            )
        else:
            # When using integer ID as custom ID, store it in both places but in appropriate formats
            # The integer in the 'id' column as the primary key, and string representation in '_id' column
            self._db.execute(
                f"""
                INSERT INTO {files_coll}
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
                    chunk_size_bytes or self._chunk_size_bytes,
                    upload_date,
                    md5_hash,
                    serialize_metadata(metadata),
                ),
            )

        # Get the integer ID that was used/created
        if isinstance(file_id, ObjectId):
            # If it was an ObjectId, get the auto-generated integer ID
            file_int_id = self._db.execute(
                f"SELECT id FROM {files_coll} WHERE _id = ?",
                (str(file_id),),
            ).fetchone()[0]
        else:
            # If it was an integer ID, that's the integer ID
            file_int_id = file_id

        # Split data into chunks and insert them
        self._insert_chunks(
            file_int_id, data, chunk_size_bytes=chunk_size_bytes
        )

        # Force sync if write concern requires it
        self._force_sync_if_needed()

    def open_upload_stream_with_id(
        self,
        file_id: ObjectId | int,
        filename: str,
        metadata: dict[str, Any] | None = None,
        content_type: str | None = None,
        aliases: list[str] | None = None,
    ) -> GridIn:
        """Open a writable ``GridIn`` stream with a custom ``_id``.

        Args:
            file_id: Custom ``ObjectId`` or integer identifier for the file.
            filename: Stored filename for the GridFS file document.
            metadata: Optional metadata dictionary for the file document.
            content_type: Optional MIME content type string.
            aliases: Optional list of alternative filenames.

        Returns:
            GridIn: Writable GridFS upload stream bound to ``file_id``.

        Raises:
            FileExists: If a file with ``file_id`` already exists.
        """
        # Check if file with this ID already exists
        if isinstance(file_id, ObjectId):
            id_for_lookup = str(file_id)
        else:
            id_for_lookup = str(
                file_id
            )  # Use string representation for consistency

        files_coll = quote_table_name(self._files_collection)
        row = self._db.execute(
            f"""
            SELECT id FROM {files_coll}
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
            content_type=content_type,
            aliases=aliases,
        )

    def delete_by_name(self, filename: str) -> None:
        """Delete all stored files and chunks matching ``filename``.

        Args:
            filename: Filename whose file documents and chunks will be deleted.

        Raises:
            NoFile: If no file with ``filename`` exists in the bucket.
        """
        # Chunks are keyed by the integer id column (files_id), not the
        # logical _id — selecting _id here orphaned every chunk (#92).
        files_coll = quote_table_name(self._files_collection)
        chunks_coll = quote_table_name(self._chunks_collection)
        cursor = self._db.execute(
            f"""
            SELECT id FROM {files_coll}
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
            DELETE FROM {chunks_coll}
            WHERE files_id IN ({placeholders})
        """,
            file_ids,
        )

        # Delete all file documents
        self._db.execute(
            f"""
            DELETE FROM {files_coll}
            WHERE filename = ?
        """,
            (filename,),
        )

    def rename(self, file_id: ObjectId | str | int, new_filename: str) -> None:
        """Rename a stored file identified by ``file_id``.

        Args:
            file_id: File identifier as an ``ObjectId``, hex string, or int.
            new_filename: Replacement filename to assign.

        Raises:
            NoFile: If no file matching ``file_id`` exists in the bucket.
        """
        if (file_int_id := self._get_integer_id_for_file(file_id)) is None:
            raise NoFile(f"File with id {file_id} not found")

        cursor = self._db.execute(
            f"""
            UPDATE {quote_table_name(self._files_collection)}
            SET filename = ?
            WHERE id = ?
        """,
            (new_filename, file_int_id),
        )

        if cursor.rowcount == 0:
            raise NoFile(f"File with id {file_id} not found")

    def rename_by_name(self, filename: str, new_filename: str) -> None:
        """Rename all stored files with ``filename`` to ``new_filename``.

        Args:
            filename: Current filename of the files to rename.
            new_filename: Replacement filename to assign.

        Raises:
            NoFile: If no file with ``filename`` exists in the bucket.
        """
        cursor = self._db.execute(
            f"""
            UPDATE {quote_table_name(self._files_collection)}
            SET filename = ?
            WHERE filename = ?
        """,
            (new_filename, filename),
        )

        if cursor.rowcount == 0:
            raise NoFile(f"File with name {filename} not found")

    def drop(self) -> None:
        """Delete all files and chunks from this GridFS bucket.

        Removes all rows from ``_chunks_collection`` before deleting all rows
        from ``_files_collection`` to satisfy foreign-key constraints.
        """
        # Delete all chunks first (foreign key constraint)
        self._db.execute(
            f"DELETE FROM {quote_table_name(self._chunks_collection)}"
        )

        # Delete all files
        self._db.execute(
            f"DELETE FROM {quote_table_name(self._files_collection)}"
        )

    def list(self) -> list[str]:
        """List all distinct filenames stored in this GridFS bucket.

        Returns:
            list[str]: Sorted list of unique filenames in the files table.
        """
        cursor = self._db.execute(
            f"SELECT DISTINCT filename FROM {quote_table_name(self._files_collection)} ORDER BY filename"
        )
        return [row[0] for row in cursor.fetchall()]
