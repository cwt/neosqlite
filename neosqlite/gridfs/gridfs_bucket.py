import datetime
import hashlib
import io
import logging
import uuid
from collections.abc import Iterator
from typing import Any

# Import JSONB support utilities to reuse existing implementation
from ..collection.jsonb_support import JSONBContext
from ..collection.schema_utils import (
    column_exists,
    invalidate_table_columns_cache,
)

# Import the centralized ID normalization and lookup functions
from ..collection.type_correction import (
    get_integer_id_for_doc_id,
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
    sql_regexp,
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
        ``PRAGMA synchronous`` settings when ``write_concern`` is provided,
        migrates any legacy dot-named GridFS tables, and creates the
        files/chunks tables and indexes.

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
        self._explicit_write_concern = write_concern is not None
        self._write_concern = write_concern or {}
        self._read_preference = read_preference
        self._disable_md5 = disable_md5

        # Register REGEXP on raw sqlite3 connections (neosqlite.Connection
        # already registers it).
        if db is raw_db and hasattr(raw_db, "create_function"):
            try:
                raw_db.create_function("REGEXP", 2, sql_regexp)
            except Exception:
                pass

        # Apply write concern settings to SQLite only when explicitly provided
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

        Skips overriding ``PRAGMA synchronous`` when ``write_concern`` was
        ``None``. Otherwise sets ``FULL`` when journaling (``j``) is enabled,
        ``NORMAL`` otherwise, and overrides to ``OFF`` when ``w == 0``.
        """
        if not self._explicit_write_concern:
            return

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

        Checks the existing files table schema, runs ``ALTER TABLE`` for each
        missing column, and invalidates cached table-column metadata so
        subsequent ``GridOut`` instances see the newly added columns (#191).
        """
        files_coll = quote_table_name(self._files_collection)
        migrated = False
        try:
            if not column_exists(
                self._db, self._files_collection, "content_type"
            ):
                self._db.execute(
                    f"ALTER TABLE {files_coll} ADD COLUMN content_type TEXT"
                )
                migrated = True

            if not column_exists(self._db, self._files_collection, "aliases"):
                column_type = JSONBContext.from_db(self._db).storage_type
                self._db.execute(
                    f"ALTER TABLE {files_coll} ADD COLUMN aliases {column_type}"
                )
                migrated = True
        except Exception as e:
            logger.debug(f"{e=}")
            pass
        finally:
            if migrated:
                invalidate_table_columns_cache(self._db, self._files_collection)

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

    _UPLOAD_CHUNK_BATCH_SIZE = 64

    @staticmethod
    def _iter_source_chunks(
        source: bytes | io.IOBase, chunk_size: int
    ) -> Iterator[bytes]:
        """Yield ``chunk_size`` byte slices from ``bytes`` or a readable stream."""
        if isinstance(source, bytes):
            for offset in range(0, len(source), chunk_size):
                yield source[offset : offset + chunk_size]
            return

        if not hasattr(source, "read"):
            raise TypeError("source must be bytes or a file-like object")

        while True:
            buf = bytearray()
            while len(buf) < chunk_size:
                piece = source.read(chunk_size - len(buf))
                if not piece:
                    break
                buf.extend(piece)
            if not buf:
                break
            yield bytes(buf)

    def _run_in_tx(self, operation: Any) -> Any:
        """Execute ``operation()`` inside a savepoint (if active tx) or SQL transaction."""
        if getattr(self._db, "in_transaction", False) is True:
            sp_name = f"gridfs_tx_{uuid.uuid4().hex[:12]}"
            self._db.execute(f"SAVEPOINT {sp_name}")
            try:
                result = operation()
            except Exception:
                self._db.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                self._db.execute(f"RELEASE SAVEPOINT {sp_name}")
                raise
            self._db.execute(f"RELEASE SAVEPOINT {sp_name}")
            return result

        self._db.execute("BEGIN")
        try:
            result = operation()
            self._db.commit()
            return result
        except Exception:
            self._db.rollback()
            raise

    def _upload_from_source(
        self,
        stored_id: Any,
        filename: str,
        source: bytes | io.IOBase,
        chunk_size_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Stream chunks from ``source`` and write file metadata transactionally."""
        if not isinstance(source, bytes) and not hasattr(source, "read"):
            raise TypeError("source must be bytes or a file-like object")

        chunk_size = chunk_size_bytes or self._chunk_size_bytes
        upload_date = datetime.datetime.now(datetime.UTC).isoformat()
        files_coll = quote_table_name(self._files_collection)
        chunks_sql = (
            f"INSERT INTO {quote_table_name(self._chunks_collection)} "
            f"(files_id, n, data) VALUES (?, ?, ?)"
        )

        def _perform_upload() -> None:
            cursor = self._db.execute(
                f"""
                INSERT INTO {files_coll}
                (id, _id, filename, length, chunkSize, uploadDate, md5, metadata)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    stored_id,
                    filename,
                    0,
                    chunk_size,
                    upload_date,
                    None,
                    serialize_metadata(metadata),
                ),
            )
            file_int_id = cursor.lastrowid
            if file_int_id is None and stored_id is not None:
                row = self._db.execute(
                    f"SELECT id FROM {files_coll} WHERE _id = ?",
                    (stored_id,),
                ).fetchone()
                file_int_id = row[0] if row else None
            if file_int_id is None:
                raise RuntimeError("Failed to get file ID")

            total_length = 0
            md5_hasher = None if self._disable_md5 else hashlib.md5()
            batch: list[tuple[int, int, bytes]] = []
            for chunk_index, chunk in enumerate(
                self._iter_source_chunks(source, chunk_size)
            ):
                total_length += len(chunk)
                if md5_hasher is not None:
                    md5_hasher.update(chunk)
                batch.append((file_int_id, chunk_index, chunk))
                if len(batch) >= self._UPLOAD_CHUNK_BATCH_SIZE:
                    self._db.executemany(chunks_sql, batch)
                    batch.clear()
            if batch:
                self._db.executemany(chunks_sql, batch)

            md5_hash = (
                md5_hasher.hexdigest() if md5_hasher is not None else None
            )
            self._db.execute(
                f"UPDATE {files_coll} SET length = ?, md5 = ? WHERE id = ?",
                (total_length, md5_hash, file_int_id),
            )

        self._run_in_tx(_perform_upload)
        self._force_sync_if_needed()

    def upload_from_stream(
        self,
        filename: str,
        source: bytes | io.IOBase,
        chunk_size_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
        session: Any | None = None,
    ) -> ObjectId:
        """Upload a file from bytes or a readable stream into GridFS.

        Streams payload chunks incrementally without buffering the entire file
        in memory, computes an incremental MD5 digest (unless disabled), and
        persists file metadata and chunks atomically.

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

        file_oid = ObjectId()
        self._upload_from_source(
            str(file_oid),
            filename,
            source,
            chunk_size_bytes=chunk_size_bytes,
            metadata=metadata,
        )
        return file_oid

    def _insert_chunks(
        self, file_id: int, data: bytes, chunk_size_bytes: int | None = None
    ):
        """Split ``data`` into chunks and insert them transactionally.

        Uses a ``SAVEPOINT`` when an outer transaction (e.g., ``ClientSession``)
        is already active so callers can roll back without hitting nested-``BEGIN``
        errors or premature commits (#191); otherwise runs ``BEGIN``/``commit()``.
        Does nothing if ``data`` is empty.

        Args:
            file_id: Integer primary key (``id``) of the parent file row.
            data: Raw file bytes to split into chunks.
            chunk_size_bytes: Bytes per chunk. Defaults to the bucket's
                ``chunk_size_bytes`` when ``None`` or ``0``.
        """
        if not data:
            return
        chunk_size = chunk_size_bytes or self._chunk_size_bytes
        chunks_sql = f"""
            INSERT INTO {quote_table_name(self._chunks_collection)}
            (files_id, n, data)
            VALUES (?, ?, ?)
        """

        def _write_batches() -> None:
            batch: list[tuple[int, int, bytes]] = []
            for chunk_index, chunk in enumerate(
                self._iter_source_chunks(data, chunk_size)
            ):
                batch.append((file_id, chunk_index, chunk))
                if len(batch) >= self._UPLOAD_CHUNK_BATCH_SIZE:
                    self._db.executemany(chunks_sql, batch)
                    batch.clear()
            if batch:
                self._db.executemany(chunks_sql, batch)

        self._run_in_tx(_write_batches)

    def _copy_grid_out_to_stream(
        self, grid_out: GridOut, destination: io.IOBase
    ) -> None:
        """Copy all bytes from an open ``GridOut`` stream into ``destination``."""
        with grid_out:
            read_size = grid_out.chunk_size or self._chunk_size_bytes
            while True:
                chunk = grid_out.read(read_size)
                if not chunk:
                    break
                destination.write(chunk)

    def download_to_stream(
        self, file_id: ObjectId | str | int, destination: io.IOBase
    ) -> None:
        """Write the contents of a stored GridFS file to ``destination``.

        Streams via :meth:`open_download_stream` so missing files, unfinalized
        uploads, missing chunks, and truncated chunks all enforce
        ``CorruptGridFile`` / ``NoFile`` checks cleanly.

        Args:
            file_id: File identifier as an ``ObjectId``, string, or integer.
            destination: Writable binary file-like object receiving the bytes.

        Raises:
            NoFile: If no file matching ``file_id`` exists in the bucket.
            CorruptGridFile: If the file length or chunks are corrupt/missing.
        """
        self._copy_grid_out_to_stream(
            self.open_download_stream(file_id), destination
        )

    def _get_integer_id_for_file(
        self, file_id: ObjectId | str | int
    ) -> int | None:
        """Resolve an ``ObjectId``, string, or int user ``_id`` to row ``id``.

        Looks up strictly by the user-facing ``_id`` column so caller-supplied
        integer IDs never collide with unrelated ``AUTOINCREMENT`` row ``id``s.

        Args:
            file_id: File identifier (``ObjectId``, string, or integer).

        Returns:
            int | None: Integer primary key ``id`` in ``_files_collection``,
                or ``None`` if not found.
        """
        try:
            return get_integer_id_for_doc_id(
                self._db, self._files_collection, file_id
            )
        except ValueError:
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
            CorruptGridFile: If the file length or chunks are corrupt/missing.
        """
        self._copy_grid_out_to_stream(
            self.open_download_stream_by_name(filename, revision),
            destination,
        )

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
            session=session,
        )

    def upload_from_stream_with_id(
        self,
        file_id: ObjectId | int | str,
        filename: str,
        source: bytes | io.IOBase,
        chunk_size_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
        session: Any | None = None,
    ):
        """Upload a file with a caller-supplied ``_id`` from bytes or stream.

        Ensures no file with ``file_id`` already exists, stores ``ObjectId``,
        integer, or string custom ids in ``fs_files._id`` while letting SQLite
        auto-generate the internal ``id`` PK, inserts chunks via
        ``_insert_chunks``, and forces sync if configured.

        Args:
            file_id: Custom ``ObjectId``, integer, or string file identifier.
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

        if self._get_integer_id_for_file(file_id) is not None:
            raise FileExists(f"File with id {file_id} already exists")

        stored_id: Any = (
            str(file_id)
            if isinstance(file_id, ObjectId)
            or not isinstance(file_id, (int, float, str))
            or isinstance(file_id, bool)
            else file_id
        )
        self._upload_from_source(
            stored_id,
            filename,
            source,
            chunk_size_bytes=chunk_size_bytes,
            metadata=metadata,
        )

    def open_upload_stream_with_id(
        self,
        file_id: ObjectId | int | str,
        filename: str,
        metadata: dict[str, Any] | None = None,
        content_type: str | None = None,
        aliases: list[str] | None = None,
        session: Any | None = None,
    ) -> GridIn:
        """Open a writable ``GridIn`` stream with a custom ``_id``.

        Args:
            file_id: Custom ``ObjectId``, integer, or string identifier.
            filename: Stored filename for the GridFS file document.
            metadata: Optional metadata dictionary for the file document.
            content_type: Optional MIME content type string.
            aliases: Optional list of alternative filenames.
            session: Optional ``ClientSession`` validated before opening.

        Returns:
            GridIn: Writable GridFS upload stream bound to ``file_id``.

        Raises:
            FileExists: If a file with ``file_id`` already exists.
        """
        validate_session(session, self._db)
        if self._get_integer_id_for_file(file_id) is not None:
            raise FileExists(f"File with id {file_id} already exists")

        return GridIn(
            self._db,
            self._bucket_name,
            self._chunk_size_bytes,
            filename,
            metadata,
            file_id,
            disable_md5=self._disable_md5,
            write_concern=self._write_concern,
            content_type=content_type,
            aliases=aliases,
            session=session,
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
