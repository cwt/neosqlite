from __future__ import annotations

import datetime
import hashlib
import json
import logging
import re
from copy import deepcopy
from typing import Any, Self

from .._sqlite import sqlite3
from ..collection.json_helpers import neosqlite_json_dumps
from ..collection.schema_utils import column_exists
from ..collection.type_correction import get_integer_id_for_doc_id
from ..collection.type_utils import regex_to_sql_pattern

# Import ObjectId for GridIn and GridOut
from ..objectid import ObjectId
from ..sql_utils import quote_table_name, validate_identifier
from .errors import NoFile
from .utils import (
    deserialize_aliases,
    deserialize_metadata,
    force_sync_if_needed,
    serialize_aliases,
    serialize_metadata,
)

logger = logging.getLogger(__name__)


class GridIn:
    """A file-like object for writing data to GridFS.

    Buffers incoming bytes, flushes fixed-size chunks to the chunks table,
    lazily creates the file metadata document, and optionally computes an MD5
    checksum. Supports context-manager usage that aborts partial uploads on
    exception and commits finalized files on normal exit.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str,
        chunk_size_bytes: int,
        filename: str,
        metadata: dict[str, Any] | None = None,
        file_id: ObjectId | int | str | None = None,
        disable_md5: bool = False,
        write_concern: dict[str, Any] | None = None,
        content_type: str | None = None,
        aliases: list[str] | None = None,
        session: Any | None = None,
    ):
        """Initialize a new ``GridIn`` writable stream.

        Args:
            db: SQLite database connection.
            bucket_name: Bucket name prefix for GridFS tables.
            chunk_size_bytes: Maximum size of each chunk in bytes.
            filename: Name of the stored file.
            metadata: Optional metadata dictionary for the file.
            file_id: Optional custom ``ObjectId``, integer, or string file ID.
            disable_md5: If True, skip MD5 digest computation.
            write_concern: Optional write concern settings for sync behavior.
            content_type: Optional MIME content type of the file.
            aliases: Optional list of alternative filenames.
            session: Optional ``ClientSession`` bound to this upload stream.
        """
        self._db = db
        self._bucket_name = bucket_name
        self._chunk_size_bytes = chunk_size_bytes
        self._filename = filename
        self._metadata = metadata
        self._file_id = file_id
        self._files_collection = validate_identifier(f"{bucket_name}_files")
        self._chunks_collection = validate_identifier(f"{bucket_name}_chunks")
        self._disable_md5 = disable_md5
        self._write_concern = write_concern or {}
        self._content_type = content_type
        self._aliases = aliases
        self._had_outer_transaction = getattr(
            db, "in_transaction", False
        ) is True or bool(
            session is not None and getattr(session, "in_transaction", False)
        )

        # Stream state
        self._buffer = bytearray()
        self._buffer_start = 0  # consumed-prefix offset (#152)
        self._chunk_number = 0
        self._position = 0
        self._closed = False
        self._md5_hasher = None if disable_md5 else hashlib.md5()
        self._file_document_created = False

    def _serialize_metadata(
        self, metadata: dict[str, Any] | None
    ) -> str | None:
        """Serialize a metadata dictionary to a JSON string.

        Args:
            metadata: Metadata dictionary to serialize.

        Returns:
            str | None: JSON-encoded metadata string, or None if unset.
        """
        return serialize_metadata(metadata)

    def _deserialize_metadata(
        self, metadata_str: str | None
    ) -> dict[str, Any] | None:
        """Deserialize metadata from a JSON string.

        Args:
            metadata_str: JSON string representation of metadata.

        Returns:
            dict[str, Any] | None: Deserialized metadata dictionary, or None.
        """
        return deserialize_metadata(metadata_str)

    def _force_sync_if_needed(self):
        """Force database synchronization if write concern requires it."""
        force_sync_if_needed(self._db, self._write_concern)

    def write(self, data: bytes | bytearray) -> int:
        """Write bytes to the ``GridIn`` stream.

        Buffers incoming bytes, updates the byte position and optional MD5
        hasher, flushes complete chunks of ``_chunk_size_bytes``, and compacts
        the consumed buffer prefix once it grows past four chunks.

        Args:
            data: Byte payload to append to the file stream.

        Returns:
            int: Number of bytes written.

        Raises:
            ValueError: If the stream has already been closed.
            TypeError: If ``data`` is not ``bytes`` or ``bytearray``.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes or bytearray")

        # Add data to buffer. Chunks are consumed via a start offset so a
        # large single write does not memmove the remainder per chunk — the
        # old 'del buffer[:n]' pattern was quadratic (#152).
        self._buffer.extend(data)
        self._position += len(data)
        if self._md5_hasher:
            self._md5_hasher.update(data)

        # Flush chunks if buffer is full
        while len(self._buffer) - self._buffer_start >= self._chunk_size_bytes:
            self._flush_chunk()
        # Compact occasionally to keep the bytearray bounded
        if self._buffer_start > 4 * self._chunk_size_bytes:
            del self._buffer[: self._buffer_start]
            self._buffer_start = 0

        return len(data)

    def _flush_chunk(self) -> None:
        """Flush a single complete chunk from the buffer to the database.

        Extracts ``_chunk_size_bytes`` from the unconsumed buffer prefix,
        lazily creates the file document before chunk ``0``, inserts the chunk
        into the chunks table, and increments ``_chunk_number``.
        """
        if (
            len(self._buffer) - getattr(self, "_buffer_start", 0)
            >= self._chunk_size_bytes
        ):
            start = getattr(self, "_buffer_start", 0)
            chunk_data = bytes(
                self._buffer[start : start + self._chunk_size_bytes]
            )
            self._buffer_start = start + self._chunk_size_bytes

            # If this is the first chunk, create the file document
            if self._chunk_number == 0:
                self._create_file_document()

            # Insert the chunk
            self._db.execute(
                f"""
                INSERT INTO {quote_table_name(self._chunks_collection)}
                (files_id, n, data)
                VALUES (?, ?, ?)
            """,
                (self._get_file_id(), self._chunk_number, chunk_data),
            )

            self._chunk_number += 1

    def _create_file_document(self) -> None:
        """Create the file document in the GridFS files table once.

        Records the UTC upload timestamp and inserts a row into the files
        table (always auto-generating SQLite ``id`` PK and storing the caller
        or generated ``_id`` in ``_id``). Caches the resulting SQLite integer
        row ID in ``_int_file_id``.
        """
        if getattr(self, "_file_document_created", False):
            return
        self._file_document_created = True

        upload_date = datetime.datetime.now(datetime.UTC).isoformat()

        if self._file_id is None:
            self._file_id = ObjectId()

        stored_id: Any = (
            str(self._file_id)
            if isinstance(self._file_id, ObjectId)
            or not isinstance(self._file_id, (int, float, str))
            or isinstance(self._file_id, bool)
            else self._file_id
        )
        cur = self._db.execute(
            f"""
            INSERT INTO {quote_table_name(self._files_collection)}
            (id, _id, filename, chunkSize, uploadDate, metadata, content_type, aliases)
            VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                stored_id,
                self._filename,
                self._chunk_size_bytes,
                upload_date,
                serialize_metadata(self._metadata),
                self._content_type,
                serialize_aliases(self._aliases),
            ),
        )
        self._int_file_id = cur.lastrowid

    def _get_file_id(self) -> int:
        """Return the SQLite integer row ID for the file document.

        Lazily creates the file document if needed and returns the cached
        ``_int_file_id``, or queries the files table by ``_id`` when not yet
        cached.

        Returns:
            int: Integer row ID in the GridFS files table.

        Raises:
            RuntimeError: If the file document cannot be found in the table.
        """
        if not getattr(self, "_file_document_created", False):
            self._create_file_document()

        cached = getattr(self, "_int_file_id", None)
        if cached is not None:
            return int(cached)
        try:
            row_id = get_integer_id_for_doc_id(
                self._db, self._files_collection, self._file_id
            )
            self._int_file_id = row_id
            return row_id
        except ValueError as exc:
            raise RuntimeError(
                f"File with ID {self._file_id} not found in database"
            ) from exc

    def close(self) -> None:
        """Close the ``GridIn`` stream and finalize file metadata.

        Flushes any remaining buffered bytes as a final chunk (or ensures an
        empty file document is created), updates ``length`` and ``md5`` on
        the files row, triggers write-concern sync if needed, commits only if
        no outer transaction was active at open time, and marks the stream
        closed.
        """
        if self._closed:
            return

        # Flush any remaining data in the buffer
        remaining = bytes(self._buffer[self._buffer_start :])
        if remaining or self._chunk_number == 0:
            # If no chunks have been written yet, we still need to create the file
            if self._chunk_number == 0:
                self._create_file_document()

            # Get the integer ID for the file document (from the created document)
            file_int_id = self._get_file_id()

            # Write the final chunk (which may be smaller than chunk_size_bytes)
            if remaining:
                self._db.execute(
                    f"""
                    INSERT INTO {quote_table_name(self._chunks_collection)}
                    (files_id, n, data)
                    VALUES (?, ?, ?)
                """,
                    (
                        file_int_id,
                        self._chunk_number,
                        remaining,
                    ),
                )
                self._chunk_number += 1

        # Update the file document with final metadata.
        # This must happen even when the last chunk filled the buffer exactly
        # (i.e., buffer is empty but chunks were written), otherwise the md5
        # and length remain NULL from the initial INSERT.
        if self._chunk_number > 0 or self._file_id is not None:
            file_int_id = self._get_file_id()
            md5_hash = None
            if self._md5_hasher:
                md5_hash = self._md5_hasher.hexdigest()
            self._db.execute(
                f"""
                UPDATE {quote_table_name(self._files_collection)}
                SET length = ?, md5 = ?
                WHERE id = ?
            """,
                (self._position, md5_hash, file_int_id),
            )

        # Force sync if write concern requires it
        self._force_sync_if_needed()

        if not getattr(self, "_had_outer_transaction", False):
            try:
                self._db.commit()
            except Exception:
                pass

        self._closed = True

    def __enter__(self) -> Self:
        """Enter the ``GridIn`` context manager.

        Returns:
            Self: This ``GridIn`` instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the ``GridIn`` context manager.

        Aborts the upload when an exception occurred inside the ``with``
        block so partial files and chunks are removed; otherwise closes and
        commits the stream (#127).

        Args:
            exc_type: Exception type raised in the block, or None.
            exc_val: Exception instance raised in the block, or None.
            exc_tb: Traceback for the raised exception, or None.
        """
        if exc_type is not None:
            try:
                self.abort()
            except Exception as abort_error:
                logger.warning(f"GridIn abort failed: {abort_error}")
            return
        self.close()

    def abort(self) -> None:
        """Discard an in-progress upload by deleting its chunks and file row.

        Returns early if the stream is already closed (so finalized files are
        never deleted); otherwise removes rows from the chunks and files tables,
        commits when no outer transaction is active, and marks the stream
        closed.
        """
        if self._closed:
            return
        try:
            if getattr(self, "_file_document_created", False):
                file_int_id = self._get_file_id()
                self._db.execute(
                    f"DELETE FROM {quote_table_name(self._chunks_collection)} WHERE files_id = ?",
                    (file_int_id,),
                )
                self._db.execute(
                    f"DELETE FROM {quote_table_name(self._files_collection)} WHERE id = ?",
                    (file_int_id,),
                )
                if not getattr(self, "_had_outer_transaction", False):
                    self._db.commit()
        finally:
            self._closed = True


class GridOut:
    """A file-like object for reading files stored in GridFS.

    Loads file metadata from the GridFS files table and streams chunked data
    sequentially from the chunks table on demand.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str,
        file_id: ObjectId | int,
        _prefetched_row: tuple[Any, ...] | None = None,
        _has_content_type: bool | None = None,
        _has_aliases: bool | None = None,
    ):
        """Initialize a new ``GridOut`` readable stream.

        Resolves ``file_id`` to an internal SQLite row ID, inspects optional
        schema columns (``content_type``, ``aliases``) for backward
        compatibility, loads the file document, and parses ``_id``, metadata,
        and aliases.

        Args:
            db: SQLite database connection.
            bucket_name: Bucket name prefix for GridFS tables.
            file_id: ``ObjectId`` or integer ID of the file to read.
            _prefetched_row: Internal tuple of pre-fetched metadata columns
                passed by ``GridOutCursor`` to avoid N+1 per-file queries.
            _has_content_type: Whether ``_prefetched_row`` includes
                ``content_type``.
            _has_aliases: Whether ``_prefetched_row`` includes ``aliases``.

        Raises:
            RuntimeError: If an ``ObjectId`` file ID is not found in DB.
            NoFile: If no file row exists for the resolved integer ID.
        """
        self._db = db
        self._bucket_name = bucket_name
        self._files_collection = validate_identifier(f"{bucket_name}_files")
        self._chunks_collection = validate_identifier(f"{bucket_name}_chunks")

        if _prefetched_row is not None:
            self._int_file_id = _prefetched_row[0]
            self._populate_from_row(
                _prefetched_row[1:],
                bool(_has_content_type),
                bool(_has_aliases),
                self._int_file_id,
            )
            return

        # Convert file_id to integer for internal use
        if isinstance(file_id, ObjectId):
            # Look up the integer ID for this ObjectId
            # Use quote_table_name to prevent SQL injection in bucket_name
            cursor = self._db.execute(
                f"SELECT id FROM {quote_table_name(f"{bucket_name}_files")} WHERE _id = ?",
                (str(file_id),),
            )
            if (row := cursor.fetchone()) is None:
                raise RuntimeError(
                    f"File with ObjectId {file_id} not found in database"
                )
            self._int_file_id = row[0]  # Store the integer ID internally
        else:
            self._int_file_id = file_id  # Store the integer ID internally

        # Get file metadata using the integer ID
        # Check if new columns exist to maintain backward compatibility
        has_content_type, has_aliases = self._detect_optional_columns(
            self._db, self._files_collection
        )

        # Build dynamic SELECT query based on available columns
        select_columns = [
            "filename",
            "length",
            "chunkSize",
            "uploadDate",
            "md5",
            "json(metadata)",
            "_id",
        ]
        if has_content_type:
            select_columns.append("content_type")
        if has_aliases:
            select_columns.append("json(aliases)")

        query = f"""
            SELECT {", ".join(select_columns)}
            FROM {quote_table_name(self._files_collection)}
            WHERE id = ?
        """

        row = self._db.execute(query, (self._int_file_id,)).fetchone()

        if row is None:
            raise NoFile(f"File with id {file_id} not found")

        self._populate_from_row(row, has_content_type, has_aliases, file_id)

    @staticmethod
    def _detect_optional_columns(
        db: sqlite3.Connection, files_collection: str
    ) -> tuple[bool, bool]:
        """Detect optional GridFS metadata columns on ``files_collection``."""
        has_content_type = False
        has_aliases = False
        try:
            has_content_type = column_exists(
                db, files_collection, "content_type"
            )
            has_aliases = column_exists(db, files_collection, "aliases")
        except (AttributeError, TypeError) as e:
            # Handle mocked databases in tests - assume old schema
            logger.debug(f"{e=}")
        return has_content_type, has_aliases

    def _populate_from_row(
        self,
        row: tuple[Any, ...],
        has_content_type: bool,
        has_aliases: bool,
        fallback_file_id: Any,
    ) -> None:
        """Populate metadata attributes from an unpacked SQLite files row."""
        row_idx = 0
        self._filename = row[row_idx]
        row_idx += 1
        self._length = row[row_idx]
        row_idx += 1
        self._chunk_size = row[row_idx]
        row_idx += 1
        self._upload_date = row[row_idx]
        row_idx += 1
        self._md5 = row[row_idx]
        row_idx += 1
        metadata_str = row[row_idx]
        row_idx += 1
        self._stored_oid = row[
            row_idx
        ]  # Store the _id value (ObjectId hex string)
        row_idx += 1

        # Handle optional columns with defaults
        self._content_type = row[row_idx] if has_content_type else None
        if has_content_type:
            row_idx += 1
        aliases_str = row[row_idx] if has_aliases else None

        # Set the actual _id field to the appropriate type based on the stored value
        if self._stored_oid is not None:
            # Determine the appropriate type for the ID
            if isinstance(self._stored_oid, int):
                # If it's already an integer, use it directly
                self._actual_id: ObjectId | int | str = self._stored_oid
            elif isinstance(self._stored_oid, str):
                if len(self._stored_oid) == 24:
                    # Check if it's a valid ObjectId hex format
                    try:
                        self._actual_id = ObjectId(
                            self._stored_oid
                        )  # Convert to ObjectId
                    except ValueError as e:
                        # If it's not a valid ObjectId hex, try to convert to int
                        logger.debug(
                            f"Failed to convert GridFS ID '{self._stored_oid}' to ObjectId: {e}"
                        )
                        try:
                            self._actual_id = int(self._stored_oid)
                        except ValueError as e:
                            # Keep as string if it's neither a valid ObjectId nor integer
                            logger.debug(
                                f"Failed to convert GridFS ID '{self._stored_oid}' to integer: {e}"
                            )
                            self._actual_id = self._stored_oid
                else:
                    # For other length strings, try to convert to integer first
                    try:
                        self._actual_id = int(self._stored_oid)
                    except ValueError as e:
                        # If it's not an integer string, keep as is
                        logger.debug(
                            f"Failed to convert non-24 char GridFS ID '{self._stored_oid}' to integer: {e}"
                        )
                        self._actual_id = self._stored_oid
            else:
                # For any other type, keep as is
                self._actual_id = self._stored_oid  # type: ignore
        else:
            # If no stored _id, fall back to the integer ID
            self._actual_id = fallback_file_id  # type: ignore

        self._metadata = deserialize_metadata(metadata_str)
        self._aliases = deserialize_aliases(aliases_str)
        self._position = 0
        self._current_chunk_data = b""
        self._current_chunk_index = -1
        self._closed = False

    @property
    def _id(self):
        """Get the file's actual ID.

        Returns:
            ObjectId | int | str: File identifier as ``ObjectId``, integer,
                or fallback string.
        """
        return self._actual_id

    @property
    def _file_id(self):
        """Get the file's actual ID for API compatibility.

        Returns:
            ObjectId | int | str: File identifier matching user expectations
                (``ObjectId``, integer, or string).
        """
        return self._actual_id

    def _deserialize_metadata(
        self, metadata_str: str | None
    ) -> dict[str, Any] | None:
        """Deserialize metadata from a JSON string.

        Args:
            metadata_str: JSON string representation of metadata.

        Returns:
            dict[str, Any] | None: Deserialized metadata dictionary, or None.
        """
        return deserialize_metadata(metadata_str)

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the ``GridOut`` stream.

        Args:
            size: Number of bytes to read, or ``-1`` to read all remaining
                bytes in the file.

        Returns:
            bytes: Bytes read from the file stream.

        Raises:
            ValueError: If the stream has already been closed.
            CorruptGridFile: If ``size == -1`` on an unfinalized upload or if
                a required chunk is missing.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if self._length is None:
            # Abandoned upload: length was never finalized (#127, #191)
            from .errors import CorruptGridFile

            raise CorruptGridFile(
                "File has no finalized length (abandoned upload)"
            )

        if size == -1:
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
        """Load the chunk containing the current stream position.

        Skips querying if the target chunk is already buffered in memory, and
        raises ``CorruptGridFile`` if the chunk row is missing or its byte
        length does not match the expected chunk size (#147, #191).

        Raises:
            CorruptGridFile: If the required chunk is missing or truncated.
        """
        if self._length is None:
            from .errors import CorruptGridFile

            raise CorruptGridFile(
                "File has no finalized length (abandoned upload)"
            )

        chunk_index = self._position // self._chunk_size

        # If we already have the right chunk, we're done
        if chunk_index == self._current_chunk_index:
            return

        # Load the required chunk using the integer file ID
        row = self._db.execute(
            f"""
            SELECT data FROM {quote_table_name(self._chunks_collection)}
            WHERE files_id = ? AND n = ?
        """,
            (self._int_file_id, chunk_index),
        ).fetchone()

        if row is None:
            # A files document whose chunk vanished mid-read is corruption,
            # not a missing file (#147)
            from .errors import CorruptGridFile

            raise CorruptGridFile(
                f"Chunk {chunk_index} for file id {self._int_file_id} missing"
            )

        chunk_data = row[0]
        expected_chunk_size = min(
            self._chunk_size,
            self._length - chunk_index * self._chunk_size,
        )
        if len(chunk_data) != expected_chunk_size:
            from .errors import CorruptGridFile

            raise CorruptGridFile(
                f"Chunk {chunk_index} for file id {self._int_file_id} has "
                f"invalid length {len(chunk_data)} (expected {expected_chunk_size})"
            )

        self._current_chunk_data = chunk_data
        self._current_chunk_index = chunk_index

    @property
    def filename(self) -> str:
        """Get the filename.

        Returns:
            str: Stored filename.
        """
        return self._filename

    @property
    def length(self) -> int:
        """Get the length of the file in bytes.

        Returns:
            int: File length in bytes.
        """
        return self._length

    @property
    def chunk_size(self) -> int:
        """Get the chunk size in bytes.

        Returns:
            int: Chunk size in bytes.
        """
        return self._chunk_size

    @property
    def upload_date(self) -> str:
        """Get the upload date.

        Returns:
            str: ISO-formatted UTC upload timestamp.
        """
        return self._upload_date

    @property
    def md5(self) -> str:
        """Get the MD5 hash of the file.

        Returns:
            str: Hexadecimal MD5 digest of the file.
        """
        return self._md5

    @property
    def metadata(self) -> dict[str, Any] | None:
        """Get the metadata of the file.

        Returns:
            dict[str, Any] | None: File metadata dictionary, or None.
        """
        return self._metadata

    @property
    def content_type(self) -> str | None:
        """Get the content type (MIME type) of the file.

        Returns:
            str | None: MIME content type of the file, or None.
        """
        return self._content_type

    @property
    def aliases(self) -> list[str] | None:
        """Get the aliases (alternative names) for the file.

        Returns:
            list[str] | None: List of alternative filenames, or None.
        """
        return self._aliases

    def close(self) -> None:
        """Close the GridOut stream."""
        self._closed = True

    def __enter__(self) -> Self:
        """Enter the ``GridOut`` context manager.

        Returns:
            Self: This ``GridOut`` instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the ``GridOut`` context manager and close the stream.

        Args:
            exc_type: Exception type raised in the block, or None.
            exc_val: Exception instance raised in the block, or None.
            exc_tb: Traceback for the raised exception, or None.
        """
        self.close()


def _mongo_dot_subpath_to_json_path(subpath: str) -> str | None:
    """Convert a MongoDB dotted subpath to a SQLite JSON path.

    Handles numeric segments as array indices (``tags.0`` -> ``$.tags[0]``)
    and quotes field names containing special characters.

    Args:
        subpath: Dotted path below ``metadata``/``aliases`` (e.g. ``size``,
            ``nested.a``, ``tags.0``).

    Returns:
        SQLite JSON path (e.g. ``$.size``) or None if invalid.
    """
    if not subpath or subpath.startswith(".") or subpath.endswith("."):
        return None
    if ".." in subpath:
        return None
    parts = subpath.split(".")
    out = "$"
    for part in parts:
        if not part:
            return None
        if part.isdigit():
            out += f"[{part}]"
        elif part.startswith("["):
            # Already bracketed (e.g. "[0]") — append directly.
            out += part
        elif "[" in part:
            # Field with trailing indices (e.g. "tags[0]").
            out += f".{part}"
        else:
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", part):
                out += f".{part}"
            else:
                esc = part.replace("\\", "\\\\").replace('"', '\\"')
                out += f'."{esc}"'
    return out


def _normalize_json_query_value(value: Any) -> Any:
    """Normalize a query value for SQLite parameter binding.

    Converts ``ObjectId`` instances to hex strings and ``datetime``/``date``
    values to ISO 8601 strings; leaves all other types unchanged.

    Args:
        value: Raw query filter value to normalize.

    Returns:
        Any: SQLite-bindable scalar value.
    """
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def _append_json_column_equality(
    column: str,
    json_path_sql: str,
    value: Any,
    where_conditions: list[str],
    params: list[Any],
) -> None:
    """Append a direct-equality SQL clause for a JSON column and path.

    Handles ``None`` (``IS NULL``), ``dict``/``list`` (``= json(?)``),
    compiled ``re.Pattern`` (``LIKE ?``), and scalar values (matching either
    the extracted value or any element when the target path is an array).

    Args:
        column: SQL column name containing JSON data (e.g. ``metadata``).
        json_path_sql: SQL-quoted JSON path expression (e.g. ``'$.size'``).
        value: Expected value or pattern to match.
        where_conditions: List of SQL predicate strings to append to.
        params: List of SQL bind parameters to extend.
    """
    extract = f"json_extract({column}, {json_path_sql})"
    json_type = f"json_type({column}, {json_path_sql})"
    if value is None:
        # MongoDB: {field: null} matches null-or-missing.
        where_conditions.append(f"{extract} IS NULL")
    elif isinstance(value, (dict, list)):
        where_conditions.append(f"{extract} = json(?)")
        params.append(json.dumps(value, default=str))
    elif isinstance(value, re.Pattern):
        where_conditions.append(f"{extract} LIKE ?")
        params.append(f"%{value.pattern}%")
    else:
        norm = _normalize_json_query_value(value)
        where_conditions.append(
            f"({extract} = ? OR "
            f"({json_type} = 'array' AND EXISTS "
            f"(SELECT 1 FROM json_each({column}, {json_path_sql}) "
            f"WHERE value = ?)))"
        )
        params.extend([norm, norm])


def _append_json_column_operators(
    column: str,
    json_path_sql: str,
    operators: dict[str, Any],
    where_conditions: list[str],
    params: list[Any],
) -> None:
    """Append operator clauses for a JSON column and path.

    Translates MongoDB comparison, array, regex, and existence operators
    (``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$in``,
    ``$nin``, ``$regex`` with ``$options``, ``$exists``, ``$size``, and
    ``$all``) into SQL predicates using ``json_extract``/``json_each``.
    Unsupported operators append ``1=0``.

    Args:
        column: SQL column name containing JSON data.
        json_path_sql: SQL-quoted JSON path expression.
        operators: Dictionary mapping MongoDB operator names to operands.
        where_conditions: List of SQL predicate strings to append to.
        params: List of SQL bind parameters to extend.
    """
    extract = f"json_extract({column}, {json_path_sql})"
    json_type = f"json_type({column}, {json_path_sql})"
    each = f"json_each({column}, {json_path_sql})"
    # $options only modifies $regex — pull it out once.
    regex_options = operators.get("$options", "")

    for op, op_val in operators.items():
        if op == "$options":
            continue
        match op:
            case "$eq":
                _append_json_column_equality(
                    column, json_path_sql, op_val, where_conditions, params
                )
            case "$ne":
                if op_val is None:
                    where_conditions.append(f"{extract} IS NOT NULL")
                elif isinstance(op_val, (dict, list)):
                    where_conditions.append(f"{extract} IS NOT json(?)")
                    params.append(json.dumps(op_val, default=str))
                elif isinstance(op_val, re.Pattern):
                    where_conditions.append(
                        f"({extract} IS NULL OR {extract} NOT LIKE ?)"
                    )
                    params.append(f"%{op_val.pattern}%")
                else:
                    norm = _normalize_json_query_value(op_val)
                    where_conditions.append(
                        f"({extract} IS NULL OR "
                        f"({extract} != ? AND "
                        f"({json_type} != 'array' OR NOT EXISTS "
                        f"(SELECT 1 FROM {each} WHERE value = ?))))"
                    )
                    params.extend([norm, norm])
            case "$gt" | "$gte" | "$lt" | "$lte":
                sql_op = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}[
                    op
                ]
                if isinstance(op_val, (dict, list)):
                    where_conditions.append("1=0")
                    continue
                norm = _normalize_json_query_value(op_val)
                where_conditions.append(
                    f"({extract} {sql_op} ? OR "
                    f"({json_type} = 'array' AND EXISTS "
                    f"(SELECT 1 FROM {each} WHERE value {sql_op} ?)))"
                )
                params.extend([norm, norm])
            case "$in":
                if not isinstance(op_val, (list, tuple)):
                    where_conditions.append("1=0")
                    continue
                if not op_val:
                    where_conditions.append("1=0")
                    continue
                has_null = any(v is None for v in op_val)
                non_nulls = [
                    (
                        _normalize_json_query_value(v)
                        if not isinstance(v, (dict, list))
                        else json.dumps(v, default=str)
                    )
                    for v in op_val
                    if v is not None
                ]
                # JSON objects/arrays need json() comparison; split them out.
                scalar_vals = [
                    v
                    for v, orig in zip(
                        non_nulls, [x for x in op_val if x is not None]
                    )
                    if not isinstance(orig, (dict, list))
                ]
                json_vals = [
                    v
                    for v, orig in zip(
                        non_nulls, [x for x in op_val if x is not None]
                    )
                    if isinstance(orig, (dict, list))
                ]
                or_parts: list[str] = []
                in_params: list[Any] = []
                if scalar_vals:
                    placeholders = ", ".join("?" * len(scalar_vals))
                    or_parts.append(f"{extract} IN ({placeholders})")
                    in_params.extend(scalar_vals)
                    or_parts.append(
                        f"({json_type} = 'array' AND EXISTS "
                        f"(SELECT 1 FROM {each} WHERE value IN ({placeholders})))"
                    )
                    in_params.extend(scalar_vals)
                for jv in json_vals:
                    or_parts.append(f"{extract} = json(?)")
                    in_params.append(jv)
                if has_null:
                    or_parts.append(f"{extract} IS NULL")
                if or_parts:
                    where_conditions.append("(" + " OR ".join(or_parts) + ")")
                    params.extend(in_params)
                else:
                    where_conditions.append("1=0")
            case "$nin":
                if not isinstance(op_val, (list, tuple)):
                    where_conditions.append("1=0")
                    continue
                if not op_val:
                    where_conditions.append("1=1")
                    continue
                has_null = any(v is None for v in op_val)
                non_nulls = [
                    _normalize_json_query_value(v)
                    for v in op_val
                    if v is not None and not isinstance(v, (dict, list))
                ]
                if has_null:
                    # Exclude null/missing plus any non-null matches.
                    if non_nulls:
                        placeholders = ", ".join("?" * len(non_nulls))
                        where_conditions.append(
                            f"({extract} IS NOT NULL AND {extract} NOT IN "
                            f"({placeholders}) AND ({json_type} != 'array' OR "
                            f"NOT EXISTS (SELECT 1 FROM {each} WHERE value IN "
                            f"({placeholders}))))"
                        )
                        params.extend(non_nulls)
                        params.extend(non_nulls)
                    else:
                        where_conditions.append(f"{extract} IS NOT NULL")
                else:
                    if non_nulls:
                        placeholders = ", ".join("?" * len(non_nulls))
                        where_conditions.append(
                            f"({extract} IS NULL OR {extract} NOT IN "
                            f"({placeholders})) AND ({json_type} != 'array' OR "
                            f"NOT EXISTS (SELECT 1 FROM {each} WHERE value IN "
                            f"({placeholders})))"
                        )
                        params.extend(non_nulls)
                        params.extend(non_nulls)
                    else:
                        # Only dict/list values — fall back to no-match guard.
                        where_conditions.append("1=0")
            case "$regex":
                opts = regex_options if isinstance(regex_options, str) else ""
                pattern = regex_to_sql_pattern(op_val, opts)
                if pattern is None:
                    where_conditions.append("1=0")
                else:
                    where_conditions.append(
                        f"({extract} REGEXP ? OR "
                        f"({json_type} = 'array' AND EXISTS "
                        f"(SELECT 1 FROM {each} WHERE value REGEXP ?)))"
                    )
                    params.extend([pattern, pattern])
            case "$exists":
                if not isinstance(op_val, bool):
                    where_conditions.append("1=0")
                elif op_val:
                    where_conditions.append(f"{json_type} IS NOT NULL")
                else:
                    where_conditions.append(f"{json_type} IS NULL")
            case "$size":
                try:
                    size_val = int(op_val)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    where_conditions.append("1=0")
                    continue
                where_conditions.append(
                    f"({json_type} = 'array' AND "
                    f"json_array_length({column}, {json_path_sql}) = ?)"
                )
                params.append(size_val)
            case "$all":
                if not isinstance(op_val, (list, tuple)) or not op_val:
                    where_conditions.append("1=0")
                    continue
                for elem in op_val:
                    if isinstance(elem, (dict, list)):
                        where_conditions.append("1=0")
                        continue
                    norm = _normalize_json_query_value(elem)
                    where_conditions.append(
                        f"EXISTS (SELECT 1 FROM {each} WHERE value = ?)"
                    )
                    params.append(norm)
            case _:
                logger.debug(f"Unsupported GridFS metadata operator '{op}'")
                where_conditions.append("1=0")


def _append_dotted_json_condition(
    column: str,
    subpath: str,
    value: Any,
    where_conditions: list[str],
    params: list[Any],
) -> None:
    """Append a dotted JSON path filter clause (e.g. ``metadata.size``).

    Converts ``subpath`` to a SQLite JSON path and dispatches to operator or
    direct-equality translation. Appends ``1=0`` if ``subpath`` is invalid.

    Args:
        column: JSON column name (e.g. ``metadata`` or ``aliases``).
        subpath: Dotted subpath below ``column``.
        value: Filter value or operator mapping to match at ``subpath``.
        where_conditions: List of SQL predicate strings to append to.
        params: List of SQL bind parameters to extend.
    """
    json_path = _mongo_dot_subpath_to_json_path(subpath)
    if json_path is None:
        logger.debug(f"Invalid dotted GridFS filter path '{column}.{subpath}'")
        where_conditions.append("1=0")
        return
    json_path_sql = f"'{json_path.replace(chr(39), chr(39) * 2)}'"
    if isinstance(value, re.Pattern):
        _append_json_column_operators(
            column, json_path_sql, {"$regex": value}, where_conditions, params
        )
    elif isinstance(value, dict) and any(
        isinstance(k, str) and k.startswith("$") for k in value
    ):
        _append_json_column_operators(
            column, json_path_sql, value, where_conditions, params
        )
    elif isinstance(value, dict):
        # Nested document equality, e.g. {"metadata.a": {"b": 1}}.
        _append_json_column_equality(
            column, json_path_sql, value, where_conditions, params
        )
    else:
        _append_json_column_equality(
            column, json_path_sql, value, where_conditions, params
        )


def build_gridfs_files_where(
    filter_dict: dict[str, Any] | None,
) -> tuple[str, list[Any]]:
    """Build a SQL WHERE clause and parameters for the GridFS files table.

    Shared by ``GridOutCursor`` and ``Collection._count_documents_as_gridfs``.
    Supports logical operators (``$and``, ``$or``, ``$nor``), dotted paths
    under ``metadata`` and ``aliases``, and standard GridFS file fields.

    Args:
        filter_dict: MongoDB-style filter dictionary, or None for all files.

    Returns:
        tuple[str, list[Any]]: Tuple of ``(where_clause, params)`` where
            ``where_clause`` is ``""`` when no conditions are generated.
    """
    if not filter_dict:
        return "", []

    where_conditions: list[str] = []
    params: list[Any] = []

    def _append_one(
        key: str,
        value: Any,
        conds: list[str],
        plist: list[Any],
    ) -> None:
        """Append SQL WHERE predicates and params for a single filter entry.

        Recursively handles ``$and``, ``$or``, and ``$nor``, dispatches dotted
        ``metadata``/``aliases`` paths, and translates top-level GridFS file
        fields. Unknown fields or unsupported operators append ``1=0``.

        Args:
            key: Filter field name, dotted path, or logical operator.
            value: Filter operand or subfilter list.
            conds: Predicate list to append SQL conditions to.
            plist: Parameter list to extend with bound values.
        """
        # Logical operators: never silently drop — build recursively.
        if key in ("$and", "$or", "$nor"):
            if not isinstance(value, (list, tuple)):
                logger.debug(f"Invalid '{key}' GridFS filter: {value!r}")
                conds.append("1=0")
                return
            sub_clauses: list[str] = []
            sub_params: list[Any] = []
            for subfilter in value:
                if not isinstance(subfilter, dict):
                    conds.append("1=0")
                    return
                if not subfilter:
                    sub_clauses.append("(1=1)")
                    continue
                temp_conds: list[str] = []
                temp_params: list[Any] = []
                for sk, sv in subfilter.items():
                    _append_one(sk, sv, temp_conds, temp_params)
                if temp_conds:
                    sub_clauses.append("(" + " AND ".join(temp_conds) + ")")
                    sub_params.extend(temp_params)
                else:
                    sub_clauses.append("(1=1)")
            if not sub_clauses:
                conds.append("1=1" if key in ("$and", "$nor") else "1=0")
                return
            if key == "$and":
                conds.append("(" + " AND ".join(sub_clauses) + ")")
            elif key == "$or":
                conds.append("(" + " OR ".join(sub_clauses) + ")")
            else:  # $nor
                conds.append(
                    "("
                    + " AND ".join(f"({c} IS NOT TRUE)" for c in sub_clauses)
                    + ")"
                )
            plist.extend(sub_params)
            return

        if isinstance(key, str) and key.startswith("$"):
            logger.debug(f"Unsupported GridFS filter operator '{key}'")
            conds.append("1=0")
            return

        if isinstance(key, str) and "." in key:
            top, _, sub = key.partition(".")
            if top in ("metadata", "aliases"):
                _append_dotted_json_condition(top, sub, value, conds, plist)
            else:
                logger.debug(f"Unsupported dotted GridFS filter '{key}'")
                conds.append("1=0")
            return

        match key:
            case "_id":

                def _id_param(val: Any) -> Any:
                    return str(val) if isinstance(val, ObjectId) else val

                if isinstance(value, dict):
                    for op, op_val in value.items():
                        match op:
                            case "$eq":
                                if op_val is None:
                                    conds.append("_id IS NULL")
                                else:
                                    conds.append("_id = ?")
                                    plist.append(_id_param(op_val))
                            case "$ne":
                                if op_val is None:
                                    conds.append("_id IS NOT NULL")
                                else:
                                    conds.append("(_id IS NULL OR _id != ?)")
                                    plist.append(_id_param(op_val))
                            case "$in" if isinstance(op_val, (list, tuple)):
                                if not op_val:
                                    conds.append("1=0")
                                else:
                                    has_null = any(v is None for v in op_val)
                                    non_nulls = [
                                        _id_param(v)
                                        for v in op_val
                                        if v is not None
                                    ]
                                    parts: list[str] = []
                                    if non_nulls:
                                        ph = ", ".join("?" for _ in non_nulls)
                                        parts.append(f"_id IN ({ph})")
                                        plist.extend(non_nulls)
                                    if has_null:
                                        parts.append("_id IS NULL")
                                    conds.append("(" + " OR ".join(parts) + ")")
                            case "$nin" if isinstance(op_val, (list, tuple)):
                                if not op_val:
                                    conds.append("1=1")
                                else:
                                    has_null = any(v is None for v in op_val)
                                    non_nulls = [
                                        _id_param(v)
                                        for v in op_val
                                        if v is not None
                                    ]
                                    if has_null and non_nulls:
                                        ph = ", ".join("?" for _ in non_nulls)
                                        conds.append(
                                            f"(_id IS NOT NULL AND _id NOT IN ({ph}))"
                                        )
                                        plist.extend(non_nulls)
                                    elif has_null:
                                        conds.append("_id IS NOT NULL")
                                    else:
                                        ph = ", ".join("?" for _ in non_nulls)
                                        conds.append(
                                            f"(_id IS NULL OR _id NOT IN ({ph}))"
                                        )
                                        plist.extend(non_nulls)
                            case "$exists" if isinstance(op_val, bool):
                                conds.append(
                                    "_id IS NOT NULL"
                                    if op_val
                                    else "_id IS NULL"
                                )
                            case _:
                                conds.append("1=0")
                    return
                if value is None:
                    conds.append("_id IS NULL")
                else:
                    conds.append("_id = ?")
                    plist.append(_id_param(value))
            case "id":
                conds.append("id = ?")
                plist.append(value)
            case "filename":
                if isinstance(value, re.Pattern):
                    pattern = regex_to_sql_pattern(value)
                    if pattern is None:
                        conds.append("1=0")
                    else:
                        conds.append("filename REGEXP ?")
                        plist.append(pattern)
                elif isinstance(value, dict):
                    regex_options = value.get("$options", "")
                    for op, val in value.items():
                        if op == "$options":
                            continue
                        match op:
                            case "$regex":
                                opts = (
                                    regex_options
                                    if isinstance(regex_options, str)
                                    else ""
                                )
                                pattern = regex_to_sql_pattern(val, opts)
                                if pattern is None:
                                    conds.append("1=0")
                                else:
                                    conds.append("filename REGEXP ?")
                                    plist.append(pattern)
                            case "$ne":
                                conds.append("filename != ?")
                                plist.append(val)
                            case "$eq":
                                conds.append("filename = ?")
                                plist.append(val)
                            case _:
                                conds.append("filename = ?")
                                plist.append(str(value))
                else:
                    conds.append("filename = ?")
                    plist.append(value)
            case "length":
                if isinstance(value, dict):
                    for op, val in value.items():
                        match op:
                            case "$gt":
                                conds.append("length > ?")
                                plist.append(val)
                            case "$gte":
                                conds.append("length >= ?")
                                plist.append(val)
                            case "$lt":
                                conds.append("length < ?")
                                plist.append(val)
                            case "$lte":
                                conds.append("length <= ?")
                                plist.append(val)
                            case "$eq":
                                conds.append("length = ?")
                                plist.append(val)
                            case "$ne":
                                conds.append("length != ?")
                                plist.append(val)
                else:
                    conds.append("length = ?")
                    plist.append(value)
            case "chunkSize":
                if isinstance(value, dict):
                    for op, val in value.items():
                        match op:
                            case "$gt":
                                conds.append("chunkSize > ?")
                                plist.append(val)
                            case "$gte":
                                conds.append("chunkSize >= ?")
                                plist.append(val)
                            case "$lt":
                                conds.append("chunkSize < ?")
                                plist.append(val)
                            case "$lte":
                                conds.append("chunkSize <= ?")
                                plist.append(val)
                            case "$eq":
                                conds.append("chunkSize = ?")
                                plist.append(val)
                            case "$ne":
                                conds.append("chunkSize != ?")
                                plist.append(val)
                else:
                    conds.append("chunkSize = ?")
                    plist.append(value)
            case "uploadDate":
                if isinstance(value, dict):
                    for op, val in value.items():
                        date_str = (
                            val.isoformat()
                            if hasattr(val, "isoformat")
                            else str(val)
                        )
                        match op:
                            case "$gt":
                                conds.append("uploadDate > ?")
                                plist.append(date_str)
                            case "$gte":
                                conds.append("uploadDate >= ?")
                                plist.append(date_str)
                            case "$lt":
                                conds.append("uploadDate < ?")
                                plist.append(date_str)
                            case "$lte":
                                conds.append("uploadDate <= ?")
                                plist.append(date_str)
                            case "$eq":
                                conds.append("uploadDate = ?")
                                plist.append(date_str)
                            case "$ne":
                                conds.append("uploadDate != ?")
                                plist.append(date_str)
                else:
                    date_str = (
                        value.isoformat()
                        if hasattr(value, "isoformat")
                        else str(value)
                    )
                    conds.append("uploadDate = ?")
                    plist.append(date_str)
            case "md5":
                if isinstance(value, dict):
                    for op, val in value.items():
                        match op:
                            case "$eq":
                                conds.append("md5 = ?")
                                plist.append(val)
                            case "$ne":
                                conds.append("md5 != ?")
                                plist.append(val)
                else:
                    conds.append("md5 = ?")
                    plist.append(value)
            case "metadata":
                if value is None:
                    conds.append("metadata IS NULL")
                elif isinstance(value, re.Pattern):
                    pattern = regex_to_sql_pattern(value)
                    if pattern is None:
                        conds.append("1=0")
                    else:
                        conds.append(
                            "(metadata IS NOT NULL AND json(metadata) REGEXP ?)"
                        )
                        plist.append(pattern)
                elif isinstance(value, dict) and any(
                    isinstance(k, str) and k.startswith("$") for k in value
                ):
                    regex_options = value.get("$options", "")
                    for op, val in value.items():
                        if op == "$options":
                            continue
                        match op:
                            case "$regex":
                                opts = (
                                    regex_options
                                    if isinstance(regex_options, str)
                                    else ""
                                )
                                pattern = regex_to_sql_pattern(val, opts)
                                if pattern is None:
                                    conds.append("1=0")
                                else:
                                    conds.append(
                                        "(metadata IS NOT NULL AND json(metadata) REGEXP ?)"
                                    )
                                    plist.append(pattern)
                            case "$eq":
                                if val is None:
                                    conds.append("metadata IS NULL")
                                elif isinstance(val, (dict, list)):
                                    conds.append("json(metadata) = json(?)")
                                    plist.append(neosqlite_json_dumps(val))
                                else:
                                    conds.append("metadata = ?")
                                    plist.append(val)
                            case "$ne":
                                if val is None:
                                    conds.append("metadata IS NOT NULL")
                                elif isinstance(val, (dict, list)):
                                    conds.append(
                                        "(metadata IS NULL OR json(metadata) IS NOT json(?))"
                                    )
                                    plist.append(neosqlite_json_dumps(val))
                                else:
                                    conds.append(
                                        "(metadata IS NULL OR metadata != ?)"
                                    )
                                    plist.append(
                                        str(val)
                                        if not isinstance(val, str)
                                        else val
                                    )
                            case "$exists" if isinstance(val, bool):
                                conds.append(
                                    "metadata IS NOT NULL"
                                    if val
                                    else "metadata IS NULL"
                                )
                            case _:
                                if not op.startswith("$"):
                                    _append_dotted_json_condition(
                                        "metadata", op, val, conds, plist
                                    )
                                else:
                                    conds.append("metadata LIKE ?")
                                    plist.append(f"%{op}%{val}%")
                elif isinstance(value, dict):
                    conds.append("json(metadata) = json(?)")
                    plist.append(neosqlite_json_dumps(value))
                else:
                    conds.append("metadata LIKE ?")
                    plist.append(f"%{value}%")
            case "aliases":
                conds.append(
                    "EXISTS (SELECT 1 FROM json_each(aliases) WHERE value = ?)"
                )
                plist.append(value)
            case "content_type" | "contentType":
                if isinstance(value, dict) and "$ne" in value:
                    conds.append("content_type != ?")
                    plist.append(value["$ne"])
                else:
                    conds.append("content_type = ?")
                    plist.append(value)
            case _:
                logger.debug(f"Unsupported GridFS filter field '{key}'")
                conds.append("1=0")

    for _k, _v in filter_dict.items():
        _append_one(_k, _v, where_conditions, params)

    where_clause = (
        "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    )
    return where_clause, params


def _gridfs_sort_to_sql(sort_spec: list[tuple[str, Any]] | None) -> str:
    """Translate a GridFS sort specification into a SQL ORDER BY clause.

    Maps standard GridFS columns and aliases (``upload_date``,
    ``chunk_size``, ``contentType``) as well as ``metadata.<subpath>`` keys
    via ``json_extract``. Unrecognized keys fall back to ``id``.

    Args:
        sort_spec: Sequence of ``(key, direction)`` pairs, or None.

    Returns:
        str: SQL ``ORDER BY ...`` clause, or ``""`` if ``sort_spec`` is empty.
    """
    if not sort_spec:
        return ""
    order_parts: list[str] = []
    for key, direction in sort_spec:
        dir_sql = (
            "DESC" if direction in (-1, "desc", "DESCENDING", False) else "ASC"
        )
        if key in (
            "_id",
            "id",
            "filename",
            "length",
            "chunkSize",
            "uploadDate",
            "md5",
            "content_type",
            "aliases",
        ):
            order_parts.append(f"{key} {dir_sql}")
        elif key == "upload_date":
            order_parts.append(f"uploadDate {dir_sql}")
        elif key == "chunk_size":
            order_parts.append(f"chunkSize {dir_sql}")
        elif key == "contentType":
            order_parts.append(f"content_type {dir_sql}")
        elif key.startswith("metadata."):
            sub = key[len("metadata.") :]
            json_path = _mongo_dot_subpath_to_json_path(sub)
            if json_path is not None:
                escaped = json_path.replace("'", "''")
                order_parts.append(
                    f"json_extract(metadata, '{escaped}') {dir_sql}"
                )
            else:
                order_parts.append(f"id {dir_sql}")
        else:
            order_parts.append(f"id {dir_sql}")
    if order_parts:
        return "ORDER BY " + ", ".join(order_parts)
    return ""


class GridOutCursor:
    """A lazy cursor for iterating over GridFS file documents.

    Supports ``sort``, ``skip``, ``limit``, ``batch_size``, ``clone``,
    ``rewind``, and ``to_list``, executing the underlying SQL query on first
    iteration and yielding ``GridOut`` instances.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        bucket_name: str,
        filter: dict[str, Any] | None = None,
        session: Any | None = None,
    ):
        """Initialize a new ``GridOutCursor`` instance.

        Args:
            db: SQLite database connection or wrapper with ``db`` attribute.
            bucket_name: Bucket name prefix for GridFS tables.
            filter: Optional filter dictionary restricting matched files.
            session: Optional ``ClientSession`` associated with the cursor.
        """
        self._db = getattr(db, "db", db)
        self._bucket_name = bucket_name
        self._filter = dict(filter) if filter else {}
        self._session = session
        self._files_collection = validate_identifier(f"{bucket_name}_files")

        self._sort: list[tuple[str, Any]] | None = None
        self._skip: int = 0
        self._limit: int = 0
        self._batch_size: int = 101

        self._file_ids: list[Any] | None = None
        self._rows: list[tuple[Any, ...]] = []
        self._sql_cursor: Any | None = None
        self._has_content_type = False
        self._has_aliases = False
        self._index: int = 0

    def sort(self, key_or_list: Any, direction: int | None = None) -> Self:
        """Sort the files returned by the cursor.

        Args:
            key_or_list: Field name, mapping of fields to directions, or list
                of ``(key, direction)`` pairs.
            direction: Optional sort direction when ``key_or_list`` is a
                field name (1 for ascending, -1 for descending).

        Returns:
            Self: This ``GridOutCursor`` instance for method chaining.

        Raises:
            RuntimeError: If called after the cursor has started iterating.
        """
        if self._file_ids is not None:
            raise RuntimeError("cannot sort after cursor has started iterating")
        if isinstance(key_or_list, str):
            self._sort = [(key_or_list, direction or 1)]
        elif isinstance(key_or_list, dict):
            self._sort = list(key_or_list.items())
        elif isinstance(key_or_list, (list, tuple)):
            self._sort = list(key_or_list)
        return self

    def skip(self, count: int) -> Self:
        """Skip the first ``count`` files matched by the cursor.

        Args:
            count: Non-negative number of files to skip.

        Returns:
            Self: This ``GridOutCursor`` instance for method chaining.

        Raises:
            RuntimeError: If called after the cursor has started iterating.
            ValueError: If ``count`` is not a non-negative integer.
        """
        if self._file_ids is not None:
            raise RuntimeError("cannot skip after cursor has started iterating")
        if not isinstance(count, int) or count < 0:
            raise ValueError("skip must be a non-negative integer")
        self._skip = count
        return self

    def limit(self, count: int) -> Self:
        """Limit the maximum number of files returned by the cursor.

        Args:
            count: Non-negative maximum number of files to return (0 for
                unlimited).

        Returns:
            Self: This ``GridOutCursor`` instance for method chaining.

        Raises:
            RuntimeError: If called after the cursor has started iterating.
            ValueError: If ``count`` is not a non-negative integer.
        """
        if self._file_ids is not None:
            raise RuntimeError(
                "cannot limit after cursor has started iterating"
            )
        if not isinstance(count, int) or count < 0:
            raise ValueError("limit must be a non-negative integer")
        self._limit = count
        return self

    def batch_size(self, size: int) -> Self:
        """Set the batch size for the cursor.

        Args:
            size: Positive integer number of documents per batch.

        Returns:
            Self: This ``GridOutCursor`` instance for method chaining.

        Raises:
            ValueError: If ``size`` is not a positive integer.
        """
        if not isinstance(size, int) or size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self._batch_size = size
        return self

    def clone(self) -> Self:
        """Create an unexecuted copy of this cursor.

        Returns:
            Self: New ``GridOutCursor`` with copied filter and modifiers.
        """
        cloned = self.__class__(
            self._db, self._bucket_name, deepcopy(self._filter), self._session
        )
        cloned._sort = list(self._sort) if self._sort else None
        cloned._skip = self._skip
        cloned._limit = self._limit
        cloned._batch_size = self._batch_size
        return cloned

    def close(self) -> None:
        """Close the underlying SQLite cursor if still open."""
        if self._sql_cursor is not None:
            try:
                self._sql_cursor.close()
            except Exception as e:
                logger.debug(f"Error closing GridOutCursor SQL cursor: {e}")
            self._sql_cursor = None

    def __del__(self) -> None:
        """Release any open SQLite cursor on garbage collection."""
        try:
            self.close()
        except Exception:
            pass

    def rewind(self) -> Self:
        """Reset the cursor to its initial unexecuted state.

        Returns:
            Self: This ``GridOutCursor`` instance for method chaining.
        """
        self.close()
        self._file_ids = None
        self._rows = []
        self._index = 0
        return self

    def _ensure_executed(self) -> None:
        """Execute the underlying files query lazily if not yet executed.

        Selects all file metadata columns in a single query and streams rows in
        ``_batch_size`` chunks via ``fetchmany()``, eliminating N+1 per-file
        metadata queries.
        """
        if self._file_ids is not None:
            return

        self._has_content_type, self._has_aliases = (
            GridOut._detect_optional_columns(self._db, self._files_collection)
        )
        select_columns = [
            "id",
            "filename",
            "length",
            "chunkSize",
            "uploadDate",
            "md5",
            "json(metadata)",
            "_id",
        ]
        if self._has_content_type:
            select_columns.append("content_type")
        if self._has_aliases:
            select_columns.append("json(aliases)")

        where_clause, params = build_gridfs_files_where(self._filter)
        sort_clause = _gridfs_sort_to_sql(self._sort)

        pagination_clause = ""
        if self._limit > 0 and self._skip > 0:
            pagination_clause = " LIMIT ? OFFSET ?"
            params.extend([self._limit, self._skip])
        elif self._limit > 0:
            pagination_clause = " LIMIT ?"
            params.append(self._limit)
        elif self._skip > 0:
            pagination_clause = " LIMIT -1 OFFSET ?"
            params.append(self._skip)

        query = f"SELECT {', '.join(select_columns)} FROM {quote_table_name(self._files_collection)} {where_clause} {sort_clause} {pagination_clause}".strip()
        self._sql_cursor = self._db.execute(query, params)
        self._file_ids = []
        self._rows = []
        self._index = 0

    def _fetch_next_batch(self, count: int | None = None) -> int:
        """Pull a batch of file metadata rows from ``_sql_cursor``."""
        if self._sql_cursor is None or self._file_ids is None:
            return 0
        batch_rows = (
            self._sql_cursor.fetchall()
            if count is None
            else self._sql_cursor.fetchmany(count)
        )
        if not batch_rows or count is None:
            self.close()
        if batch_rows:
            self._rows.extend(batch_rows)
            self._file_ids.extend(row[0] for row in batch_rows)
        return len(batch_rows)

    def _build_grid_out(self, row: tuple[Any, ...]) -> GridOut:
        """Construct a ``GridOut`` directly from a prefetched metadata row."""
        return GridOut(
            self._db,
            self._bucket_name,
            row[0],
            _prefetched_row=row,
            _has_content_type=self._has_content_type,
            _has_aliases=self._has_aliases,
        )

    def to_list(self, length: int | None = None) -> list[GridOut]:
        """Retrieve matched files as a list of ``GridOut`` objects.

        Args:
            length: Optional non-negative maximum number of files to return.

        Returns:
            list[GridOut]: List of ``GridOut`` instances for matched files.

        Raises:
            ValueError: If ``length`` is provided and is not a non-negative
                integer.
        """
        self._ensure_executed()
        assert self._file_ids is not None
        start = self._index
        if length is not None:
            if (
                not isinstance(length, int)
                or isinstance(length, bool)
                or length < 0
            ):
                raise ValueError("length must be a non-negative integer")
            while (
                len(self._rows) - start < length
                and self._sql_cursor is not None
            ):
                needed = length - (len(self._rows) - start)
                if self._fetch_next_batch(max(needed, self._batch_size)) == 0:
                    break
            stop = min(start + length, len(self._rows))
            selected_rows = self._rows[start:stop]
            self._index = stop
        else:
            if self._sql_cursor is not None:
                self._fetch_next_batch(None)
            selected_rows = self._rows[start:]
            self._index = len(self._rows)
        return [self._build_grid_out(row) for row in selected_rows]

    def __iter__(self) -> Self:
        """Return this cursor as an iterator after executing the query.

        Returns:
            Self: This ``GridOutCursor`` iterator instance.
        """
        self._ensure_executed()
        return self

    def __next__(self) -> GridOut:
        """Return the next ``GridOut`` file object from the cursor.

        Returns:
            GridOut: Next matched GridFS file stream.

        Raises:
            StopIteration: When all matched files have been yielded.
        """
        self._ensure_executed()
        if self._index >= len(self._rows) and self._sql_cursor is not None:
            self._fetch_next_batch(self._batch_size)
        if self._index >= len(self._rows):
            raise StopIteration

        row = self._rows[self._index]
        self._index += 1
        return self._build_grid_out(row)
