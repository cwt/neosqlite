"""GridFS Adapter for NX-27017 Proxy.

This adapter intercepts PyMongo's GridFS operations and translates them to
NeoSQLite's native GridFS API. It handles the schema difference between:
- MongoDB: Collections with documents
- NeoSQLite: Direct column tables (fs_files, fs_chunks)

The adapter is called from the proxy's command handlers to intercept all
operations on GridFS collections (fs.files, fs.chunks, etc.) and route them
to the appropriate NeoSQLite GridFSBucket methods.
"""

import logging
from datetime import UTC, datetime
from typing import Any

from neosqlite.collection.json_helpers import neosqlite_json_dumps
from neosqlite.gridfs import GridFSBucket
from neosqlite.gridfs.errors import NoFile
from neosqlite.objectid import ObjectId
from neosqlite.sql_utils import quote_table_name

logger = logging.getLogger(__name__)

# GridFS constants
DEFAULT_CHUNK_SIZE_BYTES = 261_120  # 256 KiB (MongoDB default)


def _table_columns(db: Any, table_name: str) -> set[str] | None:
    """Return column names for ``table_name``, or ``None`` if absent."""
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cursor.fetchone() is None:
        return None
    info_cursor = db.execute(
        f"PRAGMA table_info({quote_table_name(table_name)})"
    )
    return {row[1] for row in info_cursor.fetchall()}


def is_regular_collection_table(
    db: Any, table_name: str, *, is_chunks: bool
) -> bool:
    """Return True if ``table_name`` exists as a regular document collection."""
    columns = _table_columns(db, table_name)
    if columns is None:
        return False
    if is_chunks:
        return "data" in columns and "files_id" not in columns
    return "data" in columns and "filename" not in columns


class GridFSAdapter:
    """Adapter translating PyMongo GridFS operations to NeoSQLite GridFS.

    Intercepts operations targeting GridFS collections (such as ``fs.files``
    and ``fs.chunks``) and translates between MongoDB document semantics and
    NeoSQLite's relational ``<bucket>_files`` / ``<bucket>_chunks`` tables,
    handling schema migration, ``ObjectId`` normalization, and integer
    ``files_id`` foreign keys.
    """

    def __init__(self, db: Any, bucket_name: str = "fs"):
        """Initialize the GridFS adapter.

        Args:
            db: NeoSQLite SQLite connection (not a ``neosqlite.Connection``).
            bucket_name: Name of the GridFS bucket (defaults to ``"fs"``).
        """
        self._db = db
        self._bucket_name = bucket_name
        self._bucket: GridFSBucket | None = None  # type: ignore[assignment]
        self._ensure_bucket()

    def _validate_no_regular_collection_conflict(
        self, files_table: str, chunks_table: str
    ) -> None:
        """Raise ValueError if a regular collection collides with GridFS names."""
        if is_regular_collection_table(self._db, files_table, is_chunks=False):
            raise ValueError(
                f"Table '{files_table}' is a regular collection, not a GridFS files table"
            )
        if is_regular_collection_table(self._db, chunks_table, is_chunks=True):
            raise ValueError(
                f"Table '{chunks_table}' is a regular collection, not a GridFS chunks table"
            )

    def _ensure_gridfs_schema(self) -> None:
        """Ensure the GridFS tables have the required schema columns.

        If ``<bucket>_files`` or ``<bucket>_chunks`` tables already exist from
        an older schema, adds any missing columns via ``ALTER TABLE`` before
        ``GridFSBucket`` creates indexes on them. Regular document collections
        sharing ``<bucket>_files`` or ``<bucket>_chunks`` names are rejected
        without altering them.
        """
        files_table = f"{self._bucket_name}_files"
        chunks_table = f"{self._bucket_name}_chunks"

        self._validate_no_regular_collection_conflict(files_table, chunks_table)

        logger.debug(
            f"_ensure_gridfs_schema: bucket_name={self._bucket_name}, files_table={files_table}"
        )

        columns = _table_columns(self._db, files_table)
        table_exists = columns is not None
        logger.debug(
            f"_ensure_gridfs_schema: files_table exists={table_exists}"
        )

        if table_exists and columns is not None:
            logger.debug(
                f"GridFS files table '{files_table}' already exists, checking schema..."
            )
            logger.debug(
                f"_ensure_gridfs_schema: columns in {files_table}: {columns}"
            )

            required_files_columns = {
                "id",
                "_id",
                "filename",
                "length",
                "chunkSize",
                "uploadDate",
                "md5",
                "metadata",
                "content_type",
                "aliases",
            }
            missing_columns = required_files_columns - columns
            logger.debug(
                f"_ensure_gridfs_schema: missing_columns={missing_columns}"
            )

            if missing_columns:
                logger.debug(
                    f"Adding missing columns to {files_table}: {missing_columns}"
                )
                quoted_files = quote_table_name(files_table)
                for col in missing_columns:
                    if col in ("content_type", "aliases", "metadata"):
                        col_type = "TEXT"
                    else:
                        col_type = (
                            "TEXT"
                            if col in ("_id", "filename", "uploadDate", "md5")
                            else "INTEGER"
                        )
                    self._db.execute(
                        f"ALTER TABLE {quoted_files} ADD COLUMN {col} {col_type}"
                    )
            else:
                logger.debug(
                    f"_ensure_gridfs_schema: All required columns exist in {files_table}"
                )

            chunks_columns = _table_columns(self._db, chunks_table)
            if chunks_columns is None:
                # _ensure_bucket() recreates it via CREATE TABLE IF EXISTS.
                logger.debug(
                    f"Chunks table '{chunks_table}' missing; "
                    "leaving creation to GridFSBucket"
                )
                return
            logger.debug(
                f"_ensure_gridfs_schema: columns in {chunks_table}: {chunks_columns}"
            )
            if "files_id" not in chunks_columns:
                logger.debug(
                    f"Adding missing columns to {chunks_table}: files_id, n, data"
                )
                quoted_chunks = quote_table_name(chunks_table)
                self._db.execute(
                    f"ALTER TABLE {quoted_chunks} ADD COLUMN files_id INTEGER"
                )
                self._db.execute(
                    f"ALTER TABLE {quoted_chunks} ADD COLUMN n INTEGER"
                )
                self._db.execute(
                    f"ALTER TABLE {quoted_chunks} ADD COLUMN data BLOB"
                )

        cursor = self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"%{self._bucket_name}%",),
        )
        all_tables = [row[0] for row in cursor.fetchall()]
        logger.debug(
            f"_ensure_gridfs_schema: All tables matching '{self._bucket_name}': {all_tables}"
        )

    def _ensure_bucket(self) -> None:
        """Ensure the underlying ``GridFSBucket`` instance is initialized.

        Runs schema verification first, reuses existing properly structured
        tables directly when both files and chunks tables are present, or
        instantiates a new ``GridFSBucket`` with
        ``DEFAULT_CHUNK_SIZE_BYTES``.
        """
        if self._bucket is None:
            from neosqlite.gridfs import GridFSBucket

            logger.debug(
                f"Creating GridFSBucket: bucket_name={self._bucket_name}"
            )
            logger.debug(f"Files collection will be: {self._bucket_name}_files")
            logger.debug(
                f"Chunks collection will be: {self._bucket_name}_chunks"
            )

            self._ensure_gridfs_schema()

            # Check if both files and chunks tables already exist with GridFS
            # schemas. If so, we can skip GridFSBucket creation to avoid
            # migration overhead; otherwise let GridFSBucket recreate missing
            # tables (e.g., after fs.chunks was dropped).
            files_table = f"{self._bucket_name}_files"
            chunks_table = f"{self._bucket_name}_chunks"
            files_cols = _table_columns(self._db, files_table)
            chunks_cols = _table_columns(self._db, chunks_table)
            required_files_cols = {
                "id",
                "_id",
                "filename",
                "length",
                "chunkSize",
                "uploadDate",
                "md5",
                "metadata",
            }
            required_chunks_cols = {"_id", "files_id", "n", "data"}
            if (
                files_cols is not None
                and chunks_cols is not None
                and "data" not in files_cols
                and required_files_cols.issubset(files_cols)
                and required_chunks_cols.issubset(chunks_cols)
            ):
                logger.debug(
                    f"Tables {files_table} and {chunks_table} already exist with correct schema, skipping GridFSBucket creation"
                )
                # Create a dummy bucket object just to satisfy the adapter
                self._bucket = GridFSBucket.__new__(GridFSBucket)
                self._get_bucket()._db = self._db
                self._get_bucket()._bucket_name = self._bucket_name
                self._get_bucket()._files_collection = files_table
                self._get_bucket()._chunks_collection = chunks_table
                self._get_bucket()._chunk_size_bytes = DEFAULT_CHUNK_SIZE_BYTES
                logger.debug("GridFSBucket initialized (skipped creation)")
                return

            # Also check fs.chunks table if it exists (PyMongo might create this instead)
            cursor = self._db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (f"{self._bucket_name}.chunks",),
            )
            dot_chunks_exists = cursor.fetchone() is not None
            if dot_chunks_exists:
                logger.debug(
                    f"WARNING: {self._bucket_name}.chunks table exists!"
                )
                cursor = self._db.execute(
                    f"PRAGMA table_info('{self._bucket_name}.chunks')"
                )
                columns = {row[1] for row in cursor.fetchall()}
                logger.debug(
                    f"Columns in {self._bucket_name}.chunks: {columns}"
                )

            self._bucket = GridFSBucket(
                self._db,
                bucket_name=self._bucket_name,
                chunk_size_bytes=DEFAULT_CHUNK_SIZE_BYTES,
            )
            logger.debug("GridFSBucket created successfully")

    def _get_bucket(self) -> GridFSBucket:
        """Return the initialized ``GridFSBucket`` with a non-None assertion.

        Returns:
            GridFSBucket: Active GridFS bucket instance for this adapter.
        """
        assert self._bucket is not None, "GridFS bucket not initialized"
        return self._bucket

    def ensure_file_metadata_exists(self, files_id: Any, db: Any) -> None:
        """Ensure file metadata exists in ``<bucket>_files`` for a file.

        Called during chunk insertion to auto-create minimal file metadata
        when PyMongo writes chunks before sending the corresponding file
        metadata document.

        Args:
            files_id: The ``ObjectId`` (or string ID) of the file.
            db: SQLite database connection supporting ``execute``.
        """
        files_id_str = str(files_id)

        # Check if file already exists in fs_files
        cursor = db.execute(
            f"SELECT id FROM {self._get_bucket()._files_collection} WHERE _id = ?",
            (files_id_str,),
        )
        row = cursor.fetchone()
        if row:
            logger.debug(f"File metadata already exists for _id={files_id_str}")
            return

        logger.debug(f"Auto-creating file metadata for _id={files_id_str}")

        # Get upload date
        upload_date = datetime.now(UTC).isoformat()

        # Insert minimal file metadata
        # We don't have filename, length, etc. from PyMongo, so we use defaults
        # Columns: id, _id, filename, length, chunkSize, uploadDate, md5, metadata, content_type, aliases
        db.execute(
            f"""
            INSERT INTO {self._get_bucket()._files_collection}
            (id, _id, filename, length, chunkSize, uploadDate, md5, metadata, content_type, aliases)
            VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                files_id_str,
                "",  # filename - unknown
                0,  # length - unknown
                self._get_bucket()._chunk_size_bytes,
                upload_date,
                None,  # md5
                "{}",  # metadata as JSON
            ),
        )
        logger.debug(f"Auto-created file metadata for _id={files_id_str}")

    def _convert_objectid(self, oid: Any) -> ObjectId | str | None:
        """Convert an ObjectId value from wire or query formats.

        Unwraps ``$eq`` and ``$oid`` dict wrappers and converts BSON or hex
        string values into ``ObjectId`` instances, returning unrecognized
        strings or other values unchanged.

        Args:
            oid: Raw identifier value, query wrapper dict, or ``None``.

        Returns:
            ObjectId | str | None: Normalized ``ObjectId``, fallback string or
            raw value, or ``None`` if ``oid`` is ``None``.
        """
        if oid is None:
            return None
        from nx_27017.utils import convert_bson_to_neo_objectids

        oid = convert_bson_to_neo_objectids(oid)
        if isinstance(oid, dict) and "$eq" in oid:
            oid = convert_bson_to_neo_objectids(oid["$eq"])
        if isinstance(oid, ObjectId):
            return oid
        if isinstance(oid, dict) and "$oid" in oid:
            return ObjectId(oid["$oid"])
        if isinstance(oid, str):
            try:
                return ObjectId(oid)
            except Exception as exc:
                logger.debug("ObjectId parse fallback for %r: %s", oid, exc)
                return oid
        return oid

    def _convert_gridfs_result(self, grid_out: Any) -> dict[str, Any]:
        """Convert a ``GridOut`` object into a MongoDB-compatible file dict.

        Args:
            grid_out: NeoSQLite ``GridOut`` instance returned by bucket find.

        Returns:
            dict[str, Any]: File metadata dictionary with ``_id``,
            ``filename``, ``length``, ``chunkSize``, ``uploadDate``, ``md5``,
            and ``metadata``.
        """
        return {
            "_id": grid_out._id,
            "filename": grid_out.filename,
            "length": grid_out.length,
            "chunkSize": grid_out.chunk_size,
            "uploadDate": grid_out.upload_date,
            "md5": grid_out.md5,
            "metadata": grid_out.metadata,
        }

    def handle_insert(
        self, docs: list[dict], is_files: bool = True
    ) -> dict[str, Any]:
        """Handle insert operations on a GridFS files or chunks collection.

        Args:
            docs: List of file or chunk documents to insert.
            is_files: True if inserting into files collection, False for
                chunks.

        Returns:
            dict[str, Any]: MongoDB-style insert response with ``ok``, ``n``,
            and ``insertedIds`` (or ``errmsg`` on error).
        """
        self._ensure_bucket()

        if not docs:
            return {"ok": 1, "n": 0, "insertedIds": []}

        inserted_ids = []

        try:
            logger.debug(
                f"GridFSAdapter.handle_insert: is_files={is_files}, doc_count={len(docs)}"
            )
            logger.debug(
                f"Files collection: {self._get_bucket()._files_collection}"
            )
            logger.debug(
                f"Chunks collection: {self._get_bucket()._chunks_collection}"
            )

            for doc in docs:
                converted = _convert_objectids_in_dict(doc)
                if converted is None:
                    continue
                doc = converted

                if is_files:
                    inserted_id = self._insert_file(doc)
                    inserted_ids.append(inserted_id)
                else:
                    inserted_id = self._insert_chunk(doc)
                    inserted_ids.append(inserted_id)

            return {
                "ok": 1,
                "n": len(inserted_ids),
                "insertedIds": inserted_ids,
            }
        except Exception as e:
            logger.error(f"GridFS insert error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def _insert_file(self, doc: dict) -> ObjectId | str:
        """Insert or update a file document in the GridFS files table.

        Writes directly to ``<bucket>_files`` via SQL. If a placeholder row
        was already auto-created by ``ensure_file_metadata_exists``, updates
        that row with the incoming metadata fields.

        Args:
            doc: File document containing ``_id``, ``filename``, ``length``,
                ``chunkSize``, ``uploadDate``, ``md5``, and ``metadata``.

        Returns:
            ObjectId | str: The inserted or updated file identifier.
        """
        file_id = self._convert_objectid(doc.get("_id")) or ObjectId()
        filename = doc.get("filename", "")
        length = doc.get("length", 0)
        chunk_size = doc.get("chunkSize", self._get_bucket()._chunk_size_bytes)
        upload_date = doc.get("uploadDate")
        if isinstance(upload_date, datetime):
            upload_date = upload_date.isoformat()
        elif upload_date is None:
            upload_date = datetime.now(UTC).isoformat()
        md5 = doc.get("md5")
        metadata = doc.get("metadata", {})

        # Serialize metadata as JSON string using NeoSQLite's JSON encoder
        # so ObjectId / datetime values don't fail json.dumps.
        if isinstance(metadata, dict):
            metadata = neosqlite_json_dumps(metadata)

        logger.debug(
            f"_insert_file: file_id={file_id}, filename={filename}, length={length}"
        )

        # Check if file already exists (may have been auto-created via ensure_file_metadata_exists)
        cursor = self._db.execute(
            f"SELECT id, _id FROM {self._get_bucket()._files_collection} WHERE _id = ?",
            (str(file_id),),
        )
        existing = cursor.fetchone()

        if existing:
            # File already exists (likely auto-created from chunk insert)
            # Update it with the actual values from PyMongo
            logger.debug(
                f"_insert_file: File already exists with _id={file_id}, updating with actual values"
            )
            self._db.execute(
                f"""
                UPDATE {self._get_bucket()._files_collection}
                SET filename = ?, length = ?, chunkSize = ?, uploadDate = ?, md5 = ?, metadata = ?
                WHERE _id = ?
                """,
                (
                    filename,
                    length,
                    chunk_size,
                    upload_date,
                    md5,
                    metadata,
                    str(file_id),
                ),
            )
            return file_id

        self._db.execute(
            f"""
            INSERT INTO {self._get_bucket()._files_collection}
            (id, _id, filename, length, chunkSize, uploadDate, md5, metadata, content_type, aliases)
            VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
            (
                str(file_id),
                filename,
                length,
                chunk_size,
                upload_date,
                md5,
                metadata,
            ),
        )

        # Verify the insert
        cursor = self._db.execute(
            f"SELECT id, _id, filename FROM {self._get_bucket()._files_collection} WHERE _id = ?",
            (str(file_id),),
        )
        row = cursor.fetchone()
        logger.debug(f"_insert_file: verification - row={row}")

        return file_id

    def _insert_chunk(self, doc: dict) -> ObjectId | str:
        """Insert a chunk document into the GridFS chunks table.

        Resolves the parent file's integer row ID from ``<bucket>_files`` and
        inserts ``(files_id, n, data)`` into ``<bucket>_chunks`` so SQLite
        auto-generates the chunk row ID.

        Args:
            doc: Chunk document containing ``_id``, ``files_id``, ``n``, and
                ``data``.

        Returns:
            ObjectId | str: The chunk identifier from ``doc`` or a new
            ``ObjectId``.
        """
        chunk_id = self._convert_objectid(doc.get("_id")) or ObjectId()
        files_id = doc.get("files_id")
        n = doc.get("n", 0)
        data = doc.get("data", b"")

        logger.debug(
            f"_insert_chunk: chunk_id={chunk_id}, files_id={files_id}, n={n}, data_type={type(data)}, data_len={len(data) if isinstance(data, (bytes, bytearray)) else 'N/A'}"
        )

        # Convert ObjectId to integer ID by looking up fs_files
        # NeoSQLite's fs_chunks.files_id references fs_files.id (INTEGER)
        # but PyMongo sends the ObjectId as a string
        files_id_int = None
        if files_id is not None:
            # files_id might be ObjectId or string representation
            files_id_str = str(files_id)
            cursor = self._db.execute(
                f"SELECT id FROM {self._get_bucket()._files_collection} WHERE _id = ?",
                (files_id_str,),
            )
            row = cursor.fetchone()
            if row:
                files_id_int = row[0]
            else:
                logger.warning(
                    f"_insert_chunk: Could not find file with _id={files_id_str}"
                )
                # Try storing as string anyway (will likely fail foreign key)
                files_id_int = files_id_str

        logger.debug(f"_insert_chunk: using integer files_id={files_id_int}")

        # Don't pass _id - let SQLite auto-generate it
        # PyMongo sends ObjectId as _id but NeoSQLite uses auto-increment integers for chunks
        self._db.execute(
            f"""
            INSERT INTO {self._get_bucket()._chunks_collection}
            (files_id, n, data)
            VALUES (?, ?, ?)
        """,
            (files_id_int, n, data),
        )

        return chunk_id

    def handle_find(
        self, filter_query: dict | None = None
    ) -> list[dict[str, Any]]:
        """Handle find operations on the GridFS files collection.

        Args:
            filter_query: Optional MongoDB-style query filter dictionary.

        Returns:
            list[dict[str, Any]]: Matching file documents in MongoDB format.
        """
        self._ensure_bucket()

        if filter_query is None:
            filter_query = {}

        filter_query = _convert_objectids_in_dict(filter_query) or {}

        try:
            cursor = self._get_bucket().find(filter_query)
            files = list(cursor)
            return [self._convert_gridfs_result(f) for f in files]
        except Exception as e:
            logger.error(f"GridFS find error: {e}")
            raise

    def handle_chunks_find(
        self, filter_query: dict | None = None
    ) -> list[dict[str, Any]]:
        """Handle find operations on the GridFS chunks collection.

        Supports querying all chunks when ``files_id`` is omitted, or filtering
        by ``files_id`` (including ``{"$eq": ...}``) and chunk index ``n``
        equality or comparison operators.

        Args:
            filter_query: Optional query filter dictionary for chunks.

        Returns:
            list[dict[str, Any]]: Matching chunk documents in MongoDB format.
        """
        self._ensure_bucket()

        if filter_query is None:
            filter_query = {}

        filter_query = _convert_objectids_in_dict(filter_query) or {}

        try:
            files_id = filter_query.get("files_id")
            if isinstance(files_id, dict) and "$eq" in files_id:
                files_id = files_id["$eq"]
            if files_id is None:
                # No files_id filter: return all chunks.
                cursor = self._db.execute(
                    f"SELECT _id, files_id, n, data FROM {self._get_bucket()._chunks_collection} ORDER BY files_id, n"
                )
                return [
                    {
                        "_id": row[0],
                        "files_id": row[1],
                        "n": row[2],
                        "data": row[3],
                    }
                    for row in cursor.fetchall()
                ]
            if isinstance(files_id, (ObjectId, str, int)):
                file_int_id = self._get_bucket()._get_integer_id_for_file(
                    files_id
                )
                if file_int_id is not None:
                    n_filter = filter_query.get("n")
                    where_clauses = ["files_id = ?"]
                    params: list[Any] = [file_int_id]
                    if isinstance(n_filter, dict):
                        for op, val in n_filter.items():
                            match op:
                                case "$gte":
                                    where_clauses.append("n >= ?")
                                    params.append(val)
                                case "$gt":
                                    where_clauses.append("n > ?")
                                    params.append(val)
                                case "$lte":
                                    where_clauses.append("n <= ?")
                                    params.append(val)
                                case "$lt":
                                    where_clauses.append("n < ?")
                                    params.append(val)
                                case "$eq":
                                    where_clauses.append("n = ?")
                                    params.append(val)
                    elif isinstance(n_filter, int):
                        where_clauses.append("n = ?")
                        params.append(n_filter)

                    where_sql = " AND ".join(where_clauses)
                    cursor = self._db.execute(
                        f"""SELECT _id, files_id, n, data FROM {self._get_bucket()._chunks_collection}
                           WHERE {where_sql} ORDER BY n""",
                        params,
                    )
                    return [
                        {
                            "_id": row[0],
                            "files_id": files_id,
                            "n": row[2],
                            "data": row[3],
                        }
                        for row in cursor.fetchall()
                    ]
            return []
        except Exception as e:
            logger.error(f"GridFS chunks find error: {e}")
            raise

    def handle_delete(
        self, file_ids: list[Any], filenames: list[str] | None = None
    ) -> dict[str, Any]:
        """Handle delete operations on the GridFS files collection.

        Args:
            file_ids: List of file IDs to delete from the bucket.
            filenames: Optional list of filenames to delete (all versions
                removed via ``GridFSBucket.delete_by_name``).

        Returns:
            dict[str, Any]: MongoDB-style delete response with ``ok`` and
            ``n`` (or ``errmsg`` on error).
        """
        self._ensure_bucket()

        deleted = 0
        try:
            for file_id in file_ids:
                file_id = self._convert_objectid(file_id)
                if file_id is not None:
                    try:
                        self._get_bucket().delete(file_id)
                        deleted += 1
                    except NoFile:
                        pass
            for filename in filenames or []:
                try:
                    self._get_bucket().delete_by_name(filename)
                    deleted += 1
                except NoFile:
                    pass

            return {"ok": 1, "n": deleted}
        except Exception as e:
            logger.error(f"GridFS delete error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def handle_chunks_delete(self, file_ids: list[Any]) -> dict[str, Any]:
        """Handle delete operations on the GridFS chunks collection.

        Deletes chunks matching each resolved file integer ID when PyMongo
        cleans up chunks directly by ``files_id``.

        Args:
            file_ids: List of file IDs whose chunks should be removed.

        Returns:
            dict[str, Any]: MongoDB-style delete response with ``ok`` and
            ``n`` (or ``errmsg`` on error).
        """
        self._ensure_bucket()

        deleted = 0
        try:
            for file_id in file_ids:
                file_id = self._convert_objectid(file_id)
                if file_id is None:
                    continue
                file_int_id = self._get_bucket()._get_integer_id_for_file(
                    file_id
                )
                if file_int_id is None:
                    continue
                cursor = self._db.execute(
                    f"DELETE FROM {self._get_bucket()._chunks_collection} "
                    "WHERE files_id = ?",
                    (file_int_id,),
                )
                deleted += max(0, cursor.rowcount)
            try:
                self._db.commit()
            except Exception as exc:
                logger.debug("GridFS chunks delete commit skipped: %s", exc)
            return {"ok": 1, "n": deleted}
        except Exception as e:
            logger.error(f"GridFS chunks delete error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def handle_update(self, file_id: Any, update: dict) -> dict[str, Any]:
        """Handle update operations on the GridFS files collection.

        Supports ``$set`` updates for ``filename`` and ``metadata``.

        Args:
            file_id: Identifier of the file to update.
            update: Update specification dictionary containing ``$set``.

        Returns:
            dict[str, Any]: MongoDB-style update response with ``ok``, ``n``,
            and ``nModified`` (or ``errmsg`` on error).
        """
        self._ensure_bucket()

        try:
            file_id = self._convert_objectid(file_id)

            set_ops = update.get("$set", {})

            updates = []
            params = []

            if "filename" in set_ops:
                updates.append("filename = ?")
                params.append(set_ops["filename"])

            if "metadata" in set_ops:
                updates.append("metadata = ?")
                params.append(neosqlite_json_dumps(set_ops["metadata"]))

            if not updates:
                return {"ok": 1, "n": 0, "nModified": 0}

            # Get the integer ID for the file
            file_int_id = self._get_bucket()._get_integer_id_for_file(file_id)
            if file_int_id is None:
                return {"ok": 0, "errmsg": "File not found"}

            params.append(file_int_id)

            query = f"""
                UPDATE {self._get_bucket()._files_collection}
                SET {", ".join(updates)}
                WHERE id = ?
            """

            cursor = self._db.execute(query, params)

            return {"ok": 1, "n": cursor.rowcount, "nModified": cursor.rowcount}
        except Exception as e:
            logger.error(f"GridFS update error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def create_indexes(self) -> dict[str, Any]:
        """Return a compatibility createIndexes response for GridFS.

        Returns:
            dict[str, Any]: MongoDB-style ``createIndexes`` response dict.
        """
        self._ensure_bucket()

        try:
            return {
                "ok": 1,
                "createdCollectionAutomatically": False,
                "numIndexesBefore": 0,
                "numIndexesAfter": 0,
                "indexesCreated": [],
            }
        except Exception as e:
            logger.error(f"GridFS createIndexes error: {e}")
            return {"ok": 0, "errmsg": str(e)}


def _convert_objectids_in_dict(doc: dict) -> dict | None:
    """Recursively convert extended-JSON ``$oid`` and BSON types in a document.

    Args:
        doc: Document dictionary potentially containing ``{"$oid": ...}``
            or ``bson`` values.

    Returns:
        dict | None: Converted document with NeoSQLite ``ObjectId`` instances,
        or ``None`` if conversion returns ``None``.
    """
    from nx_27017.utils import (
        convert_bson_to_neo_objectids,
        convert_json_to_neo_objectids,
    )

    return convert_json_to_neo_objectids(convert_bson_to_neo_objectids(doc))


def create_gridfs_adapter(
    db: Any, coll_name: str
) -> tuple[GridFSAdapter | None, str | None]:
    """Create a ``GridFSAdapter`` for a GridFS collection name if applicable.

    Args:
        db: NeoSQLite SQLite database connection.
        coll_name: Collection name (e.g., ``"fs.files"``, ``"fs.chunks"``).

    Returns:
        tuple[GridFSAdapter | None, str | None]: Tuple of
        ``(GridFSAdapter, bucket_name)`` if ``coll_name`` is a GridFS
        collection without a regular-collection name conflict, otherwise
        ``(None, None)``.
    """
    if not _is_gridfs_collection(coll_name):
        return None, None

    bucket_name = _get_gridfs_bucket_name(coll_name)
    if bucket_name is None:
        return None, None

    try:
        adapter = GridFSAdapter(db, bucket_name=bucket_name)
    except ValueError as exc:
        logger.debug("GridFS adapter rejected for %r: %s", coll_name, exc)
        return None, None
    return adapter, bucket_name


def _is_gridfs_collection(coll_name: str) -> bool:
    """Check whether a collection name denotes a GridFS files or chunks table.

    Args:
        coll_name: Collection name to inspect.

    Returns:
        bool: True if ``coll_name`` ends with ``.files`` or ``.chunks``, else
        False.
    """
    return coll_name.endswith(".files") or coll_name.endswith(".chunks")


def _get_gridfs_bucket_name(coll_name: str) -> str | None:
    """Extract the GridFS bucket name from a GridFS collection name.

    Args:
        coll_name: Collection name ending with ``.files`` or ``.chunks``.

    Returns:
        str | None: Bucket prefix before ``.files`` or ``.chunks``, or
        ``None`` if ``coll_name`` is not a GridFS collection name.
    """
    if coll_name.endswith(".files"):
        return coll_name.rsplit(".files", 1)[0]
    if coll_name.endswith(".chunks"):
        return coll_name.rsplit(".chunks", 1)[0]
    return None
