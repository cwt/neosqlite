"""GridFS Adapter for NX-27017 Proxy.

This adapter intercepts PyMongo's GridFS operations and translates them to
NeoSQLite's native GridFS API. It handles the schema difference between:
- MongoDB: Collections with documents
- NeoSQLite: Direct column tables (fs_files, fs_chunks)

The adapter is called from the proxy's command handlers to intercept all
operations on GridFS collections (fs.files, fs.chunks, etc.) and route them
to the appropriate NeoSQLite GridFSBucket methods.
"""

import io
import logging
from typing import Any

from neosqlite.gridfs import GridFSBucket
from neosqlite.objectid import ObjectId

logger = logging.getLogger(__name__)

# GridFS constants
DEFAULT_CHUNK_SIZE_BYTES = 261_120  # 256 KiB (MongoDB default)


class GridFSAdapter:
    """Adapter to translate PyMongo GridFS operations to NeoSQLite GridFS."""

    def __init__(self, db: Any, bucket_name: str = "fs"):
        """Initialize the GridFS adapter.

        Args:
            db: NeoSQLite database connection (sqlite3 connection, not neosqlite Connection)
            bucket_name: Name of the GridFS bucket (default: "fs")
        """
        self._db = db
        self._bucket_name = bucket_name
        self._bucket: GridFSBucket | None = None  # type: ignore[assignment]
        self._ensure_bucket()

    def _ensure_gridfs_schema(self) -> None:
        """Ensure the GridFS tables have the correct schema.

        If the tables already exist from an older schema, this adds any missing columns
        before GridFSBucket tries to create indexes on them.
        """
        files_table = f"{self._bucket_name}_files"
        chunks_table = f"{self._bucket_name}_chunks"

        logger.debug(
            f"_ensure_gridfs_schema: bucket_name={self._bucket_name}, files_table={files_table}"
        )

        cursor = self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (files_table,),
        )
        table_exists = cursor.fetchone() is not None
        logger.debug(
            f"_ensure_gridfs_schema: files_table exists={table_exists}"
        )

        if table_exists:
            logger.debug(
                f"GridFS files table '{files_table}' already exists, checking schema..."
            )
            cursor = self._db.execute(f"PRAGMA table_info('{files_table}')")
            columns = {row[1] for row in cursor.fetchall()}
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
                for col in missing_columns:
                    if col in ("content_type", "aliases"):
                        col_type = "TEXT"
                        self._db.execute(
                            f"ALTER TABLE {files_table} ADD COLUMN {col} {col_type}"
                        )
                    elif col == "metadata":
                        col_type = "TEXT"
                        self._db.execute(
                            f"ALTER TABLE {files_table} ADD COLUMN {col} {col_type}"
                        )
                    else:
                        col_type = (
                            "TEXT"
                            if col in ("filename", "uploadDate", "md5")
                            else "INTEGER"
                        )
                        self._db.execute(
                            f"ALTER TABLE {files_table} ADD COLUMN {col} {col_type}"
                        )
            else:
                logger.debug(
                    f"_ensure_gridfs_schema: All required columns exist in {files_table}"
                )

            cursor = self._db.execute(f"PRAGMA table_info('{chunks_table}')")
            chunks_columns = {row[1] for row in cursor.fetchall()}
            logger.debug(
                f"_ensure_gridfs_schema: columns in {chunks_table}: {chunks_columns}"
            )
            if "files_id" not in chunks_columns:
                logger.debug(
                    f"Adding missing columns to {chunks_table}: files_id, n, data"
                )
                self._db.execute(
                    f"ALTER TABLE {chunks_table} ADD COLUMN files_id INTEGER"
                )
                self._db.execute(
                    f"ALTER TABLE {chunks_table} ADD COLUMN n INTEGER"
                )
                self._db.execute(
                    f"ALTER TABLE {chunks_table} ADD COLUMN data BLOB"
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
        """Ensure the GridFSBucket is initialized."""
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

            # Check if the correct tables already exist with correct schema
            # If so, we can skip GridFSBucket creation to avoid migration issues
            files_table = f"{self._bucket_name}_files"
            cursor = self._db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (files_table,),
            )
            if cursor.fetchone():
                cursor = self._db.execute(f"PRAGMA table_info('{files_table}')")
                columns = {row[1] for row in cursor.fetchall()}
                required_cols = {
                    "id",
                    "_id",
                    "filename",
                    "length",
                    "chunkSize",
                    "uploadDate",
                    "md5",
                    "metadata",
                }
                if required_cols.issubset(columns):
                    logger.debug(
                        f"Table {files_table} already exists with correct schema, skipping GridFSBucket creation"
                    )
                    # Create a dummy bucket object just to satisfy the adapter
                    self._bucket = GridFSBucket.__new__(GridFSBucket)
                    self._get_bucket()._db = self._db
                    self._get_bucket()._bucket_name = self._bucket_name
                    self._get_bucket()._files_collection = files_table
                    self._get_bucket()._chunks_collection = (
                        f"{self._bucket_name}_chunks"
                    )
                    self._get_bucket()._chunk_size_bytes = (
                        DEFAULT_CHUNK_SIZE_BYTES
                    )
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
        """Get the GridFSBucket with type safety assertion."""
        assert self._bucket is not None, "GridFS bucket not initialized"
        return self._bucket

    def ensure_file_metadata_exists(self, files_id: Any, db: Any) -> None:
        """Ensure file metadata exists in fs_files for the given files_id.

        This is called when inserting chunks to auto-create file metadata if it doesn't exist.
        Since PyMongo doesn't explicitly send file metadata inserts, we create minimal metadata.

        Args:
            files_id: The ObjectId (or string) of the file
            db: The SQLite database connection
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
        from datetime import datetime, timezone

        upload_date = datetime.now(timezone.utc).isoformat()

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
        """Convert ObjectId from various formats."""
        if oid is None:
            return None
        if isinstance(oid, ObjectId):
            return oid
        if isinstance(oid, dict) and "$oid" in oid:
            return ObjectId(oid["$oid"])
        if isinstance(oid, str):
            try:
                return ObjectId(oid)
            except Exception:
                return oid
        return oid

    def _convert_gridfs_result(self, grid_out: Any) -> dict[str, Any]:
        """Convert GridOut to MongoDB-compatible dict."""
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
        """Handle insert operation on GridFS collection.

        Args:
            docs: List of documents to insert
            is_files: True if inserting into files collection, False for chunks

        Returns:
            MongoDB-style insert response
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
        """Insert a file document into GridFS files collection.

        This directly inserts into the files table using raw SQL since
        the document structure matches the table columns.

        Args:
            doc: File document with fields like _id, filename, length, etc.

        Returns:
            The inserted file ID
        """
        import json
        from datetime import datetime, timezone

        file_id = self._convert_objectid(doc.get("_id")) or ObjectId()
        filename = doc.get("filename", "")
        length = doc.get("length", 0)
        chunk_size = doc.get("chunkSize", self._get_bucket()._chunk_size_bytes)
        upload_date = doc.get("uploadDate")
        if isinstance(upload_date, datetime):
            upload_date = upload_date.isoformat()
        elif upload_date is None:
            upload_date = datetime.now(timezone.utc).isoformat()
        md5 = doc.get("md5")
        metadata = doc.get("metadata", {})

        # Serialize metadata as JSON string
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)

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
        """Insert a chunk document into GridFS chunks collection.

        Args:
            doc: Chunk document with fields like _id, files_id, n, data

        Returns:
            The inserted chunk ID
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
        """Handle find operation on GridFS files collection.

        Args:
            filter_query: Query filter dictionary

        Returns:
            List of matching file documents in MongoDB format
        """
        self._ensure_bucket()

        if filter_query is None:
            filter_query = {}

            filter_query = _convert_objectids_in_dict(filter_query)

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
        """Handle find operation on GridFS chunks collection.

        Args:
            filter_query: Query filter dictionary with files_id

        Returns:
            List of matching chunk documents in MongoDB format
        """
        self._ensure_bucket()

        if filter_query is None:
            filter_query = {}

        filter_query = _convert_objectids_in_dict(filter_query) or {}

        try:
            chunks = []
            files_id = filter_query.get("files_id")
            if files_id is not None:
                file_int_id = self._get_bucket()._get_integer_id_for_file(
                    files_id
                )
                if file_int_id is not None:
                    cursor = self._db.execute(
                        f"""SELECT _id, files_id, n, data FROM {self._get_bucket()._chunks_collection}
                           WHERE files_id = ? ORDER BY n""",
                        (file_int_id,),
                    )
                    for row in cursor.fetchall():
                        chunks.append(
                            {
                                "_id": row[0],
                                "files_id": row[1],
                                "n": row[2],
                                "data": row[3],
                            }
                        )
            return chunks
        except Exception as e:
            logger.error(f"GridFS chunks find error: {e}")
            raise

    def handle_delete(self, file_ids: list[Any]) -> dict[str, Any]:
        """Handle delete operation on GridFS files collection.

        Args:
            file_ids: List of file IDs to delete

        Returns:
            MongoDB-style delete response
        """
        self._ensure_bucket()

        deleted = 0
        try:
            for file_id in file_ids:
                file_id = self._convert_objectid(file_id)
                if file_id:
                    self._get_bucket().delete(file_id)
                    deleted += 1

            return {"ok": 1, "n": deleted}
        except Exception as e:
            logger.error(f"GridFS delete error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def handle_update(self, file_id: Any, update: dict) -> dict[str, Any]:
        """Handle update operation on GridFS files collection.

        Args:
            file_id: ID of file to update
            update: Update document with $set and/or other operators

        Returns:
            MongoDB-style update response
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
                import json

                params.append(json.dumps(set_ops["metadata"]))

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

    def handle_upload(
        self,
        filename: str,
        data: bytes,
        metadata: dict | None = None,
        chunk_size: int | None = None,
    ) -> dict[str, Any]:
        """Handle file upload to GridFS.

        Args:
            filename: Name of the file
            data: File content as bytes
            metadata: Optional metadata dictionary
            chunk_size: Optional chunk size

        Returns:
            MongoDB-style response with fileId
        """
        self._ensure_bucket()

        try:
            if isinstance(data, str):
                data = data.encode("utf-8")

            source = io.BytesIO(data)

            file_id = self._get_bucket().upload_from_stream(
                filename,
                source,
                chunk_size_bytes=chunk_size,
                metadata=metadata,
            )

            return {"ok": 1, "fileId": file_id}
        except Exception as e:
            logger.error(f"GridFS upload error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def handle_download(self, file_id: Any) -> dict[str, Any]:
        """Handle file download from GridFS.

        Args:
            file_id: ID of the file to download

        Returns:
            MongoDB-style response with file data
        """
        self._ensure_bucket()

        try:
            file_id = self._convert_objectid(file_id)

            grid_out = self._get_bucket().open_download_stream(file_id)
            data = grid_out.read()

            return {"ok": 1, "data": data}
        except Exception as e:
            logger.error(f"GridFS download error: {e}")
            return {"ok": 0, "errmsg": str(e)}

    def create_indexes(self) -> dict[str, Any]:
        """Create indexes on GridFS collections.

        Returns:
            MongoDB-style createIndexes response
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
    """Recursively convert ObjectIds in a document."""
    if doc is None:
        return None

    result: dict = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            if "$oid" in value:
                result[key] = ObjectId(value["$oid"])
            elif key == "$oid":
                result[key] = (
                    ObjectId(value) if isinstance(value, str) else value
                )
            else:
                result[key] = _convert_objectids_in_dict(value)
        elif isinstance(value, list):
            result[key] = [
                (
                    ObjectId(v["$oid"])
                    if isinstance(v, dict) and "$oid" in v
                    else v
                )
                for v in value
            ]
        else:
            result[key] = value
    return result


def create_gridfs_adapter(
    db: Any, coll_name: str
) -> tuple[GridFSAdapter | None, str | None]:
    """Create a GridFSAdapter for the given collection.

    Args:
        db: NeoSQLite database connection
        coll_name: Collection name (e.g., "fs.files", "fs.chunks")

    Returns:
        Tuple of (GridFSAdapter, bucket_name) if GridFS collection, else (None, None)
    """
    if not _is_gridfs_collection(coll_name):
        return None, None

    bucket_name = _get_gridfs_bucket_name(coll_name)
    if bucket_name is None:
        return None, None

    adapter = GridFSAdapter(db, bucket_name=bucket_name)
    return adapter, bucket_name


def _is_gridfs_collection(coll_name: str) -> bool:
    """Check if collection name is a GridFS collection."""
    return coll_name.endswith(".files") or coll_name.endswith(".chunks")


def _get_gridfs_bucket_name(coll_name: str) -> str | None:
    """Extract bucket name from GridFS collection name."""
    if coll_name.endswith(".files"):
        return coll_name.rsplit(".files", 1)[0]
    if coll_name.endswith(".chunks"):
        return coll_name.rsplit(".chunks", 1)[0]
    return None


def _convert_gridfs_collection_name(coll_name: str) -> str:
    """Convert MongoDB-style GridFS collection name to NeoSQLite-style.

    Args:
        coll_name: MongoDB-style collection name (e.g., "fs.files")

    Returns:
        NeoSQLite-style collection name (e.g., "fs_files")
    """
    if coll_name.endswith(".files"):
        bucket = coll_name.rsplit(".files", 1)[0]
        return f"{bucket}_files"
    elif coll_name.endswith(".chunks"):
        bucket = coll_name.rsplit(".chunks", 1)[0]
        return f"{bucket}_chunks"
    return coll_name
