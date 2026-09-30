from __future__ import annotations

import json
import logging
import warnings
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal, overload

from neosqlite.collection.json_helpers import neosqlite_json_loads

from .._sqlite import sqlite3
from ..bulk_operations import BulkOperationExecutor
from ..changestream import ChangeStream
from ..index_model import IndexModel
from ..objectid import ObjectId
from ..results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from ..sql_utils import quote_table_name
from .aggregation_cursor import AggregationCursor
from .cursor import Cursor
from .index_manager import IndexManager
from .query_engine import QueryEngine
from .raw_batch_cursor import RawBatchCursor
from .schema_utils import (
    create_unique_index_on_id,
    get_table_info,
)
from .type_utils import validate_session

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from ..connection import Connection

logger = logging.getLogger(__name__)


class Collection:
    """
    Provides a class representing a collection in a SQLite database.

    This class encapsulates operations on a collection such as inserting,
    updating, deleting, and querying documents.
    """

    # Optional per-instance options set by with_options() (#typed rollout)
    _codec_options: Any | None
    _read_preference: Any | None
    _write_concern: Any | None
    _read_concern: Any | None

    def __init__(
        self,
        db: sqlite3.Connection,
        name: str,
        create: bool = True,
        database=None,
        **kwargs: Any,
    ):
        """
        Initialize a new collection object.

        Args:
            db: Database object to which the collection belongs.
            name: Name of the collection.
            create: Whether to create the collection table if it doesn't exist.
            database: Database object that contains this collection.
            **kwargs: Additional options for collection creation.
        """
        self.db = db
        self.name = name
        self._database = database
        self.indexes = IndexManager(self)
        self.query_engine = QueryEngine(self)
        self._options = kwargs
        self._ttl_specs_cache: list[dict[str, Any]] | None = None

        if create:
            self.create(**kwargs)

    def cleanup(self) -> None:
        """Clean up resources used by the collection."""
        if hasattr(self, "query_engine"):
            self.query_engine.cleanup()

    # --- Collection helper methods ---
    def _load(
        self, id: int, data: str | bytes, stored_id: Any = None
    ) -> dict[str, Any]:
        """
        Deserialize and load a document from its ID and JSON data.

        Deserialize the JSON string or bytes back into a Python dictionary,
        add the document ID to it, and return the document.

        Args:
            id (int): The document ID.
            data (str | bytes): The JSON string or bytes representing the document.
            stored_id (Any, optional): The stored _id value if already retrieved.

        Returns:
            dict[str, Any]: The deserialized document with the _id field added.
        """
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        document: dict[str, Any] = neosqlite_json_loads(data)

        if "_id" in document:
            return document

        # If stored_id is provided, parse it. Otherwise look it up or use the auto-increment id
        final_id = (
            self._parse_stored_id(stored_id) if stored_id is not None else None
        )
        if final_id is None:
            final_id = self._get_stored_id(id)

        document["_id"] = final_id if final_id is not None else id
        return document

    def _parse_stored_id(self, stored_id: Any) -> Any:
        """
        Parse a value retrieved from the _id column into its appropriate Python type.

        Args:
            stored_id: The raw value from the _id column.

        Returns:
            Any: The parsed value (e.g., ObjectId, int, str, or None).
        """
        match stored_id:
            case None:
                return None
            case str() as s if (s.startswith("{") and s.endswith("}")) or (
                s.startswith("[") and s.endswith("]")
            ):
                try:
                    return neosqlite_json_loads(s)
                except Exception as e:
                    logger.debug(
                        f"Failed to parse JSON string in _get_id_value: {e}"
                    )
                    return s
            case _:
                return stored_id

    def _load_with_stored_id(
        self, id_val: int, data: str | bytes, stored_id_val
    ) -> dict[str, Any]:
        """
        Deserialize and load a document with the stored _id value.

        Args:
            id_val (int): The auto-increment document ID.
            data (str | bytes): The JSON string or bytes representing the document.
            stored_id_val: The stored _id value from the _id column.

        Returns:
            dict[str, Any]: The deserialized document with the _id field added.
        """
        try:
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            document: dict[str, Any] = neosqlite_json_loads(data)
            if not isinstance(document, dict):
                logger.warning(
                    f"Skipping corrupted document (id={id_val}): parsed data is not a dict"
                )
                _id = self._resolve_stored_id(stored_id_val, id_val)
                return {"_id": _id, "__neosqlite_corrupted__": True}
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Skipping corrupted document (id={id_val}): {e}")
            # Return a minimal document with just the _id to allow processing to continue
            # The document won't match most filters anyway
            _id = self._resolve_stored_id(stored_id_val, id_val)
            return {"_id": _id, "__neosqlite_corrupted__": True}

        if "_id" in document:
            return document

        # Fallback for documents stored without _id in data
        _id = self._resolve_stored_id(stored_id_val, id_val)

        document["_id"] = _id
        return document

    def _resolve_stored_id(
        self, stored_id_val: Any, fallback_id: int
    ) -> ObjectId | Any:
        """
        Resolve the stored _id value, attempting to parse as ObjectId.

        Args:
            stored_id_val: The stored _id value from the _id column.
            fallback_id: Fallback auto-increment ID if stored_id_val is None.

        Returns:
            ObjectId or the original stored_id_val, or fallback_id.
        """
        if stored_id_val is not None:
            return self._parse_stored_id(stored_id_val)
        else:
            # Fallback to the auto-increment ID for backward compatibility
            return fallback_id

    def _get_stored_id(self, doc_id: int) -> ObjectId | int | str | None:
        """
        Retrieve the stored _id for a document from the _id column.

        Args:
            doc_id (int): The document ID.

        Returns:
            ObjectId | int | None: The stored _id value, or None if the column doesn't exist yet.
        """
        try:
            # Check if the _id column exists
            cursor = self.db.execute(
                "SELECT name FROM pragma_table_info(?) WHERE name = '_id'",
                (self.name,),
            )
            column_exists = cursor.fetchone() is not None

            if column_exists:
                cursor = self.db.execute(
                    f"SELECT _id FROM {quote_table_name(self.name)} WHERE id = ?",
                    (doc_id,),
                )
                if (row := cursor.fetchone()) and row[0] is not None:
                    return self._parse_stored_id(row[0])
                else:
                    # If no row is found or row[0] is None, return None
                    return None
            else:
                # For backward compatibility, if _id column doesn't exist, return the original ID
                return doc_id
        except Exception as e:
            # If there's any error retrieving the _id, return None
            logger.debug(
                f"Error in _get_stored_id for collection '{self.name}': {e}"
            )
            return None

    @property
    def _id_column(self) -> str:
        """Return the name of the column that stores the logical _id.

        Modern collections keep _id in a dedicated ``_id`` column. Legacy
        collections (before _id column was introduced) store it in the
        autoincrement ``id`` column, which is retained only as a deprecated
        fallback.
        """
        cursor = self.db.execute(
            "SELECT name FROM pragma_table_info(?) WHERE name = '_id'",
            (self.name,),
        )
        if cursor.fetchone() is not None:
            return "_id"
        return "id"

    def _get_val(self, item: dict[str, Any], key: Any) -> Any:
        """
        Retrieves a value from a dictionary using a key, handling nested keys and
        optional prefixes.

        Args:
            item (dict[str, Any]): The dictionary to search.
            key (Any): The key to retrieve. If a string, may include nested keys
                       separated by dots or be prefixed with '$'. If non-string,
                       returns the key itself (for literal values like $group _id).

        Returns:
            Any: The value associated with the key, or None if the key is not found.
        """
        if not isinstance(key, str):
            return key
        if key.startswith("$"):
            key = key[1:]
        val: Any = item
        for k in key.split("."):
            if not isinstance(val, dict):
                return None
            val = val.get(k)
        return val

    def _set_val(self, item: dict[str, Any], key: str, value: Any) -> None:
        """
        Sets a value in a dictionary using a key, handling nested keys and
        optional prefixes.

        Args:
            item (dict[str, Any]): The dictionary to modify.
            key (str): The key to set, may include nested keys separated by dots
                       or may be prefixed with \'$.
            value (Any): The value to set.
        """
        if key.startswith("$"):
            key = key[1:]

        keys = key.split(".")
        current = item

        # Navigate to the parent of the target key
        for k in keys[:-1]:
            if k not in current or not isinstance(current[k], dict):
                current[k] = {}
            current = current[k]

        # Set the value at the target key
        current[keys[-1]] = value

    # --- Collection methods ---
    def create(self, **kwargs: Any):
        """
        Initialize the collection table if it does not exist.

        This method creates a table with an 'id' column, a '_id' column for
        ObjectId storage, and a 'data' column for storing JSON data.
        If the JSONB data type is supported, it will be used,
        otherwise, TEXT data type will be used.
        """
        validator = kwargs.get("validator")
        check_clause = ""

        if validator and "$jsonSchema" in validator:
            from .query_helper.schema_compiler import compile_schema_to_sql

            schema_sql = compile_schema_to_sql(
                validator["$jsonSchema"],
                jsonb=self.query_engine.jsonb.jsonb_supported,
            )
            if schema_sql and schema_sql != "1":
                check_clause = f", CHECK ({schema_sql})"

        # Use the QueryEngine's cached JSONB support flag
        if self.query_engine.jsonb.jsonb_supported:
            self.db.execute(f"""
                CREATE TABLE IF NOT EXISTS {quote_table_name(self.name)} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    _id JSONB,
                    data JSONB NOT NULL
                    {check_clause}
                )""")
        else:
            self.db.execute(f"""
                CREATE TABLE IF NOT EXISTS {quote_table_name(self.name)} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    _id TEXT,
                    data TEXT NOT NULL
                    {check_clause}
                )
                """)

        # Create unique index on _id column for faster lookups
        create_unique_index_on_id(self.db, self.name)

        # Add the _id column if it doesn't exist (for backward compatibility)
        self._ensure_id_column_exists()

    def _ensure_id_column_exists(self):
        """
        Ensure that the _id column exists in the collection table for backward compatibility.
        """
        try:
            # Check if _id column exists using PRAGMA table_info
            cursor = self.db.execute(
                "SELECT name FROM pragma_table_info(?) WHERE name = '_id'",
                (self.name,),
            )
            column_exists = cursor.fetchone() is not None

            if not column_exists:
                # Add the _id column using the same type as the data column
                if self.query_engine.jsonb.jsonb_supported:
                    self.db.execute(
                        f"ALTER TABLE {quote_table_name(self.name)} ADD COLUMN _id JSONB"
                    )
                else:
                    self.db.execute(
                        f"ALTER TABLE {quote_table_name(self.name)} ADD COLUMN _id TEXT"
                    )
                # Create unique index on _id column for faster lookups
                create_unique_index_on_id(self.db, self.name)
        except Exception as e:
            # If we can't add the column, continue without it (for backward compatibility)
            logger.debug(
                f"Failed to add _id column to collection '{self.name}': {e}"
            )
            pass

    def __getattr__(self, name: str) -> Any:
        """
        Support GridFS-style nested access like collection.files, collection.chunks.

        For GridFS compatibility, allow access to sub-collections like 'files' and 'chunks'
        under a bucket name (e.g., db.fs.files).
        """
        if name in ("files", "chunks"):
            full_name = f"{quote_table_name(self.name)}_{name}"
            return Collection(self.db, full_name, database=self._database)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def rename(self, new_name: str) -> None:
        """
        Renames the collection to the specified new name.
        If the new name is the same as the current name, does nothing.

        Checks if a table with the new name exists and raises an error if it does.
        Renames the underlying table and updates the collection's name.

        Args:
            new_name (str): The new name for the collection.

        Raises:
            sqlite3.Error: If a collection with the new name already exists.
        """
        # If the new name is the same as the current name, do nothing
        if new_name == self.name:
            return

        # Check if a collection with the new name already exists
        if self._object_exists(type_="table", name=new_name):
            raise sqlite3.Error(f"Collection '{new_name}' already exists")

        # Rename the table
        old_name = self.name
        self.db.execute(
            f"ALTER TABLE {quote_table_name(old_name)} RENAME TO {quote_table_name(new_name)}"
        )

        # Carry FTS5 virtual tables along; otherwise text indexes are lost
        # and stale shadow tables accumulate under the old name (#134)
        all_tables = {
            r[0]
            for r in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        prefix = old_name + "_"
        fts_rows = [
            (t,)
            for t in all_tables
            if t.startswith(prefix) and t.endswith("_fts")
        ]
        for (fts_name,) in fts_rows:
            if not fts_name.startswith(f"{old_name}_"):
                continue
            new_fts = f"{new_name}_{fts_name[len(old_name) + 1 :]}"
            self.db.execute(
                f"ALTER TABLE {quote_table_name(fts_name)} "
                f"RENAME TO {quote_table_name(new_fts)}"
            )

        # Rename regular indexes so list_indexes() continues to find them
        rows = self.db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
            (new_name,),
        ).fetchall()
        old_prefix = f"idx_{old_name}_"
        for idx_name, idx_sql in rows:
            if idx_name.startswith(old_prefix):
                suffix = idx_name[len(old_prefix) :]
                new_idx_name = f"idx_{new_name}_{suffix}"
                self.db.execute(
                    f"DROP INDEX IF EXISTS {quote_table_name(idx_name)}"
                )
                new_sql = idx_sql.replace(idx_name, new_idx_name, 1)
                self.db.execute(new_sql)
                try:
                    self.db.execute(
                        "UPDATE neosqlite_index_keys SET index_name = ? WHERE index_name = ?",
                        (new_idx_name, idx_name),
                    )
                except Exception:
                    pass

        # Rename triggers on the table (such as FTS sync triggers)
        trig_rows = self.db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL",
            (new_name,),
        ).fetchall()
        old_trig_prefix = f"{old_name}_"
        for trig_name, trig_sql in trig_rows:
            if trig_name.startswith(old_trig_prefix):
                suffix = trig_name[len(old_trig_prefix) :]
                new_trig_name = f"{new_name}_{suffix}"
                self.db.execute(
                    f"DROP TRIGGER IF EXISTS {quote_table_name(trig_name)}"
                )
                new_trig_sql = trig_sql.replace(trig_name, new_trig_name, 1)
                self.db.execute(new_trig_sql)

        # Update the collection name
        self.name = new_name
        self._invalidate_ttl_cache()

    def options(self) -> dict[str, Any]:
        """
        Retrieves options set on this collection.

        Returns:
            dict: A dictionary containing various options for the collection,
                including the table's name, columns, indexes, and count of documents.
        """
        # For SQLite, we can provide information about the table structure
        options: dict[str, Any] = {
            "name": self.name,
        }

        # Get table information
        try:
            table_info = get_table_info(self.db, self.name)
            options["columns"] = table_info["columns"]
            options["indexes"] = table_info["indexes"]
            # Get row count
            if count_row := self.db.execute(
                f"SELECT COUNT(*) FROM {quote_table_name(self.name)}"
            ).fetchone():
                options["count"] = (
                    int(count_row[0]) if count_row[0] is not None else 0
                )
            else:
                options["count"] = 0

            # Add PyMongo compatibility options
            options["codec_options"] = self.codec_options
            options["read_preference"] = self.read_preference
            options["write_concern"] = self.write_concern
            options["read_concern"] = self.read_concern

            return options
        except sqlite3.Error as e:
            # If we can't get detailed information, return basic info
            logger.debug(
                f"Failed to get collection details for '{self.name}': {e}"
            )
            options["columns"] = []
            options["indexes"] = []
            options["count"] = 0

        return options

    # --- Querying methods delegated to QueryEngine ---
    def insert_one(
        self, document: dict[str, Any], session: ClientSession | None = None
    ) -> InsertOneResult:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.insert_one`.
        """
        return self.query_engine.insert_one(document, session=session)

    def insert_many(
        self,
        documents: list[dict[str, Any]],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> InsertManyResult:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.insert_many`.
        """
        return self.query_engine.insert_many(
            documents, ordered=ordered, session=session
        )

    def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.update_one`.
        """
        return self.query_engine.update_one(
            filter,
            update,
            upsert=upsert,
            array_filters=array_filters,
            session=session,
        )

    def update_many(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.update_many`.
        """
        return self.query_engine.update_many(
            filter,
            update,
            upsert=upsert,
            array_filters=array_filters,
            session=session,
        )

    def replace_one(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        upsert: bool = False,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.replace_one`.
        """
        return self.query_engine.replace_one(
            filter, replacement, upsert=upsert, session=session
        )

    def delete_one(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """
        Delete a single document.

        For GridFS system collections (e.g., fs_files, fs_chunks), this method
        automatically delegates to GridFSBucket.delete() to handle the different schema
        and properly clean up both files and chunks.

        Args:
            filter: Query filter to match document to delete
            session: A ClientSession for transactions.

        Returns:
            DeleteResult: Result of the delete operation
        """
        # Check if this is a GridFS system collection
        if self._is_gridfs_collection():
            return self._delete_one_as_gridfs(filter)

        return self.query_engine.delete_one(filter, session=session)

    def _get_gridfs_bucket(self):
        """Return a GridFSBucket for this collection's bucket.

        Extracts the bucket name by stripping the ``_files`` or ``_chunks``
        suffix from the collection name.  Raises :exc:`RuntimeError` when the
        collection name does not follow the GridFS naming convention.

        This helper eliminates the duplicate bucket-name extraction logic that
        was previously copy-pasted in ``_delete_one_as_gridfs``,
        ``_delete_many_as_gridfs`` and ``_find_as_gridfs``.
        """
        from ..gridfs import GridFSBucket

        if self.name.endswith("_files"):
            bucket_name = self.name.removesuffix("_files")
        elif self.name.endswith("_chunks"):
            bucket_name = self.name.removesuffix("_chunks")
        else:
            raise RuntimeError(
                f"Invalid GridFS collection name: {quote_table_name(self.name)}"
            )
        return GridFSBucket(self.db, bucket_name=bucket_name)

    def _delete_one_as_gridfs(self, filter: dict[str, Any]):
        """Delete a single document from a GridFS system collection."""
        from ..results import DeleteResult

        bucket = self._get_gridfs_bucket()
        cursor = bucket.find(filter)
        files = list(cursor)

        if not files:
            # No files found, nothing deleted
            return DeleteResult(0)

        # Delete the first matching file (and its chunks)
        file_to_delete = files[0]
        bucket.delete(file_to_delete._id)

        # Return DeleteResult with deleted count
        return DeleteResult(1)

    def delete_many(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """
        Delete multiple documents.

        For GridFS system collections (e.g., fs_files, fs_chunks), this method
        automatically delegates to GridFSBucket.delete() to handle the different schema
        and properly clean up both files and chunks.

        Args:
            filter: Query filter to match documents to delete
            session: A ClientSession for transactions.

        Returns:
            DeleteResult: Result of the delete operation
        """
        # Check if this is a GridFS system collection
        if self._is_gridfs_collection():
            return self._delete_many_as_gridfs(filter)

        return self.query_engine.delete_many(filter, session=session)

    def _delete_many_as_gridfs(self, filter: dict[str, Any]):
        """Delete multiple documents from a GridFS system collection."""
        from ..results import DeleteResult

        bucket = self._get_gridfs_bucket()
        cursor = bucket.find(filter)
        files = list(cursor)

        if not files:
            # No files found, nothing deleted
            return DeleteResult(0)

        # Delete all matching files (and their chunks)
        deleted_count = 0
        for file in files:
            bucket.delete(file._id)
            deleted_count += 1

        # Return DeleteResult with deleted count
        return DeleteResult(deleted_count)

    def find(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
        limit: int | None = None,
        skip: int | None = None,
        sort: (
            list[tuple[str, int]]
            | dict[str, int]
            | str
            | tuple[str, int]
            | None
        ) = None,
        **kwargs: Any,
    ) -> Cursor:
        """
        Find documents in the collection.

        For GridFS system collections (e.g., fs_files, fs_chunks), this method
        automatically delegates to GridFSBucket.find() to handle the different schema.

        Args:
            filter: Query filter
            projection: Field projection (not supported for GridFS collections)
            hint: Index hint (not supported for GridFS collections)
            session: A ClientSession for transactions.
            limit: Maximum docs to return (0/None = no limit, PyMongo parity).
            skip: Number of docs to skip (0/None = none, PyMongo parity).
            sort: Sort spec as list of (key, direction) tuples, dict,
                ``(key, direction)`` tuple, or single field name.
            **kwargs: Back-compat aliases (``limit``, ``skip``, ``sort`` may
                also arrive here from older call sites).

        Returns:
            Cursor or GridOutCursor: Query results
        """
        if "limit" in kwargs and limit is None:
            limit = kwargs.pop("limit")
        if "skip" in kwargs and skip is None:
            skip = kwargs.pop("skip")
        if "sort" in kwargs and sort is None:
            sort = kwargs.pop("sort")
        if kwargs:
            logger.debug("Ignoring extra find options: %r", sorted(kwargs))

        # Check if this is a GridFS system collection
        if self._is_gridfs_collection():
            return self._find_as_gridfs(filter, session=session)

        self._auto_purge_ttl()
        cursor = self.query_engine.find(
            filter, projection, hint, session=session
        )
        if skip:
            cursor = cursor.skip(int(skip))
        if limit:
            cursor = cursor.limit(int(limit))
        if sort is not None:
            cursor = cursor.sort(self._normalize_sort_spec(sort))
        return cursor

    @staticmethod
    def _normalize_sort_spec(
        sort: list[tuple[str, int]] | dict[str, int] | str | tuple[str, int],
    ) -> list[tuple[str, int]]:
        if isinstance(sort, str):
            return [(sort, 1)]
        if isinstance(sort, dict):
            return [(key, int(direction)) for key, direction in sort.items()]
        if isinstance(sort, tuple):
            if len(sort) == 2 and isinstance(sort[0], str):
                return [(sort[0], int(sort[1]))]
            return [(key, int(direction)) for key, direction in sort]
        return [(key, int(direction)) for key, direction in list(sort)]

    def _is_gridfs_collection(self) -> bool:
        """
        Check if this collection is a GridFS system collection.

        Uses a two-step verification:
        1. Check naming convention (ends with _files or _chunks)
        2. Verify schema has GridFS-specific columns

        Returns:
            bool: True if this is a GridFS system collection
        """
        # Step 1: Check naming convention
        if not (self.name.endswith("_files") or self.name.endswith("_chunks")):
            return False

        # Step 2: Verify schema has GridFS-specific columns
        # GridFS files table has: filename, length, chunkSize, uploadDate, md5, metadata
        # GridFS chunks table has: files_id, n, data
        try:
            cursor = self.db.execute(
                f"PRAGMA table_info({quote_table_name(self.name)})"
            )
            columns = {row[1] for row in cursor}  # Column names are in index 1

            if self.name.endswith("_files"):
                # Check for GridFS files table columns
                gridfs_columns = {
                    "filename",
                    "length",
                    "chunkSize",
                    "uploadDate",
                    "metadata",
                }
                return gridfs_columns.issubset(columns)
            elif self.name.endswith("_chunks"):
                # Check for GridFS chunks table columns
                gridfs_columns = {"files_id", "n", "data"}
                return gridfs_columns.issubset(columns)
        except Exception as e:
            # If we can't check schema, fall back to naming convention
            logger.debug(
                f"Failed to check schema for GridFS identification in '{self.name}': {e}"
            )
            pass

        # Default to naming convention if schema check fails
        return self.name.endswith("_files") or self.name.endswith("_chunks")

    def _find_as_gridfs(
        self,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ):
        """Execute find on a GridFS system collection via GridFSBucket."""
        if self.name.endswith("_chunks"):
            return self._find_chunks_direct(filter)
        bucket = self._get_gridfs_bucket()
        return bucket.find(filter, session=session)

    def _find_chunks_direct(
        self, filter: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Find chunk documents directly from the chunks table.

        ``GridFSBucket.find`` queries the *files* table and returns
        ``GridOut`` objects, so delegating ``fs.chunks.find`` to it could
        only ever pass vacuously (unknown ``files_id`` filters previously
        returned the whole bucket). Query the chunks table instead and
        return plain chunk dicts.
        """
        table = quote_table_name(self.name)
        where_parts: list[str] = []
        params: list[Any] = []
        for key, value in (filter or {}).items():
            if key in ("files_id", "n", "_id"):
                column = key
                if isinstance(value, dict):
                    for op, op_val in value.items():
                        match op:
                            case "$eq":
                                where_parts.append(f"{column} = ?")
                                params.append(op_val)
                            case "$ne":
                                where_parts.append(f"{column} != ?")
                                params.append(op_val)
                            case "$gt":
                                where_parts.append(f"{column} > ?")
                                params.append(op_val)
                            case "$gte":
                                where_parts.append(f"{column} >= ?")
                                params.append(op_val)
                            case "$lt":
                                where_parts.append(f"{column} < ?")
                                params.append(op_val)
                            case "$lte":
                                where_parts.append(f"{column} <= ?")
                                params.append(op_val)
                            case "$in" if isinstance(op_val, (list, tuple)):
                                if not op_val:
                                    where_parts.append("1=0")
                                else:
                                    placeholders = ", ".join("?" * len(op_val))
                                    where_parts.append(
                                        f"{column} IN ({placeholders})"
                                    )
                                    params.extend(op_val)
                            case _:
                                where_parts.append("1=0")
                else:
                    where_parts.append(f"{column} = ?")
                    params.append(value)
            else:
                # Unknown chunk field — match nothing, never everything.
                where_parts.append("1=0")
        where_clause = (
            "WHERE " + " AND ".join(where_parts) if where_parts else ""
        )
        query = f"SELECT _id, files_id, n, data FROM {table} {where_clause}"
        cursor = self.db.execute(query, params)
        return [
            {"_id": row[0], "files_id": row[1], "n": row[2], "data": row[3]}
            for row in cursor.fetchall()
        ]

    def find_raw_batches(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.find_raw_batches`.
        """
        return self.query_engine.find_raw_batches(
            filter, projection, hint, batch_size, session=session
        )

    def find_one(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> dict[str, Any] | None:
        """
        Find a single document.

        For GridFS system collections (e.g., fs_files, fs_chunks), this method
        automatically delegates to GridFSBucket.find() to handle the different schema.

        Args:
            filter: Query filter
            projection: Field projection (not supported for GridFS collections)
            hint: Index hint (not supported for GridFS collections)
            session: A ClientSession for transactions.

        Returns:
            Dict or GridOut or None: Query result
        """
        # Check if this is a GridFS system collection
        if self._is_gridfs_collection():
            cursor = self._find_as_gridfs(filter, session=session)
            # Return first result or None
            for doc in cursor:
                return doc
            return None

        self._auto_purge_ttl()
        return self.query_engine.find_one(
            filter, projection, hint, session=session
        )

    def count_documents(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> int:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.count_documents`.
        """
        return self.query_engine.count_documents(filter, session=session)

    def estimated_document_count(
        self,
        options: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> int:
        """
        Get an estimated count of documents in the collection.

        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.estimated_document_count`.

        Args:
            options (dict[str, Any], optional): Options for the count operation.
                Supported options (for PyMongo API compatibility):
                - maxTimeMS: Maximum execution time in milliseconds (ignored in NeoSQLite)
                - hint: Index to use for the count (ignored in NeoSQLite)
            session: A ClientSession for transactions.

        Returns:
            int: Estimated number of documents in the collection

        Note:
            This method returns an estimate based on SQLite metadata, which is fast
            but may not be exact. For an exact count, use count_documents({}).
            The options parameter is accepted for PyMongo API compatibility but
            most options are not applicable to SQLite.
        """
        # Options are accepted for API compatibility but not used
        # maxTimeMS, hint, etc. are MongoDB-specific
        return self.query_engine.estimated_document_count(session=session)

    def find_one_and_delete(
        self,
        filter: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.find_one_and_delete`.
        """
        return self.query_engine.find_one_and_delete(
            filter, projection=projection, sort=sort, session=session, **kwargs
        )

    def find_one_and_replace(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.find_one_and_replace`.
        """
        return self.query_engine.find_one_and_replace(
            filter,
            replacement,
            projection=projection,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            session=session,
            **kwargs,
        )

    def find_one_and_update(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.find_one_and_update`.
        """
        return self.query_engine.find_one_and_update(
            filter,
            update,
            projection=projection,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            array_filters=array_filters,
            session=session,
            **kwargs,
        )

    def aggregate(
        self,
        pipeline: list[dict[str, Any]],
        allowDiskUse: bool | None = None,
        batchSize: int | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> AggregationCursor:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.aggregate`.

        Args:
            pipeline: The aggregation pipeline to execute
            allowDiskUse: Ignored in NeoSQLite (kept for PyMongo compatibility)
            batchSize: Batch size for results (kept for PyMongo compatibility)
            session: A ClientSession for transactions.
            **kwargs: Additional keyword arguments for PyMongo compatibility

        Returns:
            An AggregationCursor instance
        """
        return AggregationCursor(
            self,
            pipeline,
            allowDiskUse=allowDiskUse,
            batchSize=batchSize,
            session=session,
            **kwargs,
        )

    def aggregate_raw_batches(
        self,
        pipeline: list[dict[str, Any]],
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.aggregate_raw_batches`.
        """
        return self.query_engine.aggregate_raw_batches(
            pipeline, batch_size, session=session
        )

    def distinct(
        self,
        key: str,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> list[Any]:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.distinct`.
        """
        return self.query_engine.distinct(key, filter, session=session)

    # --- Bulk Write methods delegated to QueryEngine ---
    def bulk_write(
        self,
        requests: list[Any],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> BulkWriteResult:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.bulk_write`.
        """
        return self.query_engine.bulk_write(requests, ordered, session=session)

    def initialize_ordered_bulk_op(self) -> BulkOperationExecutor:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.initialize_ordered_bulk_op`.

        .. deprecated::
            Use :meth:`bulk_write` instead.
        """
        warnings.warn(
            "initialize_ordered_bulk_op is deprecated, use bulk_write instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.query_engine.initialize_ordered_bulk_op()

    def initialize_unordered_bulk_op(self) -> BulkOperationExecutor:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.query_engine.QueryEngine.initialize_unordered_bulk_op`.

        .. deprecated::
            Use :meth:`bulk_write` instead.
        """
        warnings.warn(
            "initialize_unordered_bulk_op is deprecated, use bulk_write instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.query_engine.initialize_unordered_bulk_op()

    # --- Indexing methods delegated to IndexManager ---
    def create_index(
        self,
        key: str | list[str] | list[tuple[str, int]],
        reindex: bool = True,
        sparse: bool = False,
        unique: bool = False,
        fts: bool = False,
        tokenizer: str | None = None,
        datetime_field: bool = False,
        expireAfterSeconds: int | None = None,
        name: str | None = None,
        **kwargs: Any,
    ) -> str:
        """
        Create an index, PyMongo-compatible.

        Accepts ``expireAfterSeconds`` for TTL declaration (stored in
        ``_neosqlite_ttl_indexes``; expiry behavior is owned by the TTL
        engine) plus ``name`` and other extra options for compatibility.

        Returns the generated SQLite index name.
        """
        try:
            return self.indexes.create_index(
                key,
                reindex,
                sparse,
                unique,
                fts,
                tokenizer,
                datetime_field,
                expireAfterSeconds=expireAfterSeconds,
                name=name,
                **kwargs,
            )
        finally:
            self._invalidate_ttl_cache()

    def create_search_index(
        self,
        key: str,
        tokenizer: str | None = None,
    ):
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.create_search_index`.
        """
        return self.indexes.create_search_index(key, tokenizer)

    def create_indexes(
        self,
        indexes: list[IndexModel],
    ) -> list[str]:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.create_indexes`.
        """
        return self.indexes.create_indexes(indexes)

    def create_search_indexes(
        self,
        indexes: list[str],
    ) -> list[str]:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.create_search_indexes`.
        """
        return self.indexes.create_search_indexes(indexes)

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: list[dict[str, Any]] | None = None,
    ):
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.reindex`.
        """
        self.indexes.reindex(table, sparse, documents)

    @overload
    def list_indexes(self, as_keys: Literal[True]) -> list[list[str]]: ...
    @overload
    def list_indexes(self, as_keys: Literal[False] = False) -> list[str]: ...
    def list_indexes(
        self,
        as_keys: bool = False,
    ) -> list[str] | list[list[str]]:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.list_indexes`.
        """
        # This explicit check is the key to solving the Mypy error on overloading.
        if as_keys:
            # Inside this block, Mypy knows 'as_keys' is Literal[True].
            return self.indexes.list_indexes(as_keys)
        else:
            # Inside this block, Mypy knows 'as_keys' is Literal[False].
            return self.indexes.list_indexes(as_keys)

    def list_search_indexes(self) -> list[str]:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.list_search_indexes`.
        """
        return self.indexes.list_search_indexes()

    def update_search_index(self, key: str, tokenizer: str | None = None):
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.update_search_index`.
        """
        self.indexes.update_search_index(key, tokenizer)

    def drop_index(self, index: str):
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.drop_index`.
        """
        try:
            self.indexes.drop_index(index)
        finally:
            self._invalidate_ttl_cache()

    def drop_search_index(self, index: str):
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.drop_search_index`.
        """
        self.indexes.drop_search_index(index)

    def drop_indexes(self):
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.drop_indexes`.
        """
        try:
            self.indexes.drop_indexes()
        finally:
            self._invalidate_ttl_cache()

    def index_information(self) -> dict[str, Any]:
        """
        This is a delegating method. For implementation details, see the
        core logic in :meth:`~neosqlite.collection.index_manager.IndexManager.index_information`.
        """
        return self.indexes.index_information()

    def get_ttl_specs(self) -> list[dict[str, Any]]:
        """
        List TTL index specs declared on this collection.

        Returns:
            list[dict]: Each dict has ``index_name``, ``collection_name``,
                ``field`` and ``expireAfterSeconds`` keys.
        """
        return self.indexes.get_ttl_specs()

    def _ttl_specs_cached(self) -> list[dict[str, Any]]:
        cached = self.__dict__.get("_ttl_specs_cache")
        if cached is None:
            cached = self.get_ttl_specs()
            self._ttl_specs_cache = cached
        return cached

    def _invalidate_ttl_cache(self) -> None:
        self._ttl_specs_cache = None

    def purge_expired(self, now: datetime | None = None) -> int:
        """
        Delete documents expired under this collection's TTL indexes.

        A document expires when ``now >= field_value + expireAfterSeconds``.
        Both UTC datetimes (ISO strings) and numeric epoch seconds are
        supported for the indexed field. Deletions are plain deletes and
        fire normal ``watch()`` events.

        Args:
            now: Reference time (timezone-aware preferred; naive is assumed
                UTC). Defaults to current UTC time. Accepts an explicit value
                so tests need not sleep.

        Returns:
            int: Number of documents deleted.
        """
        specs = self._ttl_specs_cached()
        if not specs:
            return 0
        if now is None:
            ref = datetime.now(timezone.utc)
        elif now.tzinfo is None:
            ref = now.replace(tzinfo=timezone.utc)
        else:
            ref = now
        total = 0
        for spec in specs:
            field = spec["field"]
            expire = int(spec["expireAfterSeconds"])
            cutoff = ref - timedelta(seconds=expire)
            try:
                result = self.delete_many({field: {"$lt": cutoff}})
                total += result.deleted_count
            except Exception as exc:
                logger.debug("TTL datetime purge skipped: %s", exc)
            try:
                result = self.delete_many({field: {"$lt": cutoff.timestamp()}})
                total += result.deleted_count
            except Exception as exc:
                logger.debug("TTL epoch purge skipped: %s", exc)
        return total

    def _auto_purge_ttl(self) -> None:
        try:
            if not self._ttl_specs_cached():
                return
        except Exception:
            return
        try:
            self.purge_expired()
        except Exception as exc:
            logger.debug("TTL auto-purge skipped: %s", exc)

    # --- Other methods ---
    @property
    def client(self) -> Connection:
        """
        Get the MongoClient instance (returns the parent Connection).

        Returns:
            Connection: The parent connection instance.
        """
        return self.database

    @property
    def codec_options(self) -> Any:
        """
        Get the codec options for this collection.

        Returns:
            Any: The codec options.
        """
        if hasattr(self, "_codec_options") and self._codec_options is not None:
            return self._codec_options
        return self.database.codec_options if self.database else None

    @property
    def read_preference(self) -> Any:
        """
        Get the read preference for this collection.

        Returns:
            Any: The read preference.
        """
        if (
            hasattr(self, "_read_preference")
            and self._read_preference is not None
        ):
            return self._read_preference
        return self.database.read_preference if self.database else None

    @property
    def write_concern(self) -> Any:
        """
        Get the write concern for this collection.

        Returns:
            Any: The write concern.
        """
        if hasattr(self, "_write_concern") and self._write_concern is not None:
            return self._write_concern
        return self.database.write_concern if self.database else None

    @property
    def read_concern(self) -> Any:
        """
        Get the read concern for this collection.

        Returns:
            Any: The read concern.
        """
        if hasattr(self, "_read_concern") and self._read_concern is not None:
            return self._read_concern
        return self.database.read_concern if self.database else None

    @property
    def database(self) -> Connection:
        """
        Get the database that this collection is a part of.

        Returns:
            Connection: The connection object this collection is associated with.
        """
        return self._database

    @property
    def db_path(self) -> str:
        """
        Get the path to the database file.

        Returns:
            str: The database file path.
        """
        return self.database.db_path if self.database else ":memory:"

    @property
    def full_name(self) -> str:
        """
        Get the full name of the collection (database.collection).

        Returns:
            str: The full name of the collection

        Example:
            >>> db = Connection("test.db")
            >>> coll = db.my_collection
            >>> print(coll.full_name)
            'test.my_collection'
        """
        if self._database and hasattr(self._database, "name"):
            return f"{self._database.name}.{quote_table_name(self.name)}"
        return self.name

    def with_options(
        self,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        """
        Get a clone of this collection with different options.

        Note: NeoSQLite is a single-node database, so read_preference,
        write_concern, and read_concern are stored for API compatibility
        but don't affect query behavior.

        Args:
            codec_options: Codec options (stored for compatibility, not used)
            read_preference: Read preference (stored for compatibility, not used)
            write_concern: Write concern (stored for compatibility, not used)
            read_concern: Read concern (stored for compatibility, not used)

        Returns:
            Collection: A new collection instance with the specified options

        Example:
            >>> coll = db.my_collection
            >>> coll_with_options = coll.with_options(write_concern={"w": "majority"})
        """
        # Create a new collection instance (clone)
        clone = Collection(
            self.db,
            self.name,
            create=False,
            database=self._database,
        )

        # Store options for API compatibility
        clone._codec_options = codec_options
        clone._read_preference = read_preference
        clone._write_concern = write_concern
        clone._read_concern = read_concern

        return clone

    def _object_exists(self, type_: str, name: str) -> bool:
        """
        Check if an object (table or index) of a specific type and name exists within the database.

        Args:
            type_ (str): The type of object to check, either "table" or "index".
            name (str): The name of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        match type_:
            case "table":
                if row := self.db.execute(
                    "SELECT COUNT(1) FROM sqlite_master WHERE type = ? AND name = ?",
                    (type_, name.strip("[]")),
                ).fetchone():
                    return int(row[0]) > 0
                return False
            case "index":
                # For indexes, check if it exists with our naming convention
                if row := self.db.execute(
                    "SELECT COUNT(1) FROM sqlite_master WHERE type = ? AND name = ?",
                    (type_, name),
                ).fetchone():
                    return int(row[0]) > 0
                return False
            case _:
                return False

    def drop(self):
        """
        Drop the entire collection.

        This method removes the collection (table) from the database. After calling
        this method, the collection will no longer exist in the database.
        """
        self.db.execute(f"DROP TABLE IF EXISTS {quote_table_name(self.name)}")
        self._invalidate_ttl_cache()

    def watch(
        self,
        pipeline: list[dict[str, Any]] | None = None,
        full_document: str | None = None,
        resume_after: dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
        batch_size: int | None = None,
        collation: dict[str, Any] | None = None,
        start_at_operation_time: Any | None = None,
        session: ClientSession | None = None,
        start_after: dict[str, Any] | None = None,
    ) -> ChangeStream:
        """
        Monitor changes on this collection using SQLite's change tracking features.

        This method creates a change stream that allows iterating over change events
        generated by modifications to the collection. While SQLite doesn't natively
        support change streams like MongoDB, this implementation uses triggers and
        SQLite's built-in change tracking mechanisms to provide similar functionality.

        Args:
            pipeline (list[dict[str, Any]]): Aggregation pipeline stages to apply to change events.
            full_document (str): Determines how the 'fullDocument' field is populated in change events.
            resume_after (dict[str, Any]): Logical starting point for the change stream.
            max_await_time_ms (int): Maximum time to wait for new documents in milliseconds.
            batch_size (int): Number of documents to return per batch.
            collation (dict[str, Any]): Collation settings for the operation.
            start_at_operation_time (Any): Operation time to start monitoring from.
            session (ClientSession): Client session for the operation.
            start_after (dict[str, Any]): Logical starting point for the change stream.

        Returns:
            ChangeStream: A change stream object that can be iterated over to receive change events.
        """
        validate_session(session, self._database)
        return ChangeStream(
            collection=self,
            pipeline=pipeline,
            full_document=full_document,
            resume_after=resume_after,
            max_await_time_ms=max_await_time_ms,
            batch_size=batch_size,
            collation=collation,
            start_at_operation_time=start_at_operation_time,
            session=session,
            start_after=start_after,
        )
