from __future__ import annotations

import json
import logging
import warnings
from datetime import UTC, datetime, timedelta
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
from ..sql_utils import quote_table_name, validate_identifier
from .aggregation_cursor import AggregationCursor
from .cursor import Cursor
from .index_manager import IndexManager
from .query_engine import QueryEngine
from .query_helper.schema_validator import (
    delete_validator_schema,
    load_validator_schema,
    rename_validator_schema,
    save_validator_schema,
)
from .raw_batch_cursor import RawBatchCursor
from .schema_utils import (
    column_exists,
    create_unique_index_on_id,
    get_table_info,
    invalidate_table_columns_cache,
)
from .type_correction import get_integer_id_for_doc_id
from .type_utils import validate_session

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from ..connection import Connection

logger = logging.getLogger(__name__)


def cleanup_dropped_collection(db: sqlite3.Connection, name: str) -> None:
    """Drop a collection table and remove all FTS5, TTL, index-key, and schema caches."""
    # Query FTS sync triggers and index names BEFORE dropping the table, since
    # DROP TABLE removes triggers and indexes from sqlite_master automatically.
    try:
        trig_rows = db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name = ? "
            "AND name LIKE '%\\_fts\\_insert' ESCAPE '\\'",
            (name,),
        ).fetchall()
        fts_tables = [r[0].removesuffix("_insert") for r in trig_rows]
    except Exception:
        fts_tables = []

    try:
        idx_names = [
            r[0]
            for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ?",
                (name,),
            ).fetchall()
        ]
    except Exception:
        idx_names = []

    db.execute(f"DROP TABLE IF EXISTS {quote_table_name(name)}")

    try:
        db.execute(
            "DELETE FROM _neosqlite_ttl_indexes WHERE collection_name = ?",
            (name,),
        )
    except Exception:
        pass

    delete_validator_schema(db, name)

    for key_name in idx_names + fts_tables:
        try:
            db.execute(
                "DELETE FROM neosqlite_index_keys WHERE index_name = ?",
                (key_name,),
            )
        except Exception:
            pass

    for fts_table in fts_tables:
        db.execute(f"DROP TABLE IF EXISTS {quote_table_name(fts_table)}")
        for suffix in ("_data", "_idx", "_content", "_docsize", "_config"):
            db.execute(
                f"DROP TABLE IF EXISTS {quote_table_name(fts_table + suffix)}"
            )

    invalidate_table_columns_cache(db, name)


class Collection:
    """Represents a document collection backed by a SQLite table.

    Encapsulates CRUD operations, querying, aggregation pipelines, indexing,
    TTL expiration, change streams, and GridFS compatibility on top of a
    SQLite connection, delegating core query and index operations to
    ``QueryEngine`` and ``IndexManager``.
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
        """Initialize a new collection instance.

        Args:
            db: SQLite database connection backing the collection.
            name: Name of the collection table.
            create: Whether to create the collection table if it does not
                already exist. Defaults to ``True``.
            database: Parent ``Connection`` instance containing this
                collection, or ``None``.
            **kwargs: Additional collection creation options passed to
                ``create()`` (such as ``validator``).
        """
        self.db = db
        self.name = name
        self._database = database
        self.indexes = IndexManager(self)
        self.query_engine = QueryEngine(self)
        self._options = kwargs
        self._ttl_specs_cache: list[dict[str, Any]] | None = None
        validator = kwargs.get("validator")
        self._validator_schema: dict[str, Any] | None = (
            validator["$jsonSchema"]
            if isinstance(validator, dict) and "$jsonSchema" in validator
            else None
        )
        if self._validator_schema is None:
            self._validator_schema = load_validator_schema(db, name)

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
        """Deserialize and load a document from its ID and JSON data.

        Decodes JSON string or UTF-8 bytes ``data`` into a Python dict and
        populates ``_id`` from ``data``, ``stored_id``, the ``_id`` column,
        or the auto-increment ``id`` fallback.

        Args:
            id: Auto-increment integer row ID in SQLite.
            data: JSON string or UTF-8 bytes representing the document.
            stored_id: Optional raw ``_id`` column value if already fetched.

        Returns:
            dict[str, Any]: Deserialized document with ``_id`` populated.
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
        """Parse a raw value from the ``_id`` column into a Python value.

        Decodes JSON object or array strings using ``neosqlite_json_loads``
        and returns scalar values or ``None`` unchanged.

        Args:
            stored_id: Raw value retrieved from the ``_id`` column.

        Returns:
            Any: Parsed value (e.g., ``ObjectId``, ``dict``, ``int``,
            ``str``, or ``None``).
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
        """Deserialize and load a document with a pre-fetched ``_id`` value.

        Returns a minimal sentinel dictionary with
        ``{"_id": ..., "__neosqlite_corrupted__": True}`` if ``data``
        cannot be decoded into a JSON dictionary.

        Args:
            id_val: Auto-increment integer row ID in SQLite.
            data: JSON string or UTF-8 bytes representing the document.
            stored_id_val: Raw ``_id`` column value from the row.

        Returns:
            dict[str, Any]: Deserialized document with ``_id`` populated,
            or a corrupted-document sentinel dictionary.
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
        """Resolve a stored ``_id`` value with auto-increment fallback.

        Args:
            stored_id_val: Raw value from the ``_id`` column, or ``None``.
            fallback_id: Fallback auto-increment ID if ``stored_id_val`` is
                ``None``.

        Returns:
            ObjectId | Any: Parsed ``_id`` value, or ``fallback_id`` when
            ``stored_id_val`` is ``None``.
        """
        if stored_id_val is not None:
            return self._parse_stored_id(stored_id_val)
        else:
            # Fallback to the auto-increment ID for backward compatibility
            return fallback_id

    def _get_stored_id(self, doc_id: int) -> ObjectId | int | str | None:
        """Retrieve the stored ``_id`` for a document by integer row ID.

        Returns ``doc_id`` directly for legacy tables without an ``_id``
        column, or ``None`` if no row matches or lookup fails.

        Args:
            doc_id: Auto-increment integer row ID in SQLite.

        Returns:
            ObjectId | int | str | None: Parsed ``_id`` value, ``doc_id``
            for legacy schemas, or ``None`` if unavailable.
        """
        try:
            # Check if the _id column exists (cached via schema_utils)
            if column_exists(self.db, self.name, "_id"):
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
        """Return the name of the column that stores the logical ``_id``.

        Modern collections keep ``_id`` in a dedicated ``_id`` column.
        Legacy collections (before the ``_id`` column was introduced) store
        it in the auto-increment ``id`` column, which is retained only as
        a fallback.

        Returns:
            str: ``"_id"`` when the column exists, otherwise ``"id"``.
        """
        if column_exists(self.db, self.name, "_id"):
            return "_id"
        return "id"

    def _get_val(self, item: dict[str, Any], key: Any) -> Any:
        """Retrieve a value from a dictionary using a dotted key path.

        Strips an optional leading ``$`` and traverses nested dicts by
        splitting on ``"."``. Non-string keys are returned directly as
        literal values (for example, literal ``$group`` ``_id`` values).

        Args:
            item: Dictionary to search.
            key: Dotted field path (optionally ``$``-prefixed), or a
                non-string literal value.

        Returns:
            Any: Resolved value, ``key`` itself when non-string, or ``None``
            if any segment is missing or encounters a non-dict.
        """
        if not isinstance(key, str):
            return key
        key = key.removeprefix("$")
        val: Any = item
        for k in key.split("."):
            if not isinstance(val, dict):
                return None
            val = val.get(k)
        return val

    def _set_val(self, item: dict[str, Any], key: str, value: Any) -> None:
        """Set a value in a dictionary at a dotted key path.

        Strips an optional leading ``$`` and creates intermediate nested
        dictionaries when a segment is missing or not a ``dict``.

        Args:
            item: Dictionary to modify in place.
            key: Dotted target field path, optionally prefixed with ``$``.
            value: Value to assign at the target path.
        """
        key = key.removeprefix("$")

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
        """Create the collection table and ``_id`` index if not present.

        Creates the backing SQLite table with ``id``, ``_id``, and ``data``
        columns (using ``JSONB`` when supported, otherwise ``TEXT``),
        optionally adding a ``CHECK`` constraint compiled from
        ``kwargs["validator"]["$jsonSchema"]``. Also creates the unique
        index on ``_id`` and ensures the ``_id`` column exists on legacy
        tables.

        Args:
            **kwargs: Optional collection creation options such as
                ``validator``.
        """
        validator = kwargs.get("validator")
        check_clause = ""

        if isinstance(validator, dict) and "$jsonSchema" in validator:
            from .query_helper.schema_compiler import compile_schema_to_sql

            self._validator_schema = validator["$jsonSchema"]
            save_validator_schema(self.db, self.name, self._validator_schema)
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
                    _id BLOB,
                    data JSONB NOT NULL
                    {check_clause}
                )""")
        else:
            self.db.execute(f"""
                CREATE TABLE IF NOT EXISTS {quote_table_name(self.name)} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    _id BLOB,
                    data TEXT NOT NULL
                    {check_clause}
                )
                """)

        # Create unique index on _id column for faster lookups
        create_unique_index_on_id(self.db, self.name)

        # Add the _id column if it doesn't exist (for backward compatibility)
        self._ensure_id_column_exists()

    def _validate_document_schema(self, document: dict[str, Any]) -> None:
        """Validate ``document`` against the collection's ``$jsonSchema`` validator."""
        from .query_helper.schema_validator import (
            validate_document_for_collection,
        )

        validate_document_for_collection(self, document)

    def _ensure_id_column_exists(self):
        """Ensure the ``_id`` column exists on the backing table.

        For backward compatibility with legacy schemas, adds ``_id BLOB``
        and creates a unique index on it if missing.
        """
        try:
            # Check if _id column exists (cached via schema_utils)
            if not column_exists(self.db, self.name, "_id"):
                # Add the _id column with BLOB affinity (storage-class neutral)
                self.db.execute(
                    f"ALTER TABLE {quote_table_name(self.name)} ADD COLUMN _id BLOB"
                )
                invalidate_table_columns_cache(self.db, self.name)
                # Create unique index on _id column for faster lookups
                create_unique_index_on_id(self.db, self.name)
        except Exception as e:
            # If we can't add the column, continue without it (for backward compatibility)
            logger.debug(
                f"Failed to add _id column to collection '{self.name}': {e}"
            )
            pass

    def __getattr__(self, name: str) -> Any:
        """Support GridFS-style nested sub-collection attribute access.

        Allows accessing ``files`` and ``chunks`` sub-collections under a
        bucket collection (for example, ``db.fs.files``).

        Args:
            name: Attribute name requested on the collection.

        Returns:
            Any: A ``Collection`` instance for ``<collection>_files`` or
            ``<collection>_chunks``.

        Raises:
            AttributeError: If ``name`` is not ``"files"`` or ``"chunks"``.
        """
        if name in ("files", "chunks"):
            from ..gridfs import GridFSBucket

            bucket_name = validate_identifier(self.name)
            files_coll = f"{bucket_name}_files"
            chunks_coll = f"{bucket_name}_chunks"
            # Drop empty placeholder document table auto-created by `db.fs`
            # before initializing the real GridFS schema (#191).
            try:
                placeholder_cols = {
                    r[1]
                    for r in self.db.execute(
                        f"PRAGMA table_info({quote_table_name(bucket_name)})"
                    ).fetchall()
                }
                if placeholder_cols == {"id", "_id", "data"}:
                    row = self.db.execute(
                        f"SELECT COUNT(*) FROM {quote_table_name(bucket_name)}"
                    ).fetchone()
                    idx_rows = self.db.execute(
                        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ?",
                        (bucket_name,),
                    ).fetchall()
                    non_default_idx = [
                        r[0]
                        for r in idx_rows
                        if r[0] != f"idx_{bucket_name}_id"
                        and not r[0].startswith("sqlite_autoindex_")
                    ]
                    if row is not None and row[0] == 0 and not non_default_idx:
                        self.db.execute(
                            f"DROP TABLE IF EXISTS {quote_table_name(bucket_name)}"
                        )
                        database = self.__dict__.get("_database")
                        if database is not None:
                            database._collections.pop(self.name, None)
            except Exception:
                pass

            if not (
                self._object_exists("table", files_coll)
                and self._object_exists("table", chunks_coll)
            ):
                GridFSBucket(self.db, bucket_name=bucket_name)

            full_name = f"{bucket_name}_{name}"
            return Collection(
                self.db, full_name, create=False, database=self._database
            )
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def rename(self, new_name: str) -> None:
        """Rename the collection table and its associated SQLite objects.

        Does nothing when ``new_name`` equals ``self.name``. Otherwise
        renames the table, associated ``*_fts`` virtual tables, regular
        indexes (updating ``neosqlite_index_keys``), and table triggers,
        then updates ``self.name`` and clears the TTL cache.

        Args:
            new_name: New name for the collection table.

        Raises:
            sqlite3.Error: If a table named ``new_name`` already exists.
        """
        # If the new name is the same as the current name, do nothing
        if new_name == self.name:
            return

        # Check if a collection with the new name already exists
        if self._object_exists(type_="table", name=new_name):
            raise sqlite3.Error(f"Collection '{new_name}' already exists")

        # Rename the table
        old_name = self.name
        try:
            trig_fts_rows = self.db.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='trigger' AND tbl_name = ? "
                "AND name LIKE '%\\_fts\\_insert' ESCAPE '\\'",
                (old_name,),
            ).fetchall()
            owned_fts_tables = [
                r[0].removesuffix("_insert") for r in trig_fts_rows
            ]
        except Exception:
            owned_fts_tables = []

        self.db.execute(
            f"ALTER TABLE {quote_table_name(old_name)} RENAME TO {quote_table_name(new_name)}"
        )
        invalidate_table_columns_cache(self.db, old_name)
        invalidate_table_columns_cache(self.db, new_name)

        # Carry FTS5 virtual tables owned exclusively by this collection (#134, #184)
        fts_renames: list[tuple[str, str]] = []
        prefix = old_name + "_"
        for fts_name in owned_fts_tables:
            if not fts_name.startswith(prefix):
                continue
            new_fts = f"{new_name}_{fts_name[len(prefix) :]}"
            self.db.execute(
                f"ALTER TABLE {quote_table_name(fts_name)} "
                f"RENAME TO {quote_table_name(new_fts)}"
            )
            fts_renames.append((fts_name, new_fts))
            try:
                self.db.execute(
                    "UPDATE neosqlite_index_keys SET index_name = ? WHERE index_name = ?",
                    (new_fts, fts_name),
                )
            except Exception:
                pass

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
                try:
                    self.db.execute(
                        "UPDATE _neosqlite_ttl_indexes "
                        "SET index_name = ?, collection_name = ? "
                        "WHERE index_name = ? AND collection_name = ?",
                        (new_idx_name, new_name, idx_name, old_name),
                    )
                except Exception:
                    pass

        try:
            self.db.execute(
                "UPDATE _neosqlite_ttl_indexes SET collection_name = ? "
                "WHERE collection_name = ?",
                (new_name, old_name),
            )
        except Exception:
            pass

        rename_validator_schema(self.db, old_name, new_name)

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
                for old_fts, new_fts in fts_renames:
                    new_trig_sql = new_trig_sql.replace(old_fts, new_fts)
                self.db.execute(new_trig_sql)

        # Update the collection name and Connection registry
        self.name = new_name
        try:
            database = self.__dict__.get("_database")
            if (
                database is not None
                and database._collections.get(old_name) is self
            ):
                database._collections.pop(old_name, None)
                database._collections[new_name] = self
        except Exception:
            pass
        self._invalidate_ttl_cache()
        # Clear query translation caches so cached SQL cannot reference old_name
        query_engine = self.__dict__.get("query_engine")
        if query_engine is not None:
            sql_tier_aggregator = getattr(
                query_engine, "sql_tier_aggregator", None
            )
            if sql_tier_aggregator is not None:
                sql_tier_aggregator.clear_cache()
            helpers = getattr(query_engine, "helpers", None)
            tier2 = getattr(helpers, "tier2_evaluator", None)
            if tier2 is not None:
                tier2.clear_cache()

    def options(self) -> dict[str, Any]:
        """Retrieve schema metadata and PyMongo-compatible collection options.

        Returns:
            dict[str, Any]: Dictionary containing ``name``, ``columns``,
            ``indexes``, ``count``, and (when table inspection succeeds)
            ``codec_options``, ``read_preference``, ``write_concern``, and
            ``read_concern``.
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
        """Insert a single document into the collection.

        Delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.insert_one`.

        Args:
            document: Document dictionary to insert.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            InsertOneResult: Result containing the inserted document's ``_id``.
        """
        return self.query_engine.insert_one(document, session=session)

    def insert_many(
        self,
        documents: list[dict[str, Any]],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> InsertManyResult:
        """Insert multiple documents into the collection.

        Delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.insert_many`.

        Args:
            documents: List of document dictionaries to insert.
            ordered: Whether to insert sequentially in order. Defaults to
                ``True``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            InsertManyResult: Result containing the inserted document IDs.
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
        """Update a single document matching the filter.

        Delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.update_one`.

        Args:
            filter: Query filter selecting the target document.
            update: Update operators to apply.
            upsert: Insert a new document when no document matches. Defaults
                to ``False``.
            array_filters: Optional filter expressions for positional array
                updates.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            UpdateResult: Result with matched, modified, and upserted counts.
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
        """Update all documents matching the filter.

        Delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.update_many`.

        Args:
            filter: Query filter selecting documents to update.
            update: Update operators to apply to matched documents.
            upsert: Insert a new document when no document matches. Defaults
                to ``False``.
            array_filters: Optional filter expressions for positional array
                updates.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            UpdateResult: Result with matched, modified, and upserted counts.
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
        """Replace a single document matching the filter.

        Delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.replace_one`.

        Args:
            filter: Query filter selecting the document to replace.
            replacement: Replacement document dictionary.
            upsert: Insert ``replacement`` when no document matches. Defaults
                to ``False``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            UpdateResult: Result with matched, modified, and upserted info.
        """
        return self.query_engine.replace_one(
            filter, replacement, upsert=upsert, session=session
        )

    def delete_one(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """Delete a single document matching the filter.

        For GridFS system collections (e.g., ``fs_files``, ``fs_chunks``),
        delegates to ``GridFSBucket.delete()`` so both file metadata and
        chunks are removed together.

        Args:
            filter: Query filter selecting the document to delete.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            DeleteResult: Result containing the count of deleted documents.
        """
        # Check if this is a GridFS system collection
        if self._is_gridfs_collection():
            return self._delete_one_as_gridfs(filter)

        return self.query_engine.delete_one(filter, session=session)

    def _get_gridfs_bucket(self):
        """Return a ``GridFSBucket`` for this GridFS system collection.

        Extracts the bucket name by stripping the ``_files`` or ``_chunks``
        suffix from ``self.name``.

        Returns:
            GridFSBucket: Bucket instance bound to ``self.db``.

        Raises:
            RuntimeError: If ``self.name`` does not end with ``_files`` or
                ``_chunks``.
        """
        from ..gridfs import GridFSBucket

        if self.name.endswith("_files"):
            bucket_name = self.name.removesuffix("_files")
        elif self.name.endswith("_chunks"):
            bucket_name = self.name.removesuffix("_chunks")
        else:
            raise RuntimeError(f"Invalid GridFS collection name: {self.name}")
        return GridFSBucket(self.db, bucket_name=bucket_name)

    def _delete_one_as_gridfs(self, filter: dict[str, Any]):
        """Delete the first matching file (or chunk row) from a GridFS collection.

        Args:
            filter: Query filter matched against the bucket's files or chunks.

        Returns:
            DeleteResult: Result with ``deleted_count`` of ``0`` or ``1``.
        """
        from ..results import DeleteResult

        if self.name.endswith("_chunks"):
            where_clause, params = self._build_chunks_where(filter)
            table = quote_table_name(self.name)
            row = self.db.execute(
                f"SELECT _id FROM {table} {where_clause} LIMIT 1",
                params,
            ).fetchone()
            if row is None:
                return DeleteResult(0)
            cursor = self.db.execute(
                f"DELETE FROM {table} WHERE _id = ?",
                (row[0],),
            )
            return DeleteResult(cursor.rowcount)

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
        """Delete all documents matching the filter.

        For GridFS system collections (e.g., ``fs_files``, ``fs_chunks``),
        delegates to ``GridFSBucket.delete()`` for ``_files`` or deletes
        directly from ``_chunks``.

        Args:
            filter: Query filter selecting the documents to delete.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            DeleteResult: Result containing the count of deleted documents.
        """
        # Check if this is a GridFS system collection
        if self._is_gridfs_collection():
            return self._delete_many_as_gridfs(filter)

        return self.query_engine.delete_many(filter, session=session)

    def _delete_many_as_gridfs(self, filter: dict[str, Any]):
        """Delete all matching files (or chunk rows) from a GridFS collection.

        Args:
            filter: Query filter matched against the bucket's files or chunks.

        Returns:
            DeleteResult: Result with the total count of deleted rows/files.
        """
        from ..results import DeleteResult

        if self.name.endswith("_chunks"):
            where_clause, params = self._build_chunks_where(filter)
            table = quote_table_name(self.name)
            cursor = self.db.execute(
                f"DELETE FROM {table} {where_clause}",
                params,
            )
            return DeleteResult(cursor.rowcount)

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
        """Find documents in the collection matching a filter.

        For GridFS system collections (e.g., ``fs_files``, ``fs_chunks``),
        delegates to ``_find_as_gridfs``. Otherwise auto-purges expired TTL
        documents, delegates to ``QueryEngine.find``, and applies optional
        ``skip``, ``limit``, and ``sort`` modifiers.

        Args:
            filter: Optional query filter dictionary.
            projection: Optional field projection dictionary (not used for
                GridFS collections).
            hint: Optional index hint name (not used for GridFS).
            session: Optional ``ClientSession`` for transaction validation.
            limit: Maximum number of documents to return (``0`` or ``None``
                for no limit).
            skip: Number of leading documents to skip (``0`` or ``None``
                for none).
            sort: Sort specification as a list of ``(field, direction)``
                tuples, dict, ``(field, direction)`` tuple, or field name.
            **kwargs: Back-compatibility aliases for ``limit``, ``skip``,
                and ``sort``.

        Returns:
            Cursor: Query cursor (or GridFS cursor/chunk list for GridFS
            collections).
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
            return self._find_as_gridfs(
                filter,
                session=session,
                limit=limit,
                skip=skip,
                sort=sort,
            )

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
        sort: list[tuple[str, Any]] | dict[str, Any] | str | tuple[str, Any],
    ) -> list[tuple[str, Any]]:
        """Normalize PyMongo-style sort specifications to key/direction pairs.

        Args:
            sort: Field name string, dict mapping fields to directions,
                single ``(field, direction)`` tuple, or iterable of pairs.

        Returns:
            list[tuple[str, Any]]: Normalized list of ``(field, direction)``
            tuples with integer or metadata-dict directions.
        """

        def _coerce_dir(direction: Any) -> Any:
            return direction if isinstance(direction, dict) else int(direction)

        if isinstance(sort, str):
            return [(sort, 1)]
        if isinstance(sort, dict):
            return [
                (key, _coerce_dir(direction)) for key, direction in sort.items()
            ]
        if isinstance(sort, tuple):
            if len(sort) == 2 and isinstance(sort[0], str):
                return [(sort[0], _coerce_dir(sort[1]))]
            return [(key, _coerce_dir(direction)) for key, direction in sort]
        return [(key, _coerce_dir(direction)) for key, direction in list(sort)]

    def _is_gridfs_collection(self) -> bool:
        """Check whether this collection is a GridFS system collection.

        Verifies both the ``_files``/``_chunks`` naming suffix and the
        presence of GridFS-specific columns via ``PRAGMA table_info``,
        falling back to the naming convention if schema inspection fails.

        Returns:
            bool: ``True`` if this collection represents GridFS files or
            chunks, ``False`` otherwise.
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
        limit: int | None = None,
        skip: int | None = None,
        sort: (
            list[tuple[str, int]]
            | dict[str, int]
            | str
            | tuple[str, int]
            | None
        ) = None,
    ):
        """Execute ``find`` on a GridFS ``_files`` or ``_chunks`` collection.

        Args:
            filter: Optional query filter dictionary.
            session: Optional ``ClientSession`` passed to ``GridFSBucket``.
            limit: Optional maximum number of files/chunks to return.
            skip: Optional number of leading files/chunks to skip.
            sort: Optional sort specification.

        Returns:
            Any: List of chunk dicts for ``_chunks`` collections, or a
            ``GridOutCursor`` for ``_files`` collections.
        """
        if self.name.endswith("_chunks"):
            norm_sort = (
                self._normalize_sort_spec(sort) if sort is not None else None
            )
            return self._find_chunks_direct(
                filter, limit=limit, skip=skip, sort=norm_sort
            )
        bucket = self._get_gridfs_bucket()
        cursor = bucket.find(filter, session=session)
        if skip:
            cursor = cursor.skip(int(skip))
        if limit:
            cursor = cursor.limit(int(limit))
        if sort is not None:
            cursor = cursor.sort(self._normalize_sort_spec(sort))
        return cursor

    def _coerce_chunk_param(self, column: str, val: Any) -> Any:
        """Resolve ``files_id`` ObjectId/str references to ``_files`` row id."""
        if column == "files_id" and isinstance(val, (ObjectId, str)):
            files_table = f"{self.name.removesuffix('_chunks')}_files"
            try:
                int_id = get_integer_id_for_doc_id(self.db, files_table, val)
            except Exception:
                int_id = None
            return int_id if int_id is not None else -1
        return val

    _CHUNK_CMP_OPS: dict[str, str] = {
        "$eq": "=",
        "$ne": "!=",
        "$gt": ">",
        "$gte": ">=",
        "$lt": "<",
        "$lte": "<=",
    }

    def _build_chunks_where(
        self,
        filter: dict[str, Any] | None,
    ) -> tuple[str, list[Any]]:
        """Build a SQL WHERE clause and params for a GridFS ``_chunks`` table."""
        where_parts: list[str] = []
        params: list[Any] = []
        for key, value in (filter or {}).items():
            if key in ("files_id", "n", "_id"):
                column = key
                if isinstance(value, dict):
                    for op, op_val in value.items():
                        if op in self._CHUNK_CMP_OPS:
                            where_parts.append(
                                f"{column} {self._CHUNK_CMP_OPS[op]} ?"
                            )
                            params.append(
                                self._coerce_chunk_param(column, op_val)
                            )
                        elif op == "$in" and isinstance(op_val, (list, tuple)):
                            if not op_val:
                                where_parts.append("1=0")
                            else:
                                placeholders = ", ".join("?" * len(op_val))
                                where_parts.append(
                                    f"{column} IN ({placeholders})"
                                )
                                params.extend(
                                    self._coerce_chunk_param(column, elem)
                                    for elem in op_val
                                )
                        else:
                            where_parts.append("1=0")
                else:
                    where_parts.append(f"{column} = ?")
                    params.append(self._coerce_chunk_param(column, value))
            else:
                # Unknown chunk field — match nothing, never everything.
                where_parts.append("1=0")
        where_clause = (
            "WHERE " + " AND ".join(where_parts) if where_parts else ""
        )
        return where_clause, params

    def _find_chunks_direct(
        self,
        filter: dict[str, Any] | None = None,
        limit: int | None = None,
        skip: int | None = None,
        sort: list[tuple[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Query chunk documents directly from a GridFS ``_chunks`` table."""
        table = quote_table_name(self.name)
        where_clause, params = self._build_chunks_where(filter)
        order_clause = ""
        if sort:
            order_parts: list[str] = []
            for key, direction in sort:
                if key in ("files_id", "n", "_id"):
                    dir_sql = "DESC" if direction == -1 else "ASC"
                    order_parts.append(f"{key} {dir_sql}")
            if order_parts:
                order_clause = "ORDER BY " + ", ".join(order_parts)

        pagination_clause = ""
        limit_val = int(limit) if limit else 0
        skip_val = int(skip) if skip else 0
        if limit_val > 0 and skip_val > 0:
            pagination_clause = "LIMIT ? OFFSET ?"
            params.extend([limit_val, skip_val])
        elif limit_val > 0:
            pagination_clause = "LIMIT ?"
            params.append(limit_val)
        elif skip_val > 0:
            pagination_clause = "LIMIT -1 OFFSET ?"
            params.append(skip_val)

        query = (
            f"SELECT _id, files_id, n, data FROM {table} "
            f"{where_clause} {order_clause} {pagination_clause}"
        ).strip()
        cursor = self.db.execute(query, params)
        return [
            {"_id": row[0], "files_id": row[1], "n": row[2], "data": row[3]}
            for row in cursor.fetchall()
        ]

    def _count_documents_as_gridfs(
        self,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> int:
        """Count documents in a GridFS ``_files`` or ``_chunks`` table.

        Args:
            filter: Optional query filter dictionary.
            session: Optional ``ClientSession`` (accepted for signature
                parity).

        Returns:
            int: Number of matching rows in the GridFS table, or ``0`` if
            the table name does not match a GridFS suffix.
        """
        table = quote_table_name(self.name)
        if self.name.endswith("_files"):
            from ..gridfs.grid_file import build_gridfs_files_where

            where_clause, files_params = build_gridfs_files_where(filter or {})
            query = f"SELECT COUNT(*) FROM {table} {where_clause}".strip()
            cursor = self.db.execute(query, files_params)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        elif self.name.endswith("_chunks"):
            where_clause, chunks_params = self._build_chunks_where(filter)
            query = f"SELECT COUNT(*) FROM {table} {where_clause}".strip()
            cursor = self.db.execute(query, chunks_params)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        return 0

    def find_raw_batches(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """Query documents and return raw JSON batches.

        Delegates to ``QueryEngine.find_raw_batches()``.

        Args:
            filter: Optional query filter dictionary.
            projection: Optional field projection dictionary.
            hint: Optional index hint name.
            batch_size: Maximum number of documents per batch. Defaults to
                ``100``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            RawBatchCursor: Cursor yielding raw JSON document batches.
        """
        self._auto_purge_ttl()
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
        """Find a single document matching the filter.

        For GridFS system collections (e.g., ``fs_files``, ``fs_chunks``),
        delegates to ``_find_as_gridfs`` and returns the first match.
        Otherwise auto-purges expired TTL documents and delegates to
        ``QueryEngine.find_one``.

        Args:
            filter: Optional query filter dictionary.
            projection: Optional field projection dictionary (not used for
                GridFS collections).
            hint: Optional index hint name (not used for GridFS).
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            dict[str, Any] | None: First matching document (or ``GridOut``
            for GridFS ``_files``), or ``None`` if no document matches.
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
        """Count documents matching the given filter.

        Auto-purges expired TTL documents before counting and handles GridFS
        system collections directly.

        Args:
            filter: Query filter dictionary.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            int: Exact count of matching documents.
        """
        self._auto_purge_ttl()
        if self._is_gridfs_collection():
            return self._count_documents_as_gridfs(filter, session=session)
        return self.query_engine.count_documents(filter, session=session)

    def estimated_document_count(
        self,
        options: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> int:
        """Get an estimated count of documents in the collection.

        Delegates to ``_count_documents_as_gridfs`` for GridFS collections,
        or to ``QueryEngine.estimated_document_count()``.

        Args:
            options: Optional PyMongo-compatible count options (such as
                ``maxTimeMS`` or ``hint``), accepted for compatibility and
                ignored in SQLite.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            int: Estimated number of documents in the collection.
        """
        if self._is_gridfs_collection():
            return self._count_documents_as_gridfs({}, session=session)
        return self.query_engine.estimated_document_count(session=session)

    def find_one_and_delete(
        self,
        filter: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Find a single matching document and delete it atomically.

        Delegates to ``QueryEngine.find_one_and_delete()``.

        Args:
            filter: Query filter selecting the candidate document.
            projection: Optional field projection applied to the returned
                document.
            sort: Optional sort specification as ``(field, direction)``
                pairs when multiple documents match.
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional keyword arguments forwarded to
                ``QueryEngine.find_one_and_delete``.

        Returns:
            dict[str, Any] | None: The deleted document before removal, or
            ``None`` if no document matched.
        """
        self._auto_purge_ttl()
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
        """Find a single matching document and replace it.

        Delegates to ``QueryEngine.find_one_and_replace()``.

        Args:
            filter: Query filter selecting the candidate document.
            replacement: Replacement document dictionary.
            projection: Optional field projection applied to the returned
                document.
            sort: Optional sort specification as ``(field, direction)``
                pairs when multiple documents match.
            upsert: Insert ``replacement`` when no document matches. Defaults
                to ``False``.
            return_document: If truthy, return the document after
                replacement; otherwise return the original document.
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional keyword arguments forwarded to
                ``QueryEngine.find_one_and_replace``.

        Returns:
            dict[str, Any] | None: Document before or after replacement, or
            ``None`` if no document matched (and not returned on upsert).
        """
        self._auto_purge_ttl()
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
        """Find a single matching document and apply update operators.

        Delegates to ``QueryEngine.find_one_and_update()``.

        Args:
            filter: Query filter selecting the candidate document.
            update: Update operators to apply.
            projection: Optional field projection applied to the returned
                document.
            sort: Optional sort specification as ``(field, direction)``
                pairs when multiple documents match.
            upsert: Insert a new document when no document matches. Defaults
                to ``False``.
            return_document: If truthy, return the updated document;
                otherwise return the document before update.
            array_filters: Optional filter expressions for positional array
                updates.
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional keyword arguments forwarded to
                ``QueryEngine.find_one_and_update``.

        Returns:
            dict[str, Any] | None: Document before or after update, or
            ``None`` if no document matched.
        """
        self._auto_purge_ttl()
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
        """Execute an aggregation pipeline on the collection.

        Auto-purges expired TTL documents and returns a lazy
        ``AggregationCursor`` configured with the supplied pipeline and
        options.

        Args:
            pipeline: List of aggregation pipeline stage dictionaries.
            allowDiskUse: Accepted for PyMongo compatibility (ignored).
            batchSize: Optional batch size hint for cursor fetching.
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional PyMongo-compatible keyword arguments
                forwarded to ``AggregationCursor``.

        Returns:
            AggregationCursor: Cursor iterating over pipeline results.
        """
        self._auto_purge_ttl()
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
        """Execute an aggregation pipeline returning raw JSON batches.

        Delegates to ``QueryEngine.aggregate_raw_batches()``.

        Args:
            pipeline: List of aggregation pipeline stage dictionaries.
            batch_size: Maximum number of documents per batch. Defaults to
                ``100``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            RawBatchCursor: Cursor yielding raw JSON document batches.
        """
        self._auto_purge_ttl()
        return self.query_engine.aggregate_raw_batches(
            pipeline, batch_size, session=session
        )

    def distinct(
        self,
        key: str,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> list[Any]:
        """Return distinct values for a field across matching documents.

        Auto-purges expired TTL documents and delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.distinct`.

        Args:
            key: Dotted field path to extract distinct values from.
            filter: Optional query filter restricting documents considered.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            list[Any]: List of distinct values found for ``key``.
        """
        self._auto_purge_ttl()
        return self.query_engine.distinct(key, filter, session=session)

    # --- Bulk Write methods delegated to QueryEngine ---
    def bulk_write(
        self,
        requests: list[Any],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> BulkWriteResult:
        """Execute a batch of write requests against the collection.

        Delegates to
        :meth:`~neosqlite.collection.query_engine.QueryEngine.bulk_write`.

        Args:
            requests: List of write request objects (insert, update, replace,
                or delete operations).
            ordered: Whether to stop on the first write error. Defaults to
                ``True``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            BulkWriteResult: Summary of inserted, matched, modified,
            deleted, and upserted counts and IDs.
        """
        return self.query_engine.bulk_write(requests, ordered, session=session)

    def initialize_ordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize a deprecated ordered bulk operation builder.

        Delegates to ``QueryEngine.initialize_ordered_bulk_op()``.

        .. deprecated::
            Use :meth:`bulk_write` instead.

        Returns:
            BulkOperationExecutor: Executor configured for ordered writes.
        """
        warnings.warn(
            "initialize_ordered_bulk_op is deprecated, use bulk_write instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.query_engine.initialize_ordered_bulk_op()

    def initialize_unordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize a deprecated unordered bulk operation builder.

        Delegates to ``QueryEngine.initialize_unordered_bulk_op()``.

        .. deprecated::
            Use :meth:`bulk_write` instead.

        Returns:
            BulkOperationExecutor: Executor configured for unordered writes.
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
        """Create an index on the collection (PyMongo-compatible).

        Delegates to ``IndexManager.create_index()`` and invalidates the
        cached TTL specifications afterward. Supports single-field,
        compound, unique, sparse, FTS5, datetime, and TTL
        (``expireAfterSeconds``) indexes.

        Args:
            key: Field name, list of field names, or PyMongo ``(field,
                direction_or_type)`` specification list.
            reindex: Accepted for compatibility (unused by native SQLite
                indexes). Defaults to ``True``.
            sparse: Create a partial index excluding missing field values.
                Defaults to ``False``.
            unique: Enforce uniqueness on indexed values. Defaults to
                ``False``.
            fts: Create an FTS5 text search index. Defaults to ``False``.
            tokenizer: Optional FTS5 tokenizer name (e.g., ``"icu"``).
            datetime_field: Create a UTC-normalized datetime expression
                index. Defaults to ``False``.
            expireAfterSeconds: Optional TTL expiration duration in seconds
                for single-field indexes.
            name: Optional index name accepted for compatibility (SQLite
                index names are auto-generated).
            **kwargs: Additional PyMongo-compatible index options.

        Returns:
            str: Generated SQLite index or FTS virtual table name.
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
        """Create an FTS5 search index on a single field.

        Delegates to ``IndexManager.create_search_index()``.

        Args:
            key: Field path to index for full-text search.
            tokenizer: Optional FTS5 tokenizer name.

        Returns:
            str: Generated FTS5 search table name.
        """
        return self.indexes.create_search_index(key, tokenizer)

    def create_indexes(
        self,
        indexes: list[IndexModel],
    ) -> list[str]:
        """Create multiple indexes from ``IndexModel`` specifications.

        Delegates to ``IndexManager.create_indexes()``.

        Args:
            indexes: List of ``IndexModel`` definitions to create.

        Returns:
            list[str]: Names of the created SQLite indexes.
        """
        return self.indexes.create_indexes(indexes)

    def create_search_indexes(
        self,
        indexes: list[str],
    ) -> list[str]:
        """Create FTS5 search indexes for multiple fields.

        Delegates to ``IndexManager.create_search_indexes()``.

        Args:
            indexes: List of field paths to index for full-text search.

        Returns:
            list[str]: Names of the created search indexes.
        """
        return self.indexes.create_search_indexes(indexes)

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: list[dict[str, Any]] | None = None,
    ):
        """Reindex the collection (no-op compatibility wrapper).

        Delegates to ``IndexManager.reindex()``. Native SQLite JSON
        indexes update automatically on write.

        Args:
            table: Table name (unused).
            sparse: Sparse flag (unused).
            documents: Optional document list (unused).
        """
        self.indexes.reindex(table, sparse, documents)

    @overload
    def list_indexes(self, as_keys: Literal[True]) -> list[list[str]]:
        """List index key specifications for this collection.

        Args:
            as_keys: Pass ``True`` to return indexed key lists.

        Returns:
            list[list[str]]: List of key name lists per index.
        """
        ...

    @overload
    def list_indexes(self, as_keys: Literal[False] = False) -> list[str]:
        """List index names for this collection.

        Args:
            as_keys: Pass ``False`` (default) to return index names.

        Returns:
            list[str]: List of SQLite index names.
        """
        ...

    def list_indexes(
        self,
        as_keys: bool = False,
    ) -> list[str] | list[list[str]]:
        """List indexes defined on this collection.

        Delegates to ``IndexManager.list_indexes()``.

        Args:
            as_keys: If ``True``, return lists of indexed key paths instead
                of SQLite index names. Defaults to ``False``.

        Returns:
            list[str] | list[list[str]]: Index names when ``as_keys`` is
            ``False``, or key path lists when ``as_keys`` is ``True``.
        """
        # This explicit check is the key to solving the Mypy error on overloading.
        if as_keys:
            # Inside this block, Mypy knows 'as_keys' is Literal[True].
            return self.indexes.list_indexes(as_keys)
        else:
            # Inside this block, Mypy knows 'as_keys' is Literal[False].
            return self.indexes.list_indexes(as_keys)

    def list_search_indexes(self) -> list[str]:
        """List FTS5 search indexes defined on this collection.

        Delegates to ``IndexManager.list_search_indexes()``.

        Returns:
            list[str]: Field names covered by FTS5 search indexes.
        """
        return self.indexes.list_search_indexes()

    def update_search_index(self, key: str, tokenizer: str | None = None):
        """Recreate an FTS5 search index with updated options.

        Delegates to ``IndexManager.update_search_index()``.

        Args:
            key: Indexed field path to recreate.
            tokenizer: Optional new FTS5 tokenizer name.
        """
        self.indexes.update_search_index(key, tokenizer)

    def drop_index(self, index: str):
        """Drop a single index from the collection.

        Delegates to ``IndexManager.drop_index()`` and invalidates the
        cached TTL specifications afterward.

        Args:
            index: Index name or field specification to drop.
        """
        try:
            self.indexes.drop_index(index)
        finally:
            self._invalidate_ttl_cache()

    def drop_search_index(self, index: str):
        """Drop an FTS5 search index from the collection.

        Delegates to ``IndexManager.drop_search_index()``.

        Args:
            index: Field name of the search index to drop.
        """
        self.indexes.drop_search_index(index)

    def drop_indexes(self):
        """Drop all user indexes from the collection.

        Delegates to ``IndexManager.drop_indexes()`` and invalidates the
        cached TTL specifications afterward.
        """
        try:
            self.indexes.drop_indexes()
        finally:
            self._invalidate_ttl_cache()

    def index_information(self) -> dict[str, Any]:
        """Return metadata describing indexes on this collection.

        Delegates to ``IndexManager.index_information()``.

        Returns:
            dict[str, Any]: Mapping from index name to index metadata
            (version, uniqueness, key mapping, and optional TTL).
        """
        return self.indexes.index_information()

    def get_ttl_specs(self) -> list[dict[str, Any]]:
        """List TTL index specs declared on this collection.

        Returns:
            list[dict[str, Any]]: Each dict contains ``index_name``,
            ``collection_name``, ``field``, and ``expireAfterSeconds``.
        """
        return self.indexes.get_ttl_specs()

    def _ttl_specs_cached(self) -> list[dict[str, Any]]:
        """Return cached TTL specs, loading them on first access.

        Returns:
            list[dict[str, Any]]: Cached list of TTL index specification
            dicts for this collection.
        """
        cached = self.__dict__.get("_ttl_specs_cache")
        if cached is None:
            cached = self.get_ttl_specs()
            self._ttl_specs_cache = cached
        return cached

    def _invalidate_ttl_cache(self) -> None:
        """Clear the cached TTL specifications for this collection."""
        self._ttl_specs_cache = None

    def purge_expired(self, now: datetime | None = None) -> int:
        """Delete documents expired under this collection's TTL indexes.

        A document expires when ``now >= field_value + expireAfterSeconds``.
        Both UTC datetimes (ISO strings) and numeric epoch seconds are
        supported for the indexed field. Deletions are plain deletes and
        fire normal ``watch()`` events.

        Args:
            now: Reference time (timezone-aware preferred; naive is assumed
                UTC). Defaults to current UTC time. Accepts an explicit
                value so tests need not sleep.

        Returns:
            int: Number of documents deleted.
        """
        specs = self._ttl_specs_cached()
        if not specs:
            return 0
        if now is None:
            ref = datetime.now(UTC)
        elif now.tzinfo is None:
            ref = now.replace(tzinfo=UTC)
        else:
            ref = now
        total = 0
        for spec in specs:
            field = spec["field"]
            expire = int(spec["expireAfterSeconds"])
            cutoff = ref - timedelta(seconds=expire)
            try:
                result = self.delete_many(
                    {
                        "$and": [
                            {field: {"$type": "date"}},
                            {field: {"$lte": cutoff}},
                        ]
                    }
                )
                total += result.deleted_count
            except Exception as exc:
                logger.debug("TTL datetime purge skipped: %s", exc)
            try:
                result = self.delete_many(
                    {
                        "$and": [
                            {field: {"$type": "number"}},
                            {field: {"$lte": cutoff.timestamp()}},
                        ]
                    }
                )
                total += result.deleted_count
            except Exception as exc:
                logger.debug("TTL epoch purge skipped: %s", exc)
        return total

    def _auto_purge_ttl(self) -> None:
        """Lazily purge expired TTL documents when TTL indexes exist."""
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
        """Get the parent connection instance (PyMongo ``client`` alias).

        Returns:
            Connection: The parent connection instance.
        """
        return self.database

    @property
    def codec_options(self) -> Any:
        """Get the codec options configured for this collection.

        Returns:
            Any: Collection-level codec options, or the parent database's
            codec options if not overridden.
        """
        if hasattr(self, "_codec_options") and self._codec_options is not None:
            return self._codec_options
        return self.database.codec_options if self.database else None

    @property
    def read_preference(self) -> Any:
        """Get the read preference configured for this collection.

        Returns:
            Any: Collection-level read preference, or the parent database's
            read preference if not overridden.
        """
        if (
            hasattr(self, "_read_preference")
            and self._read_preference is not None
        ):
            return self._read_preference
        return self.database.read_preference if self.database else None

    @property
    def write_concern(self) -> Any:
        """Get the write concern configured for this collection.

        Returns:
            Any: Collection-level write concern, or the parent database's
            write concern if not overridden.
        """
        if hasattr(self, "_write_concern") and self._write_concern is not None:
            return self._write_concern
        return self.database.write_concern if self.database else None

    @property
    def read_concern(self) -> Any:
        """Get the read concern configured for this collection.

        Returns:
            Any: Collection-level read concern, or the parent database's
            read concern if not overridden.
        """
        if hasattr(self, "_read_concern") and self._read_concern is not None:
            return self._read_concern
        return self.database.read_concern if self.database else None

    @property
    def database(self) -> Connection:
        """Get the parent database connection for this collection.

        Returns:
            Connection: The connection object associated with this
            collection.
        """
        return self._database

    @property
    def db_path(self) -> str:
        """Get the path to the underlying SQLite database file.

        Returns:
            str: Database file path, or ``":memory:"`` if no parent database
            is attached.
        """
        return self.database.db_path if self.database else ":memory:"

    @property
    def full_name(self) -> str:
        """Get the full collection name (``<database>.<collection>``).

        Returns:
            str: Full name including database prefix when available,
            otherwise ``self.name``.

        Example:
            >>> from neosqlite import Connection
            >>> db = Connection(":memory:")
            >>> coll = db.my_collection
            >>> print(coll.full_name)
            memory.my_collection
        """
        if self._database and hasattr(self._database, "name"):
            return f"{self._database.name}.{self.name}"
        return self.name

    def with_options(
        self,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        """Return a clone of this collection with updated PyMongo options.

        Note: NeoSQLite is a single-node database, so ``read_preference``,
        ``write_concern``, and ``read_concern`` are stored for API
        compatibility and do not alter query execution.

        Args:
            codec_options: Optional codec options stored on the clone.
            read_preference: Optional read preference stored on the clone.
            write_concern: Optional write concern stored on the clone.
            read_concern: Optional read concern stored on the clone.

        Returns:
            Collection: A new ``Collection`` instance with the specified
            options attached.

        Example:
            >>> from neosqlite import Connection
            >>> db = Connection(":memory:")
            >>> coll = db.my_collection
            >>> clone = coll.with_options(write_concern={"w": "majority"})
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
        """Check whether a SQLite table or index exists in ``sqlite_master``.

        Args:
            type_: SQLite object type (``"table"`` or ``"index"``).
            name: Object name to check in ``sqlite_master``.

        Returns:
            bool: ``True`` if an object of the given type and name exists,
            ``False`` otherwise.
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
        """Drop the collection table, its FTS/TTL/index metadata, and caches.

        Delegates to ``Connection.drop_collection()`` when bound to a parent
        database connection, or runs ``cleanup_dropped_collection()`` directly
        for standalone collections, then invalidates this instance's TTL cache.
        """
        database = self.__dict__.get("_database")
        if database is not None:
            database.drop_collection(self.name)
        else:
            cleanup_dropped_collection(self.db, self.name)
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
        """Monitor changes on this collection via a ``ChangeStream``.

        Uses SQLite triggers and change tracking tables to emit change
        events for modifications to this collection.

        Args:
            pipeline: Optional aggregation pipeline stages applied to change
                events.
            full_document: Controls how ``fullDocument`` is populated on
                update events.
            resume_after: Optional resume token specifying a logical
                starting point.
            max_await_time_ms: Maximum wait time in milliseconds for new
                changes.
            batch_size: Maximum number of change events fetched per batch.
            collation: Optional collation settings for the stream.
            start_at_operation_time: Optional operation timestamp to start
                monitoring from.
            session: Optional ``ClientSession`` for transaction validation.
            start_after: Optional resume token to start strictly after.

        Returns:
            ChangeStream: Change stream iterating over collection events.
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
