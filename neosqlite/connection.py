from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator, Literal

from ._sqlite import sqlite3
from .client_session import ClientSession
from .collection import Collection
from .collection.aggregation_cursor import AggregationCursor
from .exceptions import CollectionInvalid
from .migration import migrate_autovacuum, needs_migration, should_migrate
from .options import AutoVacuumMode, JournalMode, WriteConcern
from .sql_utils import quote_table_name

logger = logging.getLogger(__name__)


class Connection:
    """
    Represents a connection to an NeoSQLite database.

    Provides methods for managing collections, executing SQL queries,
    and handling database lifecycle events. Supports PyMongo-like API.
    """

    DEFAULT_TRANSLATION_CACHE_SIZE = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize a new database connection.

        Args:
            *args: Positional arguments passed to sqlite3.connect().
            **kwargs: Keyword arguments passed to sqlite3.connect().
                      Special kwargs:
                      - tokenizers: List of tuples (name, path) for FTS5 tokenizers to load
                      - debug: Boolean flag to enable debug printing
                      - name: Optional name for the database (for PyMongo API compatibility)
                      - _is_clone: Internal flag for cloning (not for public use)
                      - journal_mode: Optional journal mode (default: "WAL")
                      - auto_vacuum: Auto vacuum mode (default: INCREMENTAL).
                        Can be 0/NONE, 1/FULL, 2/INCREMENTAL, or "NONE"/"FULL"/"INCREMENTAL".
                        If database has different auto_vacuum setting, migration may be triggered.
                      - translation_cache: SQL translation cache size (default: 100, 0 to disable)
        """
        self._collections: dict[str, Collection] = {}
        self._tokenizers: list[tuple[str, str]] = kwargs.pop("tokenizers", [])
        self.debug: bool = kwargs.pop("debug", False)
        self._is_clone = kwargs.pop("_is_clone", False)

        self._codec_options = kwargs.pop("codec_options", None)
        self._read_preference = kwargs.pop("read_preference", None)
        self._write_concern = kwargs.pop("write_concern", None)
        self._read_concern = kwargs.pop("read_concern", None)

        self.journal_mode = JournalMode.validate(
            kwargs.pop("journal_mode", "WAL")
        )

        self.auto_vacuum = AutoVacuumMode.validate(
            kwargs.pop("auto_vacuum", AutoVacuumMode.INCREMENTAL)
        )

        self._translation_cache_size: int | None = kwargs.pop(
            "translation_cache", self.DEFAULT_TRANSLATION_CACHE_SIZE
        )

        self.name: str = kwargs.pop("name", None)
        self._db_path = args[0] if args else ":memory:"
        if self.name is None:
            self.name = (
                self._db_path if self._db_path != ":memory:" else "memory"
            )

        self._closed = False
        if not self._is_clone:
            self.connect(*args, **kwargs)

    @property
    def db_path(self) -> str:
        """
        Get the path to the database file.

        Returns:
            str: The database file path.
        """
        return self._db_path

    def connect(self, *args: Any, **kwargs: Any) -> None:
        """
        Establish a connection to the SQLite database.

        Configures the database connection with the provided arguments, sets up
        SQLite-specific settings like isolation level and journal mode, and loads
        custom FTS5 tokenizers if specified. This method does not return a value.

        Args:
            *args: Positional arguments passed to sqlite3.connect().
            **kwargs: Keyword arguments passed to sqlite3.connect().
        """
        self.db = sqlite3.connect(*args, **kwargs)
        self.db.execute(f"PRAGMA auto_vacuum={self.auto_vacuum}")
        self.db.isolation_level = None
        self.db.execute(f"PRAGMA journal_mode={self.journal_mode}")

        # Set synchronous mode to NORMAL for WAL mode by default if not specified by write concern
        # This provides a good balance of performance and safety in WAL mode
        if self.journal_mode == "WAL":
            self.db.execute("PRAGMA synchronous = NORMAL")

        if hasattr(self, "_write_concern") and self._write_concern:
            self._apply_write_concern(self._write_concern)

        if self._tokenizers:
            self.db.enable_load_extension(True)
            for name, path in self._tokenizers:
                self.db.execute(f"SELECT load_extension('{path}')")

        self._check_and_migrate_autovacuum(*args, **kwargs)

    def _check_and_migrate_autovacuum(self, *args: Any, **kwargs: Any) -> None:
        """
        Check auto_vacuum setting and migrate if needed.
        Called after initial connection is established.
        """
        if self._is_clone or self._db_path == ":memory:":
            return

        if not needs_migration(self.db, self.auto_vacuum):
            return

        if not should_migrate():
            return

        self._migrate_to_autovacuum(*args, **kwargs)

    def _migrate_to_autovacuum(self, *args: Any, **kwargs: Any) -> None:
        """
        Migrate database to a new auto_vacuum mode.

        Delegates to migration.migrate_autovacuum() and then reconnects.
        """
        old_path = self._db_path

        try:
            self.db.execute("PRAGMA wal_checkpoint(FULL)")
        except sqlite3.OperationalError as e:
            logger.warning(
                f"WAL checkpoint skipped (may be no WAL or already checkpointing): {e}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error during WAL checkpoint: {e}", exc_info=True
            )

        try:
            self.db.execute("COMMIT")
        except sqlite3.OperationalError as e:
            logger.debug(f"Commit skipped (no active transaction): {e}")
        except Exception as e:
            logger.error(f"Unexpected error during commit: {e}", exc_info=True)

        try:
            self.db.close()
        except Exception as e:
            logger.error(
                f"Unexpected error closing database: {e}", exc_info=True
            )

        migrate_autovacuum(
            db_path=old_path,
            target_autovacuum=self.auto_vacuum,
            target_journal_mode=self.journal_mode,
            extra_conn_kwargs=kwargs,
        )

        self.db = sqlite3.connect(*args, **kwargs)
        self.db.execute(f"PRAGMA auto_vacuum={self.auto_vacuum}")
        self.db.isolation_level = None
        self.db.execute(f"PRAGMA journal_mode={self.journal_mode}")

        # Set synchronous mode to NORMAL for WAL mode by default if not specified by write concern
        # This provides a good balance of performance and safety in WAL mode
        if self.journal_mode == "WAL":
            self.db.execute("PRAGMA synchronous = NORMAL")

        if hasattr(self, "_write_concern") and self._write_concern:
            self._apply_write_concern(self._write_concern)

        if self._tokenizers:
            self.db.enable_load_extension(True)
            for name, path in self._tokenizers:
                self.db.execute(f"SELECT load_extension('{path}')")

    def cleanup(self) -> None:
        """Clean up all collection resources associated with this connection."""
        if hasattr(self, "_collections"):
            for collection in self._collections.values():
                collection.cleanup()

    def close(self) -> None:
        """
        Close the database connection.

        Commits any pending transaction and properly closes the underlying SQLite
        connection. This method ensures resources are released and the connection
        is no longer usable after being called.
        """
        if getattr(self, "_is_clone", False) or getattr(self, "_closed", False):
            return

        # Clean up collections before closing the database
        self.cleanup()

        if self.db is not None:
            try:
                if self.db.in_transaction:
                    logger.warning(
                        "Closing connection with pending transaction; committing automatically"
                    )
                    self.db.commit()
            except (sqlite3.ProgrammingError, sqlite3.OperationalError) as e:
                logger.debug(f"{e=}")
                pass

            try:
                self.db.close()
            except (sqlite3.ProgrammingError, sqlite3.OperationalError) as e:
                logger.debug(f"{e=}")
                pass

        self._closed = True

    @property
    def client(self) -> Connection:
        """
        Get the MongoClient instance (returns self for NeoSQLite).

        Returns:
            Connection: The connection instance itself.
        """
        return self

    @property
    def codec_options(self) -> Any:
        """
        Get the codec options for this connection.

        Returns:
            Any: The codec options.
        """
        return self._codec_options

    @property
    def read_preference(self) -> Any:
        """
        Get the read preference for this connection.

        Returns:
            Any: The read preference.
        """
        return self._read_preference

    @property
    def write_concern(self) -> Any:
        """
        Get the write concern for this connection.

        Returns:
            Any: The write concern.
        """
        return self._write_concern

    @property
    def read_concern(self) -> Any:
        """
        Get the read concern for this connection.

        Returns:
            Any: The read concern.
        """
        return self._read_concern

    def __getitem__(self, name: str) -> Collection:
        """
        Access a collection by name.

        Allows retrieving or creating a collection associated with this connection
        using dictionary-style access. If the collection does not exist, it will be
        created automatically.

        Args:
            name (str): The name of the collection to access.

        Returns:
            Collection: The collection instance associated with the given name.
        """
        if name not in self._collections:
            self._collections[name] = Collection(self.db, name, database=self)
        return self._collections[name]

    def __getattr__(self, name: str) -> Any:
        """
        Proxy attribute access to collection lookup.

        When an attribute is not found in the instance's dictionary, this method
        attempts to retrieve it using the dictionary-style collection access (via
        `__getitem__`). This enables both attribute and dictionary access to collections.

        Returns:
            Any: The value retrieved from the collection, or the attribute if it exists.
        """
        if name in self.__dict__:
            return self.__dict__[name]
        return self[name]

    def start_session(
        self,
        causal_consistency: bool | None = None,
        default_transaction_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> ClientSession:
        """
        Start a new client session for transactions.

        This method provides PyMongo-compatible session management by wrapping
        SQLite's native ACID transactions.

        Args:
            causal_consistency (bool, optional): Whether to enable causal consistency.
                Ignored in NeoSQLite (stored for API compatibility).
            default_transaction_options (dict, optional): Default transaction options.
            **kwargs: Additional session arguments.

        Returns:
            ClientSession: A new ClientSession instance.
        """
        options = kwargs.copy()
        if causal_consistency is not None:
            options["causal_consistency"] = causal_consistency
        if default_transaction_options:
            options["default_transaction_options"] = default_transaction_options

        return ClientSession(self, options=options)

    def __enter__(self) -> Connection:
        """
        Allow the connection to be used in a context manager.

        Returns:
            Connection: The connection instance itself, enabling the 'with' statement
                        to manage the connection's lifecycle.
        """
        return self

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_traceback: Any
    ) -> Literal[False]:
        """
        Ensure the connection is properly closed when exiting a context manager.

        Returns:
            Literal[False]: Indicates that the method does not handle exceptions
                            and the connection should be closed.
        """
        self.close()
        return False

    def drop_collection(self, name: str) -> None:
        """
        Drop a collection (table) from the database.

        Args:
            name (str): The name of the collection (table) to drop. If the table
                        does not exist, the operation is silently ignored due to
                        the use of `IF EXISTS` in the SQL command.
        """
        self.db.execute(f"DROP TABLE IF EXISTS {quote_table_name(name)}")

    def create_collection(self, name: str, **kwargs) -> Collection:
        """
        Create a new collection with specific options.

        Args:
            name (str): The name of the collection to create.
            **kwargs: Additional options for collection creation.

        Returns:
            Collection: The newly created collection.

        Raises:
            CollectionInvalid: If a collection with the given name already exists.
        """
        if name in self._collections:
            raise CollectionInvalid(f"Collection {name} already exists")
        collection = Collection(
            self.db, name, create=True, database=self, **kwargs
        )
        self._collections[name] = collection
        return collection

    def get_collection(self, name: str, **kwargs) -> Collection:
        """
        Get a collection by name.

        Args:
            name (str): The name of the collection to get.
            **kwargs: Additional options for collection access.

        Returns:
            Collection: The collection instance.
        """
        if name not in self._collections:
            self._collections[name] = Collection(
                self.db, name, create=False, database=self, **kwargs
            )
        return self._collections[name]

    def rename_collection(self, old_name: str, new_name: str) -> None:
        """
        Rename a collection.

        Args:
            old_name (str): The current name of the collection.
            new_name (str): The new name for the collection.

        Raises:
            CollectionInvalid: If the old collection doesn't exist or new name already exists.
        """
        if old_name not in self._collections:
            raise CollectionInvalid(f"Collection {old_name} does not exist")

        # Check if new name already exists
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (new_name,),
        )
        if cursor.fetchone():
            raise CollectionInvalid(f"Collection {new_name} already exists")

        # Rename the collection
        self._collections[old_name].rename(new_name)
        self._collections[new_name] = self._collections.pop(old_name)

    def list_collection_names(self) -> list[str]:
        """
        List all collection names in the database.

        Returns:
            list[str]: A list of all collection names in the database,
            excluding internal SQLite tables (sqlite_sequence, sqlite_stat*, etc.)
        """
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [row[0] for row in cursor.fetchall()]

    def list_collections(self) -> list[dict[str, Any]]:
        """
        Get detailed information about collections in the database.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing collection information.
                                Each dictionary has 'name' and 'options' keys.
        """
        cursor = self.db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [
            {"name": row[0], "options": row[1]} for row in cursor.fetchall()
        ]

    def command(
        self, command: str | dict[str, Any], value: Any = None, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Issue a database command and return the response.

        This method provides PyMongo-compatible command execution for SQLite.
        It supports various commands including PRAGMA commands, introspection,
        and utility commands.

        Args:
            command: The command to execute. Can be:
                    - A string (e.g., "table_info", "integrity_check")
                    - A dict with command name as key (e.g., {"ping": 1})
            value: Optional command value (for string commands)
            **kwargs: Additional command arguments

        Returns:
            dict[str, Any]: Command response

        Supported Commands:
            - "ping" or {"ping": 1} - Returns {"ok": 1}
            - "serverStatus" - Returns SQLite version info
            - "listCollections" - Returns collection list
            - "table_info" - Returns table schema (PRAGMA table_info)
            - "integrity_check" - Returns integrity check results
            - "validate" - Returns integrity check for a collection
            - "reIndex" - Rebuilds indexes for a collection
            - "foreign_key_check" - Returns foreign key check results
            - "index_list" - Returns index list for a table
            - "vacuum" - Runs full VACUUM command
            - "compact" - MongoDB-compatible compact (see below)
            - "wal_checkpoint" - WAL checkpoint control (NeoSQLite extension)
            - "cache_size" - Get/set cache size (NeoSQLite extension)
            - "busy_timeout" - Get/set busy timeout (NeoSQLite extension)
            - "analyze" - Runs ANALYZE command

        compact Command:
            MongoDB-compatible compact implementation with NeoSQLite extensions.

            Args:
                collection: Collection name (ignored, operates on entire database)
                dryRun: If true, returns estimate without actually compacting
                freeSpaceTargetMB: Target in MB (default: 20). Only compacts if
                    free space >= threshold. Uses incremental vacuum in batches.
                    Set to 0 for full VACUUM instead of incremental.
                comment: Optional comment (ignored, for MongoDB compatibility)

            Returns:
                dryRun: {"estimatedBytesFreed": <bytes>, "ok": 1}
                compact: {"bytesFreed": <bytes>, "ok": 1}

            Examples:
                >>> db.command("compact", "collection")  # 20MB threshold + incremental
                >>> db.command("compact", "collection", dryRun=True)  # Estimate only
                >>> db.command("compact", "collection", freeSpaceTargetMB=10)  # 10MB threshold + incremental
                >>> db.command("compact", "collection", freeSpaceTargetMB=0)  # Full vacuum

        Example:
            >>> db = Connection("test.db")
            >>> result = db.command("ping")
            >>> print(result)
            {'ok': 1.0}
        """
        # If command is a string and value is provided, it's equivalent to {command: value}
        if isinstance(command, str) and value is not None:
            command = {command: value}

        # Handle string commands
        if isinstance(command, str):
            cmd_name = command.lower()
        elif isinstance(command, dict):
            # Handle dict commands (PyMongo style)
            cmd_name = next(iter(command.keys())).lower()
        else:
            raise TypeError("command must be a string or dict")

        try:
            match cmd_name:
                case "ping":
                    return {"ok": 1}

                case "serverstatus":
                    import sqlite3

                    return {
                        "ok": 1,
                        "version": sqlite3.sqlite_version,
                        "python_sqlite_version": getattr(
                            sqlite3, "version", "unknown"
                        ),
                        "process": "neosqlite",
                        "pid": 1,
                    }

                case "listcollections":
                    collections = self.list_collection_names()
                    return {
                        "ok": 1,
                        "collections": [{"name": name} for name in collections],
                    }

                case "table_info":
                    table_name = kwargs.get("table")
                    if not table_name and isinstance(command, dict):
                        table_name = command.get("table_info")
                    if not table_name:
                        raise ValueError(
                            "table_info requires 'table' parameter"
                        )
                    cursor = self.db.execute(
                        f"PRAGMA table_info({quote_table_name(table_name)})"
                    )
                    columns = [
                        {
                            "cid": row[0],
                            "name": row[1],
                            "type": row[2],
                            "notnull": bool(row[3]),
                            "default": row[4],
                            "pk": bool(row[5]),
                        }
                        for row in cursor.fetchall()
                    ]
                    return {"ok": 1, "columns": columns}

                case "integrity_check":
                    cursor = self.db.execute("PRAGMA integrity_check")
                    result = cursor.fetchall()
                    return {"ok": 1, "result": [row[0] for row in result]}

                case "validate":
                    collection_name = kwargs.get("validate")
                    if not collection_name and isinstance(command, dict):
                        collection_name = command.get("validate")

                    if collection_name:
                        cursor = self.db.execute(
                            f"PRAGMA integrity_check({quote_table_name(collection_name)})"
                        )
                    else:
                        cursor = self.db.execute("PRAGMA integrity_check")

                    result = cursor.fetchall()
                    errors = [row[0] for row in result if row[0] != "ok"]

                    return {
                        "ok": 1 if not errors else 0,
                        "result": [row[0] for row in result],
                        "errors": errors,
                        "valid": len(errors) == 0,
                    }

                case "foreign_key_check":
                    table_name = kwargs.get("table")
                    if table_name:
                        cursor = self.db.execute(
                            f"PRAGMA foreign_key_check({quote_table_name(table_name)})"
                        )
                    else:
                        cursor = self.db.execute("PRAGMA foreign_key_check")
                    result = cursor.fetchall()
                    return {
                        "ok": 1,
                        "violations": [
                            {
                                "table": row[0],
                                "rowid": row[1],
                                "parent": row[2],
                                "fkid": row[3],
                            }
                            for row in result
                        ],
                    }

                case "index_list":
                    table_name = kwargs.get("table")
                    if not table_name and isinstance(command, dict):
                        table_name = command.get("index_list")
                    if not table_name:
                        raise ValueError(
                            "index_list requires 'table' parameter"
                        )
                    cursor = self.db.execute(
                        f"PRAGMA index_list({quote_table_name(table_name)})"
                    )
                    indexes = [
                        {
                            "seq": row[0],
                            "name": row[1],
                            "unique": bool(row[2]),
                            "origin": row[3] if len(row) > 3 else "c",
                            "partial": bool(row[4]) if len(row) > 4 else False,
                        }
                        for row in cursor.fetchall()
                    ]
                    return {"ok": 1, "indexes": indexes}

                case "vacuum":
                    self.db.execute("VACUUM")
                    return {"ok": 1, "message": "VACUUM completed"}

                case "compact":
                    _ = kwargs.get("compact")
                    _ = kwargs.get("comment")
                    dry_run = kwargs.get("dryRun", False)
                    free_space_target_mb = kwargs.get("freeSpaceTargetMB")

                    free_pages = self.db.execute(
                        "PRAGMA freelist_count"
                    ).fetchone()[0]
                    page_size = self.db.execute("PRAGMA page_size").fetchone()[
                        0
                    ]
                    free_bytes = free_pages * page_size

                    if dry_run:
                        return {
                            "estimatedBytesFreed": free_bytes,
                            "ok": 1,
                        }

                    if free_space_target_mb == 0:
                        page_count_before = self.db.execute(
                            "PRAGMA page_count"
                        ).fetchone()[0]
                        self.db.execute("VACUUM")
                        page_count_after = self.db.execute(
                            "PRAGMA page_count"
                        ).fetchone()[0]
                        bytes_freed = (
                            page_count_before - page_count_after
                        ) * page_size
                        return {"bytesFreed": bytes_freed, "ok": 1}

                    if free_space_target_mb is None:
                        free_space_target_mb = 20

                    target_bytes = free_space_target_mb * 1024 * 1024
                    if free_bytes < target_bytes:
                        return {"bytesFreed": 0, "ok": 1}

                    page_count_before = self.db.execute(
                        "PRAGMA page_count"
                    ).fetchone()[0]

                    target_pages = max(1, target_bytes // page_size)

                    while free_pages > 0:
                        reclaim = min(target_pages, free_pages)
                        self.db.execute(f"PRAGMA incremental_vacuum({reclaim})")
                        free_pages = self.db.execute(
                            "PRAGMA freelist_count"
                        ).fetchone()[0]

                    page_count_after = self.db.execute(
                        "PRAGMA page_count"
                    ).fetchone()[0]

                    bytes_freed = (
                        page_count_before - page_count_after
                    ) * page_size
                    return {"bytesFreed": bytes_freed, "ok": 1}

                case "wal_checkpoint":
                    mode = kwargs.get("mode", "PASSIVE")
                    result = self.db.execute(
                        f"PRAGMA wal_checkpoint({mode})"
                    ).fetchone()
                    return {
                        "ok": 1,
                        "mode": mode,
                        "busy": result[0],
                        "log": result[1],
                        "checkpointed": result[2],
                    }

                case "cache_size":
                    pages = kwargs.get("pages")
                    if pages is not None:
                        self.db.execute(f"PRAGMA cache_size = {pages}")
                        return {
                            "ok": 1,
                            "message": f"cache_size set to {pages}",
                        }
                    else:
                        current = self.db.execute(
                            "PRAGMA cache_size"
                        ).fetchone()[0]
                        return {"ok": 1, "cache_size": current}

                case "busy_timeout":
                    ms = kwargs.get("milliseconds")
                    if ms is not None:
                        self.db.execute(f"PRAGMA busy_timeout = {ms}")
                        return {
                            "ok": 1,
                            "message": f"busy_timeout set to {ms}ms",
                        }
                    else:
                        current = self.db.execute(
                            "PRAGMA busy_timeout"
                        ).fetchone()[0]
                        return {"ok": 1, "busy_timeout": current}

                case "analyze":
                    self.db.execute("ANALYZE")
                    return {"ok": 1, "message": "ANALYZE completed"}

                case "reindex":
                    collection_name = kwargs.get("reIndex")
                    if not collection_name and isinstance(command, dict):
                        collection_name = command.get("reindex")

                    if collection_name:
                        self.db.execute(
                            f"REINDEX {quote_table_name(collection_name)}"
                        )
                    else:
                        self.db.execute("REINDEX")

                    return {"ok": 1, "message": "REINDEX completed"}

                case "collstats":
                    collection_name = kwargs.get("collection")
                    if not collection_name and isinstance(command, dict):
                        collection_name = command.get("collstats")
                    if not collection_name:
                        raise ValueError(
                            "collstats requires 'collection' parameter"
                        )
                    quoted_table = quote_table_name(collection_name)

                    cursor = self.db.execute(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                        (collection_name,),
                    )
                    if not cursor.fetchone():
                        return {
                            "ok": 1,
                            "ns": collection_name,
                            "count": 0,
                            "size": 0,
                            "avgObjSize": 0,
                            "storageSize": 0,
                            "totalIndexSize": 0,
                            "indexSizes": {},
                        }

                    cursor = self.db.execute(
                        f"SELECT COUNT(*) FROM {quoted_table}"
                    )
                    count = cursor.fetchone()[0] or 0

                    size = 0
                    try:
                        size_cursor = self.db.execute(
                            f"SELECT SUM(LENGTH(data)) FROM {quoted_table}"
                        )
                        size = size_cursor.fetchone()[0] or 0
                    except Exception as e:
                        logger.debug(
                            f"Failed to calculate collection size: {e}"
                        )
                        pass

                    avg_obj_size = size / count if count > 0 else 0

                    storage_size = 0
                    total_index_size = 0
                    index_sizes: dict = {}

                    try:
                        self.db.execute(
                            "CREATE VIRTUAL TABLE IF NOT EXISTS temp.dbstat USING dbstat(main)"
                        )

                        storage_cursor = self.db.execute(
                            "SELECT SUM(pgsize) FROM dbstat WHERE name = ?",
                            (collection_name,),
                        )
                        storage_size = storage_cursor.fetchone()[0] or 0

                        index_cursor = self.db.execute(
                            "SELECT name, SUM(pgsize) as size FROM dbstat "
                            "WHERE tbl_name = ? AND type = 'index' GROUP BY name",
                            (collection_name,),
                        )
                        for row in index_cursor.fetchall():
                            idx_name, idx_size = row
                            if idx_name and idx_size:
                                index_sizes[idx_name] = idx_size
                                total_index_size += idx_size
                    except Exception as e:
                        logger.debug(f"{e=}")
                        pass

                    return {
                        "ok": 1,
                        "ns": collection_name,
                        "count": count,
                        "size": size,
                        "avgObjSize": avg_obj_size,
                        "storageSize": storage_size,
                        "totalIndexSize": total_index_size,
                        "indexSizes": index_sizes,
                    }

                case "dbstats":
                    page_count = self.db.execute(
                        "PRAGMA page_count"
                    ).fetchone()[0]
                    page_size = self.db.execute("PRAGMA page_size").fetchone()[
                        0
                    ]

                    collections = self.list_collection_names()
                    total_objects = 0
                    total_indexes = 0

                    for coll_name in collections:
                        cursor = self.db.execute(
                            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                            (coll_name,),
                        )
                        if not (row := cursor.fetchone()):
                            continue
                        sql = row[0] or ""

                        if (
                            "VIRTUAL TABLE" in sql.upper()
                            and "fts" in sql.lower()
                        ):
                            continue

                        try:
                            count = self.db.execute(
                                f"SELECT COUNT(*) FROM {quote_table_name(coll_name)}"
                            ).fetchone()[0]
                            total_objects += count
                        except Exception as e:
                            logger.debug(
                                f"Failed to count documents in '{coll_name}': {e}"
                            )
                            pass

                        try:
                            indexes = self.db.execute(
                                f"PRAGMA index_list({quote_table_name(coll_name)})"
                            ).fetchall()
                            total_indexes += len(indexes)
                        except Exception as e:
                            logger.debug(
                                f"Failed to get indexes for '{coll_name}': {e}"
                            )
                            pass

                    storage_size = page_count * page_size

                    try:
                        self.db.execute(
                            "CREATE VIRTUAL TABLE IF NOT EXISTS temp.dbstat USING dbstat(main)"
                        )
                        cursor = self.db.execute(
                            "SELECT SUM(pgsize) FROM dbstat WHERE name LIKE 'idx_%' OR name LIKE 'sqlite_autoindex%'"
                        )
                        index_size = cursor.fetchone()[0] or 0
                    except Exception as e:
                        logger.debug(
                            f"Failed to calculate index size, using estimate: {e}"
                        )
                        index_size = int(storage_size * 0.2)

                    data_size = storage_size - index_size

                    views_count = self.db.execute(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='view'"
                    ).fetchone()[0]

                    import os
                    import shutil

                    fs_total = 0
                    fs_used = 0
                    db_file_size = 0

                    if self._db_path and self._db_path != ":memory:":
                        try:
                            db_dir = os.path.dirname(self._db_path) or "."
                            fs_usage = shutil.disk_usage(db_dir)
                            fs_total = fs_usage.total
                            fs_used = fs_usage.used
                            db_file_size = os.path.getsize(self._db_path)
                            wal_path = self._db_path + "-wal"
                            if os.path.exists(wal_path):
                                db_file_size += os.path.getsize(wal_path)
                            shm_path = self._db_path + "-shm"
                            if os.path.exists(shm_path):
                                db_file_size += os.path.getsize(shm_path)
                        except OSError as e:
                            logger.debug(f"{e=}")
                            pass

                    return {
                        "ok": 1,
                        "db": self.name,
                        "collections": len(collections),
                        "views": views_count,
                        "objects": total_objects,
                        "avgObjSize": (
                            int(data_size / total_objects)
                            if total_objects
                            else 0
                        ),
                        "dataSize": data_size,
                        "storageSize": storage_size,
                        "indexes": total_indexes,
                        "indexSize": index_size,
                        "totalSize": data_size + index_size,
                        "fsTotalSize": fs_total,
                        "fsUsedSize": fs_used,
                        "scaleFactor": 1,
                    }

                case "aggregate":
                    collection_name = kwargs.get("aggregate")
                    if not collection_name and isinstance(command, dict):
                        collection_name = command.get("aggregate")
                    if not collection_name:
                        raise ValueError(
                            "aggregate requires 'aggregate' parameter"
                        )

                    pipeline = kwargs.get("pipeline", [])
                    explain = kwargs.get("explain", False)
                    kwargs.get("allowDiskUse", False)

                    if explain:
                        collection = self[collection_name]
                        return collection.query_engine.explain_aggregation(
                            pipeline, session=None
                        )
                    else:
                        collection = self[collection_name]
                        cursor_result = collection.aggregate(pipeline)
                        return {"ok": 1, "result": list(cursor_result)}

                case _:
                    try:
                        cursor = self.db.execute(f"PRAGMA {cmd_name}")
                        result = cursor.fetchall()
                        return {
                            "ok": 1,
                            "result": [
                                dict(
                                    zip([d[0] for d in cursor.description], row)
                                )
                                for row in result
                            ],
                        }
                    except Exception as e:
                        logger.debug(f"{e=}")
                        return {
                            "ok": 0,
                            "errmsg": f"Unknown command: {cmd_name}",
                            "error": str(e),
                        }

        except Exception as e:
            logger.debug(f"{e=}")
            return {"ok": 0, "errmsg": str(e), "code": 1}

    def cursor_command(
        self, command: str | dict[str, Any], value: Any = None, **kwargs: Any
    ) -> AggregationCursor:
        """
        Execute a database command and return a cursor for its results.

        This method provides PyMongo-compatible cursor-based command execution.
        It wraps the results of `command()` in an `AggregationCursor`, allowing
        them to be iterated.

        Args:
            command: The command to execute.
            value: Optional command value.
            **kwargs: Additional command arguments.

        Returns:
            AggregationCursor: A cursor over the command results.
        """
        result = self.command(command, value=value, **kwargs)

        match result:
            case {"collections": _}:
                items = result["collections"]
            case {"columns": _}:
                items = result["columns"]
            case {"result": _} as r:
                items = r["result"]
                if not isinstance(items, list):
                    items = [items]
            case {"cursor": {"firstBatch": _}}:
                items = result["cursor"]["firstBatch"]
            case _:
                items = [result]

        # Use a safe dummy collection name for AggregationCursor
        # We don't want to use existing collection names because some might be reserved (e.g. sqlite_sequence)
        collection = self["__command_results__"]

        # Create an AggregationCursor with the pre-computed results
        cursor = AggregationCursor(collection, [])
        cursor._results = items
        cursor._executed = True
        return cursor

    def dereference(self, dbref: dict[str, Any]) -> dict[str, Any] | None:
        """
        Resolve a DBRef object.

        This method provides PyMongo-compatible DBRef resolution by performing
        a `find_one` on the target collection using the provided `$id`.

        Args:
            dbref: A dictionary representing a DBRef with '$ref' and '$id' keys.

        Returns:
            dict[str, Any] | None: The referenced document, or None if not found.
        """
        if not isinstance(dbref, dict):
            return None

        collection_name = dbref.get("$ref")
        document_id = dbref.get("$id")

        if not collection_name or document_id is None:
            return None

        return self[collection_name].find_one({"_id": document_id})

    def with_options(
        self,
        codec_options: Any | None = None,
        read_preference: Any | None = None,
        write_concern: Any | None = None,
        read_concern: Any | None = None,
    ) -> Connection:
        """
        Get a clone of this database with different options.

        This method returns a new Connection instance with the specified options.
        For PyMongo API compatibility, the options are stored but not actively
        used since SQLite has different semantics.

        Args:
            codec_options (Any, optional): Codec options for encoding/decoding.
                Ignored in NeoSQLite (stored for API compatibility).
            read_preference (Any, optional): Read preference for replica sets.
                Ignored in NeoSQLite (stored for API compatibility).
            write_concern (Any, optional): Write concern for durability settings.
                Stored for API compatibility.
            read_concern (Any, optional): Read concern for consistency settings.
                Ignored in NeoSQLite (stored for API compatibility).

        Returns:
            Connection: A new Connection instance with the same underlying database
                       but with the specified options stored.

        Note:
            NeoSQLite stores these options for PyMongo API compatibility, but
            they don't affect SQLite behavior since SQLite doesn't have replica
            sets, codec options, or the same consistency/durability model.

        Example:
            >>> db = Connection("test.db")
            >>> db_with_options = db.with_options(
            ...     write_concern={"w": "majority"},
            ...     read_preference={"mode": "primaryPreferred"}
            ... )
        """
        # Return a new Connection instance that shares the same database connection.
        # This is by design: SQLite uses a single writable connection per database file,
        # so clones naturally share the same sqlite3.Connection and _collections dict.
        # The clone only differs in its stored PyMongo-compatible options
        # (codec_options, read_preference, write_concern, read_concern).
        clone = Connection(name=self.name, _is_clone=True)
        clone.db = self.db
        clone._tokenizers = self._tokenizers
        clone.debug = self.debug
        clone._collections = self._collections
        clone.journal_mode = self.journal_mode

        # Store the new options
        clone._codec_options = codec_options
        clone._read_preference = read_preference
        clone._write_concern = write_concern
        clone._read_concern = read_concern

        # NOTE: We intentionally do NOT call _apply_write_concern() here.
        # The clone shares the same sqlite3.Connection with the original, so
        # changing PRAGMAs would affect the original too. The write_concern
        # is stored for API compatibility but does not mutate the shared connection.

        return clone

    def _apply_write_concern(self, write_concern: Any) -> None:
        """
        Apply write concern to the underlying SQLite connection via PRAGMAs.

        - w: 0 -> PRAGMA synchronous = OFF
        - w: 1 -> PRAGMA synchronous = NORMAL
        - j: True -> PRAGMA synchronous = FULL

        Args:
            write_concern (Any): The write concern to apply (dict or WriteConcern object).
        """
        if not write_concern:
            return

        match write_concern:
            case WriteConcern():
                wc_doc = write_concern.document
            case dict():
                wc_doc = write_concern
            case _:
                return

        w = wc_doc.get("w")
        j = wc_doc.get("j")

        try:
            if w == 0:
                self.db.execute("PRAGMA synchronous = OFF")
            elif w == 1:
                self.db.execute("PRAGMA synchronous = NORMAL")

            if j is True:
                self.db.execute("PRAGMA synchronous = FULL")
        except Exception as e:
            logger.warning(f"Error applying write concern: {e}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Context manager for handling database transactions.

        Ensures atomicity by beginning a transaction on entry, committing on
        successful exit, and rolling back in case of exceptions. This allows
        using the connection in a 'with' statement to manage transaction
        boundaries safely.

        Yields control to the block, and automatically commits or rolls back
        based on execution outcome.
        """
        try:
            self.db.execute("BEGIN")
            yield
            self.db.commit()
        except Exception as e:
            logger.debug(f"Transaction failed, rolling back: {e}")
            self.db.rollback()
            raise

    def __del__(self):
        """
        Ensure the database connection is closed when the object is garbage collected.
        Wrapped in try/except to avoid crashes during interpreter shutdown when
        module-level imports (like sqlite3) may already be None.
        """
        try:
            self.close()
        except Exception as e:
            if logger is not None:
                logger.debug(f"{e=}")
            pass
