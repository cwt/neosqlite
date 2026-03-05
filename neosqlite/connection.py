from __future__ import annotations
from .collection import Collection
from .exceptions import CollectionInvalid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Tuple
from typing_extensions import Literal

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


class Connection:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize a new database connection.

        Sets up the connection to an SQLite database, initializes the collections
        dictionary, and establishes the underlying database connection using the
        provided arguments. This constructor does not return a value.

        Args:
            *args: Positional arguments passed to sqlite3.connect().
            **kwargs: Keyword arguments passed to sqlite3.connect().
                     Special kwargs:
                     - tokenizers: List of tuples (name, path) for FTS5 tokenizers to load
                     - debug: Boolean flag to enable debug printing
                     - name: Optional name for the database (for PyMongo API compatibility)
        """
        self._collections: Dict[str, Collection] = {}
        self._tokenizers: List[Tuple[str, str]] = kwargs.pop("tokenizers", [])
        self.debug: bool = kwargs.pop("debug", False)
        # Extract database name from args or kwargs for PyMongo API compatibility
        self.name: str = kwargs.pop("name", None)
        if self.name is None and args:
            # Use the database file path as the name
            db_path = args[0] if args else ":memory:"
            self.name = db_path if db_path != ":memory:" else "memory"
        self.connect(*args, **kwargs)

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
        self.db.isolation_level = None
        self.db.execute("PRAGMA journal_mode=WAL")

        # Enable extension loading and load custom FTS5 tokenizers if provided
        if self._tokenizers:
            self.db.enable_load_extension(True)
            for name, path in self._tokenizers:
                self.db.execute(f"SELECT load_extension('{path}')")

    def close(self) -> None:
        """
        Close the database connection.

        Commits any pending transaction and properly closes the underlying SQLite
        connection. This method ensures resources are released and the connection
        is no longer usable after being called.
        """
        if self.db is not None:
            if self.db.in_transaction:
                self.db.commit()
            self.db.close()

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
        self.db.execute(f"DROP TABLE IF EXISTS {name}")

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

    def list_collection_names(self) -> List[str]:
        """
        List all collection names in the database.

        Returns:
            List[str]: A list of all collection names in the database.
        """
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        return [row[0] for row in cursor.fetchall()]

    def list_collections(self) -> List[Dict[str, Any]]:
        """
        Get detailed information about collections in the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing collection information.
                                Each dictionary has 'name' and 'options' keys.
        """
        cursor = self.db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table'"
        )
        return [
            {"name": row[0], "options": row[1]} for row in cursor.fetchall()
        ]

    def command(
        self, command: str | Dict[str, Any], **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Issue a database command and return the response.

        This method provides PyMongo-compatible command execution for SQLite.
        It supports various commands including PRAGMA commands, introspection,
        and utility commands.

        Args:
            command: The command to execute. Can be:
                    - A string (e.g., "table_info", "integrity_check")
                    - A dict with command name as key (e.g., {"ping": 1})
            **kwargs: Additional command arguments

        Returns:
            Dict[str, Any]: Command response

        Supported Commands:
            - "ping" or {"ping": 1} - Returns {"ok": 1.0}
            - "serverStatus" - Returns SQLite version info
            - "listCollections" - Returns collection list
            - "table_info" - Returns table schema (PRAGMA table_info)
            - "integrity_check" - Returns integrity check results
            - "foreign_key_check" - Returns foreign key check results
            - "index_list" - Returns index list for a table
            - "vacuum" - Runs VACUUM command
            - "analyze" - Runs ANALYZE command

        Example:
            >>> db = Connection("test.db")
            >>> result = db.command("ping")
            >>> print(result)
            {'ok': 1.0}
        """
        # Handle string commands
        if isinstance(command, str):
            cmd_name = command.lower()
        elif isinstance(command, dict):
            # Handle dict commands (PyMongo style)
            cmd_name = next(iter(command.keys())).lower()
        else:
            raise TypeError("command must be a string or dict")

        try:
            # Handle specific commands
            if cmd_name == "ping":
                return {"ok": 1.0}

            elif cmd_name == "serverstatus":
                import sqlite3

                return {
                    "ok": 1.0,
                    "version": sqlite3.sqlite_version,
                    "python_sqlite_version": getattr(
                        sqlite3, "version", "unknown"
                    ),
                    "process": "neosqlite",
                    "pid": 1,  # SQLite is embedded, no separate process
                }

            elif cmd_name == "listcollections":
                collections = self.list_collection_names()
                return {
                    "ok": 1.0,
                    "collections": [{"name": name} for name in collections],
                }

            elif cmd_name == "table_info":
                table_name = kwargs.get("table")
                if not table_name and isinstance(command, dict):
                    table_name = command.get("table_info")
                if not table_name:
                    raise ValueError("table_info requires 'table' parameter")
                cursor = self.db.execute(f"PRAGMA table_info({table_name})")
                columns = []
                for row in cursor.fetchall():
                    columns.append(
                        {
                            "cid": row[0],
                            "name": row[1],
                            "type": row[2],
                            "notnull": bool(row[3]),
                            "default": row[4],
                            "pk": bool(row[5]),
                        }
                    )
                return {"ok": 1.0, "columns": columns}

            elif cmd_name == "integrity_check":
                cursor = self.db.execute("PRAGMA integrity_check")
                result = cursor.fetchall()
                return {"ok": 1.0, "result": [row[0] for row in result]}

            elif cmd_name == "validate":
                # MongoDB-compatible validate command
                # In MongoDB: db.runCommand({validate: "collectionName"})
                # For NeoSQLite: Use SQLite PRAGMA integrity_check
                collection_name = kwargs.get("validate")
                if not collection_name and isinstance(command, dict):
                    collection_name = command.get("validate")

                if collection_name:
                    # Validate specific collection (table)
                    cursor = self.db.execute(
                        f"PRAGMA integrity_check({collection_name})"
                    )
                else:
                    # Validate entire database
                    cursor = self.db.execute("PRAGMA integrity_check")

                result = cursor.fetchall()
                errors = [row[0] for row in result if row[0] != "ok"]

                return {
                    "ok": 1.0 if not errors else 0.0,
                    "result": [row[0] for row in result],
                    "errors": errors,
                    "valid": len(errors) == 0,
                }

            elif cmd_name == "foreign_key_check":
                table_name = kwargs.get("table")
                if table_name:
                    cursor = self.db.execute(
                        f"PRAGMA foreign_key_check({table_name})"
                    )
                else:
                    cursor = self.db.execute("PRAGMA foreign_key_check")
                result = cursor.fetchall()
                return {
                    "ok": 1.0,
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

            elif cmd_name == "index_list":
                table_name = kwargs.get("table")
                if not table_name and isinstance(command, dict):
                    table_name = command.get("index_list")
                if not table_name:
                    raise ValueError("index_list requires 'table' parameter")
                cursor = self.db.execute(f"PRAGMA index_list({table_name})")
                indexes = []
                for row in cursor.fetchall():
                    indexes.append(
                        {
                            "seq": row[0],
                            "name": row[1],
                            "unique": bool(row[2]),
                            "origin": row[3] if len(row) > 3 else "c",
                            "partial": bool(row[4]) if len(row) > 4 else False,
                        }
                    )
                return {"ok": 1.0, "indexes": indexes}

            elif cmd_name == "vacuum":
                self.db.execute("VACUUM")
                return {"ok": 1.0, "message": "VACUUM completed"}

            elif cmd_name == "analyze":
                self.db.execute("ANALYZE")
                return {"ok": 1.0, "message": "ANALYZE completed"}

            elif cmd_name == "collstats":
                # Collection statistics
                collection_name = kwargs.get("collection")
                if not collection_name and isinstance(command, dict):
                    collection_name = command.get("collstats")
                if not collection_name:
                    raise ValueError(
                        "collstats requires 'collection' parameter"
                    )
                cursor = self.db.execute(
                    f"SELECT COUNT(*) FROM {collection_name}"
                )
                count = cursor.fetchone()[0]
                return {
                    "ok": 1.0,
                    "ns": collection_name,
                    "count": count,
                    "size": 0,  # SQLite doesn't track this easily
                    "storageSize": 0,
                }

            else:
                # Try to execute as a PRAGMA command
                try:
                    cursor = self.db.execute(f"PRAGMA {cmd_name}")
                    result = cursor.fetchall()
                    return {
                        "ok": 1.0,
                        "result": [
                            dict(zip([d[0] for d in cursor.description], row))
                            for row in result
                        ],
                    }
                except Exception as e:
                    return {
                        "ok": 0,
                        "errmsg": f"Unknown command: {cmd_name}",
                        "error": str(e),
                    }

        except Exception as e:
            return {"ok": 0, "errmsg": str(e), "code": 1}

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
        # Return self for now - options are stored for API compatibility
        # but don't affect SQLite behavior
        self._codec_options = codec_options
        self._read_preference = read_preference
        self._write_concern = write_concern
        self._read_concern = read_concern
        return self

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
        except Exception:
            self.db.rollback()
            raise

    def __del__(self):
        """
        Ensure the database connection is closed when the object is garbage collected.

        This method attempts to close the database connection when the Connection
        object is being garbage collected. It checks if the connection exists and
        is not already closed before attempting to close it. Any exceptions during
        this process are caught and ignored to prevent crashes during garbage
        collection.
        """
        try:
            if hasattr(self, "db") and self.db is not None:
                # Only close if it's not already closed
                if not getattr(self.db, "closed", False):
                    self.db.close()
        except Exception:
            # Ignore exceptions in __del__ to avoid crashes during garbage collection
            pass
