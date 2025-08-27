from .collection import Collection
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Tuple
from typing_extensions import Literal

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


class Connection:
    def __init__(self, *args: Any, **kwargs: Any):
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
        """
        self._collections: Dict[str, "Collection"] = {}
        self._tokenizers: List[Tuple[str, str]] = kwargs.pop("tokenizers", [])
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any):
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

    def close(self):
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

    def __getitem__(self, name: str) -> "Collection":
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

    def __enter__(self) -> "Connection":
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

    def drop_collection(self, name: str):
        """
        Drop a collection (table) from the database.

        Args:
            name (str): The name of the collection (table) to drop. If the table
                        does not exist, the operation is silently ignored due to
                        the use of `IF EXISTS` in the SQL command.
        """
        self.db.execute(f"DROP TABLE IF EXISTS {name}")

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
