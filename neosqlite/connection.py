"""Database connection and command dispatch implementation for NeoSQLite."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shutil
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Literal, Self

from ._sqlite import sqlite3
from .client_session import ClientSession
from .collection import Collection, cleanup_dropped_collection
from .collection.aggregation_cursor import AggregationCursor
from .exceptions import CollectionInvalid
from .migration import migrate_autovacuum, needs_migration, should_migrate
from .options import AutoVacuumMode, JournalMode, WriteConcern
from .sql_utils import quote_table_name

logger = logging.getLogger(__name__)


def _is_internal_collection_name(name: str) -> bool:
    """Check if a table name is internal to SQLite or NeoSQLite.

    Detects SQLite system tables (``sqlite_*``), NeoSQLite metadata tables
    (``_neosqlite_*`` / ``neosqlite_*``), the ``__command_results__`` helper,
    and FTS5 virtual, shadow, or temporary text-search tables.

    Args:
        name: SQLite table name to inspect.

    Returns:
        bool: True if ``name`` is internal or invalid, False for user
            collections.
    """
    if not name or not isinstance(name, str):
        return True
    if name.startswith("sqlite_"):
        return True
    if name.startswith("_neosqlite_") or name.startswith("neosqlite_"):
        return True
    if name == "__command_results__":
        return True
    if name.endswith("_fts") or "_fts_" in name:
        return True
    if name.startswith("temp_text_fts_"):
        return True
    return False


class Connection:
    """Represent a connection to a NeoSQLite database.

    Provides PyMongo-compatible methods for accessing and managing
    collections, executing database commands, running transactions, and
    configuring SQLite connection pragmas and background TTL sweeps.
    """

    DEFAULT_TRANSLATION_CACHE_SIZE = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a new database connection.

        Args:
            *args: Positional arguments forwarded to ``sqlite3.connect()``
                (defaults to ``":memory:"`` for ``db_path`` when omitted).
            **kwargs: Keyword arguments forwarded to ``sqlite3.connect()`` and
                NeoSQLite-specific options, which are popped before connecting:
                ``tokenizers`` (list of ``(name, path)`` tuples for FTS5
                extensions), ``debug`` (bool), ``name`` (optional DB name),
                ``_is_clone`` (internal clone flag), ``journal_mode`` (default
                ``"WAL"``), ``auto_vacuum`` (default ``INCREMENTAL``; accepts
                ``0``/``1``/``2`` or ``"NONE"``/``"FULL"``/``"INCREMENTAL"``),
                ``translation_cache`` (SQL translation cache size, default
                ``100``, ``0`` to disable), ``ttl_sweep_interval_s`` (optional
                background TTL sweep interval in seconds for file-backed DBs),
                ``codec_options``, ``read_preference``, ``write_concern``, and
                ``read_concern``.
        """
        self._collections: dict[str, Collection] = {}
        self._tokenizers: list[tuple[str, str]] = kwargs.pop("tokenizers", [])
        self.debug: bool = kwargs.pop("debug", False)
        self._is_clone = kwargs.pop("_is_clone", False)
        self._ttl_sweep_interval: float | None = kwargs.pop(
            "ttl_sweep_interval_s", None
        )
        self._ttl_stop: threading.Event = threading.Event()
        self._ttl_thread: threading.Thread | None = None

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
        if args:
            self._db_path = args[0]
        elif "database" in kwargs:
            self._db_path = kwargs["database"]
        else:
            self._db_path = ":memory:"
        if self.name is None:
            self.name = (
                self._db_path if self._db_path != ":memory:" else "memory"
            )

        self.db: sqlite3.Connection = None  # type: ignore[assignment]
        self._closed = False
        self._parent_connection: Connection | None = None
        self._active_tx_session_state: ClientSession | None = None
        if not self._is_clone:
            self.connect(*args, **kwargs)
            self._start_ttl_sweeper()

    @property
    def _active_tx_session(self) -> ClientSession | None:
        """Get the active transactional ``ClientSession`` for this handle."""
        if self._parent_connection is not None:
            return self._parent_connection._active_tx_session
        return self._active_tx_session_state

    @_active_tx_session.setter
    def _active_tx_session(self, session: ClientSession | None) -> None:
        """Set or clear the active transactional ``ClientSession``."""
        if self._parent_connection is not None:
            self._parent_connection._active_tx_session = session
        else:
            self._active_tx_session_state = session

    @property
    def db_path(self) -> str:
        """Get the path to the database file.

        Returns:
            str: The database file path (or ``":memory:"``).
        """
        return self._db_path

    def connect(self, *args: Any, **kwargs: Any) -> None:
        """Establish a connection to the SQLite database.

        Closes any previously opened connection handle, opens a new
        ``sqlite3.Connection``, applies connection pragmas and custom SQLite
        functions, and runs auto_vacuum migration if required.

        Args:
            *args: Positional arguments passed to ``sqlite3.connect()``.
            **kwargs: Keyword arguments passed to ``sqlite3.connect()``.
        """
        # Close any previous handle so reconnecting doesn't leak it (#131).
        # Read self.__dict__ directly — getattr would route through
        # __getattr__ and CREATE a bogus "db" Collection (#130's hazard).
        old_db = self.__dict__.get("db")
        if old_db is not None:
            active_session = self.__dict__.get("_active_tx_session_state")
            if active_session is not None:
                active_session._clear_transaction_state()
                self._active_tx_session_state = None
            try:
                old_db.close()
            except sqlite3.Error as e:
                logger.debug(f"Failed to close previous connection: {e}")
        self.db = sqlite3.connect(*args, **kwargs)
        self._configure_connection()
        self._check_and_migrate_autovacuum(*args, **kwargs)

    def _configure_connection(self) -> None:
        """Apply PRAGMA settings and register custom functions on ``self.db``.

        Configures ``auto_vacuum``, disables automatic ``isolation_level``,
        sets ``journal_mode``, registers custom regex/math SQLite functions,
        sets ``PRAGMA synchronous = NORMAL`` for WAL mode unless overridden by
        ``_write_concern``, and loads any configured FTS5 tokenizer extensions.
        """
        auto_vacuum = getattr(self, "auto_vacuum", AutoVacuumMode.INCREMENTAL)
        journal_mode = getattr(self, "journal_mode", JournalMode.WAL)

        self.db.execute(f"PRAGMA auto_vacuum={auto_vacuum}")
        self.db.isolation_level = None
        self.db.execute(f"PRAGMA journal_mode={journal_mode}")
        self._register_custom_functions()

        # Set synchronous mode to NORMAL for WAL mode by default
        # unless overridden by write concern.
        if journal_mode == "WAL":
            self.db.execute("PRAGMA synchronous = NORMAL")

        write_concern = getattr(self, "_write_concern", None)
        if write_concern:
            self._apply_write_concern(write_concern)

        if self._tokenizers:
            self.db.enable_load_extension(True)
            for name, path in self._tokenizers:
                # Use parameterized query to prevent SQL injection in path
                self.db.execute("SELECT load_extension(?)", (path,))

    def _register_custom_functions(self) -> None:
        """Register custom SQLite functions, including regex and math ops."""

        def _regexp(pattern, text):
            """Check whether ``pattern`` matches anywhere inside ``text``.

            Args:
                pattern: Regular expression pattern.
                text: Input text value or ``None``.

            Returns:
                int: ``1`` when ``pattern`` matches ``text``, ``0`` otherwise.
            """
            if text is None:
                return 0
            return 1 if re.search(str(pattern), str(text)) is not None else 0

        def _regexp_find(pattern, text):
            """Find the first regex match and return JSON metadata.

            Args:
                pattern: Regular expression pattern, or ``None``.
                text: Input text value, or ``None``.

            Returns:
                str | None: JSON object with ``match``, ``idx``, and
                    ``captures`` keys, or ``None`` if no match or NULL input.
            """
            if text is None or pattern is None:
                return None
            match = re.search(str(pattern), str(text))
            if match:
                return json.dumps(
                    {
                        "match": match.group(0),
                        "idx": match.start(),
                        "captures": list(match.groups()),
                    }
                )
            return None

        def _regexp_find_all(pattern, text):
            """Find all regex matches and return a JSON array of metadata.

            Args:
                pattern: Regular expression pattern, or ``None``.
                text: Input text value, or ``None``.

            Returns:
                str: JSON array string of match dictionaries, or ``"[]"`` on
                    NULL input or no matches.
            """
            if text is None or pattern is None:
                return "[]"
            matches = [
                {
                    "match": match.group(0),
                    "idx": match.start(),
                    "captures": list(match.groups()),
                }
                for match in re.finditer(str(pattern), str(text))
            ]
            return json.dumps(matches)

        def _regexp_replace(text, pattern, replacement, count=0):
            """Replace regex matches in ``text`` using ``re.sub``.

            Args:
                text: Input text string, or ``None``.
                pattern: Regular expression pattern, or ``None``.
                replacement: Replacement string, or ``None``.
                count: Maximum replacements (``0`` replaces all matches).

            Returns:
                str | None: Updated string, ``text`` unchanged if ``pattern``
                    or ``replacement`` is ``None``, or ``None`` if ``text`` is
                    ``None``.
            """
            if text is None:
                return None
            if pattern is None or replacement is None:
                return text
            # count=0 in re.sub means replace all
            return re.sub(
                str(pattern), str(replacement), str(text), count=int(count)
            )

        self.db.create_function("REGEXP", 2, _regexp)
        self.db.create_function("REGEXP_FIND", 2, _regexp_find)
        self.db.create_function("REGEXP_FIND_ALL", 2, _regexp_find_all)
        self.db.create_function("REGEXP_REPLACE", 4, _regexp_replace)
        self._register_math_functions()

    def _register_math_functions(self) -> None:
        """Provide the SQLite 3.35+ math functions on older builds.

        SQLite math functions are a compile-time option and are absent from
        the system SQLite shipped by older distributions (for example
        AlmaLinux/RHEL 8). Registering Python equivalents keeps the SQL
        tiers usable instead of forcing every math expression to Tier 3.
        """
        try:
            self.db.execute("SELECT exp(1), sin(0), pow(2, 3), pi()")
            return
        except Exception:
            pass

        def _unary(func):
            """Wrap a 1-argument math function for SQLite NULL/error safety.

            Args:
                func: Single-argument numeric callable.

            Returns:
                Callable: Wrapper returning ``None`` on NULL or math errors.
            """

            def wrapper(value):
                """Evaluate the wrapped unary math function safely.

                Args:
                    value: Numeric input value or ``None``.

                Returns:
                    Any: Result of ``func(value)``, or ``None`` if ``value``
                        is ``None`` or raises ``TypeError``, ``ValueError``,
                        or ``OverflowError``.
                """
                if value is None:
                    return None
                try:
                    return func(value)
                except (TypeError, ValueError, OverflowError):
                    return None

            return wrapper

        def _binary(func):
            """Wrap a 2-argument math function for SQLite NULL/error safety.

            Args:
                func: Two-argument numeric callable.

            Returns:
                Callable: Wrapper returning ``None`` on NULL or math errors.
            """

            def wrapper(left, right):
                """Evaluate the wrapped binary math function safely.

                Args:
                    left: First numeric operand or ``None``.
                    right: Second numeric operand or ``None``.

                Returns:
                    Any: Result of ``func(left, right)``, or ``None`` on
                        ``None`` inputs or ``TypeError``/``ValueError``/
                        ``OverflowError``.
                """
                if left is None or right is None:
                    return None
                try:
                    return func(left, right)
                except (TypeError, ValueError, OverflowError):
                    return None

            return wrapper

        def _log_two(base, number):
            """Compute ``math.log(number, base)`` safely for SQLite.

            Args:
                base: Logarithm base or ``None``.
                number: Numeric argument or ``None``.

            Returns:
                float | None: Logarithm of ``number`` in ``base``, or ``None``
                    on ``None`` inputs or math errors.
            """
            if base is None or number is None:
                return None
            try:
                return math.log(number, base)
            except (TypeError, ValueError, OverflowError):
                return None

        def _pi():
            """Return the mathematical constant pi.

            Returns:
                float: Value of ``math.pi``.
            """
            return math.pi

        unary_functions = {
            "exp": math.exp,
            "ln": math.log,
            "log10": math.log10,
            "log2": math.log2,
            "sqrt": math.sqrt,
            "ceil": math.ceil,
            "floor": math.floor,
            "sin": math.sin,
            "cos": math.cos,
            "tan": math.tan,
            "asin": math.asin,
            "acos": math.acos,
            "atan": math.atan,
            "sinh": math.sinh,
            "cosh": math.cosh,
            "tanh": math.tanh,
            "asinh": math.asinh,
            "acosh": math.acosh,
            "atanh": math.atanh,
            "degrees": math.degrees,
            "radians": math.radians,
        }
        binary_functions = {
            "pow": math.pow,
            "atan2": math.atan2,
            "mod": math.fmod,
        }
        for unary_name, unary_func in unary_functions.items():
            self.db.create_function(unary_name, 1, _unary(unary_func))
        for binary_name, binary_func in binary_functions.items():
            self.db.create_function(binary_name, 2, _binary(binary_func))
        self.db.create_function("log", 1, _unary(math.log10))
        self.db.create_function("log", 2, _log_two)
        self.db.create_function("pi", 0, _pi)

    def _check_and_migrate_autovacuum(self, *args: Any, **kwargs: Any) -> None:
        """Check auto_vacuum setting and migrate if needed.

        Called after initial connection is established. Skips clones and
        in-memory databases.

        Args:
            *args: Positional connection arguments passed through to
                ``_migrate_to_autovacuum``.
            **kwargs: Keyword connection arguments passed through to
                ``_migrate_to_autovacuum``.
        """
        if self._is_clone or self._db_path == ":memory:":
            return

        if not needs_migration(self.db, self.auto_vacuum):
            return

        if not should_migrate():
            return

        self._migrate_to_autovacuum(*args, **kwargs)

    def _migrate_to_autovacuum(self, *args: Any, **kwargs: Any) -> None:
        """Migrate database to a new auto_vacuum mode.

        Checkpoints WAL, commits/closes the current handle, delegates to
        ``migration.migrate_autovacuum()``, and reopens and reconfigures
        ``self.db``.

        Args:
            *args: Positional arguments passed to ``sqlite3.connect()`` when
                reconnecting.
            **kwargs: Keyword arguments passed to ``migrate_autovacuum()`` and
                ``sqlite3.connect()``.
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
        self._configure_connection()

    def cleanup(self) -> None:
        """Clean up all collection resources for this connection."""
        if hasattr(self, "_collections"):
            for collection in self._collections.values():
                collection.cleanup()

    def _close_sqlite_handle(self, *, from_gc: bool = False) -> None:
        """Close the SQLite handle and roll back any uncommitted transaction.

        Shared teardown helper for ``close()`` and ``_close_rollback()`` so
        TTL sweeper shutdown, transaction rollback, active-session reset,
        collection cleanup, JSONB cache eviction, and closing ``self.db``
        remain consistent in one place.

        Args:
            from_gc: When ``True`` (called from ``__del__``), suppress
                teardown errors that can occur during interpreter shutdown.
        """
        if getattr(self, "_is_clone", False) or getattr(self, "_closed", False):
            return

        try:
            self._stop_ttl_sweeper()
        except Exception:
            pass

        db = self.__dict__.get("db")
        if db is not None:
            try:
                if db.in_transaction:
                    if not from_gc and logger is not None:
                        logger.warning(
                            "Closing connection with uncommitted transaction; "
                            "rolling back"
                        )
                    try:
                        db.rollback()
                    except Exception as e:
                        if logger is not None:
                            context = "GC" if from_gc else "close"
                            logger.debug(
                                f"Rollback failed during {context}: {e}"
                            )
            except Exception:
                # in_transaction check itself may fail during shutdown
                pass

        active_session = self.__dict__.get("_active_tx_session_state")
        if active_session is not None:
            active_session._clear_transaction_state()
            self._active_tx_session_state = None

        if from_gc:
            try:
                self.cleanup()
            except Exception:
                pass
        else:
            self.cleanup()

        if db is not None:
            try:
                from .collection.jsonb_support import clear_jsonb_cache

                clear_jsonb_cache(db)
            except Exception:
                pass

            try:
                from .changestream import clear_changestream_registry

                clear_changestream_registry(db)
            except Exception:
                pass

            try:
                db.close()
            except Exception as e:
                if not from_gc and logger is not None:
                    logger.debug(f"{e=}")

        self._closed = True

    def close(self) -> None:
        """Close the database connection.

        Stops the TTL sweeper thread if running, rolls back any uncommitted
        explicit transaction, cleans up cached collections, clears the JSONB
        capability cache, and closes the underlying SQLite connection. No-op
        for clones or already closed connections.
        """
        self._close_sqlite_handle(from_gc=False)

    def _close_rollback(self) -> None:
        """Close the database connection, rolling back pending transactions.

        Called from ``__del__`` so garbage collection does not persist partial
        uncommitted work. No-op for clones or already closed connections.
        """
        self._close_sqlite_handle(from_gc=True)

    @property
    def client(self) -> Self:
        """Get the ``MongoClient``-compatible handle (returns ``self``).

        Returns:
            Self: The connection instance itself.
        """
        return self

    @property
    def codec_options(self) -> Any:
        """Get the codec options stored on this connection.

        Returns:
            Any: The configured codec options, or ``None``.
        """
        return self._codec_options

    @property
    def read_preference(self) -> Any:
        """Get the read preference stored on this connection.

        Returns:
            Any: The configured read preference, or ``None``.
        """
        return self._read_preference

    @property
    def write_concern(self) -> Any:
        """Get the write concern stored on this connection.

        Returns:
            Any: The configured write concern, or ``None``.
        """
        return self._write_concern

    @property
    def read_concern(self) -> Any:
        """Get the read concern stored on this connection.

        Returns:
            Any: The configured read concern, or ``None``.
        """
        return self._read_concern

    def __getitem__(self, name: str) -> Collection:
        """Access a collection by name via dictionary-style indexing.

        Returns a cached ``Collection`` instance or creates one on first
        access. GridFS dot-notation names ending in ``.files`` or ``.chunks``
        are mapped to underscore-separated names (e.g., ``fs_files``).

        Args:
            name: The name of the collection to access.

        Returns:
            Collection: The collection instance for ``name``.
        """
        if name not in self._collections:
            # Map GridFS dot notation (e.g. 'fs.files' -> 'fs_files')
            if "." in name and (
                name.endswith(".files") or name.endswith(".chunks")
            ):
                bucket_name, _, suffix = name.rpartition(".")
                mapped_name = f"{bucket_name}_{suffix}"
                return self[mapped_name]
            self._collections[name] = Collection(self.db, name, database=self)
        return self._collections[name]

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to collection lookup.

        Returns an attribute from ``self.__dict__`` when present, otherwise
        delegates to ``self[name]`` to provide PyMongo-style attribute access
        to collections.

        Args:
            name: Attribute or collection name to resolve.

        Returns:
            Any: The attribute value from ``__dict__`` or the ``Collection``
                for ``name``.
        """
        if name in self.__dict__:
            return self.__dict__[name]
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        return self[name]

    def start_session(
        self,
        causal_consistency: bool | None = None,
        default_transaction_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> ClientSession:
        """Start a new client session for transactions.

        Provides PyMongo-compatible session management backed by SQLite
        transactions and savepoints.

        Args:
            causal_consistency: Optional causal consistency flag (stored for
                PyMongo API compatibility).
            default_transaction_options: Optional mapping of default
                transaction options for the session.
            **kwargs: Additional session option keyword arguments.

        Returns:
            ClientSession: A new ``ClientSession`` bound to this connection.
        """
        options = kwargs.copy()
        if causal_consistency is not None:
            options["causal_consistency"] = causal_consistency
        if default_transaction_options:
            options["default_transaction_options"] = default_transaction_options

        return ClientSession(self, options=options)

    def __enter__(self) -> Self:
        """Enter the connection context manager.

        Returns:
            Self: The connection instance itself.
        """
        return self

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_traceback: Any
    ) -> Literal[False]:
        """Close the connection when exiting a context manager.

        Args:
            exc_type: Exception class if raised inside the ``with`` block, or
                ``None``.
            exc_val: Exception instance if raised, or ``None``.
            exc_traceback: Exception traceback if raised, or ``None``.

        Returns:
            Literal[False]: Always ``False`` so exceptions propagate normally.
        """
        self.close()
        return False

    def drop_collection(self, name: str) -> None:
        """Drop a collection table and its associated metadata.

        Drops the SQLite table if it exists, evicts any cached ``Collection``
        object, removes matching rows from ``_neosqlite_ttl_indexes`` and
        ``neosqlite_index_keys``, invalidates cached schema metadata, and drops
        only FTS5 virtual and shadow tables owned by ``name``.

        Args:
            name: The name of the collection to drop.
        """
        cleanup_dropped_collection(self.db, name)
        # Evict the cached Collection so later access doesn't resurrect a
        # table-less object (#132)
        dropped = self._collections.pop(name, None)
        if dropped is not None:
            dropped._invalidate_ttl_cache()

    def create_collection(self, name: str, **kwargs) -> Collection:
        """Create a new collection with specific options.

        Args:
            name: The name of the collection to create.
            **kwargs: Additional options forwarded to ``Collection``.

        Returns:
            Collection: The newly created collection instance.

        Raises:
            CollectionInvalid: If a collection named ``name`` is already
                registered on this connection.
        """
        cursor = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        )
        if cursor.fetchone() is not None:
            raise CollectionInvalid(f"Collection {name} already exists")
        collection = Collection(
            self.db, name, create=True, database=self, **kwargs
        )
        self._collections[name] = collection
        return collection

    def get_collection(self, name: str, **kwargs) -> Collection:
        """Get a collection by name, creating a handle without forced DDL.

        Args:
            name: The name of the collection to retrieve.
            **kwargs: Additional options forwarded to ``Collection`` when the
                handle is not yet cached.

        Returns:
            Collection: The collection instance for ``name``.
        """
        if name not in self._collections:
            self._collections[name] = Collection(
                self.db, name, create=False, database=self, **kwargs
            )
        return self._collections[name]

    def get_database(self, name: str | None = None, **kwargs: Any) -> Self:
        """Get a database handle (PyMongo compatibility helper).

        NeoSQLite operates on a single database per connection, so this returns
        ``self`` while logging a debug message if ``name`` differs from
        ``self.name`` and ``self._db_path``.

        Args:
            name: Optional database name (accepted and ignored).
            **kwargs: Additional options (accepted and ignored).

        Returns:
            Self: The connection instance itself.
        """
        if name is not None and name != self.name and name != self._db_path:
            logger.debug(
                "get_database(%r) ignored: NeoSQLite has a single "
                "database %r",
                name,
                self.name,
            )
        return self

    def drop_database(self, name: str | None = None, **kwargs: Any) -> None:
        """Drop all user collections and internal helper tables.

        Drops every discovered collection via ``drop_collection`` plus internal
        ``_neosqlite_*`` and FTS helper tables, leaving ``sqlite_%`` system
        tables untouched, then commits the connection.

        Args:
            name: Optional database name (accepted and ignored; the single
                underlying database is always targeted).
            **kwargs: Additional options (accepted and ignored).
        """
        if name is not None and name != self.name and name != self._db_path:
            logger.debug(
                "drop_database(%r): dropping single NeoSQLite database %r",
                name,
                self.name,
            )
        try:
            names = self.list_collection_names()
        except Exception:
            names = list(self._collections.keys())
        for table_name in list(names):
            try:
                self.drop_collection(table_name)
            except Exception as exc:
                logger.debug("drop_database skipped %r: %s", table_name, exc)
        try:
            cursor = self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            for row in cursor.fetchall():
                tbl = row[0]
                if _is_internal_collection_name(tbl):
                    self.db.execute(
                        f"DROP TABLE IF EXISTS {quote_table_name(tbl)}"
                    )
        except Exception as exc:
            logger.debug("drop_database internal tables cleanup: %s", exc)
        self.db.commit()

    def sweep_ttl_once(self) -> int:
        """Run one TTL expiration sweep over all file-backed collections.

        Opens a short-lived helper connection to ``self._db_path`` so the
        background thread never shares the main ``sqlite3`` handle, calls
        ``purge_expired()`` on each collection with TTL specs, and commits.

        Returns:
            int: Total number of expired documents deleted across collections
                (``0`` when closed or using ``":memory:"``).
        """
        if getattr(self, "_closed", False):
            return 0
        if self._db_path == ":memory:":
            return 0
        total = 0
        helper: Connection | None = None
        try:
            from .collection.index_manager import list_ttl_specs

            probe = Connection(
                self._db_path, ttl_sweep_interval_s=None, _is_clone=True
            )
            probe.db = sqlite3.connect(self._db_path)
            helper = probe
            specs = list_ttl_specs(probe.db)
            by_collection: dict[str, bool] = {}
            for spec in specs:
                by_collection[spec["collection_name"]] = True
            for coll_name in by_collection:
                try:
                    coll = helper.get_collection(coll_name)
                    total += coll.purge_expired()
                except Exception as exc:
                    logger.debug("TTL sweep skipped %r: %s", coll_name, exc)
            try:
                helper.db.commit()
            except Exception:
                pass
        except Exception as exc:
            logger.debug("TTL sweep failed: %s", exc)
        finally:
            if helper is not None:
                try:
                    helper.db.close()
                except Exception:
                    pass
        return total

    def _start_ttl_sweeper(self) -> None:
        """Start the daemon TTL sweeper thread when configured.

        Skips starting when ``_ttl_sweep_interval`` is ``None``, non-numeric,
        non-positive, when ``_db_path`` is ``":memory:"``, or when the
        connection is a clone.
        """
        interval = getattr(self, "_ttl_sweep_interval", None)
        if interval is None:
            return
        try:
            interval_f = float(interval)
        except (TypeError, ValueError):
            logger.debug("Invalid ttl_sweep_interval_s=%r; disabled", interval)
            return
        if interval_f <= 0:
            return
        if self._db_path == ":memory:":
            logger.debug("TTL sweeper disabled for :memory: databases")
            return
        if getattr(self, "_is_clone", False):
            return
        self._ttl_sweep_interval = interval_f
        self._ttl_stop.clear()
        thread = threading.Thread(
            target=self._ttl_sweep_loop,
            name="neosqlite-ttl-sweeper",
            daemon=True,
        )
        self._ttl_thread = thread
        thread.start()

    def _ttl_sweep_loop(self) -> None:
        """Periodically execute ``sweep_ttl_once()`` until stopped."""
        interval = float(self._ttl_sweep_interval or 60.0)
        while not self._ttl_stop.wait(interval):
            try:
                self.sweep_ttl_once()
            except Exception as exc:
                logger.debug("TTL sweep loop error: %s", exc)

    def _stop_ttl_sweeper(self) -> None:
        """Signal the TTL sweeper thread to stop and wait briefly for exit."""
        stop = getattr(self, "_ttl_stop", None)
        if stop is not None:
            stop.set()
        thread = getattr(self, "_ttl_thread", None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._ttl_thread = None

    def rename_collection(self, old_name: str, new_name: str) -> None:
        """Rename an existing collection and update its TTL metadata.

        Args:
            old_name: Current name of the collection.
            new_name: Target new name for the collection.

        Raises:
            CollectionInvalid: If ``old_name`` does not exist or ``new_name``
                already exists in ``sqlite_master``.
        """
        exists_row = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (old_name,),
        ).fetchone()
        if not exists_row:
            raise CollectionInvalid(f"Collection {old_name} does not exist")

        # Check if new name already exists
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (new_name,),
        )
        if cursor.fetchone():
            raise CollectionInvalid(f"Collection {new_name} already exists")

        # Rename the collection (cache entry may not exist for tables that
        # were never accessed through this Connection — #133)
        cached = self._collections.pop(old_name, None)
        if cached is None:
            cached = Collection(self.db, old_name, database=self)
        cached.rename(new_name)
        self._collections[new_name] = cached
        try:
            self.db.execute(
                "UPDATE _neosqlite_ttl_indexes SET collection_name = ? "
                "WHERE collection_name = ?",
                (new_name, old_name),
            )
        except Exception:
            pass

    def list_collection_names(self) -> list[str]:
        """List all user collection names in the database.

        Returns:
            list[str]: Names of tables in ``sqlite_master`` excluding SQLite
                system tables and NeoSQLite internal tables.
        """
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [
            row[0]
            for row in cursor.fetchall()
            if not _is_internal_collection_name(row[0])
        ]

    def list_collections(self) -> list[dict[str, Any]]:
        """Get metadata for all user collections in the database.

        Returns:
            list[dict[str, Any]]: List of dictionaries containing ``"name"``
                and ``"options"`` (the table's SQL DDL from
                ``sqlite_master``), excluding internal tables.
        """
        cursor = self.db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [
            {"name": row[0], "options": row[1]}
            for row in cursor.fetchall()
            if not _is_internal_collection_name(row[0])
        ]

    # ------------------------------------------------------------------
    # Command dispatch table (module-level functions for clarity)
    # ------------------------------------------------------------------

    def command(
        self, command: str | dict[str, Any], value: Any = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Issue a database command and return the response.

        Provides PyMongo-compatible command execution for SQLite, dispatching
        built-in administrative/diagnostic commands (such as ``ping``,
        ``serverStatus``, ``listCollections``, ``collStats``, ``dbStats``,
        ``compact``, ``vacuum``, ``validate``, ``aggregate``, and
        ``query_only``) and falling back to safe PRAGMA evaluation.

        Args:
            command: Command name string (e.g., ``"table_info"``) or
                single-entry command dictionary (e.g., ``{"ping": 1}``).
            value: Optional command value converted into ``{command: value}``
                when ``command`` is a string.
            **kwargs: Additional command arguments (for example, ``dryRun``
                or ``freeSpaceTargetMB`` for ``compact``).

        Returns:
            dict[str, Any]: Command response dictionary, typically with
                ``"ok": 1`` on success or ``"ok": 0`` with ``"errmsg"`` on
                error.

        Raises:
            TypeError: If ``command`` is neither a ``str`` nor a ``dict``.

        Example:
            >>> db = Connection("test.db")
            >>> result = db.command("ping")
            >>> print(result)
            {'ok': 1}
        """
        # If command is a string and value is provided, it's equivalent to {command: value}
        if isinstance(command, str) and value is not None:
            command = {command: value}

        merged_kwargs = dict(kwargs)
        command_norm: dict[str, Any]

        # Handle string and dict commands cleanly, normalizing the primary key
        # to lowercase and merging remaining dict entries as default kwargs.
        if isinstance(command, str):
            cmd_name = command.lower()
            command_norm = {}
        elif isinstance(command, dict):
            if not command:
                return {
                    "ok": 0,
                    "errmsg": "Command dictionary cannot be empty",
                    "code": 1,
                }
            items = list(command.items())
            raw_cmd_key, target_val = items[0]
            cmd_name = str(raw_cmd_key).lower()
            command_norm = {cmd_name: target_val}
            merged_kwargs = {k: v for k, v in items[1:]}
            merged_kwargs.update(kwargs)
        else:
            raise TypeError("command must be a string or dict")

        try:
            handler = _COMMAND_DISPATCH.get(cmd_name)
            if handler is not None:
                return handler(self, command_norm, **merged_kwargs)
            # Fallback: validate and try as PRAGMA
            return _command_default(self, cmd_name)
        except Exception as e:
            logger.debug(f"{e=}")
            return {"ok": 0, "errmsg": str(e), "code": 1}

    def cursor_command(
        self, command: str | dict[str, Any], value: Any = None, **kwargs: Any
    ) -> AggregationCursor:
        """Execute a database command and return a cursor for its results.

        Executes ``self.command(...)`` and wraps extracted result items
        (``collections``, ``columns``, ``result``, ``cursor.firstBatch``, or
        the response dict itself) in a pre-executed ``AggregationCursor``.

        Args:
            command: Command name string or command dictionary.
            value: Optional command value when ``command`` is a string.
            **kwargs: Additional command arguments passed to ``command()``.

        Returns:
            AggregationCursor: Pre-populated cursor over the command results.
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
        collection = Collection(
            self.db, "__command_results__", database=self, create=False
        )

        # Create an AggregationCursor with the pre-computed results
        cursor = AggregationCursor(collection, [])
        cursor._results = items
        cursor._executed = True
        return cursor

    def dereference(self, dbref: dict[str, Any]) -> dict[str, Any] | None:
        """Resolve a DBRef dictionary to its referenced document.

        Performs ``find_one({"_id": dbref["$id"]})`` on the collection named
        by ``dbref["$ref"]``.

        Args:
            dbref: Dictionary containing ``"$ref"`` and ``"$id"`` keys.

        Returns:
            dict[str, Any] | None: The referenced document, or ``None`` if
                ``dbref`` is invalid or no matching document is found.
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
    ) -> Self:
        """Return a shallow clone of this connection with updated options.

        Shares the same underlying ``sqlite3.Connection`` and a shallow copy
        of ``_collections`` while storing PyMongo-compatible option attributes
        without mutating connection PRAGMAs on the shared handle.

        Args:
            codec_options: Codec options stored for PyMongo API compatibility.
            read_preference: Read preference stored for API compatibility.
            write_concern: Write concern stored for API compatibility.
            read_concern: Read concern stored for API compatibility.

        Returns:
            Self: A cloned ``Connection`` sharing ``self.db`` with the new
                option attributes attached.

        Example:
            >>> db = Connection("test.db")
            >>> db_with_options = db.with_options(
            ...     write_concern={"w": "majority"},
            ...     read_preference={"mode": "primaryPreferred"},
            ... )
        """
        # Return a new Connection instance that shares the same database connection.
        # This is by design: SQLite uses a single writable connection per database file,
        # so clones naturally share the same sqlite3.Connection.
        # The clone only differs in its stored PyMongo-compatible options
        # (codec_options, read_preference, write_concern, read_concern).
        # We use a shallow copy of _collections so that mutations to one Connection's
        # _collections dict do not affect the other.
        clone = self.__class__(name=self.name, _is_clone=True)
        clone.db = self.db
        clone._db_path = self._db_path
        clone._parent_connection = self._parent_connection or self
        clone._tokenizers = list(self._tokenizers)
        clone.debug = self.debug
        clone.auto_vacuum = self.auto_vacuum
        clone._translation_cache_size = self._translation_cache_size
        clone.journal_mode = self.journal_mode
        clone._collections = {
            coll_name: Collection(
                self.db,
                coll_name,
                create=False,
                database=clone,
                **coll._options,
            )
            for coll_name, coll in self._collections.items()
        }

        # Store the new options, inheriting prior values when omitted
        clone._codec_options = (
            codec_options if codec_options is not None else self._codec_options
        )
        clone._read_preference = (
            read_preference
            if read_preference is not None
            else self._read_preference
        )
        clone._write_concern = (
            write_concern if write_concern is not None else self._write_concern
        )
        clone._read_concern = (
            read_concern if read_concern is not None else self._read_concern
        )

        # NOTE: We intentionally do NOT call _apply_write_concern() here.
        # The clone shares the same sqlite3.Connection with the original, so
        # changing PRAGMAs would affect the original too. The write_concern
        # is stored for API compatibility but does not mutate the shared connection.

        return clone

    def _apply_write_concern(self, write_concern: Any) -> None:
        """Apply write concern to the underlying SQLite connection via PRAGMAs.

        Maps ``w=0`` to ``PRAGMA synchronous = OFF``, ``w=1`` to
        ``PRAGMA synchronous = NORMAL``, and truthy ``j`` to
        ``PRAGMA synchronous = FULL``.

        Args:
            write_concern: ``WriteConcern`` instance or dict specifying ``w``
                and/or ``j`` flags.
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

            if j:
                self.db.execute("PRAGMA synchronous = FULL")
        except Exception as e:
            logger.warning(f"Error applying write concern: {e}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager for handling top-level database transactions.

        Begins a SQLite transaction on entry, commits on normal exit, and rolls
        back if an exception is raised inside the block.

        Yields:
            None: Control to the caller inside the active transaction.

        Raises:
            sqlite3.OperationalError: If a transaction is already active on
                this connection (refused before altering transaction state).
        """
        if self.db.in_transaction:
            raise sqlite3.OperationalError(
                "Cannot start a transaction within a transaction. "
                "Use ClientSession transactions (savepoints) for nesting."
            )
        try:
            self.db.execute("BEGIN")
            yield
            self.db.commit()
        except Exception as e:
            logger.debug(f"Transaction failed, rolling back: {e}")
            self.db.rollback()
            if self._active_tx_session is not None:
                self._active_tx_session._clear_transaction_state()
            raise

    def __del__(self):
        """Close the connection on garbage collection via ``_close_rollback``.

        Rolls back any uncommitted transaction instead of committing partial
        work, and swallows shutdown errors when module globals are torn down.
        """
        try:
            self._close_rollback()
        except Exception as e:
            if logger is not None:
                logger.debug(f"{e=}")
            pass


# ------------------------------------------------------------------
# Command dispatch table & handlers
# Each handler receives (connection, command_dict, **kwargs) and
# returns {"ok": 1, ...} or {"ok": 0, ...}
# ------------------------------------------------------------------


def _command_ping(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``ping`` command to verify connection liveness.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Dictionary ``{"ok": 1}``.
    """
    return {"ok": 1}


def _command_serverstatus(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``serverStatus`` command returning SQLite and PID metadata.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Status dictionary with ``ok``, ``version``,
            ``process``, and ``pid``.
    """
    return {
        "ok": 1,
        "version": sqlite3.sqlite_version,
        "process": "neosqlite",
        "pid": os.getpid(),
    }


def _command_listcollections(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``listCollections`` command.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Dictionary with ``ok`` and ``collections`` listing
            ``{"name": <collection_name>}`` entries.
    """
    collections = self.list_collection_names()
    return {
        "ok": 1,
        "collections": [{"name": name} for name in collections],
    }


def _extract_cmd_str(
    command: dict, kwargs: dict[str, Any], *keys: str
) -> str | None:
    """Return the first non-empty string value for ``keys`` in kwargs/command."""
    for source in (kwargs, command if isinstance(command, dict) else {}):
        for key in keys:
            val = source.get(key)
            if isinstance(val, str) and val:
                return val
    return None


def _command_table_info(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``table_info`` command using ``PRAGMA table_info``.

    Args:
        command: Command payload dictionary optionally providing
            ``"table_info"``.
        **kwargs: Keyword arguments; ``table`` may supply the target table
            name.

    Returns:
        dict[str, Any]: Dictionary with ``ok`` and ``columns`` metadata dicts
            (``cid``, ``name``, ``type``, ``notnull``, ``default``, ``pk``).

    Raises:
        ValueError: If no table name is supplied via ``kwargs["table"]`` or
            ``command["table_info"]``.
    """
    table_name = _extract_cmd_str(command, kwargs, "table", "table_info")
    if not table_name:
        raise ValueError("table_info requires 'table' parameter")
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


def _command_integrity_check(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``integrity_check`` command via ``PRAGMA integrity_check``.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Dictionary with ``ok`` and ``result`` containing the
            check output strings.
    """
    cursor = self.db.execute("PRAGMA integrity_check")
    result = cursor.fetchall()
    return {"ok": 1, "result": [row[0] for row in result]}


def _command_validate(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``validate`` command using ``PRAGMA integrity_check``.

    Runs table-scoped ``PRAGMA integrity_check`` when a collection name is
    provided, or database-wide otherwise, reporting any non-``"ok"`` lines.

    Args:
        command: Command payload dictionary optionally providing
            ``"validate"``.
        **kwargs: Keyword arguments; ``validate`` or ``collection`` may specify
            the target collection name.

    Returns:
        dict[str, Any]: Dictionary containing ``ok`` (``1`` when valid, ``0``
            otherwise), ``result``, ``errors``, and ``valid``.
    """
    collection_name = _extract_cmd_str(
        command, kwargs, "validate", "collection"
    )
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
        "valid": not errors,
    }


def _command_foreign_key_check(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle ``foreign_key_check`` using ``PRAGMA foreign_key_check``.

    Args:
        command: Command payload dictionary optionally providing a table name.
        **kwargs: Optional keyword arguments; ``table`` limits the check to a
            single table.

    Returns:
        dict[str, Any]: Dictionary containing ``ok`` and ``violations`` list
            with ``table``, ``rowid``, ``parent``, and ``fkid`` keys.
    """
    table_name = _extract_cmd_str(command, kwargs, "table", "foreign_key_check")
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
            {"table": row[0], "rowid": row[1], "parent": row[2], "fkid": row[3]}
            for row in result
        ],
    }


def _command_index_list(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``index_list`` command using ``PRAGMA index_list``.

    Args:
        command: Command payload dictionary optionally providing
            ``"index_list"``.
        **kwargs: Keyword arguments; ``table`` may supply the target table
            name.

    Returns:
        dict[str, Any]: Dictionary with ``ok`` and ``indexes`` metadata dicts
            (``seq``, ``name``, ``unique``, ``origin``, ``partial``).

    Raises:
        ValueError: If no table name is supplied via ``kwargs["table"]`` or
            ``command["index_list"]``.
    """
    table_name = _extract_cmd_str(command, kwargs, "table", "index_list")
    if not table_name:
        raise ValueError("index_list requires 'table' parameter")
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


def _command_vacuum(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``vacuum`` command by executing SQLite ``VACUUM``.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Status dict
            ``{"ok": 1, "message": "VACUUM completed"}``.
    """
    self.db.execute("VACUUM")
    return {"ok": 1, "message": "VACUUM completed"}


def _command_compact(
    self: Connection, _command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``compact`` command via incremental vacuum or ``VACUUM``.

    Supports ``dryRun`` (returns ``estimatedBytesFreed`` without modifying
    pages) and ``freeSpaceTargetMB`` (defaults to ``20``; ``0`` forces a full
    ``VACUUM``, while positive values trigger batched
    ``PRAGMA incremental_vacuum`` only when free bytes meet the threshold).

    Args:
        _command: Command payload dictionary (unused).
        **kwargs: Optional ``dryRun`` and ``freeSpaceTargetMB`` arguments.

    Returns:
        dict[str, Any]: Response dictionary with ``ok`` and either
            ``estimatedBytesFreed`` (for ``dryRun``) or ``bytesFreed``.
    """
    dry_run = kwargs.get("dryRun", False)
    free_space_target_mb = kwargs.get("freeSpaceTargetMB")

    free_pages = self.db.execute("PRAGMA freelist_count").fetchone()[0]
    page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
    free_bytes = free_pages * page_size

    if dry_run:
        return {"estimatedBytesFreed": free_bytes, "ok": 1}

    if free_space_target_mb == 0:
        page_count_before = self.db.execute("PRAGMA page_count").fetchone()[0]
        self.db.execute("VACUUM")
        page_count_after = self.db.execute("PRAGMA page_count").fetchone()[0]
        return {
            "bytesFreed": (page_count_before - page_count_after) * page_size,
            "ok": 1,
        }

    if free_space_target_mb is None:
        free_space_target_mb = 20

    target_bytes = free_space_target_mb * 1024 * 1024
    if free_bytes < target_bytes:
        return {"bytesFreed": 0, "ok": 1}

    page_count_before = self.db.execute("PRAGMA page_count").fetchone()[0]
    target_pages = max(1, target_bytes // page_size)

    while free_pages > 0:
        reclaim = min(target_pages, free_pages)
        self.db.execute(f"PRAGMA incremental_vacuum({reclaim})")
        free_pages = self.db.execute("PRAGMA freelist_count").fetchone()[0]

    page_count_after = self.db.execute("PRAGMA page_count").fetchone()[0]
    return {
        "bytesFreed": (page_count_before - page_count_after) * page_size,
        "ok": 1,
    }


def _command_wal_checkpoint(
    self: Connection, _command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle ``wal_checkpoint`` using ``PRAGMA wal_checkpoint(<mode>)``.

    Args:
        _command: Command payload dictionary (unused).
        **kwargs: Optional ``mode`` argument (``"PASSIVE"``, ``"FULL"``,
            ``"RESTART"``, or ``"TRUNCATE"``; defaults to ``"PASSIVE"``).

    Returns:
        dict[str, Any]: Dictionary with ``ok``, ``mode``, ``busy``, ``log``,
            and ``checkpointed`` counts, or ``{"ok": 0, "errmsg": ...}`` if
            ``mode`` is invalid.
    """
    mode = str(kwargs.get("mode", "PASSIVE")).upper()
    if mode not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
        return {"ok": 0, "errmsg": f"Invalid wal_checkpoint mode: {mode}"}
    result = self.db.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
    return {
        "ok": 1,
        "mode": mode,
        "busy": result[0],
        "log": result[1],
        "checkpointed": result[2],
    }


def _command_cache_size(
    self: Connection, _command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``cache_size`` command to get or set ``PRAGMA cache_size``.

    Args:
        _command: Command payload dictionary (unused).
        **kwargs: Optional ``pages`` integer value to set ``cache_size``; when
            omitted, queries the current setting.

    Returns:
        dict[str, Any]: Response dictionary with ``ok`` and either
            ``message`` (when setting), ``cache_size`` (when querying), or
            ``errmsg`` if ``pages`` is not a non-boolean integer.
    """
    pages = kwargs.get("pages")
    if pages is not None:
        if not isinstance(pages, int) or isinstance(pages, bool):
            return {
                "ok": 0,
                "errmsg": f"Invalid cache_size pages value: {pages!r}",
            }
        self.db.execute(f"PRAGMA cache_size = {pages}")
        return {"ok": 1, "message": f"cache_size set to {pages}"}
    current = self.db.execute("PRAGMA cache_size").fetchone()[0]
    return {"ok": 1, "cache_size": current}


def _command_busy_timeout(
    self: Connection, _command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle ``busy_timeout`` to get or set ``PRAGMA busy_timeout``.

    Args:
        _command: Command payload dictionary (unused).
        **kwargs: Optional non-negative integer ``milliseconds`` value to set
            ``busy_timeout``; when omitted, queries the current timeout.

    Returns:
        dict[str, Any]: Response dictionary with ``ok`` and either
            ``message`` (when setting), ``busy_timeout`` (when querying), or
            ``errmsg`` if ``milliseconds`` is invalid.
    """
    ms = kwargs.get("milliseconds")
    if ms is not None:
        if not isinstance(ms, int) or isinstance(ms, bool) or ms < 0:
            return {
                "ok": 0,
                "errmsg": f"Invalid busy_timeout milliseconds value: {ms!r}",
            }
        self.db.execute(f"PRAGMA busy_timeout = {ms}")
        return {"ok": 1, "message": f"busy_timeout set to {ms}ms"}
    current = self.db.execute("PRAGMA busy_timeout").fetchone()[0]
    return {"ok": 1, "busy_timeout": current}


def _command_analyze(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``analyze`` command by executing SQLite ``ANALYZE``.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Status dict
            ``{"ok": 1, "message": "ANALYZE completed"}``.
    """
    self.db.execute("ANALYZE")
    return {"ok": 1, "message": "ANALYZE completed"}


def _command_reindex(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle ``reIndex`` by executing SQLite ``REINDEX``.

    Reindexes a single table/collection when specified via ``reIndex`` or
    ``reindex``, or reindexes the entire database otherwise.

    Args:
        command: Command payload dictionary optionally providing
            ``"reindex"``.
        **kwargs: Optional ``reIndex`` argument naming the target collection.

    Returns:
        dict[str, Any]: Status dict
            ``{"ok": 1, "message": "REINDEX completed"}``.
    """
    collection_name = _extract_cmd_str(
        command, kwargs, "reIndex", "reindex", "collection"
    )
    if collection_name:
        self.db.execute(f"REINDEX {quote_table_name(collection_name)}")
    else:
        self.db.execute("REINDEX")
    return {"ok": 1, "message": "REINDEX completed"}


def _command_collstats(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``collStats`` command for collection size and index stats.

    Gathers document count, total ``LENGTH(data)`` bytes, average object size,
    and (when SQLite ``dbstat`` is available) storage page sizes per table and
    index.

    Args:
        command: Command payload dictionary optionally providing
            ``"collstats"``.
        **kwargs: Optional ``collection`` argument naming the target
            collection.

    Returns:
        dict[str, Any]: MongoDB-compatible ``collStats`` dictionary with
            ``ok``, ``ns``, ``count``, ``size``, ``avgObjSize``,
            ``storageSize``, ``totalIndexSize``, and ``indexSizes``.

    Raises:
        ValueError: If no collection name is provided via
            ``kwargs["collection"]`` or ``command["collstats"]``.
    """
    collection_name = _extract_cmd_str(
        command, kwargs, "collection", "collstats"
    )
    if not collection_name:
        raise ValueError("collstats requires 'collection' parameter")
    quoted_table = quote_table_name(collection_name)

    cursor = self.db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (collection_name,),
    )
    row = cursor.fetchone()
    if not row or not row[0]:
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

    cursor = self.db.execute(f"SELECT COUNT(*) FROM {quoted_table}")
    count = cursor.fetchone()[0] or 0

    size = 0
    try:
        size_cursor = self.db.execute(
            f"SELECT SUM(LENGTH(data)) FROM {quoted_table}"
        )
        size = size_cursor.fetchone()[0] or 0
    except Exception as e:
        logger.debug(f"Failed to calculate collection size: {e}")

    avg_obj_size = size / count if count > 0 else 0

    storage_size = 0
    total_index_size = 0
    index_sizes: dict[str, int] = {}

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


def _command_dbstats(
    self: Connection, _command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``dbStats`` command returning database-level statistics.

    Aggregates page counts, non-FTS collection object and index counts, index
    size via ``dbstat`` (falling back to 20% of storage size), view counts,
    and filesystem disk usage for file-backed databases.

    Args:
        _command: Command payload dictionary (unused).
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: MongoDB-compatible ``dbStats`` response dictionary.
    """
    page_count = self.db.execute("PRAGMA page_count").fetchone()[0]
    page_size = self.db.execute("PRAGMA page_size").fetchone()[0]

    collections = self.list_collections()
    total_objects = 0
    total_indexes = 0

    for coll_info in collections:
        coll_name = coll_info["name"]
        sql = coll_info["options"] or ""

        if "VIRTUAL TABLE" in sql.upper() and "fts" in sql.lower():
            continue

        try:
            count = self.db.execute(
                f"SELECT COUNT(*) FROM {quote_table_name(coll_name)}"
            ).fetchone()[0]
            total_objects += count
        except Exception as e:
            logger.debug(f"Failed to count documents in '{coll_name}': {e}")

        try:
            indexes = self.db.execute(
                f"PRAGMA index_list({quote_table_name(coll_name)})"
            ).fetchall()
            total_indexes += len(indexes)
        except Exception as e:
            logger.debug(f"Failed to get indexes for '{coll_name}': {e}")

    storage_size = page_count * page_size

    try:
        self.db.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS temp.dbstat USING dbstat(main)"
        )
        cursor = self.db.execute(
            "SELECT SUM(pgsize) FROM dbstat WHERE name LIKE 'idx_%' "
            "OR name LIKE 'sqlite_autoindex%'"
        )
        index_size = cursor.fetchone()[0] or 0
    except Exception as e:
        logger.debug(f"Failed to calculate index size, using estimate: {e}")
        index_size = int(storage_size * 0.2)

    data_size = storage_size - index_size

    views_count = self.db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='view'"
    ).fetchone()[0]

    fs_total = 0
    fs_used = 0

    if self._db_path and self._db_path != ":memory:":
        try:
            db_dir = os.path.dirname(self._db_path) or "."
            fs_usage = shutil.disk_usage(db_dir)
            fs_total = fs_usage.total
            fs_used = fs_usage.used
        except OSError as e:
            logger.debug(f"{e=}")

    return {
        "ok": 1,
        "db": self.name,
        "collections": len(collections),
        "views": views_count,
        "objects": total_objects,
        "avgObjSize": int(data_size / total_objects) if total_objects else 0,
        "dataSize": data_size,
        "storageSize": storage_size,
        "indexes": total_indexes,
        "indexSize": index_size,
        "totalSize": data_size + index_size,
        "fsTotalSize": fs_total,
        "fsUsedSize": fs_used,
        "scaleFactor": 1,
    }


def _command_aggregate(
    self: Connection, command: dict, **kwargs: Any
) -> dict[str, Any]:
    """Handle the ``aggregate`` database command on a collection.

    Args:
        command: Command payload dictionary optionally providing
            ``"aggregate"``.
        **kwargs: Keyword arguments supporting ``aggregate`` (collection name),
            ``pipeline`` (aggregation stage list), and ``explain`` (bool).

    Returns:
        dict[str, Any]: Query explanation dictionary when ``explain`` is
            truthy, or ``{"ok": 1, "result": <documents>}``.

    Raises:
        ValueError: If no collection name is provided via
            ``kwargs["aggregate"]`` or ``command["aggregate"]``.
    """
    collection_name = _extract_cmd_str(
        command, kwargs, "aggregate", "collection"
    )
    if not collection_name:
        raise ValueError("aggregate requires 'aggregate' parameter")

    pipeline = kwargs.get("pipeline", [])
    explain = kwargs.get("explain", False)

    if explain:
        collection = self[collection_name]
        return collection.query_engine.explain_aggregation(
            pipeline, session=None
        )
    collection = self[collection_name]
    cursor_result = collection.aggregate(pipeline)
    return {"ok": 1, "result": list(cursor_result)}


def _command_query_only(
    self: Connection, command: dict, **__: Any
) -> dict[str, Any]:
    """Handle the ``query_only`` command to get or set ``PRAGMA query_only``.

    When ``command`` is a dict containing a non-``None`` ``"query_only"``
    value (bool, string such as ``"ON"``/``"TRUE"``/``"1"``, or number),
    updates ``PRAGMA query_only``. Otherwise reads the current setting.

    Args:
        command: Command payload dictionary optionally containing
            ``"query_only"``.
        **__: Additional unused keyword arguments.

    Returns:
        dict[str, Any]: Dictionary with ``ok`` and boolean ``query_only``, or
            an error dict when the supplied value has an unsupported type.
    """
    pragma_value = (
        command.get("query_only") if isinstance(command, dict) else None
    )
    if pragma_value is not None:
        if isinstance(pragma_value, bool):
            val = 1 if pragma_value else 0
        elif isinstance(pragma_value, str):
            val = 1 if pragma_value.upper() in ("ON", "TRUE", "1") else 0
        elif isinstance(pragma_value, (int, float)):
            val = 1 if pragma_value else 0
        else:
            return {
                "ok": 0,
                "errmsg": f"Invalid query_only value: {pragma_value!r}",
            }
        self.db.execute(f"PRAGMA query_only = {val}")
        return {"ok": 1, "query_only": bool(val)}
    cursor = self.db.execute("PRAGMA query_only")
    return {"ok": 1, "query_only": bool(cursor.fetchone()[0])}


_READ_ONLY_PRAGMA_ALLOWLIST: frozenset[str] = frozenset(
    {
        "application_id",
        "auto_vacuum",
        "automatic_index",
        "busy_timeout",
        "cache_size",
        "cache_spill",
        "cell_size_check",
        "checkpoint_fullfsync",
        "collation_list",
        "compile_options",
        "data_version",
        "database_list",
        "encoding",
        "foreign_key_check",
        "foreign_keys",
        "freelist_count",
        "fullfsync",
        "function_list",
        "hard_heap_limit",
        "ignore_check_constraints",
        "integrity_check",
        "journal_mode",
        "journal_size_limit",
        "legacy_alter_table",
        "legacy_file_format",
        "locking_mode",
        "max_page_count",
        "mmap_size",
        "module_list",
        "page_count",
        "page_size",
        "pragma_list",
        "query_only",
        "quick_check",
        "read_uncommitted",
        "recursive_triggers",
        "reverse_unordered_selects",
        "schema_version",
        "secure_delete",
        "soft_heap_limit",
        "synchronous",
        "table_list",
        "temp_store",
        "threads",
        "trusted_schema",
        "user_version",
        "wal_autocheckpoint",
    }
)


def _command_default(self: Connection, cmd_name: str) -> dict[str, Any]:
    """Fallback handler: run an allowlisted read-only SQLite PRAGMA.

    Rejects unknown or mutating command names against
    ``_READ_ONLY_PRAGMA_ALLOWLIST``, executes ``PRAGMA <cmd_name>``, and maps
    result rows into dictionaries keyed by column names from
    ``cursor.description``.

    Args:
        cmd_name: Candidate read-only SQLite PRAGMA identifier.

    Returns:
        dict[str, Any]: Dictionary ``{"ok": 1, "result": [...]}`` on success,
            or ``{"ok": 0, "errmsg": ...}`` if validation or execution fails.
    """
    normalized = cmd_name.lower() if isinstance(cmd_name, str) else ""
    if normalized not in _READ_ONLY_PRAGMA_ALLOWLIST:
        return {"ok": 0, "errmsg": f"Unknown command: {cmd_name}"}
    try:
        cursor = self.db.execute(f"PRAGMA {normalized}")
        result = cursor.fetchall()
        return {
            "ok": 1,
            "result": [
                dict(zip([d[0] for d in cursor.description], row))
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


_COMMAND_DISPATCH: dict[str, Any] = {
    "ping": _command_ping,
    "serverstatus": _command_serverstatus,
    "listcollections": _command_listcollections,
    "table_info": _command_table_info,
    "integrity_check": _command_integrity_check,
    "validate": _command_validate,
    "foreign_key_check": _command_foreign_key_check,
    "index_list": _command_index_list,
    "vacuum": _command_vacuum,
    "compact": _command_compact,
    "wal_checkpoint": _command_wal_checkpoint,
    "cache_size": _command_cache_size,
    "busy_timeout": _command_busy_timeout,
    "analyze": _command_analyze,
    "reindex": _command_reindex,
    "collstats": _command_collstats,
    "dbstats": _command_dbstats,
    "aggregate": _command_aggregate,
    "query_only": _command_query_only,
}
