from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Iterator
from copy import deepcopy
from functools import partial
from itertools import islice
from typing import TYPE_CHECKING, Any, Self

from .._sqlite import sqlite3
from ..objectid import ObjectId
from ..sql_utils import quote_table_name
from .json_path_utils import parse_json_path
from .type_utils import (
    bson_sort_key,
    is_case_insensitive_collation,
    serialize_sql_query_value,
    validate_session,
)

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from . import Collection

logger = logging.getLogger(__name__)

ASCENDING = 1
DESCENDING = -1


def _sanitize_sql_comment(comment: str) -> str:
    """Neutralize SQL block and line comment markers in user comments.

    Strips null bytes and iteratively breaks up ``/*``, ``*/``, and ``--``
    sequences (including nested combinations such as ``*--/`` or ``/**/``) so
    a SQL comment passed to :meth:`Cursor.comment` cannot terminate the
    enclosing ``/* ... */`` SQL comment block prematurely.

    Args:
        comment: Raw comment string from ``Cursor.comment()``.

    Returns:
        str: Comment string safe to embed inside ``/* ... */``.
    """
    sanitized = str(comment).replace("\x00", "")
    while "/*" in sanitized or "*/" in sanitized or "--" in sanitized:
        sanitized = (
            sanitized.replace("/*", "/ *")
            .replace("*/", "* /")
            .replace("--", "- -")
        )
    return sanitized


def parse_mongo_index_name(name: str) -> list[str] | None:
    """Parse a MongoDB-style index name into its component field keys.

    Handles ``"_id_"`` as ``["_id"]`` and names formatted like ``"age_1"``
    or ``"a_1_b_-1"`` (preserving underscores inside field names).

    Args:
        name: MongoDB index name string.

    Returns:
        list[str] | None: List of indexed field names, or ``None`` if
            ``name`` is empty or malformed.
    """
    if not name or name == "_id_":
        return ["_id"] if name == "_id_" else None
    tokens = name.split("_")
    keys: list[str] = []
    buf: list[str] = []
    for tok in tokens:
        if tok in ("1", "-1") and buf:
            keys.append("_".join(buf))
            buf = []
        else:
            buf.append(tok)
    if buf or not keys:
        return None
    return keys


def resolve_hint_index(
    db: Any,
    collection_name: str,
    hint: str | list[tuple[str, int]] | None,
) -> str | None:
    """Resolve a cursor index ``hint`` to an existing SQLite index name.

    Inspects ``sqlite_master`` for non-autoindex indexes on the collection
    table and matches raw index names, ``"_id_"``, MongoDB-style index
    names, or lists of ``(field, direction)`` tuples via stored index
    metadata or naming convention.

    Args:
        db: SQLite database connection object.
        collection_name: Name of the collection table.
        hint: Index name string or sequence of ``(field, direction)`` tuples.

    Returns:
        str | None: Matching SQLite index name, or ``None`` if no hint is
            set, fallback is forced, or no matching index exists.
    """
    from .index_manager import _load_index_keys
    from .query_helper.utils import get_force_fallback

    if hint is None or get_force_fallback():
        return None

    try:
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
            (collection_name,),
        ).fetchall()
    except Exception:
        return None

    idx_names = [
        r[0]
        for r in rows
        if isinstance(r[0], str) and not r[0].startswith("sqlite_autoindex_")
    ]
    if not idx_names:
        return None

    quoted_tbl = quote_table_name(collection_name)
    id_idx = f"idx_{quoted_tbl}_id"

    if isinstance(hint, str):
        if hint in idx_names:
            return hint
        if hint == "_id_":
            return id_idx if id_idx in idx_names else None
        target_keys = parse_mongo_index_name(hint) or [hint]
    elif isinstance(hint, (list, tuple)) and hint:
        if not all(
            isinstance(item, (list, tuple))
            and len(item) == 2
            and isinstance(item[0], str)
            for item in hint
        ):
            return None
        target_keys = [item[0] for item in hint]
        if target_keys == ["_id"]:
            return id_idx if id_idx in idx_names else None
    else:
        return None

    expected_suffix = "_" + "_".join(k.replace(".", "_") for k in target_keys)
    for idx_name in idx_names:
        if idx_name == id_idx:
            continue
        stored = _load_index_keys(db, idx_name)
        if stored is not None and stored == target_keys:
            return idx_name
        if idx_name == f"idx_{quoted_tbl}{expected_suffix}":
            return idx_name
    return None


def execute_sql_with_hint(
    db: Any,
    cmd_with_hint: str,
    cmd_without_hint: str,
    params: list[Any] | tuple[Any, ...],
) -> Any:
    """Execute a SQL query with an optional ``INDEXED BY`` fallback.

    Attempts ``cmd_with_hint`` first and retries ``cmd_without_hint`` if
    SQLite raises ``OperationalError`` indicating the hinted index cannot
    be used.

    Args:
        db: SQLite database connection object.
        cmd_with_hint: SQL query containing an ``INDEXED BY`` hint clause.
        cmd_without_hint: Equivalent SQL query without ``INDEXED BY``.
        params: Positional parameter sequence for the SQL statement.

    Returns:
        Any: Executed SQLite cursor object.

    Raises:
        sqlite3.OperationalError: Re-raised when the error is unrelated to
            index hint usability or no fallback SQL differs.
    """
    try:
        return db.execute(cmd_with_hint, params)
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if cmd_with_hint != cmd_without_hint and (
            "cannot use index" in msg
            or "no such index" in msg
            or "no query solution" in msg
        ):
            return db.execute(cmd_without_hint, params)
        raise


class Cursor:
    """Cursor for iterating over query results in a NeoSQLite collection.

    Provides a PyMongo-compatible interface supporting query filtering,
    field projection, sorting, skip/limit pagination, index hints, min/max
    index bounds, collation, Python predicate filtering, query plan
    inspection, and automatic temporary-table/generator cleanup.
    """

    def __init__(
        self,
        collection: Collection,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
        tables_to_cleanup: list[str] | None = None,
    ):
        """Initialize a new cursor instance and validate its session.

        Args:
            collection: The collection to query.
            filter: Optional MongoDB-style filter criteria dictionary.
            projection: Optional projection specification dictionary.
            hint: Optional index hint name or specification.
            session: Optional ``ClientSession`` for transaction validation.
            tables_to_cleanup: Optional list of temporary table names to drop
                when the cursor is closed.
        """
        self._collection = collection
        self._query_helpers = collection.query_engine.helpers
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint: str | list[tuple[str, int]] | None = hint
        self._comment: str | None = None
        self._min: dict[str, Any] | None = None
        self._max: dict[str, Any] | None = None
        self._collation: dict[str, Any] | None = None
        self._where_predicate: Callable[[dict[str, Any]], bool] | None = None
        self._skip = 0
        self._limit: int | None = None
        self._sort: dict[str, int] | None = None
        self._retrieved: int = 0
        self._batch_size = 101  # MongoDB-compatible default
        self._session = session
        self._tables_to_cleanup = tables_to_cleanup or []
        self._closed = False
        self._active_generator: Any = None
        self._options = 0
        self._max_await_time_ms: int | None = None
        self._exhausted = False
        self._iter: Iterator[dict[str, Any]] | None = None

        # Validate session
        validate_session(session, collection._database)

    def max_await_time_ms(self, max_await_time_ms: int | None) -> Self:
        """Set the maximum time to wait for new documents.

        Args:
            max_await_time_ms: Maximum await duration in milliseconds, or
                ``None`` to clear the setting.

        Returns:
            Self: This cursor instance for method chaining.
        """
        self._max_await_time_ms = max_await_time_ms
        return self

    def add_option(self, mask: int) -> Self:
        """Set query option flags using a bitmask.

        Args:
            mask: Bitmask of query flags to enable on this cursor.

        Returns:
            Self: This cursor instance for method chaining.
        """
        if not hasattr(self, "_options"):
            self._options = 0
        self._options |= mask
        return self

    def remove_option(self, mask: int) -> Self:
        """Unset query option flags using a bitmask.

        Args:
            mask: Bitmask of query flags to clear from this cursor.

        Returns:
            Self: This cursor instance for method chaining.
        """
        if not hasattr(self, "_options"):
            self._options = 0
        self._options &= ~mask
        return self

    @property
    def session(self) -> Any | None:
        """Get the ``ClientSession`` associated with this cursor.

        Returns:
            Any | None: Associated ``ClientSession``, or ``None`` if unset.
        """
        return getattr(self, "_session", None)

    @property
    def cursor_id(self) -> int:
        """Get the numeric identifier for this cursor.

        Returns:
            int: The cursor identifier (always ``0`` in NeoSQLite).
        """
        return 0

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Return an iterator over documents matching the cursor query.

        Returns:
            Iterator[dict[str, Any]]: Iterator yielding documents after
                filtering, sorting, pagination, and projection.
        """
        if self._closed:
            return iter(())
        if self._iter is not None and not self._exhausted:
            return self._iter
        self._exhausted = False
        self._iter = self._execute_query()
        return self._iter

    def __next__(self) -> dict[str, Any]:
        """Return the next matching document from the cursor.

        Returns:
            dict[str, Any]: Next matching document dictionary.

        Raises:
            StopIteration: When the cursor is closed or exhausted.
        """
        if self._closed or self._exhausted:
            raise StopIteration
        if self._iter is None:
            self._iter = self._execute_query()
        try:
            return next(self._iter)
        except StopIteration:
            self._exhausted = True
            self._iter = None
            raise

    def next(self) -> dict[str, Any]:
        """Return the next document in the cursor (PyMongo compatibility)."""
        return self.__next__()

    def rewind(self) -> Self:
        """Reset this cursor back to its initial unevaluated state.

        Closes any active streaming generator, drops temporary tables, and
        resets iteration state and ``retrieved`` count to ``0``.

        Returns:
            Self: This cursor instance for method chaining.
        """
        gen = getattr(self, "_active_generator", None)
        if gen is not None:
            try:
                gen.close()
            except Exception as e:
                logger.debug(f"Failed to close generator on rewind: {e}")
            self._active_generator = None
        self._cleanup_tables()
        self._iter = None
        self._retrieved = 0
        self._exhausted = False
        return self

    def limit(self, limit: int) -> Self:
        """Limit the maximum number of documents returned by this cursor.

        Args:
            limit: Maximum number of documents to return (``0`` for unlimited).

        Returns:
            Self: This cursor instance for method chaining.
        """
        self._limit = limit
        return self

    def skip(self, skip: int) -> Self:
        """Skip leading documents when iterating over this cursor.

        Args:
            skip: Number of matching documents to skip.

        Returns:
            Self: This cursor instance for method chaining.
        """
        self._skip = skip
        return self

    def sort(
        self,
        key_or_list: str | list[tuple],
        direction: int | None = None,
    ) -> Self:
        """Set the sort order for documents returned by this cursor.

        Args:
            key_or_list: Field name string or sequence of ``(key, direction)``
                tuples.
            direction: Sort direction (``ASCENDING`` or ``DESCENDING``) when
                ``key_or_list`` is a string; defaults to ``ASCENDING``.

        Returns:
            Self: This cursor instance for method chaining.
        """
        if isinstance(key_or_list, str):
            self._sort = {key_or_list: direction or ASCENDING}
        else:
            self._sort = dict(key_or_list)
        return self

    def batch_size(self, size: int) -> Self:
        """Set the number of SQLite rows fetched per internal batch.

        Args:
            size: Positive integer batch size (default is ``101``).

        Returns:
            Self: This cursor instance for method chaining.

        Raises:
            ValueError: If ``size`` is less than ``1``.

        Example:
            >>> cursor = collection.find().batch_size(50)
        """
        if size < 1:
            raise ValueError("batch_size must be at least 1")
        self._batch_size = size
        return self

    def hint(self, hint: str | list[tuple[str, int]]) -> Self:
        """Set the index hint for query execution.

        Args:
            hint: Index name string or list of ``(field, direction)`` tuples.

        Returns:
            Self: This cursor instance for method chaining.
        """
        self._hint = hint  # type: ignore[assignment]
        return self

    def min(self, min_spec: list[tuple[str, Any]] | tuple[str, Any]) -> Self:
        """Set the inclusive lower bound for index queries.

        Restricts results to documents whose specified field values are
        greater than or equal to the given bounds.

        Args:
            min_spec: Sequence of ``(field, value)`` pairs defining the
                inclusive lower bound, e.g., ``[("age", 18)]``.

        Returns:
            Self: This cursor instance for method chaining.

        Raises:
            TypeError: If ``min_spec`` is not a ``list`` or ``tuple``.

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}}).min(
            ...     [("age", 18)]
            ... )
        """
        if not isinstance(min_spec, (list, tuple)):
            raise TypeError(
                f"spec must be an instance of list or tuple, not {type(min_spec)}"
            )
        self._min = dict(min_spec)  # type: ignore[arg-type]
        return self

    def max(self, max_spec: list[tuple[str, Any]] | tuple[str, Any]) -> Self:
        """Set the exclusive upper bound for index queries.

        Restricts results to documents whose specified field values are
        strictly less than the given bounds.

        Args:
            max_spec: Sequence of ``(field, value)`` pairs defining the
                exclusive upper bound, e.g., ``[("age", 65)]``.

        Returns:
            Self: This cursor instance for method chaining.

        Raises:
            TypeError: If ``max_spec`` is not a ``list`` or ``tuple``.

        Example:
            >>> cursor = collection.find({"age": {"$lte": 65}}).max(
            ...     [("age", 65)]
            ... )
        """
        if not isinstance(max_spec, (list, tuple)):
            raise TypeError(
                f"spec must be an instance of list or tuple, not {type(max_spec)}"
            )
        self._max = dict(max_spec)  # type: ignore[arg-type]
        return self

    def collation(self, collation: dict[str, Any]) -> Self:
        """Set the string collation configuration for this cursor.

        Allows specifying locale and comparison strength options for string
        comparisons and sorting.

        Args:
            collation: Collation specification dictionary (e.g., ``locale``,
                ``caseLevel``, ``caseFirst``, ``strength``,
                ``numericOrdering``, ``alternate``, ``backwards``).

        Returns:
            Self: This cursor instance for method chaining.

        Example:
            >>> cursor = collection.find({"name": {"$gte": "A"}}).collation(
            ...     {"locale": "fr_FR", "strength": 2}
            ... )

        Note:
            NeoSQLite maps default/unknown locales to ``BINARY``,
            case-insensitive settings to ``NOCASE``, and supports custom
            collations registered on the connection.
        """
        self._collation = collation
        return self

    def where(self, predicate: Callable[[dict[str, Any]], bool]) -> Self:
        """Filter cursor results using a Python predicate callable.

        Applies ``predicate`` in Python (Tier-3) after documents are retrieved
        from SQLite, disabling SQL-level pagination so all candidates are
        evaluated first.

        Args:
            predicate: Callable receiving a document dictionary and returning
                ``True`` if the document should be kept.

        Returns:
            Self: This cursor instance for method chaining.

        Example:
            >>> def is_high_value(doc):
            ...     return doc.get("value", 0) > 10
            >>> cursor = collection.find({}).where(is_high_value)
        """
        self._where_predicate = predicate
        return self

    def comment(self, comment: str) -> Self:
        """Attach a SQL comment string to the generated query for profiling.

        Args:
            comment: Comment text sanitized and prepended as ``/* ... */``
                to executed SQL queries.

        Returns:
            Self: This cursor instance for method chaining.

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}}).comment(
            ...     "find adults"
            ... )
        """
        self._comment = comment
        return self

    def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        """Materialize documents from the cursor into a list.

        Args:
            length: Maximum number of documents to collect. If ``None``, all
                matching documents are returned; negative values yield ``[]``.

        Returns:
            list[dict[str, Any]]: Collected matching documents.

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}})
            >>> adults = cursor.to_list()
            >>> first_5 = cursor.to_list(5)
        """
        if self._closed or self._exhausted:
            return []
        if length is None:
            return list(self)
        if length <= 0:
            return []
        # islice bounds work/memory to exactly `length` docs (#153)
        return list(islice(self, length))

    def clone(self) -> Self:
        """Create an unevaluated copy of this cursor with identical options.

        Deep-copies filter, projection, hint, sort, min, max, and collation
        settings while preserving pagination, batch size, options, await
        timeout, comment, session, and ``_where_predicate`` with
        ``retrieved`` reset to ``0``.

        Returns:
            Self: New cursor instance with the same query configuration.

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}}).limit(10)
            >>> clone = cursor.clone()
            >>> results1 = list(cursor)
            >>> results2 = list(clone)
        """
        cloned = self.__class__(
            self._collection,
            filter=deepcopy(self._filter),
            projection=deepcopy(self._projection),
            hint=deepcopy(self._hint),  # type: ignore[arg-type]
        )
        cloned._skip = self._skip
        cloned._limit = self._limit
        cloned._batch_size = self._batch_size
        cloned._options = self._options
        cloned._max_await_time_ms = self._max_await_time_ms
        cloned._sort = deepcopy(self._sort) if self._sort else None
        cloned._comment = self._comment
        cloned._min = deepcopy(self._min) if self._min else None
        cloned._max = deepcopy(self._max) if self._max else None
        cloned._collation = (
            deepcopy(self._collation) if self._collation else None
        )
        cloned._where_predicate = (
            self._where_predicate
        )  # Functions can't be deep copied
        cloned._session = self._session
        cloned._retrieved = 0  # Clone starts fresh
        return cloned

    @property
    def retrieved(self) -> int:
        """Return the number of documents yielded by this cursor so far.

        Returns:
            int: Count of documents retrieved during iteration.
        """
        return self._retrieved

    @property
    def alive(self) -> bool:
        """Check whether the cursor may still have documents to yield.

        Returns:
            bool: ``False`` once closed, iteration completes, or ``_limit``
                documents have been retrieved; ``True`` otherwise.
        """
        if self._closed or self._exhausted:
            return False
        if self._limit is not None and self._limit != 0:
            return self._retrieved < abs(self._limit)
        return True

    @property
    def collection(self):
        """Return the collection this cursor queries.

        Returns:
            Collection: The collection associated with this cursor.
        """
        return self._collection

    @property
    def address(self) -> tuple | None:
        """Return the SQLite database address tuple after iteration begins.

        Returns:
            tuple | None: ``None`` before iteration starts; otherwise
                ``("sqlite::memory:", 0)`` for in-memory databases or
                ``("sqlite://<path>", 0)`` for file-backed databases.
        """
        # Match PyMongo behavior: None before iteration, tuple after
        if self._retrieved == 0 and not hasattr(self, "_iterated"):
            return None

        # Get database path from connection
        db_name = self._collection.db.execute(
            "PRAGMA database_list"
        ).fetchone()[2]
        if db_name == ":memory:" or db_name == "":
            return ("sqlite::memory:", 0)
        else:
            return (f"sqlite://{db_name}", 0)

    def _parse_mongo_index_name(self, name: str) -> list[str] | None:
        """Delegate MongoDB index name parsing to ``parse_mongo_index_name``."""
        return parse_mongo_index_name(name)

    def _resolve_hint_index(self) -> str | None:
        """Delegate index hint resolution to ``resolve_hint_index``."""
        return resolve_hint_index(
            self._collection.db, self._collection.name, self._hint
        )

    def _build_sql_projection_expr(
        self, sql_sorted: bool, data_col: str = "data"
    ) -> str | None:
        """Build a SQL ``SELECT`` expression for simple top-level projections.

        Handles pure inclusion or pure exclusion projections of up to 40
        non-``_id`` top-level fields without dotted paths, ``$`` operators, or
        unsafe characters, when fallback mode, ``_where_predicate``, and
        Python-only sorting do not prevent SQL projection.

        Args:
            sql_sorted: Whether sorting (if any) was handled in SQL.
            data_col: SQL column or expression containing the JSON document.

        Returns:
            str | None: SQL expression yielding the projected JSON text, or
                ``None`` if Python projection is required.
        """
        from .query_helper.utils import get_force_fallback

        if (
            get_force_fallback()
            or not self._projection
            or self._where_predicate is not None
        ):
            return None
        if self._sort and not sql_sorted:
            return None

        for k, v in self._projection.items():
            if not isinstance(k, str) or not k:
                return None
            if "." in k or k.startswith("$"):
                return None
            if any(ch in k for ch in ("\x00", "\\", "'", '"')):
                return None
            if not isinstance(v, (int, bool)) or v not in (0, 1):
                return None

        other_items = [
            (k, int(v)) for k, v in self._projection.items() if k != "_id"
        ]
        if len(other_items) > 40:
            return None

        jsonb = self._collection.query_engine.jsonb
        ext_fn = jsonb.function("extract")
        rm_fn = jsonb.function("remove")
        include_id = int(self._projection.get("_id", 1)) == 1

        if not other_items:
            if include_id:
                return "'{}'"
            return jsonb.json_text(f"{rm_fn}({data_col}, '$._id')")

        has_inc = any(v == 1 for _, v in other_items)
        has_exc = any(v == 0 for _, v in other_items)
        if has_inc and has_exc:
            return None

        if has_inc:
            kv_pairs: list[str] = []
            missing_removals: list[str] = []
            for key, _ in other_items:
                path_str = parse_json_path(key)
                p_lit = f"'{path_str}'"
                k_lit = f"'{key}'"
                val_expr = (
                    f"CASE json_type({data_col}, {p_lit}) "
                    f"WHEN 'true' THEN json('true') "
                    f"WHEN 'false' THEN json('false') "
                    f"ELSE {ext_fn}({data_col}, {p_lit}) END"
                )
                kv_pairs.append(f"{k_lit}, {val_expr}")
                missing_removals.append(
                    f"CASE WHEN json_type({data_col}, {p_lit}) IS NULL "
                    f"THEN {p_lit} ELSE '$.__neosqlite_noop__' END"
                )
            obj_sql = f"json_object({', '.join(kv_pairs)})"
            return f"json_remove({obj_sql}, {', '.join(missing_removals)})"

        remove_paths = [f"'{parse_json_path(k)}'" for k, _ in other_items]
        if not include_id:
            remove_paths.append("'$._id'")
        return jsonb.json_text(
            f"{rm_fn}({data_col}, {', '.join(remove_paths)})"
        )

    def _execute_sql_with_hint(
        self,
        cmd_with_hint: str,
        cmd_without_hint: str,
        params: list[Any] | tuple[Any, ...],
    ) -> Any:
        """Delegate hinted SQL execution to ``execute_sql_with_hint``."""
        return execute_sql_with_hint(
            self._collection.db, cmd_with_hint, cmd_without_hint, params
        )

    def explain(self, verbosity: str = "queryPlanner") -> dict[str, Any]:
        """Return the SQLite query execution plan for this cursor.

        Uses SQLite's ``EXPLAIN QUERY PLAN`` to inspect query stages and index
        usage, optionally running the query to populate ``executionStats``.

        Args:
            verbosity: Plan verbosity level, either ``"queryPlanner"`` or
                ``"executionStats"``. Defaults to ``"queryPlanner"``.

        Returns:
            dict[str, Any]: Plan dictionary containing ``queryPlanner`` (with
                ``winningPlan`` and ``indexUsage``), ``executionStats`` when
                ``verbosity == "executionStats"``, or ``error`` on failure.

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}})
            >>> plan = cursor.explain()
            >>> print(plan["queryPlanner"]["winningPlan"])
        """
        # Build the SQL query that would be executed
        if self._query_helpers._is_text_search_query(self._filter):
            fts_res = self._query_helpers._build_full_fts_query(
                self._filter,
                self._sort,
                self._collation,
                self._limit,
                self._skip,
            )
            if fts_res is not None:
                sql, params = fts_res
            else:
                where_result = self._query_helpers._build_simple_where_clause(
                    self._filter
                )
                sort_clause = self._query_helpers._build_sort_clause(
                    self._sort, self._collation
                )
                pagination_clause = (
                    self._query_helpers._build_pagination_clause(
                        self._limit, self._skip
                    )
                )
                if where_result is not None:
                    where_clause, params, tables = where_result
                    jsonb = self._collection.query_engine.jsonb
                    sql = (
                        f"SELECT id, _id, {jsonb.data_column()} as data "
                        f"FROM {quote_table_name(self._collection.name)} {where_clause}{sort_clause}{pagination_clause}"
                    )
                else:
                    jsonb = self._collection.query_engine.jsonb
                    sql = (
                        f"SELECT id, _id, {jsonb.data_column()} as data "
                        f"FROM {quote_table_name(self._collection.name)}{sort_clause}{pagination_clause}"
                    )
                    params = []
        else:
            where_result = self._query_helpers._build_simple_where_clause(
                self._filter
            )

            # Build sorting and pagination clauses for SQL
            sort_clause = self._query_helpers._build_sort_clause(
                self._sort, self._collation
            )
            pagination_clause = self._query_helpers._build_pagination_clause(
                self._limit, self._skip
            )

            if where_result is not None:
                where_clause, params, tables = where_result
                jsonb = self._collection.query_engine.jsonb
                table_name = quote_table_name(self._collection.name)
                hint_idx = self._resolve_hint_index()
                indexed_by = (
                    f" INDEXED BY {quote_table_name(hint_idx)}"
                    if hint_idx
                    else ""
                )
                sql_plain = (
                    f"SELECT id, _id, {jsonb.data_column()} as data "
                    f"FROM {table_name} {where_clause}{sort_clause}{pagination_clause}"
                )
                sql = (
                    f"SELECT id, _id, {jsonb.data_column()} as data "
                    f"FROM {table_name}{indexed_by} {where_clause}{sort_clause}{pagination_clause}"
                )
            else:
                # No filter - simple select
                jsonb = self._collection.query_engine.jsonb
                table_name = quote_table_name(self._collection.name)
                hint_idx = self._resolve_hint_index()
                indexed_by = (
                    f" INDEXED BY {quote_table_name(hint_idx)}"
                    if hint_idx
                    else ""
                )
                sql_plain = (
                    f"SELECT id, _id, {jsonb.data_column()} as data "
                    f"FROM {table_name}{sort_clause}{pagination_clause}"
                )
                sql = (
                    f"SELECT id, _id, {jsonb.data_column()} as data "
                    f"FROM {table_name}{indexed_by}{sort_clause}{pagination_clause}"
                )
                params = []

        # Get the query plan from SQLite
        try:
            try:
                plan_rows = self._collection.db.execute(
                    f"EXPLAIN QUERY PLAN {sql}", params
                ).fetchall()
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "sql_plain" in locals() and (
                    "cannot use index" in msg
                    or "no such index" in msg
                    or "no query solution" in msg
                ):
                    plan_rows = self._collection.db.execute(
                        f"EXPLAIN QUERY PLAN {sql_plain}", params
                    ).fetchall()
                else:
                    raise

            # Parse the plan rows
            # Row format: (id, parent, notused, detail)
            winning_plan = []
            index_usage = []

            for row in plan_rows:
                detail = row[3] if len(row) > 3 else str(row)
                winning_plan.append({"detail": detail})

                # Extract index usage information
                if "USING INDEX" in detail or "USING COVERING INDEX" in detail:
                    index_usage.append({"detail": detail})

            result: dict[str, Any] = {
                "queryPlanner": {
                    "winningPlan": winning_plan,
                    "indexUsage": index_usage,
                }
            }

            # Add execution stats if requested
            if verbosity == "executionStats":
                # Actually execute the query to get stats
                start_time = time.time()
                results = list(self)
                execution_time_ms: float = (time.time() - start_time) * 1000

                result["executionStats"] = {
                    "nReturned": len(results),
                    "executionTimeMillis": round(execution_time_ms, 2),
                }

            return result

        except Exception as e:
            logger.debug(f"{e=}")
            return {
                "queryPlanner": {
                    "winningPlan": [{"detail": f"Error getting plan: {e}"}],
                    "indexUsage": [],
                },
                "error": str(e),
            }

    def _execute_query(self) -> Iterator[dict[str, Any]]:
        """Execute the cursor query and yield processed documents.

        Validates the session, fetches filtered documents, applies Python
        sorting, pagination, and projection for any stage not handled by SQL,
        strips transient ``_textScore`` fields unless projected, increments
        ``_retrieved``, and marks the cursor exhausted when done.

        Yields:
            dict[str, Any]: Each matching document dictionary.
        """
        if self._closed:
            return
        validate_session(self._session, self._collection._database)

        # track if SQL handled sorting, pagination, and projection
        self._sql_handled_sort = False
        self._sql_handled_pagination = False
        self._sql_handled_projection = False

        # Get the documents based on filter
        docs = self._get_filtered_documents()

        # Apply sorting if not handled by SQL
        if self._sort and not self._sql_handled_sort:
            docs = self._apply_sorting(docs)

        # Apply skip and limit if not handled by SQL
        if not self._sql_handled_pagination and (
            self._skip != 0 or (self._limit is not None and self._limit != 0)
        ):
            docs = self._apply_pagination(docs)

        # Apply projection if not handled by SQL
        if self._projection and not self._sql_handled_projection:
            docs = self._apply_projection(docs)

        # Mark cursor as having been iterated (for address property)
        self._iterated = True

        # Yield results and track count
        for doc in docs:
            if self._closed:
                return
            if "_textScore" in doc and (
                not self._projection or "_textScore" not in self._projection
            ):
                doc.pop("_textScore", None)
            self._retrieved += 1
            yield doc

        # Mark as exhausted
        self._exhausted = True
        self._iter = None

    def _get_filtered_documents(self) -> Iterable[dict[str, Any]]:
        """Retrieve documents matching ``self._filter`` using optimal tiers.

        Dispatches to ``DateTimeQueryProcessor`` for datetime queries,
        ``_handle_expr_query`` for ``$expr``, FTS5 queries for ``$text``,
        SQL ``WHERE``/sort/pagination/projection queries when translatable, or
        ``_handle_python_fallback`` otherwise. Applies ``_where_predicate``
        and notifies the query engine of the selected execution tier.

        Returns:
            Iterable[dict[str, Any]]: Iterable of documents matching the query
                filter (and optional ``_where_predicate``).
        """
        # Check if this is a datetime query that should use the specialized processor
        if self._contains_datetime_operations(self._filter):
            # Use the datetime query processor for datetime-specific queries
            try:
                from .datetime_query_processor import DateTimeQueryProcessor

                datetime_processor = DateTimeQueryProcessor(
                    self._collection, use_global_kill_switch=True
                )
                dt_docs: Iterable[dict[str, Any]] = (
                    datetime_processor.process_datetime_query(self._filter)
                )
                if self._min or self._max:
                    dt_docs = (d for d in dt_docs if self._matches_minmax(d))
                if self._where_predicate:
                    predicate = self._where_predicate
                    dt_docs = (d for d in dt_docs if predicate(d))
                return dt_docs
            except Exception as e:
                # If datetime processor fails, fall back to normal processing
                logger.debug(f"DateTime processor failed, falling back: {e}")
                pass

        # Special handling for $expr queries
        if "$expr" in self._filter:
            return self._handle_expr_query()

        from .query_helper.utils import get_force_fallback

        # Special handling for $text queries with native BM25 score calculation
        if (
            not get_force_fallback()
            and self._query_helpers._is_text_search_query(self._filter)
        ):
            has_python_post_filter = bool(
                self._where_predicate or self._min or self._max
            )
            fts_query_result = self._query_helpers._build_full_fts_query(
                self._filter,
                self._sort,
                self._collation,
                self._limit if not has_python_post_filter else 0,
                self._skip if not has_python_post_filter else 0,
            )
            if fts_query_result is not None:
                cmd, params = fts_query_result
                if self._sort:
                    self._sql_handled_sort = True
                if not has_python_post_filter and (self._limit or self._skip):
                    self._sql_handled_pagination = True

                if self._comment:
                    safe_comment = _sanitize_sql_comment(self._comment)
                    cmd = f"/* {safe_comment} */ {cmd}"

                db_cursor = self._collection.db.execute(cmd, params)

                def doc_generator():
                    """Stream FTS query results in batches and close cursor.

                    Yields:
                        dict[str, Any]: Loaded document dictionaries.
                    """
                    try:
                        while True:
                            rows = db_cursor.fetchmany(self._batch_size)
                            if not rows:
                                break
                            yield from self._load_documents(rows)
                    finally:
                        db_cursor.close()

                fts_docs: Iterable[dict[str, Any]] = doc_generator()
                self._active_generator = fts_docs
                if self._min or self._max:
                    fts_docs = (d for d in fts_docs if self._matches_minmax(d))
                if self._where_predicate:
                    predicate = self._where_predicate
                    fts_docs = (d for d in fts_docs if predicate(d))
                qe = getattr(self._collection, "query_engine", None)
                if qe is not None and hasattr(qe, "_notify_tier_change"):
                    tier = "tier3" if has_python_post_filter else "tier1"
                    qe._notify_tier_change(tier, [self._filter])
                return fts_docs

        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        docs: Iterable[dict[str, Any]]
        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params, tables = where_result

            # Track tables for cleanup
            if tables:
                self._tables_to_cleanup.extend(tables)

            # Add min/max bounds if specified
            if self._min or self._max:
                minmax_clause, minmax_params = self._build_minmax_clause(
                    where_clause, tuple(params), self._min, self._max
                )
                where_clause = minmax_clause
                params = minmax_params  # type: ignore[assignment]

            # Build sorting and pagination clauses for SQL
            sort_clause = self._query_helpers._build_sort_clause(
                self._sort, self._collation
            )
            # If we have a where predicate (Python filter), we CANNOT do SQL pagination
            # because we need to see all documents that matched the SQL filter first.
            if self._where_predicate:
                pagination_clause = ""
            else:
                pagination_clause = (
                    self._query_helpers._build_pagination_clause(
                        self._limit, self._skip
                    )
                )

            # Track which parts were handled by SQL
            if sort_clause:
                self._sql_handled_sort = True
            if pagination_clause:
                self._sql_handled_pagination = True

            # Use the collection's JSONB support flag to determine how to select data
            jsonb = self._collection.query_engine.jsonb
            table_name = quote_table_name(self._collection.name)
            hint_idx = self._resolve_hint_index()
            indexed_by = (
                f" INDEXED BY {quote_table_name(hint_idx)}" if hint_idx else ""
            )
            proj_expr = self._build_sql_projection_expr(
                self._sql_handled_sort, data_col="data"
            )
            if proj_expr is not None:
                select_data = f"{proj_expr} as data"
                self._sql_handled_projection = True
            else:
                select_data = f"{jsonb.data_column()} as data"

            suffix = f" {where_clause}{sort_clause}{pagination_clause}"
            cmd_without_hint = (
                f"SELECT id, _id, {select_data} FROM {table_name}{suffix}"
            )
            cmd_with_hint = (
                f"SELECT id, _id, {select_data} "
                f"FROM {table_name}{indexed_by}{suffix}"
            )

            # Add comment if specified
            if self._comment:
                safe_comment = _sanitize_sql_comment(self._comment)
                cmd_without_hint = f"/* {safe_comment} */ {cmd_without_hint}"
                cmd_with_hint = f"/* {safe_comment} */ {cmd_with_hint}"

            db_cursor = self._execute_sql_with_hint(
                cmd_with_hint, cmd_without_hint, params
            )

            def doc_generator():
                """Stream SQL query results in batches and close cursor.

                Yields:
                    dict[str, Any]: Loaded document dictionaries.
                """
                try:
                    while True:
                        rows = db_cursor.fetchmany(self._batch_size)
                        if not rows:
                            break
                        yield from self._load_documents(rows)
                finally:
                    # Release the sqlite statement even when iteration is
                    # abandoned early (#135)
                    db_cursor.close()

            docs = doc_generator()
            self._active_generator = docs
            tier = "tier2" if tables else "tier1"
        else:
            # Fall back to Python-based filtering
            docs = self._handle_python_fallback()
            tier = "tier3"

        # Apply where predicate if specified (Tier-3 Python filtering)
        if self._where_predicate:
            docs = filter(self._where_predicate, docs)  # type: ignore[arg-type]
            tier = "tier3"

        qe = getattr(self._collection, "query_engine", None)
        if qe is not None and hasattr(qe, "_notify_tier_change"):
            qe._notify_tier_change(tier, [self._filter])

        return docs

    def _handle_python_fallback(self) -> Iterable[dict[str, Any]]:
        """Filter collection documents in Python for complex queries.

        Fetches rows in batches (using SQL sort and min/max bounds unless
        ``get_force_fallback()`` is active) and yields each decoded document
        satisfying ``_apply_query`` and any Python-side min/max bounds.

        Yields:
            dict[str, Any]: Matching document dictionaries.
        """
        from .query_helper.utils import get_force_fallback

        force_fallback = get_force_fallback()
        # Build sorting and pagination clauses for SQL
        # Even in fallback mode, we can still use SQL to sort if possible
        sort_clause = (
            ""
            if force_fallback
            else self._query_helpers._build_sort_clause(
                self._sort, self._collation
            )
        )

        # Use the collection's JSONB support flag to determine how to select data
        jsonb = self._collection.query_engine.jsonb
        cmd = (
            f"SELECT id, _id, {jsonb.data_column()} as data "
            f"FROM {quote_table_name(self._collection.name)}"
        )

        # Add min/max bounds if specified (no filter case)
        if not force_fallback and (self._min or self._max):
            minmax_clause, minmax_params = self._build_minmax_clause(
                "", (), self._min, self._max
            )
            # Remove leading " WHERE" if present and add our own
            minmax_clause = minmax_clause.strip().removeprefix("WHERE").lstrip()
            cmd = f"{cmd} WHERE {minmax_clause}"
            params = minmax_params
        else:
            params = ()

        # Append sort clause (SQL pagination cannot be used in fallback mode
        # because Python filtering may exclude rows)
        cmd = f"{cmd}{sort_clause}"

        # Track which parts were handled by SQL
        if sort_clause:
            self._sql_handled_sort = True

        # Add comment if specified
        if self._comment:
            safe_comment = _sanitize_sql_comment(self._comment)
            cmd = f"/* {safe_comment} */ {cmd}"

        db_cursor = self._collection.db.execute(cmd, params)
        query_context: dict[str, Any] = {}
        apply = partial(
            self._query_helpers._apply_query,
            self._filter,
            _context=query_context,
        )
        check_minmax = force_fallback and bool(self._min or self._max)

        while True:
            rows = db_cursor.fetchmany(self._batch_size)
            if not rows:
                break
            for doc in self._load_documents(rows):
                if (not check_minmax or self._matches_minmax(doc)) and apply(
                    doc
                ):
                    yield doc

    def _handle_expr_query(self) -> Iterable[dict[str, Any]]:
        """Execute ``$expr`` queries via SQL tiers or Python fallback.

        Attempts SQL evaluation using ``_build_expr_where_clause`` (Tier 2
        ``SELECT`` with temporary tables or Tier 1 ``WHERE`` clause). When
        not translatable, falls back to Tier 3 Python evaluation over fetched
        documents.

        Returns:
            Iterable[dict[str, Any]]: List or filtered iterator of documents
                matching the ``$expr`` filter and min/max bounds.
        """
        # Try to build SQL query/WHERE clause with query helper
        # This handles Tier 1 (WHERE) and Tier 2 (Full SELECT with temp tables)
        expr_result = self._query_helpers._build_expr_where_clause(self._filter)

        if expr_result is not None:
            # Use SQL-based evaluation
            sql_or_where, params, tables = expr_result

            # Track tables for cleanup
            if tables:
                self._tables_to_cleanup.extend(tables)

            qe = getattr(self._collection, "query_engine", None)
            if sql_or_where.strip().upper().startswith("SELECT"):
                if qe is not None and hasattr(qe, "_notify_tier_change"):
                    qe._notify_tier_change("tier2", [self._filter])
                # Tier 2: full SELECT query with temp tables
                sort_clause = self._query_helpers._build_sort_clause(
                    self._sort,
                    self._collation,
                    table_name=self._collection.name,
                )
                has_minmax = bool(self._min or self._max)
                if self._where_predicate or has_minmax:
                    pagination_clause = ""
                else:
                    pagination_clause = (
                        self._query_helpers._build_pagination_clause(
                            self._limit, self._skip
                        )
                    )
                cmd_with_hint = cmd_without_hint = (
                    f"{sql_or_where}{sort_clause}{pagination_clause}"
                )
                self._sql_handled_sort = bool(sort_clause)
                self._sql_handled_pagination = bool(pagination_clause)
            else:
                if qe is not None and hasattr(qe, "_notify_tier_change"):
                    qe._notify_tier_change("tier1", [self._filter])
                # Tier 1: WHERE clause only
                where_clause = sql_or_where
                if self._min or self._max:
                    minmax_clause, minmax_params = self._build_minmax_clause(
                        where_clause, tuple(params), self._min, self._max
                    )
                    where_clause = minmax_clause
                    params = minmax_params  # type: ignore[assignment]

                # Build sorting and pagination clauses for SQL
                sort_clause = self._query_helpers._build_sort_clause(
                    self._sort, self._collation
                )
                # If we have a where predicate (Python filter), we CANNOT do SQL pagination
                if self._where_predicate:
                    pagination_clause = ""
                else:
                    pagination_clause = (
                        self._query_helpers._build_pagination_clause(
                            self._limit, self._skip
                        )
                    )

                if sort_clause:
                    self._sql_handled_sort = True
                if pagination_clause:
                    self._sql_handled_pagination = True

                jsonb = self._collection.query_engine.jsonb
                table_name = quote_table_name(self._collection.name)
                hint_idx = self._resolve_hint_index()
                indexed_by = (
                    f" INDEXED BY {quote_table_name(hint_idx)}"
                    if hint_idx
                    else ""
                )
                proj_expr = self._build_sql_projection_expr(
                    self._sql_handled_sort, data_col="data"
                )
                if proj_expr is not None:
                    select_data = f"{proj_expr} as data"
                    self._sql_handled_projection = True
                else:
                    select_data = f"{jsonb.data_column()} as data"

                suffix = f" {where_clause}{sort_clause}{pagination_clause}"
                cmd_without_hint = (
                    f"SELECT id, _id, {select_data} FROM {table_name}{suffix}"
                )
                cmd_with_hint = (
                    f"SELECT id, _id, {select_data} "
                    f"FROM {table_name}{indexed_by}{suffix}"
                )

            # Add comment if specified
            if self._comment:
                safe_comment = _sanitize_sql_comment(self._comment)
                cmd_without_hint = f"/* {safe_comment} */ {cmd_without_hint}"
                cmd_with_hint = f"/* {safe_comment} */ {cmd_with_hint}"
            db_cursor = self._execute_sql_with_hint(
                cmd_with_hint, cmd_without_hint, params
            )
            # Use fetchmany for memory-efficient batch fetching
            docs: list[dict[str, Any]] = []
            try:
                while True:
                    rows = db_cursor.fetchmany(self._batch_size)
                    if not rows:
                        break
                    docs.extend(self._load_documents(rows))
            finally:
                db_cursor.close()
            if (
                self._min or self._max
            ) and sql_or_where.strip().upper().startswith("SELECT"):
                docs = [d for d in docs if self._matches_minmax(d)]
            return docs
        else:
            # Fallback to Python evaluation
            qe = getattr(self._collection, "query_engine", None)
            if qe is not None and hasattr(qe, "_notify_tier_change"):
                qe._notify_tier_change("tier3", [self._filter])
            from .query_helper.utils import get_force_fallback

            force_fallback = get_force_fallback()
            # Build sorting clause for SQL even in fallback
            sort_clause = (
                ""
                if force_fallback
                else self._query_helpers._build_sort_clause(
                    self._sort, self._collation
                )
            )
            # Cannot do SQL pagination for expression fallback
            pagination_clause = ""

            # Get all documents
            jsonb = self._collection.query_engine.jsonb
            cmd = (
                f"SELECT id, _id, {jsonb.data_column()} as data "
                f"FROM {quote_table_name(self._collection.name)}{sort_clause}{pagination_clause}"
            )

            # Track which parts were handled by SQL
            if sort_clause:
                self._sql_handled_sort = True
            if pagination_clause:
                self._sql_handled_pagination = True

            # Add comment if specified
            if self._comment:
                safe_comment = _sanitize_sql_comment(self._comment)
                cmd = f"/* {safe_comment} */ {cmd}"

            db_cursor = self._collection.db.execute(cmd)
            # Use fetchmany for memory-efficient batch fetching
            all_docs: list[dict[str, Any]] = []
            while True:
                rows = db_cursor.fetchmany(self._batch_size)
                if not rows:
                    break
                all_docs.extend(self._load_documents(rows))

            # Filter documents using $expr Python evaluation
            has_minmax = bool(self._min or self._max)
            expr_context: dict[str, Any] = {}

            def expr_filter(doc: dict[str, Any]) -> bool:
                """Evaluate the full ``$expr`` filter and bounds on a document.

                Args:
                    doc: The document dictionary to evaluate.

                Returns:
                    bool: ``True`` if ``doc`` satisfies ``self._filter`` and
                        min/max bounds, ``False`` otherwise or on error.
                """
                try:
                    if not self._query_helpers._apply_query(
                        self._filter, doc, _context=expr_context
                    ):
                        return False
                    return not has_minmax or self._matches_minmax(doc)
                except Exception as e:
                    logger.warning(f"$expr evaluation failed for document: {e}")
                    return False

            return filter(expr_filter, all_docs)  # type: ignore[arg-type]

    def _matches_minmax(self, doc: dict[str, Any]) -> bool:
        """Check cursor ``min`` (>=) and ``max`` (<) bounds in Python.

        Args:
            doc: Document dictionary to test against configured bounds.

        Returns:
            bool: ``True`` if ``doc`` satisfies all ``_min`` and ``_max``
                bounds using BSON sort ordering, ``False`` otherwise.
        """
        if self._min:
            for field, bound in self._min.items():
                val = self._collection._get_val(doc, field)
                if val is None or bson_sort_key(val) < bson_sort_key(bound):
                    return False
        if self._max:
            for field, bound in self._max.items():
                val = self._collection._get_val(doc, field)
                if val is None or bson_sort_key(val) >= bson_sort_key(bound):
                    return False
        return True

    def _build_minmax_clause(
        self,
        where_clause: str,
        params: tuple,
        min_spec: dict[str, Any] | None,
        max_spec: dict[str, Any] | None,
    ) -> tuple:
        """Build an augmented SQL ``WHERE`` clause for min/max index bounds.

        Args:
            where_clause: Existing ``WHERE`` clause string (may be empty).
            params: Existing SQL parameter tuple.
            min_spec: Inclusive minimum bound mapping, or ``None``.
            max_spec: Exclusive maximum bound mapping, or ``None``.

        Returns:
            tuple: Pair of ``(new_where_clause, new_params_tuple)``.
        """
        additional_conditions = []
        additional_params = list(params)

        jsonb = self._collection.query_engine.jsonb
        json_func = jsonb.json_function_prefix

        # Add minimum bounds (>=) and maximum bounds (<)
        for spec, op in ((min_spec, ">="), (max_spec, "<")):
            if not spec:
                continue
            for field, value in spec.items():
                if field == "_id":
                    # _id lives in a dedicated column, not inside data (#113)
                    additional_conditions.append(f"_id {op} ?")
                    param_val = (
                        str(value) if isinstance(value, ObjectId) else value
                    )
                else:
                    json_path = parse_json_path(field)
                    param_val, placeholder = serialize_sql_query_value(
                        value, jsonb.jsonb_supported
                    )
                    additional_conditions.append(
                        f"{json_func}_extract(data, '{json_path}') {op} {placeholder}"
                    )
                additional_params.append(param_val)

        if additional_conditions:
            conditions_sql = " AND ".join(additional_conditions)
            # If there's an existing WHERE clause, append to it
            if where_clause and where_clause.strip():
                base_clause = where_clause.rstrip()
                new_clause = f"{base_clause} AND {conditions_sql}"
            else:
                # No existing WHERE, create new one
                new_clause = f"WHERE {conditions_sql}"
            return new_clause, tuple(additional_params)

        return where_clause, params

    def _contains_datetime_operations(self, query: dict[str, Any]) -> bool:
        """Check if a query contains datetime-related filter conditions.

        Delegates to :func:`datetime_utils.contains_datetime_operations`.
        """
        from .datetime_utils import contains_datetime_operations

        return contains_datetime_operations(query)

    def _is_datetime_value(self, value: Any) -> bool:
        """Check if a value is a datetime object or datetime string.

        Args:
            value: Value to check.

        Returns:
            bool: ``True`` if ``value`` is datetime-related, ``False``
                otherwise.
        """
        from .datetime_utils import is_datetime_value

        return is_datetime_value(value)

    def _is_datetime_regex(self, pattern: str) -> bool:
        """Check if a regex pattern appears intended for datetime matching.

        Args:
            pattern: Regex pattern string to inspect.

        Returns:
            bool: ``True`` if ``pattern`` is likely datetime-related,
                ``False`` otherwise.
        """
        from .datetime_utils import is_datetime_regex

        return is_datetime_regex(pattern)

    def _load_documents(self, rows) -> Iterable[dict[str, Any]]:
        """Decode SQLite result rows into document dictionaries.

        Reconstructs each document via ``_load_with_stored_id``, removes
        ``_id`` when excluded by a SQL-handled projection, and attaches
        ``_textScore`` when a fourth score column is present.

        Args:
            rows: Iterable of SQLite rows containing ``(id, _id, data)`` and
                an optional fourth FTS score column.

        Yields:
            dict[str, Any]: Loaded document dictionary for each row.
        """
        for row in rows:
            id_val = row[0]
            stored_id_val = row[1]
            data_val = row[2]
            # Use the collection's _load method which now handles both id and _id
            doc = self._collection._load_with_stored_id(
                id_val, data_val, stored_id_val
            )
            if (
                getattr(self, "_sql_handled_projection", False)
                and self._projection
                and int(self._projection.get("_id", 1)) != 1
            ):
                doc.pop("_id", None)
            if len(row) >= 4 and row[3] is not None:
                doc["_textScore"] = float(row[3])
            yield doc

    def _apply_sorting(
        self, docs: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        """Sort documents in Python according to ``self._sort``.

        Applies multi-key stable sorting in reverse key priority order,
        supporting ``$meta: "textScore"`` sorting, collation case folding, and
        BSON type sort order via ``bson_sort_key``.

        Args:
            docs: Iterable of document dictionaries to sort.

        Returns:
            Iterable[dict[str, Any]]: Sorted list of documents, or ``docs``
                unmodified when ``self._sort`` is unset.
        """
        if not self._sort:
            return docs

        sort_keys = list(self._sort.keys())
        sort_keys.reverse()
        sorted_docs = list(docs)

        # Get collation settings for case-insensitive sorting
        case_insensitive = is_case_insensitive_collation(self._collation)

        for key in sort_keys:
            direction = self._sort[key]
            is_text_score = direction == {"$meta": "textScore"} or (
                isinstance(direction, dict)
                and direction.get("$meta") == "textScore"
            )
            if is_text_score:
                reverse = True

                def make_score_key():
                    """Create a sort key callable for FTS text scores.

                    Returns:
                        Callable[[dict[str, Any]], float]: Key function
                            extracting the document's text score.
                    """

                    def score_key_func(doc: dict[str, Any]) -> Any:
                        """Extract the numeric text score for ``doc``.

                        Args:
                            doc: Document dictionary to score.

                        Returns:
                            float: ``_textScore`` or field value as float.
                        """
                        return float(doc.get("_textScore", doc.get(key, 0.0)))

                    return score_key_func

                sorted_docs.sort(key=make_score_key(), reverse=reverse)
                continue

            get_val = partial(self._collection._get_val, key=key)
            reverse = direction == DESCENDING

            if case_insensitive:
                # Use case-insensitive sorting
                def make_key(gv=get_val):
                    """Create a case-insensitive BSON sort key callable.

                    Args:
                        gv: Field-value accessor bound to the current sort key.

                    Returns:
                        Callable[[dict[str, Any]], Any]: Key function returning
                            a ``bson_sort_key`` tuple.
                    """

                    def key_func(doc):
                        """Extract and lowercase string values for sorting.

                        Args:
                            doc: Document dictionary to inspect.

                        Returns:
                            Any: BSON sort key for the normalized field value.
                        """
                        val = gv(doc)
                        if isinstance(val, str):
                            val = val.lower()
                        return bson_sort_key(val)

                    return key_func

                sorted_docs.sort(key=make_key(), reverse=reverse)
            else:

                def make_default_key(gv=get_val):
                    """Create a standard BSON sort key callable.

                    Args:
                        gv: Field-value accessor bound to the current sort key.

                    Returns:
                        Callable[[dict[str, Any]], Any]: Key function returning
                            a ``bson_sort_key`` tuple.
                    """

                    def key_func(doc):
                        """Extract field value and return its BSON sort key.

                        Args:
                            doc: Document dictionary to inspect.

                        Returns:
                            Any: BSON sort key for the field value.
                        """
                        return bson_sort_key(gv(doc))

                    return key_func

                sorted_docs.sort(
                    key=make_default_key(),
                    reverse=reverse,
                )
        return sorted_docs

    def _apply_pagination(
        self, docs: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        """Apply ``self._skip`` and ``self._limit`` lazily via ``islice``.

        Args:
            docs: Iterable of document dictionaries to paginate.

        Returns:
            Iterable[dict[str, Any]]: Lazy iterator over paginated documents.
        """
        # Use islice for memory-efficient skip without loading all docs first
        skipped = islice(docs, self._skip, None)

        # Apply limit if specified
        if self._limit is not None and self._limit != 0:
            return islice(skipped, abs(self._limit))
        return skipped

    def _apply_projection(
        self, docs: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        """Apply ``self._projection`` lazily to each document in Python.

        Args:
            docs: Iterable of document dictionaries to project.

        Returns:
            Iterable[dict[str, Any]]: Lazy iterator over projected documents.
        """
        if not self._projection:
            return docs
        project = partial(
            self._query_helpers._apply_projection, self._projection
        )
        return map(project, docs)

    def close(self) -> None:
        """Close the cursor, stop active generators, and drop temp tables."""
        if self._closed:
            return
        # Stop active generator and release its sqlite statement (#135)
        gen = getattr(self, "_active_generator", None)
        if gen is not None:
            try:
                gen.close()
            except Exception as e:
                logger.debug(f"Failed to close generator: {e}")
            self._active_generator = None
        self._cleanup_tables()
        self._closed = True

    def __del__(self) -> None:
        """Clean up cursor resources when garbage collected."""
        try:
            self.close()
        except Exception as e:
            if logger is not None:
                logger.debug(f"{e=}")
            pass

    def __enter__(self) -> Self:
        """Enter the cursor context manager.

        Returns:
            Self: This cursor instance.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager and close the cursor.

        Args:
            exc_type: Exception class if raised inside the ``with`` block.
            exc_val: Exception instance if raised inside the ``with`` block.
            exc_tb: Exception traceback if raised inside the ``with`` block.
        """
        self.close()

    def _cleanup_tables(self) -> None:
        """Drop any temporary SQLite tables registered on this cursor."""
        if not self._tables_to_cleanup:
            return

        for table in self._tables_to_cleanup:
            try:
                # Use a new connection or the shared connection to drop tables
                self._collection.db.execute(
                    f"DROP TABLE IF EXISTS {quote_table_name(table)}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop temporary table '{table}' during cleanup: {e}"
                )
                pass
        self._tables_to_cleanup = []
