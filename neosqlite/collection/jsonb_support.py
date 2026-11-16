"""
JSONB support detection utilities for NeoSQLite.

Provides efficient detection of JSONB capabilities with automatic caching
to avoid redundant database queries.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Self

from .._sqlite import sqlite3  # noqa: F401  (re-exported for compatibility)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class JSONBContext:
    """Immutable snapshot of JSONB capabilities for a database connection.

    This is the single source of truth for deciding which SQLite JSON
    functions to emit.  ``jsonb_*`` scalar functions exist from SQLite 3.45,
    but the table-valued ``jsonb_each()``/``jsonb_tree()`` functions were only
    added in 3.51.  Keep both capabilities separate and expose the resulting
    function names here instead of probing or hard-coding them at call sites.
    """

    jsonb_supported: bool
    jsonb_each_supported: bool
    json_function_prefix: str
    json_each_function: str
    json_group_array_function: str

    @classmethod
    def from_db(cls, db_connection) -> Self:
        """Build a context by probing a database connection once.

        Probes ``supports_jsonb()`` and ``supports_jsonb_each()`` on the
        given connection and populates the derived function name attributes.

        Args:
            db_connection: SQLite database connection to probe for JSONB
                support.

        Returns:
            Self: A frozen ``JSONBContext`` populated for the connection.
        """
        b = supports_jsonb(db_connection)
        be = supports_jsonb_each(db_connection)
        return cls(
            jsonb_supported=b,
            jsonb_each_supported=be,
            json_function_prefix=_get_json_function_prefix(b),
            json_each_function=_get_json_each_function(b, be),
            json_group_array_function=_get_json_group_array_function(b),
        )

    @classmethod
    def disabled(cls) -> Self:
        """Return the portable json_* context used when no DB is available.

        Configures all capability flags to ``False`` and sets function names
        to standard SQLite ``json`` / ``json_each`` / ``json_group_array``.

        Returns:
            Self: A ``JSONBContext`` with JSONB capabilities disabled.
        """
        return cls(
            jsonb_supported=False,
            jsonb_each_supported=False,
            json_function_prefix="json",
            json_each_function="json_each",
            json_group_array_function="json_group_array",
        )

    @property
    def json_tree_function(self) -> str:
        """Return ``json_tree`` or ``jsonb_tree`` for this connection.

        Delegates to ``_get_json_tree_function()`` using both
        ``jsonb_supported`` and ``jsonb_each_supported`` flags.

        Returns:
            str: ``"jsonb_tree"`` when both JSONB and table-valued JSONB
            functions are supported, or ``"json_tree"`` otherwise.
        """
        return _get_json_tree_function(
            self.jsonb_supported, self.jsonb_each_supported
        )

    @property
    def json_value_function(self) -> str:
        """Return the JSON value constructor: ``json`` or ``jsonb``.

        Returns:
            str: ``"jsonb"`` if ``jsonb_supported`` is True, otherwise
            ``"json"``.
        """
        return json_value_function(self.jsonb_supported)

    @property
    def storage_type(self) -> str:
        """Return the SQLite column type used for JSON documents.

        Returns:
            str: ``"JSONB"`` when ``jsonb_supported`` is True, or ``"TEXT"``
            otherwise.
        """
        return "JSONB" if self.jsonb_supported else "TEXT"

    def function(self, name: str) -> str:
        """Return ``json_<name>`` or ``jsonb_<name>`` for this connection.

        Args:
            name: Base SQLite JSON function suffix without the ``json_`` or
                ``jsonb_`` prefix.

        Returns:
            str: Prefixed SQLite JSON function name.
        """
        return json_function(name, self.jsonb_supported)

    def data_column(self, source: str = "data") -> str:
        """Wrap a SQL source expression so JSONB blobs convert to JSON text.

        Delegates to ``json_data_column()`` using ``self.jsonb_supported``.

        Args:
            source: SQL column name or expression to wrap. Defaults to
                ``"data"``.

        Returns:
            str: SQL expression yielding JSON text when JSONB is enabled, or
            ``source`` unchanged otherwise.
        """
        return json_data_column(self.jsonb_supported, source)

    def json_constructor(self, expression: str) -> str:
        """Wrap a SQL expression in ``json()`` or ``jsonb()``.

        Args:
            expression: SQL expression to pass to the JSON constructor.

        Returns:
            str: Call expression using ``json()`` or ``jsonb()`` depending on
            ``self.jsonb_supported``.
        """
        return json_constructor(expression, self.jsonb_supported)

    def json_text(self, expression: str) -> str:
        """Convert JSONB output to TEXT for Python consumers.

        Args:
            expression: SQL expression producing a JSONB or JSON value.

        Returns:
            str: ``json(<expression>)`` when ``jsonb_supported`` is True, or
            ``expression`` unchanged otherwise.
        """
        return json_text_conversion(expression, self.jsonb_supported)

    def json_placeholder(self, *, wrap_text: bool = False) -> str:
        """Return a bind placeholder for a JSON value.

        With JSONB support this is always ``jsonb(?)``.  Without JSONB,
        callers that need an explicit ``json()`` conversion can set
        *wrap_text*; otherwise a plain ``?`` preserves the stored text.

        Args:
            wrap_text: Whether to wrap the ``?`` placeholder in ``json()``
                when JSONB is not supported. Defaults to False.

        Returns:
            str: ``"jsonb(?)"``, ``"json(?)"``, or ``"?"``.
        """
        if self.jsonb_supported:
            return "jsonb(?)"
        return "json(?)" if wrap_text else "?"

    def json_literal(self, literal: str) -> str:
        """Return a JSON literal such as ``json('[]')`` or ``jsonb('[]')``.

        Args:
            literal: Raw JSON string content to wrap as a SQL literal.

        Returns:
            str: SQL JSON literal wrapped with ``json()`` or ``jsonb()``.
        """
        return json_literal(literal, self.jsonb_supported)


# Module-level cache for JSONB support detection results.
# Stored ON the connection object (sqlite connections accept attribute
# assignment is NOT possible, so we key by id but ALSO pin the connection
# object in the value) — see #129: a bare id() key let a recycled address
# inherit a dead connection's capability flags.
_jsonb_support_cache: dict[int, tuple[Any, dict[str, bool]]] = {}

_JSONB_CACHE_MAX = 64


def clear_jsonb_cache(db_connection=None) -> None:
    """Clear the JSONB support cache.

    Removes cached capability flags for a specific connection when provided,
    or clears the entire ``_jsonb_support_cache`` mapping when omitted.

    Args:
        db_connection: Optional specific connection to remove from the cache.
            If None, clears the entire cache.
    """
    if db_connection is None:
        _jsonb_support_cache.clear()
    else:
        conn_id = id(db_connection)
        _jsonb_support_cache.pop(conn_id, None)


def supports_jsonb(db_connection) -> bool:
    """Check if the SQLite connection supports JSONB functions.

    Tests whether the SQLite installation has JSONB support by executing
    ``SELECT jsonb('{"test": "value"}')``. Results are cached per connection
    identity (up to ``_JSONB_CACHE_MAX`` entries) to avoid redundant queries.

    Args:
        db_connection: SQLite database connection to test.

    Returns:
        bool: True if JSONB is supported, False otherwise.
    """
    conn_id = id(db_connection)

    entry = _jsonb_support_cache.get(conn_id)
    if entry is None or entry[0] is not db_connection:
        # Pin the connection so its id cannot be recycled while cached,
        # and bound growth (#129)
        if len(_jsonb_support_cache) >= _JSONB_CACHE_MAX:
            _jsonb_support_cache.clear()
        entry = (db_connection, {})
        _jsonb_support_cache[conn_id] = entry

    cache = entry[1]

    if "jsonb_supported" not in cache:
        try:
            db_connection.execute('SELECT jsonb(\'{"test": "value"}\')')
            cache["jsonb_supported"] = True
        except Exception as e:
            # The connection may come from a different DB-API module
            # (stdlib sqlite3 vs pysqlite3), so do not assume the exception
            # class matches our imported module.
            logger.debug(f"JSONB not supported: {e}")
            cache["jsonb_supported"] = False

    return cache["jsonb_supported"]


def supports_jsonb_each(db_connection) -> bool:
    """Check if the connection supports the ``jsonb_each()`` table function.

    The ``jsonb_each()`` and ``jsonb_tree()`` functions were added in SQLite
    3.51.0. Tests availability by running a query against
    ``jsonb_each('[1,2,3]')`` and caches the result per connection.

    Args:
        db_connection: SQLite database connection to test.

    Returns:
        bool: True if ``jsonb_each`` is supported, False otherwise.
    """
    conn_id = id(db_connection)

    entry = _jsonb_support_cache.get(conn_id)
    if entry is None or entry[0] is not db_connection:
        # Pin the connection so its id cannot be recycled while cached,
        # and bound growth (#129)
        if len(_jsonb_support_cache) >= _JSONB_CACHE_MAX:
            _jsonb_support_cache.clear()
        entry = (db_connection, {})
        _jsonb_support_cache[conn_id] = entry

    cache = entry[1]

    if "jsonb_each_supported" not in cache:
        try:
            db_connection.execute(
                "SELECT key, value FROM jsonb_each('[1,2,3]')"
            )
            cache["jsonb_each_supported"] = True
        except Exception as e:
            # See supports_jsonb(): the connection may use a foreign DB-API
            # module whose exception classes are not ours.
            logger.debug(f"jsonb_each not supported: {e}")
            cache["jsonb_each_supported"] = False

    return cache["jsonb_each_supported"]


def _get_json_function_prefix(jsonb_supported: bool) -> str:
    """Get the appropriate JSON function prefix based on JSONB support.

    Args:
        jsonb_supported: Whether JSONB functions are supported.

    Returns:
        str: ``"jsonb"`` if JSONB is supported, ``"json"`` otherwise.
    """
    return "jsonb" if jsonb_supported else "json"


def json_function(name: str, jsonb_supported: bool) -> str:
    """Return ``json_<name>`` or ``jsonb_<name>`` based on JSONB support.

    Prefer ``JSONBContext.function()`` when a context is available; this
    helper exists for the few standalone call sites that only carry the
    capability flag.

    Args:
        name: Base SQLite JSON function name without prefix.
        jsonb_supported: Whether JSONB functions are supported.

    Returns:
        str: Full SQLite JSON function name with ``json_`` or ``jsonb_``
        prefix.
    """
    return f"{_get_json_function_prefix(jsonb_supported)}_{name}"


def json_value_function(jsonb_supported: bool) -> str:
    """Return the JSON value constructor name: ``json`` or ``jsonb``.

    Args:
        jsonb_supported: Whether JSONB functions are supported.

    Returns:
        str: ``"jsonb"`` if ``jsonb_supported`` is True, ``"json"`` otherwise.
    """
    return "jsonb" if jsonb_supported else "json"


def json_constructor(expression: str, jsonb_supported: bool) -> str:
    """Wrap a SQL expression in ``json()`` or ``jsonb()``.

    Args:
        expression: SQL expression to pass to the JSON constructor.
        jsonb_supported: Whether JSONB functions are supported.

    Returns:
        str: SQL expression wrapped with ``json(...)`` or ``jsonb(...)``.
    """
    return f"{json_value_function(jsonb_supported)}({expression})"


def json_text_conversion(expression: str, jsonb_supported: bool) -> str:
    """Convert JSONB output to TEXT for Python consumers.

    When JSONB is supported, this wraps the expression with ``json()`` to
    convert the binary JSONB value back to a readable TEXT string for Python
    consumption. When JSONB is not supported, the expression is returned
    unchanged.

    Args:
        expression: The SQL expression representing a JSONB value.
        jsonb_supported: Whether the database connection supports JSONB.

    Returns:
        str: A SQL expression that returns a TEXT value instead of JSONB.
    """
    return f"json({expression})" if jsonb_supported else expression


def json_literal(literal: str, jsonb_supported: bool) -> str:
    """Return a JSON literal such as ``json('[]')`` or ``jsonb('[]')``.

    This function wraps a string literal in the appropriate JSON constructor
    function based on whether JSONB is supported by the database connection.

    Args:
        literal: The string value to be converted into a JSON literal.
        jsonb_supported: Whether JSONB functions are supported by the database.

    Returns:
        str: The JSON literal wrapped with ``json()`` or ``jsonb()``.
    """
    return json_constructor(f"'{literal}'", jsonb_supported)


def _get_json_each_function(
    jsonb_supported: bool, jsonb_each_supported: bool
) -> str:
    """Get the appropriate ``json_each`` function name based on support.

    Args:
        jsonb_supported: Whether JSONB scalar functions are supported.
        jsonb_each_supported: Whether ``jsonb_each`` is supported
            (SQLite 3.51.0+).

    Returns:
        str: ``"jsonb_each"`` when both flags are True, ``"json_each"``
        otherwise.
    """
    return (
        "jsonb_each"
        if (jsonb_supported and jsonb_each_supported)
        else "json_each"
    )


def _get_json_tree_function(
    jsonb_supported: bool, jsonb_each_supported: bool
) -> str:
    """Get the appropriate ``json_tree`` function name based on support.

    Args:
        jsonb_supported: Whether JSONB scalar functions are supported.
        jsonb_each_supported: Whether ``jsonb_each``/``jsonb_tree`` are
            supported (SQLite 3.51.0+).

    Returns:
        str: ``"jsonb_tree"`` when both flags are True, ``"json_tree"``
        otherwise.
    """
    return (
        "jsonb_tree"
        if (jsonb_supported and jsonb_each_supported)
        else "json_tree"
    )


def _get_json_group_array_function(jsonb_supported: bool) -> str:
    """Get the ``json_group_array`` function name based on JSONB support.

    Args:
        jsonb_supported: Whether JSONB functions are supported.

    Returns:
        str: ``"jsonb_group_array"`` if supported, ``"json_group_array"``
        otherwise.
    """
    return "jsonb_group_array" if jsonb_supported else "json_group_array"


def should_use_json_functions(
    query: dict | None = None, jsonb_supported: bool = False
) -> bool:
    """Determine whether to use ``json_*`` instead of ``jsonb_*`` functions.

    Decides between ``json_*`` and ``jsonb_*`` based on:

    1. JSONB support availability.
    2. Query content (``$text`` search queries require FTS compatibility and
       therefore use ``json_*``).

    Args:
        query: Optional MongoDB query dictionary to inspect for ``$text``
            operations. Defaults to None.
        jsonb_supported: Whether JSONB functions are supported by the
            database. Defaults to False.

    Returns:
        bool: True if ``json_*`` functions should be used, False if
        ``jsonb_*`` functions should be used.
    """
    # If JSONB is not supported, we must use json_* functions
    if not jsonb_supported:
        return True

    # If no query provided, default to using jsonb_* functions for better performance
    if query is None:
        return False

    # Check if query contains text search operations which require FTS compatibility
    return _contains_text_operator(query)


def _contains_text_operator(query: dict) -> bool:
    """Check if a query contains any ``$text`` operators recursively.

    Traverses a MongoDB query specification to detect ``$text`` operators,
    which require special handling for FTS compatibility. Inspects both
    top-level ``$text`` keys and conditions nested inside ``$and``,
    ``$or``, ``$nor``, and ``$not`` logical operators.

    Args:
        query: The query dictionary to inspect.

    Returns:
        bool: True if the query contains any ``$text`` operator, False
        otherwise.
    """
    if not isinstance(query, dict):
        return False

    for field, value in query.items():
        if field in ("$and", "$or", "$nor"):
            # Check each condition in logical operators
            if isinstance(value, list):
                for condition in value:
                    if isinstance(condition, dict) and _contains_text_operator(
                        condition
                    ):
                        return True
        elif field == "$not":
            # Check the condition in $not operator
            if isinstance(value, dict) and _contains_text_operator(value):
                return True
        elif field == "$text":
            # Found a $text operator
            return True
    return False


def json_data_column(jsonb_supported: bool, source: str = "data") -> str:
    """Return the data column expression for SELECT queries.

    When JSONB is supported, wraps the source with json() to convert
    JSONB to text.  Otherwise returns the source unchanged.

    When JSONB is supported, also handles malformed JSON gracefully by
    checking json_valid() first for text values. This prevents the entire
    query from failing when a single row has corrupted data. JSONB blobs
    are always passed through json() since they're stored in SQLite's
    binary JSON format.

    Args:
        jsonb_supported: Whether the database connection supports JSONB.
        source: The column name or expression to wrap. Defaults to "data".

    Returns:
        str: A SQL expression that safely converts JSONB to TEXT for Python
             consumption, or the source unchanged if JSONB is not supported.
    """
    if jsonb_supported:
        # Handle three cases:
        # 1. JSONB blobs (typeof='blob'): always valid, convert with json()
        # 2. Valid JSON text: convert with json() for consistency
        # 3. Malformed text or other types: return as-is (Python handles errors)
        return (
            f"CASE WHEN typeof({source})='blob' THEN json({source}) "
            f"WHEN typeof({source})='text' AND json_valid({source}) "
            f"THEN json({source}) ELSE {source} END"
        )
    return source
