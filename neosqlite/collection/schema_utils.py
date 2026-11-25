"""Schema inspection and modification utilities for NeoSQLite.

This module provides common functionality for inspecting and modifying
database schemas, avoiding code duplication across multiple modules.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def get_table_columns(db_connection: Any, table_name: str) -> set[str]:
    """Return the set of column names for a SQLite table.

    Queries ``PRAGMA table_info`` using a quoted table identifier and collects
    the column name field from each returned row.

    Args:
        db_connection: SQLite database connection.
        table_name: Name of the table to inspect.

    Returns:
        set[str]: Set of column names defined on ``table_name``.
    """
    from ..sql_utils import quote_table_name

    cursor = db_connection.execute(
        f"PRAGMA table_info({quote_table_name(table_name)})"
    )
    return {row[1] for row in cursor.fetchall()}


_table_columns_cache: dict[tuple[int, str], tuple[Any, frozenset[str]]] = {}


def get_table_columns_cached(db_connection: Any, table_name: str) -> set[str]:
    """Return cached ``get_table_columns`` results per connection (#152, #191).

    GridFS constructs many ``GridOut`` objects, where repeated ``PRAGMA
    table_info`` calls dominated read paths. Results are stored as
    ``(db_connection, frozenset_cols)`` keyed by ``(id(db_connection),
    table_name)`` so memory-address reuse across closed connections never leaks
    stale column metadata, and cleared once the cache reaches 128 entries.

    Args:
        db_connection: SQLite database connection.
        table_name: Name of the table to inspect.

    Returns:
        set[str]: Copy of the cached set of column names for ``table_name``.
    """
    key = (id(db_connection), table_name)
    entry = _table_columns_cache.get(key)
    if entry is None or entry[0] is not db_connection:
        cols = frozenset(get_table_columns(db_connection, table_name))
        if len(_table_columns_cache) >= 128:
            _table_columns_cache.clear()
        _table_columns_cache[key] = (db_connection, cols)
        return set(cols)
    return set(entry[1])


def invalidate_table_columns_cache(
    db_connection: Any, table_name: str | None = None
) -> None:
    """Invalidate cached table column entries for ``db_connection``.

    Args:
        db_connection: SQLite database connection whose entries should clear.
        table_name: Specific table name to invalidate, or ``None`` to clear
            all entries associated with ``db_connection``.
    """
    conn_id = id(db_connection)
    if table_name is not None:
        _table_columns_cache.pop((conn_id, table_name), None)
        return
    keys_to_remove = [key for key in _table_columns_cache if key[0] == conn_id]
    for key in keys_to_remove:
        _table_columns_cache.pop(key, None)


def column_exists(
    db_connection: Any,
    table_name: str,
    column_name: str,
) -> bool:
    """Check whether a column exists in a SQLite table.

    Uses ``get_table_columns_cached`` to avoid repeated ``PRAGMA table_info``
    lookups on hot paths.

    Args:
        db_connection: SQLite database connection.
        table_name: Name of the table to inspect.
        column_name: Name of the column to check for.

    Returns:
        bool: ``True`` if ``column_name`` exists on ``table_name``, else
        ``False``.
    """
    columns = get_table_columns_cached(db_connection, table_name)
    return column_name in columns


def create_unique_index_on_id(
    db_connection: Any,
    table_name: str,
) -> bool:
    """Create a unique index on the ``_id`` column if it does not exist.

    Logs a debug message and returns ``False`` if index creation fails (for
    example, when duplicate ``_id`` values exist).

    Args:
        db_connection: SQLite database connection.
        table_name: Name of the table on which to create the index.

    Returns:
        bool: ``True`` if the statement succeeded, ``False`` on error.
    """
    from ..sql_utils import (
        quote_identifier,
        quote_table_name,
        validate_identifier,
    )

    try:
        idx_name = quote_identifier(f"idx_{validate_identifier(table_name)}_id")
        db_connection.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"{idx_name} "
            f"ON {quote_table_name(table_name)}(_id)"
        )
        return True
    except Exception as e:
        # If we can't create the index (e.g., due to duplicate values),
        # continue without it
        logger.debug(
            f"Failed to create unique index on _id for {table_name}: {e}"
        )
        return False


def get_table_info(db_connection: Any, table_name: str) -> dict[str, Any]:
    """Return column and index metadata for a SQLite table.

    Inspects ``PRAGMA table_info`` for column definitions (``name``, ``type``,
    ``notnull``, ``default``, ``pk``) and ``sqlite_master`` for associated
    indexes (``name``, ``definition``).

    Args:
        db_connection: SQLite database connection.
        table_name: Name of the table to inspect.

    Returns:
        dict[str, Any]: Dictionary containing ``"columns"`` and ``"indexes"``
        metadata lists.
    """
    from ..sql_utils import quote_table_name

    # Get column information
    table_info = db_connection.execute(
        f"PRAGMA table_info({quote_table_name(table_name)})"
    ).fetchall()

    columns = [
        {
            "name": str(col[1]),
            "type": str(col[2]),
            "notnull": bool(col[3]),
            "default": col[4] if len(col) > 4 else None,
            "pk": bool(col[5]),
        }
        for col in table_info
    ]

    # Get index information
    indexes = db_connection.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name=?",
        (table_name,),
    ).fetchall()

    index_info = [
        {
            "name": str(idx[0]),
            "definition": str(idx[1]) if idx[1] is not None else "",
        }
        for idx in indexes
    ]

    return {
        "columns": columns,
        "indexes": index_info,
    }


__all__ = [
    "get_table_columns",
    "get_table_columns_cached",
    "invalidate_table_columns_cache",
    "column_exists",
    "create_unique_index_on_id",
    "get_table_info",
]
