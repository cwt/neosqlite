"""
Schema inspection and modification utilities for NeoSQLite.

This module provides common functionality for inspecting and modifying
database schemas, avoiding code duplication across multiple modules.
"""

from typing import Any, Dict, Set


def get_table_columns(db_connection: Any, table_name: str) -> Set[str]:
    """
    Get set of column names for a table.

    Args:
        db_connection: SQLite database connection
        table_name: Name of the table to inspect

    Returns:
        Set of column names
    """
    from ..sql_utils import quote_table_name

    cursor = db_connection.execute(
        f"PRAGMA table_info({quote_table_name(table_name)})"
    )
    return {row[1] for row in cursor.fetchall()}


def column_exists(
    db_connection: Any,
    table_name: str,
    column_name: str,
) -> bool:
    """
    Check if column exists in table.

    Args:
        db_connection: SQLite database connection
        table_name: Name of the table
        column_name: Name of the column to check

    Returns:
        True if column exists, False otherwise
    """
    columns = get_table_columns(db_connection, table_name)
    return column_name in columns


def add_column_if_not_exists(
    db_connection: Any,
    table_name: str,
    column_name: str,
    column_type: str = "TEXT",
) -> bool:
    """
    Add column if it doesn't exist.

    Args:
        db_connection: SQLite database connection
        table_name: Name of the table
        column_name: Name of the column to add
        column_type: SQL type for the new column (default: TEXT)

    Returns:
        True if column was added, False if it already existed
    """
    if column_exists(db_connection, table_name, column_name):
        return False

    from ..sql_utils import quote_table_name

    db_connection.execute(
        f"ALTER TABLE {quote_table_name(table_name)} "
        f"ADD COLUMN {column_name} {column_type}"
    )
    return True


def create_unique_index_on_id(
    db_connection: Any,
    table_name: str,
) -> bool:
    """
    Create a unique index on the _id column if it doesn't exist.

    Args:
        db_connection: SQLite database connection
        table_name: Name of the table

    Returns:
        True if index was created successfully, False otherwise
    """
    from ..sql_utils import quote_identifier, quote_table_name

    try:
        db_connection.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"idx_{quote_identifier(table_name)}_id "
            f"ON {quote_table_name(table_name)}(_id)"
        )
        return True
    except Exception:
        # If we can't create the index (e.g., due to duplicate values),
        # continue without it
        return False


def get_table_info(db_connection: Any, table_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a table.

    Args:
        db_connection: SQLite database connection
        table_name: Name of the table to inspect

    Returns:
        Dictionary with table information including columns and indexes
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
    "column_exists",
    "add_column_if_not_exists",
    "create_unique_index_on_id",
    "get_table_info",
]
