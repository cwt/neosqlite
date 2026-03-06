"""Helper functions for QueryHelper operations."""

from ...sql_utils import quote_table_name
from ..type_correction import normalize_id_query_for_db
from typing import Any, Dict

try:
    from pysqlite3 import dbapi2 as sqlite3  # noqa: F401
except ImportError:
    pass  # type: ignore


def _normalize_id_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize ID types in a query dictionary to correct common mismatches.

    This method delegates to the centralized normalize_id_query_for_db function
    to ensure consistent ID handling across all NeoSQLite components.

    Args:
        query: The query dictionary to process

    Returns:
        A new query dictionary with corrected ID types
    """
    return normalize_id_query_for_db(query)


def _get_integer_id_for_oid(collection: Any, oid: Any) -> int:
    """
    Get the integer ID for a given ObjectId or other ID type.

    Args:
        collection: The collection instance
        oid: The ID value (can be ObjectId, int, str, etc.)

    Returns:
        int: The integer ID from the database
    """
    # If it's already an integer, return it directly
    if isinstance(oid, int):
        return oid

    # For other types (ObjectId, str), we need to query the _id column to get the integer id
    cursor = collection.db.execute(
        f"SELECT id FROM {quote_table_name(collection.name)} WHERE _id = ?",
        (
            (str(oid) if hasattr(oid, "__str__") else oid,)
            if oid is not None
            else (None,)
        ),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        # If not found and it's an int, return as is for backward compatibility
        if isinstance(oid, int):
            return oid
        raise ValueError(f"Could not find integer ID for ObjectId: {oid}")


def _validate_json_document(db: Any, json_str: str) -> bool:
    """
    Validate JSON document using SQLite's json_valid function.

    This method validates a JSON string using SQLite's built-in json_valid function
    if available. For databases without JSON1 support, it falls back to Python's
    json.loads for validation.

    Args:
        db: Database connection
        json_str (str): The JSON string to validate

    Returns:
        bool: True if the JSON is valid, False otherwise
    """
    try:
        # Try to use SQLite's json_valid function
        cursor = db.execute("SELECT json_valid(?)", (json_str,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return bool(result[0])
        else:
            # json_valid not supported, fall back to Python validation
            import json

            json.loads(json_str)
            return True
    except (json.JSONDecodeError, Exception):
        return False


def _get_json_error_position(db: Any, json_str: str) -> int:
    """
    Get position of JSON error using json_error_position().

    This method attempts to get the position of the first syntax error in a
    JSON string using SQLite's json_error_position function if available.
    Returns -1 if the function is not supported or if the JSON is valid.

    Args:
        db: Database connection
        json_str (str): The JSON string to check for errors

    Returns:
        int: Position of the first syntax error, or -1 if valid/not supported
    """
    try:
        # Try to use SQLite's json_error_position function (SQLite 3.38.0+)
        cursor = db.execute("SELECT json_error_position(?)", (json_str,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return int(result[0])
        else:
            return -1
    except Exception:
        # json_error_position not supported
        return -1
