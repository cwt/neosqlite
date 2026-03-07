"""Helper functions for QueryHelper operations."""

from ..._sqlite import sqlite3
from ..type_correction import get_integer_id_for_oid
from typing import Any


def _get_integer_id_for_oid(collection: Any, oid: Any) -> int:
    """
    Get the integer ID for a given ObjectId or other ID type.

    This function delegates to the centralized get_integer_id_for_oid function
    to ensure consistent ID handling across all NeoSQLite components.

    Args:
        collection: The collection instance
        oid: The ID value (can be ObjectId, int, str, etc.)

    Returns:
        int: The integer ID from the database
    """
    return get_integer_id_for_oid(collection.db, collection.name, oid)


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
