"""Helper functions for QueryHelper operations."""

import json
import logging
from typing import Any

from ..type_correction import get_integer_id_for_oid

logger = logging.getLogger(__name__)


def _get_integer_id_for_oid(collection: Any, oid: Any) -> int:
    """Get the integer ID for a given ObjectId or other ID type.

    Delegates to the centralized ``get_integer_id_for_oid`` helper to keep ID
    resolution consistent across NeoSQLite components.

    Args:
        collection: The collection instance owning the database and table.
        oid: The ID value (ObjectId, int, str, etc.) to resolve.

    Returns:
        int: The integer row ID from the database.
    """
    return get_integer_id_for_oid(collection.db, collection.name, oid)


def _validate_json_document(db: Any, json_str: str) -> bool:
    """Validate a JSON document using SQLite's ``json_valid`` function.

    Validates ``json_str`` via SQLite's built-in ``json_valid`` when
    available, falling back to Python's ``json.loads`` when JSON1 support is
    absent or returns ``None``.

    Args:
        db: Database connection supporting ``execute``.
        json_str: The JSON string to validate.

    Returns:
        bool: True if the JSON is valid, False otherwise.
    """
    try:
        # Try to use SQLite's json_valid function
        cursor = db.execute("SELECT json_valid(?)", (json_str,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return bool(result[0])
        else:
            # json_valid not supported, fall back to Python validation
            json.loads(json_str)
            return True
    except Exception as e:
        logger.debug("JSON validation failed: %s", e)
        return False


def _get_json_error_position(db: Any, json_str: str) -> int:
    """Get position of a JSON syntax error using ``json_error_position()``.

    Queries SQLite's ``json_error_position`` function when available and
    returns ``-1`` if the JSON is valid or the function is unsupported.

    Args:
        db: Database connection supporting ``execute``.
        json_str: The JSON string to check for syntax errors.

    Returns:
        int: Position of the first syntax error, or ``-1`` if valid or
        unsupported.
    """
    try:
        # Try to use SQLite's json_error_position function (SQLite 3.38.0+)
        cursor = db.execute("SELECT json_error_position(?)", (json_str,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return int(result[0])
        else:
            return -1
    except Exception as e:
        # json_error_position not supported
        logger.debug(
            f"SQLite json_error_position function not supported or failed: {e}"
        )
        return -1
