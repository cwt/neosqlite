"""
Type correction utilities for NeoSQLite to handle automatic conversion
between integer IDs and ObjectIds in queries.
"""

import logging
from typing import Any

from ..objectid import ObjectId
from ..sql_utils import quote_table_name

logger = logging.getLogger(__name__)


def normalize_id_query(query: dict[str, Any]) -> dict[str, Any]:
    """
    Public function to normalize ID types in a query.

    This function is provided for backward compatibility. The actual
    normalization logic is implemented in QueryHelper._normalize_id_query
    method to avoid code duplication. This function is not actively used
    but kept for API compatibility.

    Args:
        query: The query dictionary to normalize

    Returns:
        A normalized query dictionary with corrected ID types
    """
    # This function is kept for API compatibility but doesn't do anything
    # since the normalization happens in the QueryHelper
    return query


def _try_convert_to_int(value: str) -> int | str:
    """Try to convert a string to int, return original if fails."""
    try:
        return int(value)
    except ValueError as e:
        logger.debug(f"{e=}")
        return value


def _is_valid_objectid_hex(value: str) -> bool:
    """Check if string is a valid ObjectId hex string (24 chars, valid hex)."""
    if len(value) != 24:
        return False
    try:
        ObjectId(value)
        return True
    except ValueError as e:
        logger.debug(f"Invalid ObjectId hex string '{value}': {e}")
        return False


def _convert_list_item(item: Any) -> Any:
    """Convert a single list item - ObjectIds to strings, recurse on dicts."""
    match item:
        case dict():
            return normalize_id_query_for_db(item)
        case ObjectId():
            return str(item)
        case _:
            return item


def normalize_objectid_for_db_query(value: Any) -> str:
    """
    Normalize an ObjectId value for database queries, converting ObjectId objects
    to string representations and validating hex strings.

    Args:
        value: The value to normalize (ObjectId, hex string, or other)

    Returns:
        The normalized string representation suitable for database queries
    """
    match value:
        case ObjectId():
            return str(value)
        case str() if _is_valid_objectid_hex(value):
            return value
        case _:
            return value


def normalize_id_query_for_db(query: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize ID types in a query dictionary to correct common mismatches.

    This method automatically detects and corrects common ID type mismatches:
    - When 'id' field is queried with an ObjectId/hex string, it's converted to '_id'
    - When 'id' field is queried with an integer string, it's converted to integer
    - When '_id' field is queried with an integer string, it's converted to integer
    - When any other field is queried with an ObjectId, it's converted to string

    Args:
        query: The query dictionary to process

    Returns:
        A new query dictionary with corrected ID types
    """
    if not isinstance(query, dict):
        return query

    corrected_query: dict[str, Any] = {}

    for key, value in query.items():
        match key, value:
            # Case: 'id' field with ObjectId -> convert to '_id' with string
            case ("id", ObjectId()):
                corrected_query["_id"] = str(value)

            # Case: 'id' field with valid ObjectId hex string -> convert to '_id'
            case ("id", str() as s) if _is_valid_objectid_hex(s):
                corrected_query["_id"] = s

            # Case: 'id' field with string that can be converted to int
            case ("id", str() as s):
                corrected_query["id"] = _try_convert_to_int(s)

            # Case: 'id' field with any other type -> keep as-is
            case ("id", _):
                corrected_query["id"] = value

            # Case: '_id' field with ObjectId -> convert to string
            case ("_id", ObjectId()):
                corrected_query["_id"] = str(value)

            # Case: '_id' field with string
            case ("_id", str() as s):
                # First try to convert to int, otherwise check if valid ObjectId hex
                match _try_convert_to_int(s):
                    case int() as int_val:
                        corrected_query["_id"] = int_val
                    case str() as str_val if _is_valid_objectid_hex(str_val):
                        corrected_query["_id"] = str_val
                    case _ as other:
                        corrected_query["_id"] = other

            # Case: '_id' field with any other type -> keep as-is
            case ("_id", _):
                corrected_query["_id"] = value

            # Case: nested dictionary -> recurse
            case (_, dict()):
                corrected_query[key] = normalize_id_query_for_db(value)

            # Case: list -> process items
            case (_, list()):
                corrected_query[key] = [
                    _convert_list_item(item) for item in value
                ]

            # Case: ObjectId in other fields -> convert to string
            case (_, ObjectId()):
                corrected_query[key] = str(value)

            # Case: default -> keep as-is
            case _:
                corrected_query[key] = value

    return corrected_query


def get_integer_id_for_oid(
    db_connection: Any,
    collection_name: str,
    oid: Any,
) -> int:
    """
    Get the integer ID for a given ObjectId or other ID type.

    Args:
        db_connection: SQLite database connection
        collection_name: Name of the collection/table
        oid: The ID value (ObjectId, int, str, etc.)

    Returns:
        Integer ID from the database

    Raises:
        ValueError: If the ID cannot be found
    """
    match oid:
        case int():
            return oid
        case _:
            cursor = db_connection.execute(
                f"SELECT id FROM {quote_table_name(collection_name)} WHERE _id = ?",
                (str(oid) if hasattr(oid, "__str__") else oid,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            raise ValueError(f"Could not find integer ID for ObjectId: {oid}")


def get_integer_id_for_table(
    db_connection: Any,
    table_name: str,
    oid: Any,
) -> int:
    """
    Get the integer ID for a file in GridFS tables.

    This is an alias for get_integer_id_for_oid, provided for clarity
    when working with GridFS tables.

    Args:
        db_connection: SQLite database connection
        table_name: Name of the GridFS table
        oid: The file ID

    Returns:
        Integer ID from the database

    Raises:
        ValueError: If the ID cannot be found
    """
    return get_integer_id_for_oid(db_connection, table_name, oid)
