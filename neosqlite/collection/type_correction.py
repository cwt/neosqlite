"""
Type correction utilities for NeoSQLite to handle automatic conversion
between integer IDs and ObjectIds in queries.
"""

from typing import Any, Dict
from ..objectid import ObjectId


def normalize_id_query(query: Dict[str, Any]) -> Dict[str, Any]:
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


def normalize_objectid_for_db_query(value: Any) -> str:
    """
    Normalize an ObjectId value for database queries, converting ObjectId objects
    to string representations and validating hex strings.

    Args:
        value: The value to normalize (ObjectId, hex string, or other)

    Returns:
        The normalized string representation suitable for database queries
    """
    if isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, str) and len(value) == 24:
        # Check if it's a valid ObjectId hex string
        try:
            ObjectId(value)  # Validate the hex string
            return value
        except ValueError:
            # Not a valid ObjectId hex, return as-is
            return value
    else:
        # For non-ObjectId values, return as-is
        return value


def normalize_id_query_for_db(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize ID types in a query dictionary to correct common mismatches.

    This method automatically detects and corrects common ID type mismatches:
    - When 'id' field is queried with an ObjectId/hex string, it's converted to '_id'
    - When 'id' field is queried with an integer string, it's converted to integer
    - When '_id' field is queried with an integer string, it's converted to integer

    Args:
        query: The query dictionary to process

    Returns:
        A new query dictionary with corrected ID types
    """
    if not isinstance(query, dict):
        return query

    corrected_query: Dict[str, Any] = {}

    for key, value in query.items():
        if key == "id":
            # Check if the value is an ObjectId or hex string that should go to '_id'
            if isinstance(value, ObjectId):
                # User is querying 'id' field with ObjectId - they probably meant '_id'
                corrected_query["_id"] = str(value)  # Convert to string for SQL
            elif isinstance(value, str):
                # Check if it's a 24-character hex string (ObjectId format)
                if len(value) == 24:
                    try:
                        # Try to parse as ObjectId to validate
                        ObjectId(value)
                        # Valid ObjectId hex - user probably meant '_id'
                        corrected_query["_id"] = value
                    except ValueError:
                        # Not a valid ObjectId hex, but might be integer string
                        try:
                            int_val = int(value)
                            corrected_query["id"] = int_val
                        except ValueError:
                            # Neither ObjectId nor integer - keep as is
                            corrected_query["id"] = value
                else:
                    # Not 24 chars, try to parse as integer
                    try:
                        int_val = int(value)
                        corrected_query["id"] = int_val
                    except ValueError:
                        # Not an integer string, keep as is
                        corrected_query["id"] = value
            else:
                # Non-string, non-ObjectId value - keep as is
                corrected_query["id"] = value
        elif key == "_id":
            # Check if the value is an ObjectId object
            if isinstance(value, ObjectId):
                # Convert ObjectId to string representation for SQL compatibility
                corrected_query["_id"] = str(value)
            # Check if the value is an integer string that should be converted to int
            elif isinstance(value, str):
                # Check if it's a valid integer string
                try:
                    int_val = int(value)
                    corrected_query["_id"] = int_val
                except ValueError:
                    # Not an integer string, but check if it's ObjectId hex
                    if len(value) == 24:
                        try:
                            ObjectId(value)
                            # Valid ObjectId hex, keep as is
                            corrected_query["_id"] = value
                        except ValueError:
                            # Not ObjectId hex, keep as string
                            corrected_query["_id"] = value
                    else:
                        # Not 24 chars and not integer, keep as string
                        corrected_query["_id"] = value
            else:
                # Non-string, non-ObjectId value - keep as is
                corrected_query["_id"] = value
        elif isinstance(value, dict):
            # Recursively process nested queries (e.g., $and, $or)
            corrected_query[key] = normalize_id_query_for_db(value)
        elif isinstance(value, list):
            # Process list values (e.g., for $in, $or operators)
            corrected_query[key] = [
                (
                    normalize_id_query_for_db(item)
                    if isinstance(item, dict)
                    else item
                )
                for item in value
            ]
        else:
            # Non-dict, non-list value - keep as is
            corrected_query[key] = value

    return corrected_query
