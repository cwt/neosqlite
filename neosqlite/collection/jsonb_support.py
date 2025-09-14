"""
JSONB support detection utilities for NeoSQLite.
"""

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


def supports_jsonb(db_connection) -> bool:
    """
    Check if the SQLite connection supports JSONB functions.

    This function tests whether the SQLite installation has JSONB support
    by attempting to call the jsonb() function. If successful, it returns True,
    indicating that JSONB functions can be used for better performance.

    Args:
        db_connection: SQLite database connection to test

    Returns:
        bool: True if JSONB is supported, False otherwise
    """
    try:
        db_connection.execute('SELECT jsonb(\'{"test": "value"}\')')
        return True
    except sqlite3.OperationalError:
        return False


def _get_json_function_prefix(jsonb_supported: bool) -> str:
    """
    Get the appropriate JSON function prefix based on JSONB support.

    Args:
        jsonb_supported: Whether JSONB functions are supported

    Returns:
        str: "jsonb" if JSONB is supported, "json" otherwise
    """
    return "jsonb" if jsonb_supported else "json"


def should_use_json_functions(
    query: dict | None = None, jsonb_supported: bool = False
) -> bool:
    """
    Determine if we should use json_* functions instead of jsonb_* functions.

    This function determines whether to use json_* or jsonb_* functions based on:
    1. JSONB support availability
    2. Query content (specifically text search queries which require FTS compatibility)

    Args:
        query: MongoDB query dictionary to check for text search operations
        jsonb_supported: Whether JSONB functions are supported by the database

    Returns:
        bool: True if json_* functions should be used, False if jsonb_* functions should be used
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
    """
    Check if a query contains any $text operators, including nested in logical operators.

    This method recursively traverses a MongoDB query specification to detect the presence
    of $text operators, which require special handling and fallback to Python implementation.
    It checks both top-level $text operators and those nested within logical operators
    ($and, $or, $nor, $not).

    Args:
        query: The query to check

    Returns:
        True if the query contains $text operators, False otherwise
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
