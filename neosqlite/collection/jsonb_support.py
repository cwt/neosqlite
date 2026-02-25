"""
JSONB support detection utilities for NeoSQLite.

Provides efficient detection of JSONB capabilities with automatic caching
to avoid redundant database queries.
"""

from typing import Dict

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


# Module-level cache for JSONB support detection results
# Key: connection id (int), Value: dict with support flags
_jsonb_support_cache: Dict[int, Dict[str, bool]] = {}


def supports_jsonb(db_connection) -> bool:
    """
    Check if the SQLite connection supports JSONB functions.

    This function tests whether the SQLite installation has JSONB support
    by attempting to call the jsonb() function. Results are cached to avoid
    redundant queries.

    Args:
        db_connection: SQLite database connection to test

    Returns:
        bool: True if JSONB is supported, False otherwise
    """
    conn_id = id(db_connection)

    if conn_id not in _jsonb_support_cache:
        _jsonb_support_cache[conn_id] = {}

    cache = _jsonb_support_cache[conn_id]

    if "jsonb_supported" not in cache:
        try:
            db_connection.execute('SELECT jsonb(\'{"test": "value"}\')')
            cache["jsonb_supported"] = True
        except sqlite3.OperationalError:
            cache["jsonb_supported"] = False

    return cache["jsonb_supported"]


def supports_jsonb_each(db_connection) -> bool:
    """
    Check if the SQLite connection supports jsonb_each() table-valued function.

    The jsonb_each() and jsonb_tree() functions were added in SQLite 3.51.0.
    This function tests whether they're available by attempting to use them.
    Results are cached to avoid redundant queries.

    Args:
        db_connection: SQLite database connection to test

    Returns:
        bool: True if jsonb_each is supported, False otherwise
    """
    conn_id = id(db_connection)

    if conn_id not in _jsonb_support_cache:
        _jsonb_support_cache[conn_id] = {}

    cache = _jsonb_support_cache[conn_id]

    if "jsonb_each_supported" not in cache:
        try:
            db_connection.execute(
                "SELECT key, value FROM jsonb_each('[1,2,3]')"
            )
            cache["jsonb_each_supported"] = True
        except sqlite3.OperationalError:
            cache["jsonb_each_supported"] = False

    return cache["jsonb_each_supported"]


def _get_json_function_prefix(jsonb_supported: bool) -> str:
    """
    Get the appropriate JSON function prefix based on JSONB support.

    Args:
        jsonb_supported: Whether JSONB functions are supported

    Returns:
        str: "jsonb" if JSONB is supported, "json" otherwise
    """
    return "jsonb" if jsonb_supported else "json"


def _get_json_each_function(
    jsonb_supported: bool, jsonb_each_supported: bool
) -> str:
    """
    Get the appropriate json_each function name based on support.

    Args:
        jsonb_supported: Whether JSONB functions are supported
        jsonb_each_supported: Whether jsonb_each is supported (SQLite 3.51.0+)

    Returns:
        str: "jsonb_each" if supported, "json_each" otherwise
    """
    return (
        "jsonb_each"
        if (jsonb_supported and jsonb_each_supported)
        else "json_each"
    )


def _get_json_tree_function(
    jsonb_supported: bool, jsonb_each_supported: bool
) -> str:
    """
    Get the appropriate json_tree function name based on support.

    Args:
        jsonb_supported: Whether JSONB functions are supported
        jsonb_each_supported: Whether jsonb_each/jsonb_tree is supported (SQLite 3.51.0+)

    Returns:
        str: "jsonb_tree" if supported, "json_tree" otherwise
    """
    return (
        "jsonb_tree"
        if (jsonb_supported and jsonb_each_supported)
        else "json_tree"
    )


def _get_json_group_array_function(jsonb_supported: bool) -> str:
    """
    Get the appropriate json_group_array function name based on support.

    Args:
        jsonb_supported: Whether JSONB functions are supported

    Returns:
        str: "jsonb_group_array" if supported, "json_group_array" otherwise
    """
    return "jsonb_group_array" if jsonb_supported else "json_group_array"


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
