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
