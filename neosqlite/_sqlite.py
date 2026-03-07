"""
Centralized SQLite import with pysqlite3 fallback.

This module provides a single import point for SQLite functionality,
preferring pysqlite3 (with SQLCipher support) when available.
"""

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore

__all__ = ["sqlite3"]
