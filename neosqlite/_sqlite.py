"""
Centralized SQLite import with pysqlite3 fallback.

This module provides a single import point for SQLite functionality,
preferring pysqlite3 (with SQLCipher support) when available.
"""

import logging

logger = logging.getLogger(__name__)

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError as e:
    logger.debug(f"pysqlite3 not found, falling back to standard sqlite3: {e}")
    import sqlite3  # type: ignore

__all__ = ["sqlite3"]
