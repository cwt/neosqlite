"""Runtime capability checks for the SQLite build in use."""

from __future__ import annotations

from .._sqlite import sqlite3


def supports_aggregate_order_by() -> bool:
    """Return True when aggregate functions accept an ORDER BY clause.

    SQLite added aggregate ``ORDER BY`` support (for ``json_group_array``
    and related aggregate functions) in version 3.44.0.

    Returns:
        bool: True if the SQLite version is 3.44.0 or newer, False otherwise.
    """
    return sqlite3.sqlite_version_info >= (3, 44, 0)
