"""Shared utility functions for GridFS operations.

Contains common serialization, write-concern synchronization, and cached schema
inspection helpers used across GridFS components.
"""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

from ..collection.schema_utils import get_table_columns

logger = logging.getLogger(__name__)


def serialize_metadata(metadata: dict[str, Any] | None) -> str | None:
    """Serialize metadata dictionary to a JSON string for storage.

    Falls back to ``str(metadata)`` if JSON serialization raises ``TypeError``
    or ``ValueError``.

    Args:
        metadata: Metadata dictionary to serialize, or ``None``.

    Returns:
        str | None: JSON string representation, or ``None`` if ``metadata`` is
            ``None``.
    """
    if metadata is None:
        return None
    try:
        return json.dumps(metadata)
    except (TypeError, ValueError) as e:
        logger.debug(
            f"Failed to serialize GridFS metadata to JSON: {e}. Falling back to str()."
        )
        return str(metadata)


def deserialize_metadata(
    metadata_str: str | None,
) -> dict[str, Any] | None:
    """Deserialize a metadata JSON string back to a dictionary.

    Tries ``json.loads`` first, then ``ast.literal_eval`` (when it yields a
    ``dict``), and finally wraps the raw string in ``{"_metadata": ...}``.

    Args:
        metadata_str: Serialized metadata string, or ``None``.

    Returns:
        dict[str, Any] | None: Metadata dictionary, or ``None`` if input is
            ``None``.
    """
    if metadata_str is None:
        return None
    try:
        return json.loads(metadata_str)
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.debug(
            f"Failed to deserialize GridFS metadata JSON: {e}. Trying ast.literal_eval."
        )
        try:
            result = ast.literal_eval(metadata_str)
            if isinstance(result, dict):
                return result
        except (ValueError, SyntaxError) as e2:
            logger.debug(
                f"ast.literal_eval also failed for GridFS metadata: {e2}. Wrapping in dict."
            )
            pass
        return {"_metadata": metadata_str}


def serialize_aliases(aliases: list[str] | None) -> str | None:
    """Serialize an aliases list to a JSON string.

    Falls back to ``str(aliases)`` if JSON serialization raises ``TypeError``
    or ``ValueError``.

    Args:
        aliases: List of alias strings to serialize, or ``None``.

    Returns:
        str | None: JSON string representation, or ``None`` if ``aliases`` is
            ``None``.
    """
    if aliases is None:
        return None
    try:
        return json.dumps(aliases)
    except (TypeError, ValueError) as e:
        logger.debug(
            f"Failed to serialize GridFS aliases to JSON: {e}. Falling back to str()."
        )
        return str(aliases)


def deserialize_aliases(aliases_str: str | None) -> list[str] | None:
    """Deserialize an aliases JSON string back to a list of strings.

    Tries ``json.loads`` first; if the decoded value is not a ``list``, wraps
    its string form in a single-element list. If JSON parsing fails, wraps
    non-empty strings in a single-element list or returns ``None`` for empty
    strings.

    Args:
        aliases_str: Serialized aliases string, or ``None``.

    Returns:
        list[str] | None: List of alias strings, or ``None`` if input is
            ``None`` or an empty unparseable string.
    """
    if aliases_str is None:
        return None
    try:
        result = json.loads(aliases_str)
        if isinstance(result, list):
            return result
        # If it's not a list, wrap it in a list
        return [str(result)]
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.debug(
            f"Failed to deserialize GridFS aliases JSON: {e}. Falling back to simple string wrap."
        )
        # Fallback to parsing as a simple string or return as-is
        if aliases_str:
            return [aliases_str]
        return None


def force_sync_if_needed(
    db_connection: Any,
    write_concern: dict[str, Any],
) -> None:
    """Force a SQLite WAL checkpoint if write concern requires durability.

    Executes ``PRAGMA wal_checkpoint(PASSIVE)`` when ``write_concern["j"]``
    is truthy or ``write_concern["w"] == "majority"``.

    Args:
        db_connection: SQLite database connection.
        write_concern: Write concern dictionary checked for ``"j"`` and
            ``"w"`` keys.
    """
    j = write_concern.get("j")
    w = write_concern.get("w")
    if j or w == "majority":
        db_connection.execute("PRAGMA wal_checkpoint(PASSIVE)")


__all__ = [
    "serialize_metadata",
    "deserialize_metadata",
    "serialize_aliases",
    "deserialize_aliases",
    "force_sync_if_needed",
]


_table_columns_cache: dict[tuple[int, str], frozenset[str]] = {}


def get_table_columns_cached(db_connection: Any, table_name: str) -> set:
    """Return column names for a table with a per-connection cache (#152).

    GridFS constructs many ``GridOut`` objects; ``PRAGMA table_info`` per
    object dominated read paths. Results are cached by ``(id(db_connection),
    table_name)`` and cleared whenever the cache reaches 128 entries (schema
    changes are rare, and the connection itself pins its ``id``).

    Args:
        db_connection: SQLite database connection to inspect.
        table_name: Name of the table whose column names are returned.

    Returns:
        set: Copy of the cached set of column names for ``table_name``.
    """
    key = (id(db_connection), table_name)
    cols = _table_columns_cache.get(key)
    if cols is None:
        cols = frozenset(get_table_columns(db_connection, table_name))
        if len(_table_columns_cache) >= 128:
            _table_columns_cache.clear()
        _table_columns_cache[key] = cols
    return set(cols)
