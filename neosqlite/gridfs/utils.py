"""
Shared utility functions for GridFS operations.

This module contains common functionality used across GridFS components
to avoid code duplication and ensure consistent behavior.
"""

from __future__ import annotations

import ast
import json
from typing import Any, Dict, Optional


def serialize_metadata(metadata: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Serialize metadata dictionary to JSON string for storage.

    Args:
        metadata: Metadata dictionary or None

    Returns:
        JSON string representation, or None if metadata is None

    Note:
        Falls back to string representation if JSON serialization fails.
    """
    if metadata is None:
        return None
    try:
        return json.dumps(metadata)
    except (TypeError, ValueError):
        return str(metadata)


def deserialize_metadata(
    metadata_str: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Deserialize metadata JSON string back to dictionary.

    Args:
        metadata_str: JSON string or None

    Returns:
        Metadata dictionary, or None if input is None

    Note:
        Tries JSON parsing first, then ast.literal_eval as fallback,
        then wraps in dict as last resort.
    """
    if metadata_str is None:
        return None
    try:
        return json.loads(metadata_str)
    except (TypeError, ValueError, json.JSONDecodeError):
        try:
            result = ast.literal_eval(metadata_str)
            if isinstance(result, dict):
                return result
        except (ValueError, SyntaxError):
            pass
        return {"_metadata": metadata_str}


def serialize_aliases(aliases: Optional[list[str]]) -> Optional[str]:
    """
    Serialize aliases list to JSON string.

    Args:
        aliases: List of alias strings or None

    Returns:
        JSON string representation, or None if aliases is None

    Note:
        Falls back to string representation if JSON serialization fails.
    """
    if aliases is None:
        return None
    try:
        return json.dumps(aliases)
    except (TypeError, ValueError):
        return str(aliases)


def deserialize_aliases(aliases_str: Optional[str]) -> Optional[list[str]]:
    """
    Deserialize aliases JSON string back to list.

    Args:
        aliases_str: JSON string or None

    Returns:
        List of aliases, or None if input is None

    Note:
        Tries JSON parsing first, then wraps single value in list as fallback.
    """
    if aliases_str is None:
        return None
    try:
        result = json.loads(aliases_str)
        if isinstance(result, list):
            return result
        # If it's not a list, wrap it in a list
        return [str(result)]
    except (TypeError, ValueError, json.JSONDecodeError):
        # Fallback to parsing as a simple string or return as-is
        if aliases_str:
            return [aliases_str]
        return None


def force_sync_if_needed(
    db_connection: Any,
    write_concern: Dict[str, Any],
) -> None:
    """
    Force a SQLite WAL checkpoint if write concern requires durability.

    Args:
        db_connection: SQLite database connection
        write_concern: Write concern dictionary with 'j' and 'w' keys
    """
    if write_concern.get("j") is True or write_concern.get("w") == "majority":
        db_connection.execute("PRAGMA wal_checkpoint(PASSIVE)")


__all__ = [
    "serialize_metadata",
    "deserialize_metadata",
    "serialize_aliases",
    "deserialize_aliases",
    "force_sync_if_needed",
]
