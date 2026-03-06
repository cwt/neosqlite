"""
Type conversion utilities for expression evaluation.

This module provides type conversion functions and BSON type detection
used by the $convert and $type operators.
"""

from __future__ import annotations
from typing import Any


def _convert_to_int(value: Any) -> Any:
    """Convert value to int."""
    return int(value)


def _convert_to_long(value: Any) -> Any:
    """Convert value to long (64-bit int)."""
    return int(value)


def _convert_to_double(value: Any) -> Any:
    """Convert value to double (float)."""
    return float(value)


def _convert_to_decimal(value: Any) -> Any:
    """Convert value to decimal (float, as SQLite lacks Decimal128)."""
    return float(value)


def _convert_to_string(value: Any) -> Any:
    """Convert value to string."""
    return str(value)


def _convert_to_bool(value: Any) -> Any:
    """Convert value to bool."""
    return bool(value)


def _convert_to_objectid(value: Any) -> Any:
    """Convert value to ObjectId."""
    from neosqlite.objectid import ObjectId

    return ObjectId(str(value)) if value else None


def _convert_to_bindata(value: Any) -> Any:
    """Convert value to Binary (binData)."""
    from neosqlite.binary import Binary

    if value is None:
        return None
    if isinstance(value, str):
        return Binary(value.encode("utf-8"))
    return Binary(value)


def _convert_to_bsonbindata(value: Any) -> Any:
    """Convert value to Binary (bsonBinData)."""
    from neosqlite.binary import Binary

    if value is None:
        return None
    if isinstance(value, str):
        return Binary(value.encode("utf-8"))
    return Binary(value)


def _convert_to_regex(value: Any) -> Any:
    """Convert value to regex pattern."""
    import re

    return re.compile(str(value)) if value else None


def _convert_to_bsonregex(value: Any) -> Any:
    """Convert value to regex pattern (bsonRegex)."""
    import re

    return re.compile(str(value)) if value else None


def _convert_to_date(value: Any) -> Any:
    """Convert value to date (returns as-is; proper conversion requires parsing)."""
    return value


def _convert_to_null(value: Any) -> None:
    """Convert any value to None."""
    return None


def get_bson_type(value: Any) -> str:
    """
    Get BSON type name for a value.

    Args:
        value: The value to check

    Returns:
        BSON type name (e.g., 'null', 'bool', 'int', 'double', 'string', 'array', 'object')
    """
    match value:
        case None:
            return "null"
        case bool():
            return "bool"
        case int():
            return "int"
        case float():
            return "double"
        case str():
            return "string"
        case list():
            return "array"
        case dict():
            return "object"
        case _:
            return "unknown"
