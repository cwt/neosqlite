"""Utility functions for query helper operations."""

from ...binary import Binary
from ...exceptions import MalformedQueryException
from typing import Any, Dict, Optional

# Import JSON function helpers from shared module to avoid duplication
from ..jsonb_support import (
    _get_json_function_prefix as _get_json_function_prefix,
)

# Import type checking helpers from shared module to avoid duplication
from ..type_utils import _is_numeric_value as _is_numeric_value


import sqlite3

# Global cache for SQLite features
_SQLITE_FEATURES: Dict[str, Optional[bool]] = {
    "relative_indexing": None,
}


def _check_sqlite_version(min_version: str) -> bool:
    """
    Check if the current SQLite version meets the minimum requirement.

    Args:
        min_version (str): Minimum version string (e.g., "3.42.0")

    Returns:
        bool: True if requirements met, False otherwise
    """
    current = [int(x) for x in sqlite3.sqlite_version.split(".")]
    required = [int(x) for x in min_version.split(".")]

    # Pad versions to same length
    while len(current) < 3:
        current.append(0)
    while len(required) < 3:
        required.append(0)

    return tuple(current) >= tuple(required)


def _supports_relative_json_indexing() -> bool:
    """
    Check if current SQLite version supports [#-N] relative indexing in JSON paths.
    Supported in SQLite 3.42.0 (2023-05-16) and later.

    Returns:
        bool: True if supported, False otherwise
    """
    val = _SQLITE_FEATURES["relative_indexing"]
    if val is None:
        val = _check_sqlite_version("3.42.0")
        _SQLITE_FEATURES["relative_indexing"] = val
    return val


def _get_json_function(name: str, jsonb_supported: bool) -> str:
    """
    Get the appropriate JSON function name based on JSONB support.

    Args:
        name: The base function name (without json/jsonb prefix)
        jsonb_supported: Whether JSONB functions are supported

    Returns:
        str: The full function name with appropriate prefix
    """
    prefix = _get_json_function_prefix(jsonb_supported)
    return f"{prefix}_{name}"


# Global flag to force fallback - for benchmarking and debugging
_FORCE_FALLBACK = False


def _convert_bytes_to_binary(obj: Any) -> Any:
    """
    Recursively convert bytes objects to Binary objects in a document.

    This function traverses a document structure (dict, list, etc.) and converts
    any bytes objects to Binary objects, which can be properly serialized to JSON.
    Existing Binary objects are left unchanged to preserve their subtype information.

    Args:
        obj: The object to process (can be dict, list, bytes, Binary, or other types)

    Returns:
        The processed object with bytes converted to Binary objects
    """
    # Check for Binary first, since Binary inherits from bytes
    if isinstance(obj, Binary):
        # Leave Binary objects unchanged to preserve subtype information
        return obj
    elif isinstance(obj, bytes):
        return Binary(obj)
    elif isinstance(obj, dict):
        return {
            key: _convert_bytes_to_binary(value) for key, value in obj.items()
        }
    elif isinstance(obj, list):
        return [_convert_bytes_to_binary(item) for item in obj]
    else:
        return obj


def set_force_fallback(force: bool = True) -> None:
    """Set global flag to force all aggregation queries to use Python fallback.

    This function is useful for benchmarking and debugging to compare performance
    between the optimized SQL path and the Python fallback path.

    Args:
        force (bool): If True, forces all aggregation queries to use Python fallback.
                     If False, allows normal optimization behavior.
    """
    global _FORCE_FALLBACK
    _FORCE_FALLBACK = force


def get_force_fallback() -> bool:
    """Get the current state of the force fallback flag.

    Returns:
        bool: True if fallback is forced, False otherwise.
    """
    global _FORCE_FALLBACK
    return _FORCE_FALLBACK


# Note: _is_numeric_value is now imported from ..type_utils above to avoid
# code duplication. It is re-exported from this module for backward compatibility.


def _validate_inc_mul_field_value(
    field_name: str, field_value: Any, operation: str
) -> None:
    """
    Validate that a field value is appropriate for $inc or $mul operations.

    Args:
        field_name: The name of the field being validated
        field_value: The current value of the field
        operation: The operation being performed ("$inc" or "$mul")

    Raises:
        MalformedQueryException: If the field value is not appropriate for the operation
    """
    # If the field doesn't exist, it's acceptable as it will be treated as 0
    if field_value is None:
        return

    # Check if the field value is numeric
    if not _is_numeric_value(field_value):
        raise MalformedQueryException(
            f"Cannot apply {operation} to a value of non-numeric type. "
            f"Field '{field_name}' has non-numeric type {type(field_value).__name__} "
            f"with value {repr(field_value)}"
        )
