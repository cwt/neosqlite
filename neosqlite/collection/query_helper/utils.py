"""Utility functions for query helper operations."""

from ...binary import Binary
from ...exceptions import MalformedQueryException
from typing import Any

# Global flag to force fallback - for benchmarking and debugging
_FORCE_FALLBACK = False


def _get_json_function_prefix(jsonb_supported: bool) -> str:
    """
    Get the appropriate JSON function prefix based on JSONB support.

    Args:
        jsonb_supported: Whether JSONB functions are supported

    Returns:
        str: "jsonb" if JSONB is supported, "json" otherwise
    """
    return "jsonb" if jsonb_supported else "json"


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


def _is_numeric_value(value: Any) -> bool:
    """
    Check if a value is numeric (int or float) or can be converted to a numeric value.

    This function determines if a value can be safely used in arithmetic operations
    like $inc and $mul. It considers:
    - int and float values as numeric (excluding bool, NaN, and infinity)
    - None as non-numeric (would cause issues in arithmetic)
    - String representations of numbers as non-numeric (to match MongoDB behavior)

    Args:
        value: The value to check

    Returns:
        bool: True if the value is numeric, False otherwise
    """
    # Explicitly exclude boolean values (even though bool is subclass of int in Python)
    if isinstance(value, bool):
        return False

    # Check for actual numeric types
    if isinstance(value, (int, float)):
        # Special case: check for NaN and infinity
        if isinstance(value, float):
            import math

            if math.isnan(value) or math.isinf(value):
                return False
        return True

    # Everything else is considered non-numeric for MongoDB compatibility
    return False


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
