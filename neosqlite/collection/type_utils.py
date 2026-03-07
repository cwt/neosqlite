"""
Shared type checking and conversion utilities for the collection package.

This module consolidates type-related utility functions that are used across
multiple submodules (expr_evaluator, query_helper, etc.) to avoid code duplication
and provide a single source of truth for type operations.
"""

from __future__ import annotations
from typing import Any


# =============================================================================
# Type Conversion Functions
# =============================================================================


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


# =============================================================================
# Type Checking Helpers
# =============================================================================


def _is_expression(value: Any) -> bool:
    """
    Check if value is an aggregation expression.

    An expression is a dict with exactly one key starting with '$'
    that is not a reserved field name.

    Args:
        value: Value to check

    Returns:
        True if value is an expression, False otherwise

    Examples:
        >>> _is_expression({"$sin": "$angle"})
        True
        >>> _is_expression({"$field": "value"})  # Reserved
        False
        >>> _is_expression("$field")
        False
        >>> _is_expression(42)
        False
    """
    # Reserved field names that are NOT operators (copied from expr_evaluator.constants
    # to avoid circular import)
    RESERVED_FIELDS = {"$field", "$index"}

    if not isinstance(value, dict):
        return False
    if len(value) != 1:
        return False  # Could be a literal dict
    key = next(iter(value.keys()))
    return key.startswith("$") and key not in RESERVED_FIELDS


def _is_field_reference(value: Any) -> bool:
    """
    Check if value is a field reference.

    Field references start with '$' but are not expressions
    (i.e., they're simple strings like "$field" or "$nested.field").

    Args:
        value: Value to check

    Returns:
        True if value is a field reference, False otherwise

    Examples:
        >>> _is_field_reference("$field")
        True
        >>> _is_field_reference("$nested.field")
        True
        >>> _is_field_reference("$$ROOT")
        False
        >>> _is_field_reference({"$sin": "$angle"})
        False
    """
    return (
        isinstance(value, str)
        and value.startswith("$")
        and not value.startswith("$$")
    )


def _is_literal(value: Any) -> bool:
    """
    Check if value is a literal (not an expression or field reference).

    Literals include: numbers, strings, booleans, None, arrays, and plain dicts.

    Args:
        value: Value to check

    Returns:
        True if value is a literal, False otherwise

    Examples:
        >>> _is_literal(42)
        True
        >>> _is_literal("string")
        True
        >>> _is_literal(True)
        True
        >>> _is_literal(None)
        True
        >>> _is_literal([1, 2, 3])
        True
        >>> _is_literal("$field")
        False
    """
    if isinstance(value, str):
        # Strings starting with $ are field refs or variables, not literals
        return not value.startswith("$")
    # All other types are literals
    return True


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


__all__ = [
    # Type conversion functions
    "_convert_to_int",
    "_convert_to_long",
    "_convert_to_double",
    "_convert_to_decimal",
    "_convert_to_string",
    "_convert_to_bool",
    "_convert_to_objectid",
    "_convert_to_bindata",
    "_convert_to_bsonbindata",
    "_convert_to_regex",
    "_convert_to_bsonregex",
    "_convert_to_date",
    "_convert_to_null",
    "get_bson_type",
    # Type checking helpers
    "_is_expression",
    "_is_field_reference",
    "_is_literal",
    "_is_numeric_value",
]
