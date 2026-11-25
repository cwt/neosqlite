"""
Shared utility module for datetime-related functionality.

This module provides common datetime patterns and utility functions
to avoid code duplication across multiple modules.
"""

import datetime
import re
from typing import Any

# Define common datetime patterns as constants
DATETIME_PATTERNS = [
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",  # ISO format: 2023-01-15T10:30:00
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}",  # With timezone: 2023-01-15T10:30:00+05:30
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",  # UTC: 2023-01-15T10:30:00Z
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",  # Alternative format: 2023-01-15 10:30:00
    r"\d{4}-\d{2}-\d{2}",  # Date only: 2023-01-15
    r"\d{2}/\d{2}/\d{4}",  # US format: 01/15/2023
    r"\d{2}-\d{2}-\d{4}",  # Common format: 01-15-2023
    r"\d{4}/\d{2}/\d{2}",  # Another common format: 2023/01/15
]

# Pre-compile the datetime patterns for better performance
COMPILED_DATETIME_PATTERNS = [
    re.compile(pattern) for pattern in DATETIME_PATTERNS
]


def is_datetime_value(value: Any) -> bool:
    """Check if a value is a datetime object or datetime string.

    Evaluates ``datetime.datetime`` and ``datetime.date`` instances directly,
    matches stripped strings of at least 10 characters starting with a digit
    against ``COMPILED_DATETIME_PATTERNS``, and recursively inspects
    dictionaries (with a fast path for MongoDB ``$date`` dicts).

    Args:
        value: Value to check.

    Returns:
        bool: True if ``value`` represents or contains a datetime value,
        False otherwise.
    """
    if isinstance(value, (datetime.datetime, datetime.date)):
        return True

    if isinstance(value, str):
        stripped_value = value.strip()
        # Quick exit for strings that are too short to be dates (YYYY-MM-DD is 10 chars)
        if len(stripped_value) < 10:
            return False
        # Quick exit for strings that don't start with a digit
        if not stripped_value[0].isdigit():
            return False

        for pattern in COMPILED_DATETIME_PATTERNS:
            if pattern.match(stripped_value):
                return True

    # If it's a dict, check nested values
    if isinstance(value, dict):
        # Optimization: Special case for MongoDB $date helper
        if "$date" in value:
            return is_datetime_value(value["$date"])
        # Otherwise, recursive check for all values (required by some tests)
        for v in value.values():
            if is_datetime_value(v):
                return True

    return False


_INDICATOR_SOURCES = [
    r"\\d\{4\}-\\d\{2\}-\\d\{2\}",
    r"\\d\{2\}/\\d\{2\}/\\d\{4\}",
    r"\\d\{4\}/\\d\{2\}/\\d\{2\}",
    r"\\d\{2\}-\\d\{2\}-\\d\{4\}",
    r"\\d\{4\}-\\d\{2\}-\\d\{2\}T\\d\{2\}:\d{2}:\d{2}",
]

_COMPILED_INDICATORS = [re.compile(ind) for ind in _INDICATOR_SOURCES]


def is_datetime_regex(pattern: str) -> bool:
    """Check if a regex pattern is likely to be for datetime matching.

    Returns False for non-string inputs. Otherwise checks whether the
    pattern string itself matches a datetime value via
    ``is_datetime_value()`` or searches for common escaped date/time digit
    regex fragments defined in ``_COMPILED_INDICATORS``.

    Args:
        pattern: Regex pattern string to inspect.

    Returns:
        bool: True if the pattern appears datetime-related, False otherwise.
    """

    if not isinstance(pattern, str):
        return False

    # Check if the pattern itself looks like a datetime value
    if is_datetime_value(pattern):
        return True

    # Check if the pattern contains common datetime-related regex patterns
    return any(ind.search(pattern) for ind in _COMPILED_INDICATORS)


def contains_datetime_operations(query: Any) -> bool:
    """Return True when ``query`` references datetime values, ranges, ``$type``, or regexes."""
    if not isinstance(query, dict):
        return False

    for field, value in query.items():
        if (
            field == "_id"
            and isinstance(value, dict)
            and any(op in value for op in ("$gt", "$gte", "$lt", "$lte"))
        ):
            continue
        if field in ("$and", "$or", "$nor"):
            if isinstance(value, list) and any(
                isinstance(cond, dict) and contains_datetime_operations(cond)
                for cond in value
            ):
                return True
        elif field == "$not":
            if isinstance(value, dict) and contains_datetime_operations(value):
                return True
        elif isinstance(value, dict):
            for operator, op_value in value.items():
                if operator in ("$gte", "$gt", "$lte", "$lt", "$eq", "$ne"):
                    if is_datetime_value(op_value):
                        return True
                elif operator in ("$in", "$nin"):
                    if isinstance(op_value, list) and any(
                        is_datetime_value(item) for item in op_value
                    ):
                        return True
                elif operator == "$type" and op_value in (9, "date", "Date"):
                    return True
                elif operator == "$regex" and is_datetime_regex(op_value):
                    return True
        elif is_datetime_value(value):
            return True
    return False


__all__ = [
    "is_datetime_value",
    "is_datetime_regex",
    "contains_datetime_operations",
    "DATETIME_PATTERNS",
    "COMPILED_DATETIME_PATTERNS",
]
