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

# Pre-compile the datetime indicators for regex checking - these handle regex string patterns
# When regex patterns are passed as strings, they appear as '\\d{4}-\\d{2}-\\d{2}' in memory
DATETIME_INDICATORS = [
    r"\\d\{4\}-\\d\{2\}-\\d\{2\}",  # This matches '\\d{4}-\\d{2}-\\d{2}' in a string
    r"\\d\{2\}/\\d\{2\}/\\d\{4\}",
    r"\\d\{4\}/\\d\{2\}/\\d\{2\}",
    r"\\d\{2\}-\\d\{2\}-\\d\{4\}",
    r"\\d\{4\}-\\d\{2\}-\\d\{2\}T\\d\{2\}:\\d\{2\}:\\d\{2\}",
]

COMPILED_DATETIME_INDICATORS = [
    re.compile(indicator) for indicator in DATETIME_INDICATORS
]


def is_datetime_value(value: Any) -> bool:
    """
    Check if a value is a datetime object or datetime string.

    Args:
        value: Value to check

    Returns:
        True if value is datetime-related, False otherwise
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


def is_datetime_regex(pattern: str) -> bool:
    """
    Check if a regex pattern is likely to be for datetime matching.

    Args:
        pattern: Regex pattern string

    Returns:
        True if pattern is likely datetime-related, False otherwise
    """
    import re

    if not isinstance(pattern, str):
        return False

    # Check if the pattern itself looks like a datetime value
    if is_datetime_value(pattern):
        return True

    # Check if the pattern contains common datetime-related regex patterns
    datetime_indicators = [
        r"\\d\{4\}-\\d\{2\}-\\d\{2\}",  # Date format: \d{4}-\d{2}-\d{2}
        r"\\d\{2\}/\\d\{2\}/\\d\{4\}",  # US date format: \d{2}/\d{2}/\d{4}
        r"\\d\{4\}/\\d\{2\}/\\d\{2\}",  # Alternative date format: \d{4}/\d{2}/\d{2}
        r"\\d\{2\}-\\d\{2\}-\\d\{4\}",  # Common date format: \d{2}-\d{2}-\d{4}
        r"\\d\{4\}-\\d\{2\}-\\d\{2\}T\\d\{2\}:\\d\{2\}:\\d\{2\}",  # Datetime format
    ]

    for indicator in datetime_indicators:
        if re.search(indicator, pattern):
            return True

    return False


__all__ = [
    "is_datetime_value",
    "is_datetime_regex",
    "DATETIME_PATTERNS",
    "COMPILED_DATETIME_PATTERNS",
    "DATETIME_INDICATORS",
    "COMPILED_DATETIME_INDICATORS",
]
