"""
Shared utility module for datetime-related functionality.

This module provides common datetime patterns and utility functions
to avoid code duplication across multiple modules.
"""

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
    import datetime

    if isinstance(value, (datetime.datetime, datetime.date)):
        return True

    if isinstance(value, str):
        for pattern in COMPILED_DATETIME_PATTERNS:
            if pattern.match(value.strip()):
                return True

    # If it's a dict, check nested values
    if isinstance(value, dict):
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
    if not isinstance(pattern, str):
        return False

    for indicator in COMPILED_DATETIME_INDICATORS:
        if indicator.search(pattern):
            return True

    return False
