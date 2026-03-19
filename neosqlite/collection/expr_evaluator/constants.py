"""
Constants for the expression evaluator.

This module contains constant values and sentinel objects used throughout
the expression evaluator.
"""

from __future__ import annotations

# Reserved field names that are NOT operators
RESERVED_FIELDS = {
    "$field",
    "$index",  # Used in $let
    # Add other reserved names as needed
}


class _RemoveSentinel:
    """
    Sentinel value for $$REMOVE in $project stage.

    When a field is set to this value, it should be removed from the output document.
    This is a singleton pattern - only one instance should exist.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "$$REMOVE"

    def __bool__(self):
        return False


# Singleton instance for $$REMOVE
REMOVE_SENTINEL = _RemoveSentinel()
