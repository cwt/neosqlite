"""Constants for the expression evaluator.

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
    """Sentinel value for ``$$REMOVE`` in ``$project`` stages.

    When a field resolves to this singleton instance, it should be removed
    from the output document. Only one instance of this class exists.
    """

    _instance = None

    def __new__(cls):
        """Return the singleton ``_RemoveSentinel`` instance.

        Returns:
            _RemoveSentinel: The unique sentinel instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        """Return the MongoDB ``$$REMOVE`` string representation.

        Returns:
            str: The literal string ``"$$REMOVE"``.
        """
        return "$$REMOVE"

    def __bool__(self):
        """Return ``False`` so the sentinel evaluates as falsy.

        Returns:
            bool: Always ``False``.
        """
        return False


# Singleton instance for $$REMOVE
REMOVE_SENTINEL = _RemoveSentinel()
