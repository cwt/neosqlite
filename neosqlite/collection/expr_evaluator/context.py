"""
Context management and helper functions for expression evaluation.

This module provides the AggregationContext class for managing variable scoping
and helper functions for identifying different types of expression values.
"""

from __future__ import annotations
from typing import Any, Dict

# Import RESERVED_FIELDS from constants to avoid circular dependency
from .constants import RESERVED_FIELDS


class AggregationContext:
    """
    Manages variable scoping for aggregation expressions.

    Aggregation expressions have different variable contexts than query expressions.
    This class manages the lifecycle of aggregation variables like $$ROOT, $$CURRENT,
    and $$REMOVE throughout pipeline execution.

    Attributes:
        variables: Dictionary mapping variable names to their values
        stage_index: Current stage index in the pipeline
        current_field: Name of the field being computed (for context)
        pipeline_id: Unique identifier for the pipeline (for temp table correlation)
    """

    def __init__(self) -> None:
        """Initialize aggregation context with default variables."""
        self.variables: Dict[str, Any] = {
            "$$ROOT": None,  # Original document
            "$$CURRENT": None,  # Current document (may be modified)
            "$$REMOVE": None,  # Sentinel for field removal
        }
        self.stage_index: int = 0
        self.current_field: str | None = None
        self.pipeline_id: str | None = None

    def bind_document(self, doc: Dict[str, Any]) -> None:
        """
        Bind document to context.

        Called at the start of pipeline execution to initialize
        $$ROOT and $$CURRENT with the input document.

        Args:
            doc: The document to bind
        """
        self.variables["$$ROOT"] = doc
        self.variables["$$CURRENT"] = doc

    def update_current(self, doc: Dict[str, Any]) -> None:
        """
        Update current document after stage processing.

        Called after each stage that modifies the document to update
        the $$CURRENT variable.

        Args:
            doc: The updated document
        """
        self.variables["$$CURRENT"] = doc

    def get_variable(self, name: str) -> Any:
        """
        Get variable value.

        Args:
            name: Variable name (e.g., "$$ROOT", "$$CURRENT")

        Returns:
            Variable value or None if not found
        """
        return self.variables.get(name)

    def set_variable(self, name: str, value: Any) -> None:
        """
        Set variable value.

        Args:
            name: Variable name
            value: Value to set
        """
        self.variables[name] = value


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


def _is_aggregation_variable(value: Any) -> bool:
    """
    Check for aggregation variables.

    Aggregation variables start with '$$' (e.g., $$ROOT, $$CURRENT).

    Args:
        value: Value to check

    Returns:
        True if value is an aggregation variable, False otherwise

    Examples:
        >>> _is_aggregation_variable("$$ROOT")
        True
        >>> _is_aggregation_variable("$$CURRENT")
        True
        >>> _is_aggregation_variable("$field")
        False
    """
    return isinstance(value, str) and value.startswith("$$")


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
