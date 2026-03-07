"""
Context management and helper functions for expression evaluation.

This module provides the AggregationContext class for managing variable scoping
and helper functions for identifying different types of expression values.

Note: Type checking helpers (_is_expression, _is_field_reference, _is_literal)
have been moved to collection.type_utils for shared use across subpackages.
They are re-exported here for backward compatibility.
"""

from __future__ import annotations
from typing import Any, Dict

# Import type checking helpers from shared module
# These are re-exported for backward compatibility
from ..type_utils import (
    _is_expression as _is_expression,
    _is_field_reference as _is_field_reference,
    _is_literal as _is_literal,
)


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
