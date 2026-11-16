"""Context management and helper functions for expression evaluation.

This module provides the ``AggregationContext`` class for managing variable
scoping and helper functions for identifying different types of expression
values.

Note: Type checking helpers (``_is_expression``, ``_is_field_reference``,
``_is_literal``) have been moved to ``collection.type_utils`` for shared use
across subpackages. They are re-exported here for backward compatibility.
"""

from __future__ import annotations

from typing import Any, Self

# Import type checking helpers from shared module
# These are re-exported for backward compatibility
from ..type_utils import (
    _is_expression as _is_expression,
)
from ..type_utils import (
    _is_field_reference as _is_field_reference,
)
from ..type_utils import (
    _is_literal as _is_literal,
)


class AggregationContext:
    """Manages variable scoping for aggregation expressions.

    Aggregation expressions have different variable contexts than query
    expressions. This class manages the lifecycle of aggregation variables
    such as ``$$ROOT``, ``$$CURRENT``, and ``$$REMOVE``, as well as scoped
    variables bound by ``$let``, ``$map``, ``$filter``, and ``$reduce``.

    Attributes:
        variables: Dictionary mapping variable names to their values.
        stage_index: Current stage index in the pipeline.
        current_field: Name of the field currently being computed.
        pipeline_id: Unique identifier for the pipeline.
        root_column: SQL column name holding the original root document,
            or ``None`` when using the default data column.
    """

    def __init__(self) -> None:
        """Initialize aggregation context with default variables.

        Sets ``$$ROOT``, ``$$CURRENT``, and ``$$REMOVE`` to ``None`` and
        initializes pipeline stage tracking attributes.
        """
        self.variables: dict[str, Any] = {
            "$$ROOT": None,  # Original document
            "$$CURRENT": None,  # Current document (may be modified)
            "$$REMOVE": None,  # Sentinel for field removal
        }
        self.stage_index: int = 0
        self.current_field: str | None = None
        self.pipeline_id: str | None = None
        self.root_column: str | None = None

    def bind_document(self, doc: dict[str, Any]) -> None:
        """Bind the input document to the aggregation context.

        Called at the start of pipeline execution to initialize both
        ``$$ROOT`` and ``$$CURRENT`` with the input document.

        Args:
            doc: The input document to bind.
        """
        self.variables["$$ROOT"] = doc
        self.variables["$$CURRENT"] = doc

    def update_current(self, doc: dict[str, Any]) -> None:
        """Update the current document after stage processing.

        Called after each pipeline stage that modifies the document to update
        the ``$$CURRENT`` variable while keeping ``$$ROOT`` unchanged.

        Args:
            doc: The updated document for subsequent stages.
        """
        self.variables["$$CURRENT"] = doc

    def get_variable(self, name: str) -> Any:
        """Retrieve a variable value from the context.

        Args:
            name: Variable name (for example, ``"$$ROOT"`` or
                ``"$$CURRENT"``).

        Returns:
            Any: The stored variable value, or ``None`` if not defined.
        """
        return self.variables.get(name)

    def set_variable(self, name: str, value: Any) -> None:
        """Store a variable value in the context.

        Args:
            name: Variable name to bind or overwrite.
            value: Value or SQL binding associated with the variable.
        """
        self.variables[name] = value

    def clone(self) -> Self:
        """Create a shallow copy of the context for nested scoping.

        Copies the ``variables`` mapping and transfers ``stage_index``,
        ``current_field``, ``pipeline_id``, and ``root_column`` onto a new
        instance.

        Returns:
            Self: A new context instance with copied variable bindings and
                metadata.
        """
        new_ctx = self.__class__()
        new_ctx.variables = self.variables.copy()
        new_ctx.stage_index = self.stage_index
        new_ctx.current_field = self.current_field
        new_ctx.pipeline_id = self.pipeline_id
        new_ctx.root_column = self.root_column
        return new_ctx


def _is_aggregation_variable(value: Any) -> bool:
    """Check whether a value is an aggregation variable reference.

    Aggregation variables are strings that start with ``"$$"`` (such as
    ``"$$ROOT"`` and ``"$$CURRENT"``).

    Args:
        value: Value to inspect.

    Returns:
        bool: ``True`` if ``value`` is a string starting with ``"$$"``,
            otherwise ``False``.

    Examples:
        >>> _is_aggregation_variable("$$ROOT")
        True
        >>> _is_aggregation_variable("$$CURRENT")
        True
        >>> _is_aggregation_variable("$field")
        False
    """
    return isinstance(value, str) and value.startswith("$$")
