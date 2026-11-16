"""PipelineContext — tracks field state across aggregation pipeline stages.

Extracted from sql_tier_aggregator.py for reuse and testability.
"""

from __future__ import annotations

from typing import Any, Self


class PipelineContext:
    """Track field aliases, computed fields, and document state across stages.

    Maintains per-pipeline metadata during SQL aggregation compilation,
    including computed field SQL expressions, removed fields, ``$$ROOT``
    preservation state, CTE naming counters, and auxiliary pre-CTE clauses.
    """

    def __init__(self) -> None:
        """Initialize pipeline context with empty field and CTE state."""
        self.computed_fields: dict[str, str] = {}
        self.removed_fields: set[str] = set()
        self.stage_index: int = 0
        self.has_root: bool = False
        self.has_computed: bool = False
        self.needs_recursive: bool = False
        self.cte_prefix: str = "stage"
        self.pre_ctes: list[tuple[str, str, list[Any]]] = []
        self._aux_counter: int = 0

    def next_aux_id(self) -> int:
        """Return a unique incrementing integer id for auxiliary CTE naming.

        Returns:
            int: Monotonically increasing auxiliary identifier starting at 1.
        """
        self._aux_counter += 1
        return self._aux_counter

    def mark_recursive(self) -> None:
        """Mark that the compiled WITH clause must use ``WITH RECURSIVE``."""
        self.needs_recursive = True

    def add_pre_cte(
        self, name: str, sql: str, params: list[Any] | None = None
    ) -> None:
        """Register an auxiliary top-level CTE before the current stage CTE.

        Args:
            name: Name of the auxiliary CTE to register.
            sql: SQL query body for the CTE.
            params: Optional bind parameters associated with ``sql``.
        """
        self.pre_ctes.append((name, sql, list(params) if params else []))

    def drain_pre_ctes(self) -> list[tuple[str, str, list[Any]]]:
        """Return and clear pending auxiliary pre-CTEs in registration order.

        Returns:
            list[tuple[str, str, list[Any]]]: List of ``(name, sql, params)``
            tuples accumulated since the last drain.
        """
        ctes = self.pre_ctes
        self.pre_ctes = []
        return ctes

    def add_computed_field(self, field: str, sql_expr: str) -> None:
        """Track a computed field and mark that computed fields exist.

        Args:
            field: Field path or name produced by a pipeline stage.
            sql_expr: SQL expression evaluating the computed field value.
        """
        self.computed_fields[field] = sql_expr
        self.has_computed = True

    def remove_field(self, field: str) -> None:
        """Mark a field as removed in the current pipeline context.

        Args:
            field: Field name or path excluded by a prior stage.
        """
        self.removed_fields.add(field)

    def get_field_sql(self, field: str) -> str | None:
        """Return the registered SQL expression for a computed field.

        Args:
            field: Field name or path to look up.

        Returns:
            str | None: SQL expression if ``field`` was computed, else
            ``None``.
        """
        return self.computed_fields.get(field)

    def is_field_available(self, field: str) -> bool:
        """Check whether a field has not been removed in the current context.

        Args:
            field: Field name or path to check.

        Returns:
            bool: True if ``field`` is not in ``removed_fields``, else False.
        """
        return field not in self.removed_fields

    def is_field_computed(self, field: str) -> bool:
        """Check whether a field has a tracked computed SQL expression.

        Args:
            field: Field name or path to check.

        Returns:
            bool: True if ``field`` is in ``computed_fields``, else False.
        """
        return field in self.computed_fields

    def preserve_root(self) -> None:
        """Mark that ``$$ROOT`` document state must be preserved."""
        self.has_root = True

    def needs_root(self) -> bool:
        """Check whether ``$$ROOT`` document preservation is required.

        Returns:
            bool: True if ``$$ROOT`` must be preserved, False otherwise.
        """
        return self.has_root

    def clone(self) -> Self:
        """Create a shallow copy of this pipeline context.

        Copies tracked computed/removed field sets, flags, ``cte_prefix``,
        and ``_aux_counter``, leaving ``pre_ctes`` empty on the clone.

        Returns:
            Self: New ``PipelineContext`` instance with copied state.
        """
        new_ctx = self.__class__()
        new_ctx.computed_fields = self.computed_fields.copy()
        new_ctx.removed_fields = self.removed_fields.copy()
        new_ctx.stage_index = self.stage_index
        new_ctx.has_root = self.has_root
        new_ctx.has_computed = self.has_computed
        new_ctx.needs_recursive = self.needs_recursive
        new_ctx.cte_prefix = self.cte_prefix
        new_ctx._aux_counter = self._aux_counter
        return new_ctx
