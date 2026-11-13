"""PipelineContext — tracks field state across aggregation pipeline stages.

Extracted from sql_tier_aggregator.py for reuse and testability.
"""

from __future__ import annotations

from typing import Any


class PipelineContext:
    """
    Tracks field aliases, computed fields, and document state across pipeline stages.
    """

    def __init__(self) -> None:
        """Initialize pipeline context with default state."""
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
        """Return a unique integer id for auxiliary CTE naming."""
        self._aux_counter += 1
        return self._aux_counter

    def mark_recursive(self) -> None:
        """Mark that the compiled WITH clause must use WITH RECURSIVE."""
        self.needs_recursive = True

    def add_pre_cte(
        self, name: str, sql: str, params: list[Any] | None = None
    ) -> None:
        """Register an auxiliary top-level CTE emitted before the current stage CTE."""
        self.pre_ctes.append((name, sql, list(params) if params else []))

    def drain_pre_ctes(self) -> list[tuple[str, str, list[Any]]]:
        """Return and clear pending auxiliary pre-CTEs in registration order."""
        ctes = self.pre_ctes
        self.pre_ctes = []
        return ctes

    def add_computed_field(self, field: str, sql_expr: str) -> None:
        """Track a computed field."""
        self.computed_fields[field] = sql_expr
        self.has_computed = True

    def remove_field(self, field: str) -> None:
        """Mark field as removed."""
        self.removed_fields.add(field)

    def get_field_sql(self, field: str) -> str | None:
        """Get SQL expression for a field."""
        return self.computed_fields.get(field)

    def is_field_available(self, field: str) -> bool:
        """Check if field is available in current context."""
        return field not in self.removed_fields

    def is_field_computed(self, field: str) -> bool:
        """Check if field is a computed field."""
        return field in self.computed_fields

    def preserve_root(self) -> None:
        """Mark that $$ROOT should be preserved."""
        self.has_root = True

    def needs_root(self) -> bool:
        """Check if $$ROOT is needed."""
        return self.has_root

    def clone(self) -> "PipelineContext":
        """Create a copy of this context."""
        new_ctx = PipelineContext()
        new_ctx.computed_fields = self.computed_fields.copy()
        new_ctx.removed_fields = self.removed_fields.copy()
        new_ctx.stage_index = self.stage_index
        new_ctx.has_root = self.has_root
        new_ctx.has_computed = self.has_computed
        new_ctx.needs_recursive = self.needs_recursive
        new_ctx.cte_prefix = self.cte_prefix
        new_ctx._aux_counter = self._aux_counter
        return new_ctx
