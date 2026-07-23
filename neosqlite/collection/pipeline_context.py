"""PipelineContext — tracks field state across aggregation pipeline stages.

Extracted from sql_tier_aggregator.py for reuse and testability.
"""

from __future__ import annotations


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
        return new_ctx
