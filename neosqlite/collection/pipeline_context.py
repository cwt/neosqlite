"""PipelineContext — tracks field state across aggregation pipeline stages.

Extracted from sql_tier_aggregator.py for reuse and testability.
"""

from __future__ import annotations

from typing import Any, Self


class PipelineContext:
    """Track pipeline metadata and document state across stages.

    Maintains per-pipeline metadata during SQL aggregation compilation,
    including ``$$ROOT`` preservation state, recursion requirements, CTE naming
    counters, and auxiliary pre-CTE clauses.
    """

    def __init__(self) -> None:
        """Initialize pipeline context with empty CTE and stage state."""
        self.stage_index: int = 0
        self.has_root: bool = False
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

        Copies flags, ``cte_prefix``, and ``_aux_counter``, leaving
        ``pre_ctes`` empty on the clone.

        Returns:
            Self: New ``PipelineContext`` instance with copied state.
        """
        new_ctx = self.__class__()
        new_ctx.stage_index = self.stage_index
        new_ctx.has_root = self.has_root
        new_ctx.needs_recursive = self.needs_recursive
        new_ctx.cte_prefix = self.cte_prefix
        new_ctx._aux_counter = self._aux_counter
        return new_ctx
