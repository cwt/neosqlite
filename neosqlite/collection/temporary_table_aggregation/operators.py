"""Composite operator mixin for temporary table aggregation."""

from __future__ import annotations

from .operators_advanced import OperatorsAdvancedMixin
from .operators_group import OperatorsGroupMixin
from .operators_lookup import HASH_JOIN_MEMORY_THRESHOLD, OperatorsLookupMixin
from .operators_match import OperatorsMatchMixin
from .operators_sort_proj import OperatorsSortProjMixin
from .operators_text import OperatorsTextMixin

__all__ = ["OperatorsMixin", "HASH_JOIN_MEMORY_THRESHOLD"]


class OperatorsMixin(
    OperatorsMatchMixin,
    OperatorsLookupMixin,
    OperatorsSortProjMixin,
    OperatorsGroupMixin,
    OperatorsTextMixin,
    OperatorsAdvancedMixin,
):
    """Compose all temporary-table aggregation stage operator mixins.

    Combines ``OperatorsMatchMixin``, ``OperatorsLookupMixin``,
    ``OperatorsSortProjMixin``, ``OperatorsGroupMixin``,
    ``OperatorsTextMixin``, and ``OperatorsAdvancedMixin`` into a single
    mixin class without defining additional method bodies here.
    """
