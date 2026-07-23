from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..jsonb_support import JSONBContext


class OperatorsBaseMixin:
    """Shared base for temporary-table aggregation stage operators."""

    # Attribute annotations for state supplied by
    # TemporaryTableAggregationProcessor.__init__. Declared on the shared base so every
    # mixin sees them via MRO. Values are assigned by the concrete processor subclass.
    collection: Any
    db: Any
    query_engine: Any
    expr_evaluator: Any
    sql_translator: Any
    jsonb: "JSONBContext"
    _has_sort_stage: bool
    _text_on_temp_table_warned: bool
    _has_unwind_in_pipeline: bool

    # Stub for the single method called across mixin boundaries
    # (_process_match_stage -> _process_text_search_stage). The real implementation lives
    # in OperatorsTextMixin; declaring it here lets static analysis resolve the call via MRO.
    def _process_text_search_stage(self, *args: Any, **kwargs: Any) -> Any: ...
