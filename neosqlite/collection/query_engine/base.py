"""Base protocol for QueryEngine mixins."""

from __future__ import annotations

from typing import Any, Dict, TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from ..cursor import Cursor
    from ..query_helper import QueryHelper
    from ..sql_translator_unified import SQLTranslator


class QueryEngineProtocol(Protocol):
    """Protocol defining the interface expected by mixin classes."""

    collection: Any
    helpers: "QueryHelper"
    sql_translator: "SQLTranslator"
    _jsonb_supported: bool

    def _get_integer_id_for_oid(self, oid: Any) -> int:
        """Get integer ID for an ObjectId."""
        ...

    def find(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> "Cursor":
        """Find documents."""
        ...

    def find_one(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> Dict[str, Any] | None:
        """Find a single document."""
        ...

    def insert_one(self, document: Dict[str, Any]) -> Any:
        """Insert a single document."""
        ...
