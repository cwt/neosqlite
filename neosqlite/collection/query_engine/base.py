"""Base protocol for QueryEngine mixins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ..client_session import ClientSession
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
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> "Cursor":
        """Find documents."""
        ...

    def find_one(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document."""
        ...

    def insert_one(
        self, document: dict[str, Any], session: ClientSession | None = None
    ) -> Any:
        """Insert a single document."""
        ...

    def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> Any:
        """Update a single document."""
        ...

    def delete_one(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> Any:
        """Delete a single document."""
        ...
