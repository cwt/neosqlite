"""Base protocol for QueryEngine mixins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from ..cursor import Cursor
    from ..jsonb_support import JSONBContext
    from ..query_helper import QueryHelper
    from ..sql_translator_unified import SQLTranslator


class QueryEngineProtocol(Protocol):
    """Protocol defining the interface expected by QueryEngine mixins.

    Declares the shared collection collaborators and cross-mixin methods
    supplied by ``QueryEngine`` so CRUD, find, and query-method mixins can
    reference one another safely under static type checking.

    Attributes:
        collection: Parent collection instance owning the SQLite table.
        helpers: Internal helper collaborator for document normalization and
            low-level writes.
        sql_translator: Unified SQL translator for ``$match`` and expressions.
        jsonb: Connection-scoped JSONB capability context.
    """

    collection: Any
    helpers: QueryHelper
    sql_translator: SQLTranslator
    jsonb: JSONBContext

    def _get_integer_id_for_oid(self, oid: Any) -> int:
        """Look up the SQLite integer row ID for a document ``_id``.

        Args:
            oid: Document identifier (``ObjectId`` or scalar ID) to resolve.

        Returns:
            int: Corresponding integer primary key in the collection table.
        """
        ...

    def _notify_tier_change(self, new_tier: str, pipeline: list[Any]) -> None:
        """Notify registered callbacks when the execution tier changes.

        Args:
            new_tier: Identifier of the tier selected for query execution.
            pipeline: Query filter, update specification, or aggregation
                pipeline associated with the operation.
        """
        ...

    def find(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> Cursor:
        """Query the collection and return a cursor over matching documents.

        Args:
            filter: Optional query filter dictionary.
            projection: Optional field inclusion or exclusion specification.
            hint: Optional index hint name.
            session: Optional client session for transactional reads.

        Returns:
            Cursor: Cursor yielding matching documents.
        """
        ...

    def find_one(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document matching the provided filter.

        Args:
            filter: Optional query filter dictionary.
            projection: Optional field inclusion or exclusion specification.
            hint: Optional index hint name.
            session: Optional client session for transactional reads.

        Returns:
            dict[str, Any] | None: First matching document, or ``None`` if no
                document matches.
        """
        ...

    def insert_one(
        self, document: dict[str, Any], session: ClientSession | None = None
    ) -> Any:
        """Insert a single document into the collection.

        Args:
            document: Document dictionary to insert.
            session: Optional client session for transactional writes.

        Returns:
            Any: Result object describing the insert operation.
        """
        ...

    def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> Any:
        """Update at most one document matching the provided filter.

        Args:
            filter: Query filter selecting the target document.
            update: Update operator dictionary to apply.
            upsert: If ``True``, insert a new document when no match exists.
            array_filters: Optional filter predicates for positional array
                updates.
            session: Optional client session for transactional writes.

        Returns:
            Any: Result object describing matched, modified, and upserted
                counts or IDs.
        """
        ...

    def delete_one(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> Any:
        """Delete at most one document matching the provided filter.

        Args:
            filter: Query filter selecting the document to delete.
            session: Optional client session for transactional writes.

        Returns:
            Any: Result object describing the deleted document count.
        """
        ...
