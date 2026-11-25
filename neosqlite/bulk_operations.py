from __future__ import annotations

import logging
import uuid
from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    import neosqlite

    from .client_session import ClientSession

from .exceptions import BulkWriteError  # noqa: F401 - re-exported for callers
from .requests import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
    _parse_bulk_request,
)
from .results import BulkWriteResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BulkOperation(ABC):
    """Abstract base class for queued bulk write operations.

    Subclasses represent individual insert, update, replace, and delete
    operations accumulated by ``BulkOperationExecutor`` prior to execution.
    """

    pass


@dataclass(slots=True)
class InsertOperation(BulkOperation):
    """Represents a document insertion in a bulk write batch.

    Attributes:
        document: Document dictionary to insert into the collection.
    """

    document: dict[str, Any]


@dataclass(slots=True)
class UpdateOperation(BulkOperation):
    """Represents a single- or multi-document update in a bulk write batch.

    Attributes:
        filter: Query filter dictionary matching documents to update.
        update: Update specification dictionary applied to matched documents.
        upsert: Whether to insert a new document when no document matches.
        multi: Whether to update all matching documents or only the first.
        array_filters: Optional list of filter dictionaries for positional
            filtered array updates.
    """

    filter: dict[str, Any]
    update: dict[str, Any]
    upsert: bool = False
    multi: bool = False
    array_filters: list[dict[str, Any]] | None = None


@dataclass(slots=True)
class ReplaceOperation(BulkOperation):
    """Represents a document replacement operation in a bulk write batch.

    Attributes:
        filter: Query filter dictionary matching the document to replace.
        replacement: Replacement document dictionary.
        upsert: Whether to insert the replacement when no document matches.
    """

    filter: dict[str, Any]
    replacement: dict[str, Any]
    upsert: bool = False


@dataclass(slots=True)
class DeleteOperation(BulkOperation):
    """Represents a single- or multi-document deletion in a bulk write batch.

    Attributes:
        filter: Query filter dictionary matching documents to delete.
        multi: Whether to delete all matching documents or only the first.
    """

    filter: dict[str, Any]
    multi: bool = False


class BulkOperationContext:
    """Fluent query-scoped builder context for bulk write operations.

    Holds the shared filter dictionary and transient upsert flag for chaining
    update, replace, and delete operations started from ``find()``.
    """

    def __init__(
        self, bulk_operations: list[BulkOperation], filter: dict[str, Any]
    ):
        """Initialize the BulkOperationContext.

        Args:
            bulk_operations: Shared list where built operations are appended.
            filter: Query filter dictionary applied to operations from this
                context.
        """
        self._bulk_operations = bulk_operations
        self._filter = filter
        self._upsert = False

    def upsert(self) -> Self:
        """Set the upsert flag for the next operation.

        Sets the upsert flag so the next update or replace operation inserts a
        new document if no matching document is found.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._upsert = True
        return self

    def update_one(
        self,
        update: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
    ) -> Self:
        """Add an update-one operation to the bulk operations list.

        Appends an ``UpdateOperation`` targeting a single matching document
        using the current filter and upsert flag, then resets ``_upsert``.

        Args:
            update: Update specification dictionary containing fields or
                update operators to apply.
            array_filters: Optional list of filter dictionaries for positional
                filtered array updates.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            UpdateOperation(
                filter=self._filter,
                update=update,
                upsert=self._upsert,
                multi=False,
                array_filters=array_filters,
            )
        )
        self._upsert = False  # Reset upsert flag
        return self

    def update_many(
        self,
        update: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
    ) -> Self:
        """Add an update-many operation to the bulk operations list.

        Appends an ``UpdateOperation`` targeting all matching documents using
        the current filter and upsert flag, then resets ``_upsert``.

        Args:
            update: Update specification dictionary containing fields or
                update operators to apply.
            array_filters: Optional list of filter dictionaries for positional
                filtered array updates.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            UpdateOperation(
                filter=self._filter,
                update=update,
                upsert=self._upsert,
                multi=True,
                array_filters=array_filters,
            )
        )
        self._upsert = False  # Reset upsert flag
        return self

    def delete_one(self) -> Self:
        """Add a delete-one operation to the bulk operations list.

        Appends a ``DeleteOperation`` that removes a single document matching
        the context filter.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            DeleteOperation(filter=self._filter, multi=False)
        )
        return self

    def delete_many(self) -> Self:
        """Add a delete-many operation to the bulk operations list.

        Appends a ``DeleteOperation`` that removes all documents matching the
        context filter.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            DeleteOperation(filter=self._filter, multi=True)
        )
        return self

    def replace_one(self, replacement: dict[str, Any]) -> Self:
        """Add a replace-one operation to the bulk operations list.

        Appends a ``ReplaceOperation`` targeting a single document matching the
        context filter and resets ``_upsert``.

        Args:
            replacement: Replacement document dictionary containing the new
                document fields.

        Returns:
            Self: The current context object for chaining further operations.
        """
        replacement_doc = (
            dict(replacement) if isinstance(replacement, dict) else replacement
        )
        self._bulk_operations.append(
            ReplaceOperation(
                filter=self._filter,
                replacement=replacement_doc,
                upsert=self._upsert,
            )
        )
        self._upsert = False  # Reset upsert flag
        return self


class BulkOperationExecutor:
    """Executor that accumulates and runs bulk write operations.

    Supports both PyMongo-style request objects via ``add()`` and fluent
    builder methods via ``insert()`` and ``find()``, running queued operations
    either in order or continuing past individual failures when unordered.
    """

    def __init__(self, collection: neosqlite.Collection, ordered: bool = True):
        """Initialize the BulkOperationExecutor.

        Creates an executor bound to ``collection`` with either ordered or
        unordered execution semantics and an isolated savepoint identifier.

        Args:
            collection: Target collection on which operations are executed.
            ordered: Whether to execute operations strictly in insertion order
                and stop on the first error. Defaults to ``True``.
        """
        self._collection = collection
        self._ordered = ordered
        # Unique per executor so interleaved executors cannot pop each
        # other's savepoint off the shared connection's stack (#138)
        self._savepoint_name = f"bulk_ops_{uuid.uuid4().hex[:12]}"
        self._operations: list[BulkOperation] = []

    def add(self, operation: Any) -> Self:
        """Add an operation to the bulk operations list.

        For PyMongo API compatibility, accepts ``InsertOne``, ``UpdateOne``,
        ``DeleteOne``, ``ReplaceOne``, ``UpdateMany``, ``DeleteMany``, and
        duck-typed operations parsed via ``_parse_bulk_request``.

        Args:
            operation: Write request object to parse and enqueue.

        Returns:
            Self: The current executor for chaining further operations.
        """
        op_type, kwargs = _parse_bulk_request(operation)
        if op_type == "insert_one":
            self._operations.append(
                InsertOperation(document=kwargs["document"])
            )
        elif op_type == "update_one":
            self._operations.append(
                UpdateOperation(
                    filter=kwargs["filter"],
                    update=kwargs["update"],
                    upsert=kwargs["upsert"],
                    multi=False,
                    array_filters=kwargs.get("array_filters"),
                )
            )
        elif op_type == "update_many":
            self._operations.append(
                UpdateOperation(
                    filter=kwargs["filter"],
                    update=kwargs["update"],
                    upsert=kwargs["upsert"],
                    multi=True,
                    array_filters=kwargs.get("array_filters"),
                )
            )
        elif op_type == "replace_one":
            replacement = kwargs["replacement"]
            replacement_doc = (
                dict(replacement)
                if isinstance(replacement, dict)
                else replacement
            )
            self._operations.append(
                ReplaceOperation(
                    filter=kwargs["filter"],
                    replacement=replacement_doc,
                    upsert=kwargs["upsert"],
                )
            )
        elif op_type == "delete_one":
            self._operations.append(
                DeleteOperation(filter=kwargs["filter"], multi=False)
            )
        elif op_type == "delete_many":
            self._operations.append(
                DeleteOperation(filter=kwargs["filter"], multi=True)
            )
        return self

    def insert(self, document: dict[str, Any]) -> Self:
        """Add an insert operation to the bulk operations list.

        Appends an ``InsertOperation`` for ``document`` to the queued
        operations list.

        Args:
            document: Document dictionary to insert into the collection.

        Returns:
            Self: The current executor for chaining further operations.
        """
        self._operations.append(InsertOperation(document=document))
        return self

    def find(self, filter: dict[str, Any]):
        """Create a context for filter-scoped bulk operations.

        Returns a ``BulkOperationContext`` bound to this executor's operation
        queue and the given query filter.

        Args:
            filter: Query filter dictionary for subsequent chained operations.

        Returns:
            BulkOperationContext: Context object for chaining update, replace,
                or delete operations.
        """
        return BulkOperationContext(self._operations, filter)

    def execute(self, session: ClientSession | None = None) -> BulkWriteResult:
        """Execute all queued bulk operations.

        Runs operations sequentially via ``_execute_ordered`` when
        ``ordered`` is ``True``, or via ``_execute_unordered`` otherwise.

        Args:
            session: Optional ``ClientSession`` for transactional execution.

        Returns:
            BulkWriteResult: Result containing counts of inserted, matched,
                modified, deleted, and upserted documents.
        """
        if self._ordered:
            return self._execute_ordered(session=session)
        else:
            return self._execute_unordered(session=session)

    def _execute(
        self, ordered: bool, session: ClientSession | None = None
    ) -> BulkWriteResult:
        """Convert queued legacy operations and delegate to ``Collection.bulk_write``.

        Translates queued ``InsertOperation``, ``UpdateOperation``,
        ``ReplaceOperation``, and ``DeleteOperation`` records into standard
        bulk write request objects and delegates execution to
        ``self._collection.bulk_write``.

        Args:
            ordered: Whether to abort on the first failing operation.
            session: Optional ``ClientSession`` passed to ``bulk_write``.

        Returns:
            BulkWriteResult: Aggregated write counts and upserted ID mapping.

        Raises:
            BulkWriteError: If one or more operations fail during execution.
        """
        requests: list[Any] = []
        for op in self._operations:
            match op:
                case InsertOperation(document=doc):
                    requests.append(InsertOne(doc))
                case UpdateOperation(
                    filter=f,
                    update=u,
                    upsert=up,
                    multi=multi,
                    array_filters=af,
                ):
                    if multi:
                        requests.append(
                            UpdateMany(f, u, upsert=up, array_filters=af)
                        )
                    else:
                        requests.append(
                            UpdateOne(f, u, upsert=up, array_filters=af)
                        )
                case ReplaceOperation(filter=f, replacement=r, upsert=up):
                    requests.append(ReplaceOne(f, r, upsert=up))
                case DeleteOperation(filter=f, multi=multi):
                    if multi:
                        requests.append(DeleteMany(f))
                    else:
                        requests.append(DeleteOne(f))

        return self._collection.bulk_write(
            requests, ordered=ordered, session=session
        )

    def _execute_ordered(
        self, session: ClientSession | None = None
    ) -> BulkWriteResult:
        """Execute queued bulk operations in strict order.

        Args:
            session: Optional ``ClientSession`` for transactional execution.

        Returns:
            BulkWriteResult: Result containing counts of inserted, matched,
                modified, deleted, and upserted documents.
        """
        return self._execute(ordered=True, session=session)

    def _execute_unordered(
        self, session: ClientSession | None = None
    ) -> BulkWriteResult:
        """Execute queued operations, continuing past individual failures.

        Unlike ordered execution, a failure in one operation does not prevent
        subsequent operations from being attempted.

        Args:
            session: Optional ``ClientSession`` for transactional execution.

        Returns:
            BulkWriteResult: Result containing counts of inserted, matched,
                modified, deleted, and upserted documents.
        """
        return self._execute(ordered=False, session=session)
