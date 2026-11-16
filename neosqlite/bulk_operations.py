from __future__ import annotations

import logging
import uuid
from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    import neosqlite

    from .client_session import ClientSession

from .exceptions import BulkWriteError
from .requests import _parse_bulk_request
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
    """

    filter: dict[str, Any]
    update: dict[str, Any]
    upsert: bool = False
    multi: bool = False


@dataclass(slots=True)
class ReplaceOperation(BulkOperation):
    """Represents a document replacement operation in a bulk write batch.

    Attributes:
        filter: Query filter dictionary matching the document to replace.
        replacement: Replacement document dictionary without ``_id``.
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

    def update_one(self, update: dict[str, Any]) -> Self:
        """Add an update-one operation to the bulk operations list.

        Appends an ``UpdateOperation`` targeting a single matching document
        using the current filter and upsert flag, then resets ``_upsert``.

        Args:
            update: Update specification dictionary containing fields or
                update operators to apply.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            UpdateOperation(
                filter=self._filter,
                update=update,
                upsert=self._upsert,
                multi=False,
            )
        )
        self._upsert = False  # Reset upsert flag
        return self

    def update_many(self, update: dict[str, Any]) -> Self:
        """Add an update-many operation to the bulk operations list.

        Appends an ``UpdateOperation`` targeting all matching documents using
        the current filter and upsert flag, then resets ``_upsert``.

        Args:
            update: Update specification dictionary containing fields or
                update operators to apply.

        Returns:
            Self: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            UpdateOperation(
                filter=self._filter,
                update=update,
                upsert=self._upsert,
                multi=True,
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
        context filter, stripping ``_id`` from ``replacement`` when it is a
        dictionary, and resets ``_upsert``.

        Args:
            replacement: Replacement document dictionary containing the new
                document fields.

        Returns:
            Self: The current context object for chaining further operations.
        """
        replacement_doc = (
            {k: v for k, v in replacement.items() if k != "_id"}
            if isinstance(replacement, dict)
            else replacement
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
                )
            )
        elif op_type == "update_many":
            self._operations.append(
                UpdateOperation(
                    filter=kwargs["filter"],
                    update=kwargs["update"],
                    upsert=kwargs["upsert"],
                    multi=True,
                )
            )
        elif op_type == "replace_one":
            replacement = kwargs["replacement"]
            replacement_doc = (
                {k: v for k, v in replacement.items() if k != "_id"}
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
        """Execute queued operations with optional stop-on-error behavior.

        Dispatches each queued operation to the collection, accumulating
        operation counts and ``upserted_ids``. Exceptions are captured as
        write errors (mapping unique constraint failures to code 11000 and
        others to code 8); when ``ordered`` is ``True``, execution stops at
        the first failure.

        Args:
            ordered: Whether to abort on the first failing operation.
            session: Optional ``ClientSession`` passed to collection calls.

        Returns:
            BulkWriteResult: Aggregated write counts and upserted ID mapping.

        Raises:
            BulkWriteError: If one or more operations fail during execution.
        """
        inserted_count = 0
        matched_count = 0
        modified_count = 0
        deleted_count = 0
        upserted_count = 0
        upserted_ids: dict[int, Any] = {}
        write_errors: list[dict[str, Any]] = []

        for idx, op in enumerate(self._operations):
            try:
                match op:
                    case InsertOperation(document=doc):
                        self._collection.insert_one(doc, session=session)
                        inserted_count += 1
                    case UpdateOperation(
                        filter=f, update=u, upsert=up, multi=multi
                    ):
                        if multi:
                            update_res = self._collection.update_many(
                                f, u, upsert=up, session=session
                            )
                        else:
                            update_res = self._collection.update_one(
                                f, u, upsert=up, session=session
                            )
                        matched_count += update_res.matched_count
                        modified_count += update_res.modified_count
                        if update_res.upserted_id is not None:
                            upserted_count += 1
                            upserted_ids[idx] = update_res.upserted_id
                    case ReplaceOperation(filter=f, replacement=r, upsert=up):
                        replace_res = self._collection.replace_one(
                            f, r, upsert=up, session=session
                        )
                        matched_count += replace_res.matched_count
                        modified_count += replace_res.modified_count
                        if replace_res.upserted_id is not None:
                            upserted_count += 1
                            upserted_ids[idx] = replace_res.upserted_id
                    case DeleteOperation(filter=f, multi=multi):
                        if multi:
                            delete_res = self._collection.delete_many(
                                f, session=session
                            )
                        else:
                            delete_res = self._collection.delete_one(
                                f, session=session
                            )
                        deleted_count += delete_res.deleted_count
            except Exception as e:
                from ._sqlite import sqlite3

                code = (
                    11000
                    if "UNIQUE" in str(e).upper()
                    or isinstance(e, sqlite3.IntegrityError)
                    else 8
                )
                write_errors.append(
                    {
                        "index": idx,
                        "code": code,
                        "errmsg": str(e),
                        "op": op,
                    }
                )
                if ordered:
                    break

        if write_errors:
            error_details = {
                "writeErrors": write_errors,
                "writeConcernErrors": [],
                "nInserted": inserted_count,
                "nUpserted": upserted_count,
                "nMatched": matched_count,
                "nModified": modified_count,
                "nRemoved": deleted_count,
                "upserted": [
                    {"index": i, "_id": oid} for i, oid in upserted_ids.items()
                ],
            }
            raise BulkWriteError(error_details)

        return BulkWriteResult(
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            upserted_count=upserted_count,
            upserted_ids=upserted_ids,
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
