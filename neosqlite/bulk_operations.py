# coding: utf-8
from abc import ABC
from dataclasses import dataclass
from typing import Dict, Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    import neosqlite

from .results import BulkWriteResult


@dataclass
class BulkOperation(ABC):
    """Base class for bulk operations."""

    pass


@dataclass
class InsertOperation(BulkOperation):
    """Represents an insert operation in a bulk operation."""

    document: Dict[str, Any]


@dataclass
class UpdateOperation(BulkOperation):
    """Represents an update operation in a bulk operation."""

    filter: Dict[str, Any]
    update: Dict[str, Any]
    upsert: bool = False
    multi: bool = False


@dataclass
class DeleteOperation(BulkOperation):
    """Represents a delete operation in a bulk operation."""

    filter: Dict[str, Any]
    multi: bool = False


class BulkOperationContext:
    """Context for bulk operations that supports find/update/delete operations."""

    def __init__(
        self, bulk_operations: List[BulkOperation], filter: Dict[str, Any]
    ):
        """
        Initializes the BulkOperationContext.

        Args:
            bulk_operations: A list to which bulk operations will be added.
            filter: The filter to be used for the operations.
        """
        self._bulk_operations = bulk_operations
        self._filter = filter
        self._upsert = False

    def upsert(self):
        """
        Set the upsert flag for the next operation.

        This method sets the upsert flag, which determines whether the next
        operation should insert a new document if no matching document is found.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
        """
        self._upsert = True
        return self

    def update_one(self, update: Dict[str, Any]):
        """
        Add an update one operation to the bulk operations list.

        This method appends an update one operation to the bulk operations list.
        The operation will update a single document that matches the filter with
        the specified update and handle the upsert flag.

        Args:
            update (Dict[str, Any]): The update dictionary containing the fields to be updated.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
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

    def update_many(self, update: Dict[str, Any]):
        """
        Add an update many operation to the bulk operations list.

        This method appends an update many operation to the bulk operations list.
        The operation will update all documents that match the filter with the
        specified update and handle the upsert flag.

        Args:
            update (Dict[str, Any]): The update dictionary containing the fields to be updated.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
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

    def delete_one(self):
        """
        Add a delete one operation to the bulk operations list.

        This method appends a delete one operation to the bulk operations list.
        The operation will delete a single document that matches the filter.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            DeleteOperation(filter=self._filter, multi=False)
        )
        return self

    def delete_many(self):
        """
        Add a delete many operation to the bulk operations list.

        This method appends a delete many operation to the bulk operations list.
        The operation will delete all documents that match the filter.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
        """
        self._bulk_operations.append(
            DeleteOperation(filter=self._filter, multi=True)
        )
        return self

    def replace_one(self, replacement: Dict[str, Any]):
        """
        Add a replace one operation to the bulk operations list.

        This method appends a replace one operation to the bulk operations list.
        The operation will replace a single document that matches the filter with
        the specified replacement.

        The replacement dictionary should contain the fields to be updated.
        The method will exclude the `_id` field from the replacement to prevent
        replacing the document's identifier.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
        """
        replacement_doc = {k: v for k, v in replacement.items() if k != "_id"}
        self._bulk_operations.append(
            UpdateOperation(
                filter=self._filter,
                update={"$set": replacement_doc},
                upsert=self._upsert,
                multi=False,
            )
        )
        self._upsert = False  # Reset upsert flag
        return self


class BulkOperationExecutor:
    """Executor for bulk operations."""

    def __init__(
        self, collection: "neosqlite.Collection", ordered: bool = True
    ):
        """
        Initialize the BulkOperationExecutor.

        This method initializes a new BulkOperationExecutor with the given collection
        and ordering flag. The executor will execute operations in the order they
        are added if the ordered flag is True. Otherwise, the executor may execute
        operations in any order.

        Args:
            collection (neosqlite.Collection): The collection to perform operations on.
            ordered (bool, optional): Whether to execute operations in order. Defaults to True.
        """
        self._collection = collection
        self._ordered = ordered
        self._operations: List[BulkOperation] = []

    def insert(self, document: Dict[str, Any]):
        """
        Add an insert operation to the bulk operations list.

        This method appends an insert operation to the bulk operations list.
        The operation will insert the specified document into the collection.

        Args:
            document (Dict[str, Any]): The document to be inserted.

        Returns:
            BulkOperationContext: The current context object for chaining further operations.
        """
        self._operations.append(InsertOperation(document=document))
        return self

    def find(self, filter: Dict[str, Any]):
        """
        Create a context for find-based operations.

        This method creates a new BulkOperationContext for find-based operations
        with the given filter.

        Args:
            filter (Dict[str, Any]): The filter to be used for find operations.

        Returns:
            BulkOperationContext: A new BulkOperationContext object for chaining find operations.
        """
        return BulkOperationContext(self._operations, filter)

    def execute(self) -> "BulkWriteResult":
        """
        Execute all bulk operations.

        This method executes all bulk operations in the current context. If ordered
        is True, operations are executed in the order they were added. Otherwise,
        operations may be executed in any order.

        Returns:
            BulkWriteResult: A result object containing the counts of inserted, matched, modified, deleted, and upserted documents.
        """
        if self._ordered:
            return self._execute_ordered()
        else:
            return self._execute_unordered()

    def _execute_ordered(self) -> "BulkWriteResult":
        """
        Execute operations in order.

        This method executes each bulk operation in the `_operations` list in the
        order they were added. It handles different types of operations (insert,
        update, delete) using conditional logic. The method uses savepoints and
        transactions to ensure atomicity and handle exceptions by rolling back
        changes.

        Returns:
            BulkWriteResult: A result object containing the counts of inserted, matched, modified, deleted, and upserted documents.
        """
        inserted_count = 0
        matched_count = 0
        modified_count = 0
        deleted_count = 0
        upserted_count = 0

        self._collection.db.execute("SAVEPOINT bulk_operations")
        try:
            for op in self._operations:
                match op:
                    case InsertOperation(document=doc):
                        self._collection.insert_one(doc)
                        inserted_count += 1
                    case UpdateOperation(
                        filter=f, update=u, upsert=up, multi=multi
                    ):
                        if multi:
                            update_res = self._collection.update_many(f, u)
                        else:
                            update_res = self._collection.update_one(
                                f, u, upsert=up
                            )
                        matched_count += update_res.matched_count
                        modified_count += update_res.modified_count
                        if update_res.upserted_id:
                            upserted_count += 1
                    case DeleteOperation(filter=f, multi=multi):
                        if multi:
                            delete_res = self._collection.delete_many(f)
                        else:
                            delete_res = self._collection.delete_one(f)
                        deleted_count += delete_res.deleted_count

            self._collection.db.execute("RELEASE SAVEPOINT bulk_operations")
        except Exception as e:
            self._collection.db.execute("ROLLBACK TO SAVEPOINT bulk_operations")
            raise e

        return BulkWriteResult(
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            upserted_count=upserted_count,
        )

    def _execute_unordered(self) -> "BulkWriteResult":
        """
        Execute operations in any order (for now, we'll just execute them in order).

        Returns:
            BulkWriteResult: A result object containing the counts of inserted, matched, modified, deleted, and upserted documents.
        """
        # For simplicity, we'll execute unordered operations the same as ordered
        # In a more advanced implementation, we might group operations by type
        # or execute them in parallel
        return self._execute_ordered()
