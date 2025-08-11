# coding: utf-8
from typing import Dict, Any, List, TYPE_CHECKING
from dataclasses import dataclass
from abc import ABC

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
        self._bulk_operations = bulk_operations
        self._filter = filter
        self._upsert = False

    def upsert(self):
        """Set the upsert flag for the next operation."""
        self._upsert = True
        return self

    def update_one(self, update: Dict[str, Any]):
        """Add an update one operation."""
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
        """Add an update many operation."""
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
        """Add a delete one operation."""
        self._bulk_operations.append(
            DeleteOperation(filter=self._filter, multi=False)
        )
        return self

    def delete_many(self):
        """Add a delete many operation."""
        self._bulk_operations.append(
            DeleteOperation(filter=self._filter, multi=True)
        )
        return self

    def replace_one(self, replacement: Dict[str, Any]):
        """Add a replace one operation."""
        # For replace, we treat it as an update with $set to the replacement
        # But we need to exclude the _id field from replacement
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
        self._collection = collection
        self._ordered = ordered
        self._operations: List[BulkOperation] = []

    def insert(self, document: Dict[str, Any]):
        """Add an insert operation."""
        self._operations.append(InsertOperation(document=document))
        return self

    def find(self, filter: Dict[str, Any]):
        """Create a context for find-based operations."""
        return BulkOperationContext(self._operations, filter)

    def execute(self) -> "BulkWriteResult":
        """Execute all bulk operations."""

        if self._ordered:
            return self._execute_ordered()
        else:
            return self._execute_unordered()

    def _execute_ordered(self) -> "BulkWriteResult":
        """Execute operations in order."""

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
        """Execute operations in any order (for now, we'll just execute them in order)."""

        # For simplicity, we'll execute unordered operations the same as ordered
        # In a more advanced implementation, we might group operations by type
        # or execute them in parallel
        return self._execute_ordered()
