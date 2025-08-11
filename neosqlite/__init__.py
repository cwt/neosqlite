# coding: utf-8
"""neosqlite - A wrapper for sqlite3 to have schemaless, document-store features."""

from .neosqlite import (
    Connection,
    Collection,
    Cursor,
    MalformedDocument,
    MalformedQueryException,
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    DeleteResult,
    BulkWriteResult,
    InsertOne,
    UpdateOne,
    DeleteOne,
    ASCENDING,
    DESCENDING,
)

from .bulk_operations import (
    BulkOperationExecutor,
)

__all__ = [
    "Connection",
    "Collection",
    "Cursor",
    "MalformedDocument",
    "MalformedQueryException",
    "InsertOneResult",
    "InsertManyResult",
    "UpdateResult",
    "DeleteResult",
    "BulkWriteResult",
    "InsertOne",
    "UpdateOne",
    "DeleteOne",
    "ASCENDING",
    "DESCENDING",
    "BulkOperationExecutor",
]
