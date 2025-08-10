"""A wrapper for sqlite3 to have schemaless, document-store features."""

from .neosqlite import (
    Connection,
    Collection,
    Cursor,
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    DeleteResult,
    BulkWriteResult,
    MalformedQueryException,
    MalformedDocument,
    InsertOne,
    UpdateOne,
    DeleteOne,
    ASCENDING,
    DESCENDING,
)

__all__ = [
    "Connection",
    "Collection",
    "Cursor",
    "InsertOneResult",
    "InsertManyResult",
    "UpdateResult",
    "DeleteResult",
    "BulkWriteResult",
    "MalformedQueryException",
    "MalformedDocument",
    "InsertOne",
    "UpdateOne",
    "DeleteOne",
    "ASCENDING",
    "DESCENDING",
]
