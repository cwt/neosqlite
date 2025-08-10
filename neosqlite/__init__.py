"""A wrapper for sqlite3 to have schemaless, document-store features."""

from .neosqlite import (
    Connection,
    Collection,
    Cursor,
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    DeleteResult,
    MalformedQueryException,
    MalformedDocument,
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
    "MalformedQueryException",
    "MalformedDocument",
    "ASCENDING",
    "DESCENDING",
]
