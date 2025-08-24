from .connection import Connection
from .collection import Collection
from .cursor import Cursor, ASCENDING, DESCENDING
from .results import (
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    DeleteResult,
    BulkWriteResult,
)
from .requests import InsertOne, UpdateOne, DeleteOne
from .exceptions import MalformedQueryException, MalformedDocument
from .changestream import ChangeStream
from .raw_batch_cursor import RawBatchCursor
from .bulk_operations import BulkOperationExecutor

# GridFS support
try:
    from .gridfs import GridFSBucket

    _HAS_GRIDFS = True
except ImportError:
    _HAS_GRIDFS = False

__all__ = [
    "Connection",
    "Collection",
    "Cursor",
    "ASCENDING",
    "DESCENDING",
    "InsertOneResult",
    "InsertManyResult",
    "UpdateResult",
    "DeleteResult",
    "BulkWriteResult",
    "InsertOne",
    "UpdateOne",
    "DeleteOne",
    "MalformedQueryException",
    "MalformedDocument",
    "ChangeStream",
    "RawBatchCursor",
    "BulkOperationExecutor",
]

# Add GridFS to __all__ if available
if _HAS_GRIDFS:
    __all__.extend(["GridFSBucket"])
