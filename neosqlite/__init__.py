from .binary import Binary
from .bulk_operations import BulkOperationExecutor
from .changestream import ChangeStream
from .collection import Collection
from .connection import Connection
from .cursor import Cursor, ASCENDING, DESCENDING
from .exceptions import MalformedQueryException, MalformedDocument
from .raw_batch_cursor import RawBatchCursor
from .requests import InsertOne, UpdateOne, DeleteOne
from .results import (
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    DeleteResult,
    BulkWriteResult,
)

# GridFS support
try:
    from .gridfs import GridFSBucket, GridFS

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
    "Binary",
]

# Add GridFS to __all__ if available
if _HAS_GRIDFS:
    __all__.extend(["GridFSBucket", "GridFS"])
