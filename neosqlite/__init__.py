from .binary import Binary
from .bulk_operations import BulkOperationExecutor
from .changestream import ChangeStream
from .collection import Collection
from .connection import Connection
from .exceptions import (
    MalformedQueryException,
    MalformedDocument,
    CollectionInvalid,
)
from .requests import InsertOne, UpdateOne, DeleteOne
from .results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

# Import cursor classes from collection module
from .collection.aggregation_cursor import AggregationCursor
from .collection.cursor import Cursor, ASCENDING, DESCENDING
from .collection.raw_batch_cursor import RawBatchCursor

# GridFS support
try:
    from .gridfs import GridFSBucket, GridFS

    _HAS_GRIDFS = True
except ImportError:
    _HAS_GRIDFS = False

__all__ = [
    "ASCENDING",
    "AggregationCursor",
    "Binary",
    "BulkOperationExecutor",
    "BulkWriteResult",
    "ChangeStream",
    "Collection",
    "CollectionInvalid",
    "Connection",
    "Cursor",
    "DESCENDING",
    "DeleteOne",
    "DeleteResult",
    "InsertManyResult",
    "InsertOne",
    "InsertOneResult",
    "MalformedDocument",
    "MalformedQueryException",
    "RawBatchCursor",
    "UpdateOne",
    "UpdateResult",
]

# Add GridFS to __all__ if available
if _HAS_GRIDFS:
    __all__.extend(["GridFSBucket", "GridFS"])
