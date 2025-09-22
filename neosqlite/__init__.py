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

import importlib.util

# GridFS support
# Use importlib.util.find_spec to test for availability without triggering ruff F401
gridfs_spec = importlib.util.find_spec(".gridfs", package=__package__)
if gridfs_spec is not None:
    from .gridfs import GridFSBucket, GridFS  # noqa: F401

    _HAS_GRIDFS = True
else:
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
