from __future__ import annotations

import importlib.util

from .binary import Binary
from .bulk_operations import BulkOperationExecutor
from .changestream import ChangeStream
from .collection import Collection

# Import cursor classes from collection module
from .collection.aggregation_cursor import AggregationCursor
from .collection.cursor import ASCENDING, DESCENDING, Cursor
from .collection.raw_batch_cursor import RawBatchCursor
from .connection import Connection
from .exceptions import (
    CollectionInvalid,
    MalformedDocument,
    MalformedQueryException,
)
from .options import (
    AutoVacuumMode,
    CodecOptions,
    ReadConcern,
    ReadPreference,
    WriteConcern,
)
from .requests import DeleteOne, InsertOne, UpdateOne
from .results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

# GridFS support
# Use importlib.util.find_spec to test for availability without triggering ruff F401
gridfs_spec = importlib.util.find_spec(".gridfs", package=__package__)
if gridfs_spec is not None:
    from .gridfs import GridFS, GridFSBucket  # noqa: F401

    _HAS_GRIDFS = True
else:
    _HAS_GRIDFS = False

__all__ = [
    "ASCENDING",
    "AggregationCursor",
    "AutoVacuumMode",
    "Binary",
    "BulkOperationExecutor",
    "BulkWriteResult",
    "ChangeStream",
    "CodecOptions",
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
    "ReadConcern",
    "ReadPreference",
    "UpdateOne",
    "UpdateResult",
    "WriteConcern",
]

# Add GridFS to __all__ if available
if _HAS_GRIDFS:
    __all__.extend(["GridFSBucket", "GridFS"])
