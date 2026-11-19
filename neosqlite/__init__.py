from __future__ import annotations

import importlib.util
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("neosqlite")
except PackageNotFoundError:
    # Package is not installed (e.g., development mode)
    __version__ = "dev"

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
    BulkWriteError,
    CollectionInvalid,
    InvalidOperation,
    MalformedDocument,
    MalformedQueryException,
)
from .index_model import IndexModel
from .js import (
    JsEngineInfo,
    JsWorkerError,
    close_js_workers,
    get_active_js_engine,
    get_js_engine,
    list_js_engines,
    set_js_engine,
    use_js_engine,
)
from .options import (
    AutoVacuumMode,
    CodecOptions,
    ReadConcern,
    ReadPreference,
    WriteConcern,
)
from .requests import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
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
    "DESCENDING",
    "AggregationCursor",
    "AutoVacuumMode",
    "Binary",
    "BulkOperationExecutor",
    "BulkWriteError",
    "BulkWriteResult",
    "ChangeStream",
    "CodecOptions",
    "Collection",
    "CollectionInvalid",
    "Connection",
    "Cursor",
    "DeleteMany",
    "DeleteOne",
    "DeleteResult",
    "IndexModel",
    "InsertManyResult",
    "InsertOne",
    "InsertOneResult",
    "InvalidOperation",
    "JsEngineInfo",
    "JsWorkerError",
    "MalformedDocument",
    "MalformedQueryException",
    "RawBatchCursor",
    "ReadConcern",
    "ReadPreference",
    "ReplaceOne",
    "UpdateMany",
    "UpdateOne",
    "UpdateResult",
    "WriteConcern",
    "close_js_workers",
    "get_active_js_engine",
    "get_js_engine",
    "list_js_engines",
    "set_js_engine",
    "use_js_engine",
]

# Add GridFS to __all__ if available
if _HAS_GRIDFS:
    __all__.extend(["GridFS", "GridFSBucket"])
