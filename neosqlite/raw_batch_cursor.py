from typing import Any, Dict, Iterator, List, Optional, Iterable, TYPE_CHECKING
import json

if TYPE_CHECKING:
    from .neosqlite import Collection


class RawBatchCursor:
    """A cursor that returns raw batches of JSON data instead of individual documents."""
    
    def __init__(
        self,
        collection: "Collection",
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
        batch_size: int = 100,
    ):
        self._collection = collection
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint = hint
        self._batch_size = batch_size
        self._skip = 0
        self._limit: Optional[int] = None
        self._sort: Optional[Dict[str, int]] = None

    def batch_size(self, batch_size: int) -> "RawBatchCursor":
        """Set the batch size for this cursor."""
        self._batch_size = batch_size
        return self

    def __iter__(self) -> Iterator[bytes]:
        """Return an iterator over raw batches of JSON data."""
        # Get all documents first by using the collection's find method
        cursor = self._collection.find(self._filter, self._projection, self._hint)
        # Apply any cursor modifications
        if self._sort:
            cursor._sort = self._sort
        cursor._skip = self._skip
        cursor._limit = self._limit
        
        # Get all documents
        docs = list(cursor)
        
        # Split into batches
        for i in range(0, len(docs), self._batch_size):
            batch = docs[i:i + self._batch_size]
            # Convert each document to JSON and join with newlines
            batch_json = "\n".join(json.dumps(doc) for doc in batch)
            yield batch_json.encode("utf-8")