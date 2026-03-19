from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from . import Collection

# Try to import quez, but make it optional
try:
    from quez import CompressedQueue

    QUEZ_AVAILABLE = True
except ImportError:
    CompressedQueue = None
    QUEZ_AVAILABLE = False


from .type_utils import validate_session


class AggregationCursor:
    """
    A cursor that iterates over the results of an aggregation pipeline.

    This cursor implements the same interface as PyMongo's CommandCursor,
    allowing iteration over aggregation results.
    """

    def __init__(
        self,
        collection: Collection,
        pipeline: List[Dict[str, Any]],
        allowDiskUse: bool | None = None,
        batchSize: int | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ):
        """
        Initialize the AggregationCursor.

        Args:
            collection: The collection to run the aggregation on
            pipeline: The aggregation pipeline to execute
            allowDiskUse: Ignored in NeoSQLite (kept for PyMongo compatibility)
            batchSize: Batch size for results (kept for PyMongo compatibility)
            session: A ClientSession for transactions
            **kwargs: Additional keyword arguments for PyMongo compatibility
        """
        self._collection = collection
        self.pipeline = pipeline
        self._results: List[Dict[str, Any]] | CompressedQueue | None = None
        self._position = 0
        self._executed = False
        # Memory constraint settings
        self._batch_size = batchSize if batchSize is not None else 101
        self._memory_threshold = 100 * 1024 * 1024  # 100MB default threshold
        # Quez settings
        self._use_quez = False
        # Store allowDiskUse for API compatibility (ignored in NeoSQLite)
        self._allow_disk_use = allowDiskUse
        self._session = session

        # Validate session
        validate_session(session, collection._database)

    def max_await_time_ms(
        self, max_await_time_ms: int | None
    ) -> AggregationCursor:
        """
        Set the maximum time to wait for new documents (for tailable cursors).

        Args:
            max_await_time_ms (int, optional): The maximum time to wait in milliseconds.

        Returns:
            AggregationCursor: The cursor object with the max_await_time_ms applied.
        """
        self._max_await_time_ms = max_await_time_ms
        return self

    def add_option(self, mask: int) -> AggregationCursor:
        """
        Set query flags (bitmask) for this cursor.

        Args:
            mask (int): The bitmask of options to set.

        Returns:
            AggregationCursor: The cursor object with the options applied.
        """
        if not hasattr(self, "_options"):
            self._options = 0
        self._options |= mask
        return self

    def remove_option(self, mask: int) -> AggregationCursor:
        """
        Unset query flags (bitmask) for this cursor.

        Args:
            mask (int): The bitmask of options to unset.

        Returns:
            AggregationCursor: The cursor object with the options removed.
        """
        if not hasattr(self, "_options"):
            self._options = 0
        self._options &= ~mask
        return self

    @property
    def session(self) -> Any | None:
        """
        Get the ClientSession associated with this cursor.

        Returns:
            Any | None: The ClientSession, or None if no session is associated.
        """
        return getattr(self, "_session", None)

    @property
    def cursor_id(self) -> int:
        """
        Get the ID of this cursor.

        Returns:
            int: The cursor ID (always 0 for NeoSQLite).
        """
        return 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Return the iterator object.

        Returns:
            The cursor itself as an iterator
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        # Reset position to allow multiple iterations
        self._position = 0
        return self

    def __next__(self) -> Dict[str, Any]:
        """
        Get the next document in the aggregation result.

        Returns:
            The next document in the result set

        Raises:
            StopIteration: When there are no more documents
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        # Check if we have results
        if self._results is None:
            raise StopIteration

        # Handle CompressedQueue results
        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            try:
                return self._results.get(block=False)
            except Exception:
                raise StopIteration

        # Handle list results
        if isinstance(self._results, list):
            # Check if we have more results
            if self._position < len(self._results):
                result = self._results[self._position]
                self._position += 1
                return result
            else:
                raise StopIteration

        raise StopIteration

    def __len__(self) -> int:
        """
        Get the number of documents in the aggregation result.

        Returns:
            The number of documents in the result set
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        if self._results is None:
            return 0

        # Handle CompressedQueue results
        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            return self._results.qsize()

        # Handle list results
        if isinstance(self._results, list):
            return len(self._results)

        return 0

    def __getitem__(self, index: int) -> Dict[str, Any]:
        """
        Get a document by index.

        Args:
            index: The index of the document to retrieve

        Returns:
            The document at the specified index
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        if self._results is None:
            raise IndexError("Cursor has no results")

        # Handle CompressedQueue results
        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            raise NotImplementedError(
                "Indexing not supported with quez memory-constrained processing"
            )

        # Handle list results
        if isinstance(self._results, list):
            return self._results[index]

        raise IndexError("Cursor has no results")

    def sort(self, key=None, reverse=False):
        """
        Sort the results in-place.

        Args:
            key: A function to extract a comparison key from each element
            reverse: If True, sort in descending order

        Returns:
            The cursor itself for chaining
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        # Sorting is not supported with quez
        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            raise NotImplementedError(
                "Sorting not supported with quez memory-constrained processing"
            )

        # Check if we have results
        if self._results is None:
            return self

        # Handle list results
        if isinstance(self._results, list):
            # Sort the results
            self._results.sort(key=key, reverse=reverse)
            return self

        return self

    def _execute(self) -> None:
        """
        Execute the aggregation pipeline and store the results.
        """
        # Estimate the result size to determine if we need memory-constrained processing
        estimated_size = self._estimate_result_size()

        if (
            estimated_size > self._memory_threshold
            and QUEZ_AVAILABLE
            and self._use_quez
        ):
            # Use memory-constrained processing with quez
            self._results = (
                self.collection.query_engine.aggregate_with_constraints(
                    self.pipeline,
                    batch_size=self._batch_size,
                    memory_constrained=True,
                    session=self._session,
                )
            )
        else:
            # Use normal processing - pass batch_size for fetchmany
            self._results = self.collection.query_engine.aggregate(
                self.pipeline,
                batch_size=self._batch_size,
                session=self._session,
            )

        self._executed = True

    def _estimate_result_size(self) -> int:
        """
        Estimate the size of the aggregation result in bytes.

        Returns:
            Estimated size in bytes
        """
        # Use the helper method from the collection's query engine
        return self.collection.query_engine.helpers._estimate_result_size(
            self.pipeline
        )

    def next(self) -> Dict[str, Any]:
        """
        Get the next document in the aggregation result.

        Returns:
            The next document in the result set

        Raises:
            StopIteration: When there are no more documents
        """
        return self.__next__()

    def to_list(self) -> List[Dict[str, Any]]:
        """
        Convert the cursor to a list of documents.

        Returns:
            A list containing all documents in the result set
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        if self._results is None:
            return []

        # Handle CompressedQueue results
        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            # Extract all items from the queue
            results = []
            while not self._results.empty():
                try:
                    results.append(self._results.get(block=False))
                except Exception:
                    break
            return results

        # Handle list results
        if isinstance(self._results, list):
            return self._results[:]

        return []

    def batch_size(self, size: int) -> AggregationCursor:
        """
        Set the batch size for memory-constrained processing.

        Args:
            size: The batch size to use

        Returns:
            The cursor itself for chaining
        """
        self._batch_size = size
        return self

    def allow_disk_use(self, allow: bool = True) -> AggregationCursor:
        """
        Enable or disable disk use for aggregation operations.

        This method supports both method chaining (PyMongo fluent style)
        and is also settable via the allowDiskUse parameter to aggregate().

        Args:
            allow: Whether to allow disk use

        Returns:
            The cursor itself for chaining
        """
        self._allow_disk_use = allow
        return self

    def use_quez(self, use_quez: bool = True) -> AggregationCursor:
        """
        Enable or disable quez memory-constrained processing.

        Args:
            use_quez: Whether to use quez for memory-constrained processing

        Returns:
            The cursor itself for chaining
        """
        self._use_quez = use_quez and QUEZ_AVAILABLE
        return self

    @property
    def retrieved(self) -> int:
        """
        Return the number of documents retrieved from the cursor.

        Returns:
            int: The number of documents retrieved so far
        """
        return self._position

    @property
    def alive(self) -> bool:
        """
        Check if the cursor has more documents to iterate.

        Returns:
            bool: True if the cursor may have more documents, False if exhausted
        """
        if not self._executed:
            return True

        if self._results is None:
            return False

        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            return not self._results.empty()

        if isinstance(self._results, list):
            return self._position < len(self._results)

        return False

    @property
    def collection(self):
        """
        Return a reference to the collection this cursor is iterating over.

        Returns:
            Collection: The collection associated with this cursor
        """
        return self._collection

    @property
    def address(self) -> tuple | None:
        """
        Return the address of the database.

        Returns:
            tuple | None: A tuple of (database_path, 0) after execution starts,
                          None before the cursor has been executed.
        """
        if not self._executed:
            return None
        return (
            (
                self.collection.db_path
                if hasattr(self.collection, "db_path")
                else "memory"
            ),
            0,
        )

    def get_quez_stats(self) -> Dict[str, Any] | None:
        """
        Get quez compression statistics if quez is being used.

        Returns:
            Dict with compression statistics or None if quez is not being used.
            Statistics include:
            - 'count': Number of items in the queue
            - 'raw_size_bytes': Total raw size of items in bytes
            - 'compressed_size_bytes': Total compressed size of items in bytes
            - 'compression_ratio_pct': Compression ratio as percentage (None if empty)
        """
        if (
            QUEZ_AVAILABLE
            and self._executed
            and isinstance(self._results, CompressedQueue)
        ):
            return self._results.stats
        return None
