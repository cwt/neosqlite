from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from . import Collection

logger = logging.getLogger(__name__)

# Try to import quez, but make it optional
try:
    from quez import CompressedQueue

    QUEZ_AVAILABLE = True
except ImportError as e:
    logger.debug(f"quez module not available, using fallback Queue: {e}")
    CompressedQueue = None
    QUEZ_AVAILABLE = False

from .type_utils import validate_session


class AggregationCursor:
    """Cursor that iterates over the results of an aggregation pipeline.

    Implements an interface compatible with PyMongo's ``CommandCursor``,
    supporting lazy execution, iteration, indexing, sorting, and optional
    ``quez`` compressed queue buffering for large result sets.
    """

    def __init__(
        self,
        collection: Collection,
        pipeline: list[dict[str, Any]],
        allowDiskUse: bool | None = None,
        batchSize: int | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ):
        """Initialize the AggregationCursor.

        Args:
            collection: The collection to run the aggregation on.
            pipeline: List of aggregation pipeline stage dictionaries.
            allowDiskUse: Ignored in NeoSQLite (kept for PyMongo
                compatibility). Defaults to None.
            batchSize: Batch size used when fetching results. Defaults to None
                (uses 101 internally).
            session: Optional ``ClientSession`` for transactions. Defaults to
                None.
            **kwargs: Additional keyword arguments accepted for PyMongo
                compatibility.
        """
        self._collection = collection
        self.pipeline = pipeline
        self._results: list[dict[str, Any]] | CompressedQueue | None = None
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
        self._options = 0
        self._max_await_time_ms: int | None = None

        # Validate session
        validate_session(session, collection._database)

    def max_await_time_ms(self, max_await_time_ms: int | None) -> Self:
        """Set the maximum time to wait for new documents.

        Stores the wait limit for PyMongo cursor compatibility (for example, on
        tailable cursors).

        Args:
            max_await_time_ms: Maximum wait time in milliseconds, or None.

        Returns:
            Self: The cursor itself for method chaining.
        """
        self._max_await_time_ms = max_await_time_ms
        return self

    def add_option(self, mask: int) -> Self:
        """Set query flags (bitmask) for this cursor.

        Args:
            mask: Bitmask of cursor option flags to set via bitwise OR.

        Returns:
            Self: The cursor itself for method chaining.
        """
        if not hasattr(self, "_options"):
            self._options = 0
        self._options |= mask
        return self

    def remove_option(self, mask: int) -> Self:
        """Unset query flags (bitmask) for this cursor.

        Args:
            mask: Bitmask of cursor option flags to clear via bitwise AND with
                ``~mask``.

        Returns:
            Self: The cursor itself for method chaining.
        """
        if not hasattr(self, "_options"):
            self._options = 0
        self._options &= ~mask
        return self

    @property
    def session(self) -> Any | None:
        """Get the ClientSession associated with this cursor.

        Returns:
            Any | None: Associated ``ClientSession``, or None if unset.
        """
        return getattr(self, "_session", None)

    @property
    def cursor_id(self) -> int:
        """Get the ID of this cursor.

        Returns:
            int: Cursor ID, which is always ``0`` in NeoSQLite.
        """
        return 0

    def __iter__(self) -> Self:
        """Return the cursor as an iterator, executing lazily if needed.

        Executes the aggregation pipeline if it has not run yet and resets the
        cursor position to ``0`` to allow re-iteration over list results.

        Returns:
            Self: The cursor instance as an iterator.
        """
        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        # Reset position to allow multiple iterations
        self._position = 0
        return self

    def __next__(self) -> dict[str, Any]:
        """Get the next document in the aggregation result.

        Executes the pipeline on first access if necessary, then pops from the
        ``CompressedQueue`` (when using ``quez``) or advances through the
        buffered result list.

        Returns:
            dict[str, Any]: Next document in the aggregation result set.

        Raises:
            StopIteration: When no more documents remain.
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
            except Exception as e:
                logger.debug(
                    f"AggregationCursor: CompressedQueue.get() failed, raising StopIteration: {e}"
                )
                raise StopIteration from e

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
        """Get the number of documents in the aggregation result.

        Executes the pipeline if it has not yet run and returns ``qsize()`` for
        ``CompressedQueue`` results or ``len()`` for list results.

        Returns:
            int: Number of documents in the result set.
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

    def __getitem__(self, index: int) -> dict[str, Any]:
        """Get a result document by index.

        Args:
            index: Zero-based index of the document to retrieve.

        Returns:
            dict[str, Any]: Document at the requested index.

        Raises:
            IndexError: If the cursor has no results or ``index`` is out of
                range.
            NotImplementedError: If ``quez`` memory-constrained queueing is in
                use.
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

    def sort(self, key: Any = None, reverse: bool = False) -> Self:
        """Sort the cursor results in-place.

        Executes the pipeline if not yet run and sorts buffered list results
        using ``list.sort()``.

        Args:
            key: Optional key extraction callable passed to ``list.sort()``.
                Defaults to None.
            reverse: If True, sort in descending order. Defaults to False.

        Returns:
            Self: The cursor itself for method chaining.

        Raises:
            NotImplementedError: If ``quez`` memory-constrained queueing is in
                use.
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
        """Execute the aggregation pipeline and store the results.

        Chooses between ``aggregate_with_constraints()`` (when the estimated
        result size exceeds ``_memory_threshold``, ``quez`` is available, and
        ``_use_quez`` is enabled) and standard ``aggregate()``, then marks the
        cursor as executed.
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
        """Estimate the size of the aggregation result in bytes.

        Returns:
            int: Estimated result size in bytes from the query engine helper.
        """
        # Use the helper method from the collection's query engine
        return self.collection.query_engine.helpers._estimate_result_size(
            self.pipeline
        )

    def next(self) -> dict[str, Any]:
        """Get the next document in the aggregation result.

        Delegates directly to ``__next__()`` for PyMongo compatibility.

        Returns:
            dict[str, Any]: Next document in the result set.

        Raises:
            StopIteration: When there are no more documents.
        """
        return self.__next__()

    def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        """Convert remaining documents in the cursor to a list.

        Executes the pipeline if not yet executed. Draws up to ``length``
        documents (or all remaining documents when ``length`` is ``None``)
        starting from the current cursor position and advances ``_position``.

        Args:
            length: Maximum number of documents to return, or ``None`` for all
                remaining documents. Non-positive integers return ``[]``.

        Returns:
            list[dict[str, Any]]: List containing up to ``length`` documents
            from the current position in the result set.

        Raises:
            ValueError: If ``length`` is provided and is not an integer.
        """
        if length is not None and (
            not isinstance(length, int) or isinstance(length, bool)
        ):
            raise ValueError("length must be an integer or None")

        # Execute the pipeline if not already done
        if not self._executed:
            self._execute()

        if self._results is None:
            return []

        if length is not None and length <= 0:
            return []

        # Handle CompressedQueue results
        if QUEZ_AVAILABLE and isinstance(self._results, CompressedQueue):
            # Extract up to length items from the queue
            results: list[dict[str, Any]] = []
            while not self._results.empty() and (
                length is None or len(results) < length
            ):
                try:
                    results.append(self._results.get(block=False))
                except Exception as e:
                    logger.debug(
                        f"Error getting item from CompressedQueue: {e}"
                    )
                    break
            return results

        # Handle list results from current cursor position
        if isinstance(self._results, list):
            start = self._position
            stop = (
                len(self._results)
                if length is None
                else min(start + length, len(self._results))
            )
            self._position = stop
            return self._results[start:stop]

        return []

    def batch_size(self, size: int) -> Self:
        """Set the batch size for result fetching and constrained execution.

        Args:
            size: Number of documents per batch.

        Returns:
            Self: The cursor itself for method chaining.
        """
        self._batch_size = size
        return self

    def allow_disk_use(self, allow: bool = True) -> Self:
        """Enable or disable disk use for aggregation operations.

        Supports fluent method chaining alongside the ``allowDiskUse`` keyword
        argument to ``aggregate()`` (stored for PyMongo API compatibility).

        Args:
            allow: Whether to allow disk use. Defaults to True.

        Returns:
            Self: The cursor itself for method chaining.
        """
        self._allow_disk_use = allow
        return self

    def use_quez(self, use_quez: bool = True) -> Self:
        """Enable or disable ``quez`` memory-constrained processing.

        Only enables ``quez`` when ``QUEZ_AVAILABLE`` is True.

        Args:
            use_quez: Whether to use ``quez`` for memory-constrained
                processing. Defaults to True.

        Returns:
            Self: The cursor itself for method chaining.
        """
        self._use_quez = use_quez and QUEZ_AVAILABLE
        return self

    @property
    def retrieved(self) -> int:
        """Return the number of documents retrieved from the cursor.

        Returns:
            int: Current read position (number of documents yielded from list
            results).
        """
        return self._position

    @property
    def alive(self) -> bool:
        """Check if the cursor has more documents to iterate.

        Returns True prior to execution, or while unread items remain in either
        the ``CompressedQueue`` or the result list.

        Returns:
            bool: True if the cursor may yield more documents, False if
            exhausted.
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
        """Return a reference to the collection this cursor iterates over.

        Returns:
            Collection: The collection associated with this cursor.
        """
        return self._collection

    @property
    def address(self) -> tuple | None:
        """Return the database address tuple once execution has started.

        Returns:
            tuple | None: ``(db_path, 0)`` (or ``("memory", 0)`` if
            ``db_path`` is unavailable) after execution, or None before the
            cursor has executed.
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

    def get_quez_stats(self) -> dict[str, Any] | None:
        """Get ``quez`` compression statistics if ``quez`` is active.

        Returns:
            dict[str, Any] | None: Compression statistics dictionary from
            ``CompressedQueue.stats`` (including ``count``,
            ``raw_size_bytes``, ``compressed_size_bytes``, and
            ``compression_ratio_pct``), or None if ``quez`` is not in use or
            execution has not occurred.
        """
        if (
            QUEZ_AVAILABLE
            and self._executed
            and isinstance(self._results, CompressedQueue)
        ):
            return self._results.stats
        return None
