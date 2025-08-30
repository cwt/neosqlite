from typing import Any, Dict, List, Iterator, TYPE_CHECKING, Optional, Union
import threading
import time

if TYPE_CHECKING:
    from .collection import Collection
    try:
        from quez import CompressedQueue
    except ImportError:
        CompressedQueue = Any

# Try to import quez, but make it optional
try:
    from quez import CompressedQueue
    QUEZ_AVAILABLE = True
except ImportError:
    CompressedQueue = None
    QUEZ_AVAILABLE = False


class AggregationCursor:
    """
    A cursor that iterates over the results of an aggregation pipeline.

    This cursor implements the same interface as PyMongo's CommandCursor,
    allowing iteration over aggregation results.
    """

    def __init__(
        self, collection: "Collection", pipeline: List[Dict[str, Any]]
    ):
        """
        Initialize the AggregationCursor.

        Args:
            collection: The collection to run the aggregation on
            pipeline: The aggregation pipeline to execute
        """
        self.collection = collection
        self.pipeline = pipeline
        self._results: Union[List[Dict[str, Any]], "CompressedQueue", None] = None
        self._position = 0
        self._executed = False
        # Memory constraint settings
        self._batch_size = 1000
        self._memory_threshold = 100 * 1024 * 1024  # 100MB default threshold
        # Quez settings
        self._use_quez = False
        self._quez_queue: Optional["CompressedQueue"] = None
        self._quez_thread: Optional[threading.Thread] = None
        self._quez_finished = False

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

        # Check if we're using quez
        if self._use_quez and self._quez_queue is not None:
            try:
                # For quez, we need to get items from the queue
                # This will block until an item is available or queue is empty
                if not self._quez_finished or not self._quez_queue.empty():
                    return self._quez_queue.get(block=True, timeout=0.1)
                else:
                    raise StopIteration
            except:
                raise StopIteration
        else:
            # Check if we have results
            if self._results is None:
                raise StopIteration

            # Check if we have more results
            if isinstance(self._results, list):
                if self._position < len(self._results):
                    result = self._results[self._position]
                    self._position += 1
                    return result
                else:
                    raise StopIteration
            else:
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

        if self._use_quez and self._quez_queue is not None:
            return self._quez_queue.qsize()
        elif self._results is None:
            return 0
        elif isinstance(self._results, list):
            return len(self._results)
        else:
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

        if self._use_quez:
            raise NotImplementedError("Indexing not supported with quez memory-constrained processing")
        elif self._results is None:
            raise IndexError("Cursor has no results")
        elif isinstance(self._results, list):
            return self._results[index]
        else:
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
        if self._use_quez:
            raise NotImplementedError("Sorting not supported with quez memory-constrained processing")
        elif self._results is None:
            return self
        elif isinstance(self._results, list):
            # Sort the results
            self._results.sort(key=key, reverse=reverse)
            return self
        else:
            return self

    def _execute(self) -> None:
        """
        Execute the aggregation pipeline and store the results.
        """
        # Estimate the result size to determine if we need memory-constrained processing
        estimated_size = self._estimate_result_size()

        if estimated_size > self._memory_threshold and QUEZ_AVAILABLE:
            # Use memory-constrained processing with quez
            self._use_quez = True
            self._execute_memory_constrained_with_quez()
        else:
            # Use normal processing
            self._results = self.collection.query_engine.aggregate(
                self.pipeline
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

    def _execute_memory_constrained_with_quez(self) -> None:
        """
        Execute the aggregation pipeline with memory constraints using quez.
        
        This method processes the pipeline and streams results into a quez queue
        for memory-efficient processing.
        """
        if not QUEZ_AVAILABLE:
            # Fall back to regular memory-constrained processing
            self._results = self._execute_memory_constrained()
            return

        # Create a compressed queue for results
        self._quez_queue = CompressedQueue(maxsize=self._batch_size * 2)
        
        # Start a thread to populate the queue
        self._quez_thread = threading.Thread(
            target=self._populate_quez_queue,
            daemon=True
        )
        self._quez_thread.start()

    def _populate_quez_queue(self) -> None:
        """
        Populate the quez queue with results from the aggregation.
        This runs in a separate thread.
        """
        if self._quez_queue is None:
            return
            
        try:
            # Get results from the query engine
            results = self.collection.query_engine.aggregate(self.pipeline)
            
            # Put each result into the queue
            for result in results:
                # This will block if the queue is full
                self._quez_queue.put(result)
        except Exception:
            # Handle any errors during population
            pass
        finally:
            # Mark that we're finished
            self._quez_finished = True

    def _execute_memory_constrained(self) -> List[Dict[str, Any]]:
        """
        Execute the aggregation pipeline with memory constraints.

        This method processes the pipeline in batches to avoid memory issues
        with large result sets.

        Returns:
            List of documents from the aggregation result
        """
        # For now, we'll just call the normal aggregate method
        # In a full implementation, this would handle batching and memory constraints
        # based on the batch_size and memory limits
        return self.collection.query_engine.aggregate_with_constraints(
            self.pipeline, batch_size=self._batch_size, memory_constrained=True
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

        # If using quez, we need to consume all items from the queue
        if self._use_quez and self._quez_queue is not None:
            results = []
            # Wait for the population thread to finish
            if self._quez_thread is not None:
                self._quez_thread.join(timeout=30)  # Wait up to 30 seconds
            
            # Get all items from the queue
            while not self._quez_queue.empty():
                try:
                    results.append(self._quez_queue.get(block=False))
                except:
                    break
            return results
        elif self._results is None:
            return []
        elif isinstance(self._results, list):
            return self._results[:]
        else:
            return []

    def batch_size(self, size: int) -> "AggregationCursor":
        """
        Set the batch size for memory-constrained processing.

        Args:
            size: The batch size to use

        Returns:
            The cursor itself for chaining
        """
        self._batch_size = size
        return self

    def max_await_time_ms(self, time_ms: int) -> "AggregationCursor":
        """
        Set the maximum time to wait for new documents.

        This is a placeholder method for API compatibility with PyMongo.

        Args:
            time_ms: Time in milliseconds

        Returns:
            The cursor itself for chaining
        """
        # This is a placeholder for API compatibility
        return self

    def use_quez(self, use_quez: bool = True) -> "AggregationCursor":
        """
        Enable or disable quez memory-constrained processing.

        Args:
            use_quez: Whether to use quez for memory-constrained processing

        Returns:
            The cursor itself for chaining
        """
        self._use_quez = use_quez and QUEZ_AVAILABLE
        return self
