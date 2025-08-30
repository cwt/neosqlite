from typing import Any, Dict, List, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from .collection import Collection


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
        self._results: List[Dict[str, Any]] | None = None
        self._position = 0
        self._executed = False
        # Memory constraint settings
        self._batch_size = 1000
        self._memory_threshold = 100 * 1024 * 1024  # 100MB default threshold

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

        # Check if we have more results
        if self._position < len(self._results):
            result = self._results[self._position]
            self._position += 1
            return result
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

        if self._results is None:
            return 0

        return len(self._results)

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

        return self._results[index]

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

        # Check if we have results
        if self._results is None:
            return self

        # Sort the results
        self._results.sort(key=key, reverse=reverse)
        return self

    def _execute(self) -> None:
        """
        Execute the aggregation pipeline and store the results.
        """
        # Estimate the result size to determine if we need memory-constrained processing
        estimated_size = self._estimate_result_size()

        if estimated_size > self._memory_threshold:
            # Use memory-constrained processing
            self._results = self._execute_memory_constrained()
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

        if self._results is None:
            return []

        return self._results[:]

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
