from typing import Any, Dict, Iterator, Optional, TYPE_CHECKING
import json

if TYPE_CHECKING:
    from .collection import Collection


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
        """
        Initialize a RawBatchCursor object.

        Args:
            collection (Collection): The collection associated with this cursor.
            filter (Dict[str, Any]): A dictionary representing the filter criteria for the documents.
            projection (Dict[str, Any]): A dictionary representing the projection criteria for the documents.
            hint (str): A string hinting at the index to use for the query.
            batch_size (int): The number of documents to return in each batch.
        """
        self._collection = collection
        self._query_helpers = collection.query_engine.helpers
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint = hint
        self._batch_size = batch_size
        self._skip = 0
        self._limit: Optional[int] = None
        self._sort: Optional[Dict[str, int]] = None

    def batch_size(self, batch_size: int) -> "RawBatchCursor":
        """
        Set the batch size for this cursor.

        Args:
            batch_size (int): The number of documents to return in each batch.

        Returns:
            RawBatchCursor: This cursor object, for method chaining.
        """
        self._batch_size = batch_size
        return self

    def __iter__(self) -> Iterator[bytes]:
        """
        Return an iterator over raw batches of JSON data.

        Returns:
            Iterator[bytes]: An iterator that yields raw batches of JSON data.
        """
        # Build the query using the collection's SQL-building methods
        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params = where_result

            # Build ORDER BY clause if sorting is specified
            order_by = ""
            if self._sort:
                sort_clauses = []
                for key, direction in self._sort.items():
                    sort_clauses.append(
                        f"json_extract(data, '$.{key}') {'DESC' if direction == -1 else 'ASC'}"
                    )
                order_by = "ORDER BY " + ", ".join(sort_clauses)

            # Build the full query with proper WHERE clause handling
            if where_clause and where_clause.strip():
                cmd = f"SELECT id, data FROM {self._collection.name} {where_clause} {order_by}"
            else:
                cmd = f"SELECT id, data FROM {self._collection.name} {order_by}"

            # Execute and process in batches
            offset = self._skip
            total_returned = 0

            while True:
                # Calculate how many records to fetch in this batch
                batch_limit = self._batch_size
                if self._limit is not None:
                    remaining_limit = self._limit - total_returned
                    if remaining_limit <= 0:
                        break
                    batch_limit = min(batch_limit, remaining_limit)

                # Add LIMIT and OFFSET for this batch
                batch_cmd = f"{cmd} LIMIT {batch_limit} OFFSET {offset}"
                db_cursor = self._collection.db.execute(batch_cmd, params)
                rows = db_cursor.fetchall()

                if not rows:
                    break

                # Convert rows to documents
                docs = [self._collection._load(row[0], row[1]) for row in rows]

                # Convert to JSON batch
                batch_json = "\n".join(json.dumps(doc) for doc in docs)
                yield batch_json.encode("utf-8")

                # Update counters
                returned_count = len(rows)
                total_returned += returned_count
                offset += returned_count

                # If we got fewer rows than requested, we're done
                if returned_count < batch_limit:
                    break

                # If we've hit our limit, we're done
                if self._limit is not None and total_returned >= self._limit:
                    break
        else:
            # Fallback to the original method for complex queries
            # Get all documents first by using the collection's find method
            cursor = self._collection.find(
                self._filter, self._projection, self._hint
            )
            # Apply any cursor modifications
            if self._sort:
                cursor._sort = self._sort
            cursor._skip = self._skip
            cursor._limit = self._limit

            # Get all documents
            docs = list(cursor)

            # Split into batches
            for i in range(0, len(docs), self._batch_size):
                batch = docs[i : i + self._batch_size]
                # Convert each document to JSON and join with newlines
                batch_json = "\n".join(json.dumps(doc) for doc in batch)
                yield batch_json.encode("utf-8")
