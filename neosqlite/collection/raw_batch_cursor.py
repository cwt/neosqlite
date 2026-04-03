from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List

logger = logging.getLogger(__name__)

from .json_helpers import neosqlite_json_dumps
from .json_path_utils import parse_json_path
from .jsonb_support import json_data_column

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from . import Collection


class RawBatchCursor:
    """A cursor that returns raw batches of JSON data instead of individual documents."""

    def __init__(
        self,
        collection: Collection,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
        pipeline: List[Dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ):
        """
        Initialize a RawBatchCursor object.

        Args:
            collection (Collection): The collection associated with this cursor.
            filter (Dict[str, Any]): A dictionary representing the filter criteria for the documents.
            projection (Dict[str, Any]): A dictionary representing the projection criteria for the documents.
            hint (str): A string hinting at the index to use for the query.
            batch_size (int): The number of documents to return in each batch.
            pipeline (List[Dict[str, Any]]): An optional aggregation pipeline to execute.
            session (ClientSession, optional): A ClientSession for transactions.
        """
        self._collection = collection
        self._query_helpers = collection.query_engine.helpers
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint = hint
        self._batch_size = batch_size
        self._skip = 0
        self._limit: int | None = None
        self._sort: Dict[str, int] | None = None
        self._pipeline = pipeline
        self._session = session
        self._tables_to_cleanup: List[str] = []

    def batch_size(self, batch_size: int) -> RawBatchCursor:
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
        # If we have a pipeline, use aggregation
        if self._pipeline is not None:
            # Execute the aggregation pipeline
            results = list(self._collection.aggregate(self._pipeline))

            # Split results into batches
            for i in range(0, len(results), self._batch_size):
                batch = results[i : i + self._batch_size]
                # Convert each document to JSON using the custom encoder and join with newlines
                batch_json = "\n".join(
                    neosqlite_json_dumps(doc) for doc in batch
                )
                yield batch_json.encode("utf-8")
            return

        # Build the query using the collection's SQL-building methods
        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params, tables = where_result

            # Track tables for cleanup
            if tables:
                self._tables_to_cleanup.extend(tables)

            # Build ORDER BY clause if sorting is specified
            order_by = ""
            if self._sort:
                sort_clauses = []
                for key, direction in self._sort.items():
                    sort_clauses.append(
                        f"json_extract(data, '{parse_json_path(key)}') {'DESC' if direction == -1 else 'ASC'}"
                    )
                order_by = "ORDER BY " + ", ".join(sort_clauses)

            # Use the collection's JSONB support flag to determine how to select data
            jsonb = self._collection.query_engine._jsonb_supported

            # Build the full query with proper WHERE clause handling
            if where_clause and where_clause.strip():
                cmd = (
                    f"SELECT id, {json_data_column(jsonb)} as data "
                    f"FROM {self._collection.name} {where_clause} {order_by}"
                )
            else:
                cmd = (
                    f"SELECT id, {json_data_column(jsonb)} as data "
                    f"FROM {self._collection.name} {order_by}"
                )

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

                # Convert to JSON batch using custom encoder to handle ObjectIds
                batch_json = "\n".join(
                    neosqlite_json_dumps(doc) for doc in docs
                )
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
                # Convert each document to JSON using the custom encoder and join with newlines
                batch_json = "\n".join(
                    neosqlite_json_dumps(doc) for doc in batch
                )
                yield batch_json.encode("utf-8")

    def __del__(self) -> None:
        """Clean up resources on garbage collection."""
        self._cleanup()

    def _cleanup(self) -> None:
        """Clean up temporary tables."""
        if not self._tables_to_cleanup:
            return
        for table in self._tables_to_cleanup:
            try:
                self._collection._database.execute(
                    f"DROP TABLE IF EXISTS {table}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop temporary table {table} in RawBatchCursor cleanup: {e}"
                )
                pass
        self._tables_to_cleanup = []
