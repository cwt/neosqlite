"""Raw batch cursor yielding newline-delimited JSON byte batches."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from itertools import batched
from typing import TYPE_CHECKING, Any, Self

logger = logging.getLogger(__name__)

from ..sql_utils import quote_table_name
from .json_helpers import neosqlite_json_dumps
from .json_path_utils import parse_json_path

if TYPE_CHECKING:
    from ..client_session import ClientSession
    from . import Collection


def _iter_json_batches(
    docs: list[dict[str, Any]], batch_size: int
) -> Iterator[bytes]:
    """Serialize document batches into newline-delimited UTF-8 JSON bytes.

    Args:
        docs: Sequence of document dictionaries to serialize.
        batch_size: Maximum number of documents included in each yielded chunk.

    Yields:
        bytes: UTF-8 encoded newline-delimited JSON bytes for one batch.
    """
    for batch in batched(docs, batch_size):
        batch_json = "\n".join(neosqlite_json_dumps(doc) for doc in batch)
        yield batch_json.encode("utf-8")


class RawBatchCursor:
    """Cursor that yields raw batches of newline-delimited JSON data.

    Iterates over query or aggregation pipeline results in serialized UTF-8
    JSON byte chunks rather than individual Python document dictionaries.
    Supports SQL-backed batched pagination with deterministic ordering and
    falls back to ``Collection.find()`` for complex queries.
    """

    def __init__(
        self,
        collection: Collection,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
        pipeline: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ):
        """Initialize a RawBatchCursor instance.

        Args:
            collection: The collection queried by this cursor.
            filter: Optional query filter dictionary; defaults to None ({}).
            projection: Optional field projection dictionary; defaults to
                None ({}).
            hint: Optional index hint string for the query.
            batch_size: Number of documents to serialize per yielded batch.
                Defaults to 100.
            pipeline: Optional aggregation pipeline stages to execute instead
                of a find query.
            session: Optional ClientSession associated with the operation.
        """
        self._collection = collection
        self._query_helpers = collection.query_engine.helpers
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint = hint
        self._batch_size = batch_size
        self._skip = 0
        self._limit: int | None = None
        self._sort: dict[str, int] | None = None
        self._pipeline = pipeline
        self._session = session
        self._tables_to_cleanup: list[str] = []

    def batch_size(self, batch_size: int) -> Self:
        """Set the maximum number of documents per yielded batch.

        Args:
            batch_size: Number of documents to return in each batch.

        Returns:
            Self: This cursor instance for method chaining.
        """
        self._batch_size = batch_size
        return self

    def __iter__(self) -> Iterator[bytes]:
        """Yield raw batches of newline-delimited UTF-8 JSON bytes.

        Executes either the configured aggregation pipeline, a SQL-paginated
        query with deterministic ordering, or a Python fallback via
        ``collection.find()`` when the filter cannot be translated directly.

        Yields:
            bytes: UTF-8 encoded newline-delimited JSON documents for a batch.
        """
        # If we have a pipeline, use aggregation
        if self._pipeline is not None:
            # Execute the aggregation pipeline
            results = list(self._collection.aggregate(self._pipeline))

            # Split results into batches
            yield from _iter_json_batches(results, self._batch_size)
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

            # Use the collection's JSONB context to select data
            jsonb = self._collection.query_engine.jsonb
            json_func = jsonb.json_function_prefix

            # Build ORDER BY clause. With no explicit sort we still order
            # by the rowid so OFFSET pagination is deterministic across
            # batches (#149); unordered LIMIT/OFFSET can skip or repeat
            # documents when the plan changes between statements.
            order_by = ""
            if not self._sort:
                order_by = (
                    f"ORDER BY {quote_table_name(self._collection.name)}.id ASC"
                )
            if self._sort:
                sort_clauses = []
                table_ref = quote_table_name(self._collection.name)
                for key, direction in self._sort.items():
                    direction_sql = "DESC" if direction == -1 else "ASC"
                    if key == "_id":
                        # _id lives in a dedicated column (#113); ordering by
                        # it also makes OFFSET pagination deterministic.
                        sort_clauses.append(f"{table_ref}._id {direction_sql}")
                    else:
                        sort_clauses.append(
                            f"{json_func}_extract(data, '{parse_json_path(key)}') "
                            f"{direction_sql}"
                        )
                order_by = "ORDER BY " + ", ".join(sort_clauses)

            # Build the full query with proper WHERE clause handling
            if where_clause and where_clause.strip():
                cmd = (
                    f"SELECT id, _id, {jsonb.data_column()} as data "
                    f"FROM {quote_table_name(self._collection.name)} {where_clause} {order_by}"
                )
            else:
                cmd = (
                    f"SELECT id, _id, {jsonb.data_column()} as data "
                    f"FROM {quote_table_name(self._collection.name)} {order_by}"
                )

            # Execute and process in batches
            offset = self._skip
            total_returned = 0

            while True:
                # Calculate how many records to fetch in this batch
                batch_limit = self._batch_size
                if self._limit is not None and self._limit != 0:
                    remaining_limit = abs(self._limit) - total_returned
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
                docs = [
                    self._collection._load_with_stored_id(
                        row[0], row[2], row[1]
                    )
                    for row in rows
                ]

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
                if (
                    self._limit is not None
                    and self._limit != 0
                    and total_returned >= abs(self._limit)
                ):
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
            yield from _iter_json_batches(docs, self._batch_size)

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
