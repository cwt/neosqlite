from functools import partial
from itertools import starmap
from typing import Any, Dict, List, Iterator, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from .collection import Collection

ASCENDING = 1
DESCENDING = -1


class Cursor:
    """
    Class representing a cursor for iterating over documents in a collection with
    applied filters, projections, sorting, and pagination.
    """

    def __init__(
        self,
        collection: "Collection",
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ):
        """
        Initialize a new cursor instance.

        Args:
            collection (Collection): The collection to operate on.
            filter (Dict[str, Any], optional): Filter criteria to apply to the documents.
            projection (Dict[str, Any], optional): Projection criteria to specify which fields to include.
            hint (str, optional): Hint for the database to improve query performance.
        """
        self._collection = collection
        self._query_helpers = collection.query_engine.helpers
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint = hint
        self._skip = 0
        self._limit: int | None = None
        self._sort: Dict[str, int] | None = None

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Return an iterator over the documents in the cursor.

        Returns:
            Iterator[Dict[str, Any]]: An iterator yielding documents that match the filter,
                                      projection, sorting, and pagination criteria.
        """
        return self._execute_query()

    def limit(self, limit: int) -> "Cursor":
        """
        Limit the number of documents returned by the cursor.

        Args:
            limit (int): The maximum number of documents to return.

        Returns:
            Cursor: The cursor object with the limit applied.
        """
        self._limit = limit
        return self

    def skip(self, skip: int) -> "Cursor":
        """
        Skip the specified number of documents when iterating over the cursor.

        Args:
            skip (int): The number of documents to skip.

        Returns:
            Cursor: The cursor object with the skip applied.
        """
        self._skip = skip
        return self

    def sort(
        self,
        key_or_list: str | List[tuple],
        direction: int | None = None,
    ) -> "Cursor":
        """
        Sort the documents returned by the cursor.

        Args:
            key_or_list (str | List[tuple]): The key or list of keys to sort by.
            direction (int, optional): The sorting direction (ASCENDING or DESCENDING).
                                       Defaults to ASCENDING if None.

        Returns:
            Cursor: The cursor object with the sorting applied.
        """
        if isinstance(key_or_list, str):
            self._sort = {key_or_list: direction or ASCENDING}
        else:
            self._sort = dict(key_or_list)
        return self

    def _execute_query(self) -> Iterator[Dict[str, Any]]:
        """
        Execute the query and yield the results after applying filters, sorting,
        pagination, and projection.

        Yields:
            Dict[str, Any]: A dictionary representing each document in the result set.
        """
        # Get the documents based on filter
        docs = self._get_filtered_documents()

        # Apply sorting if specified
        docs = self._apply_sorting(docs)

        # Apply skip and limit
        docs = self._apply_pagination(docs)

        # Apply projection
        docs = self._apply_projection(docs)

        # Yield results
        yield from docs

    def _get_filtered_documents(self) -> Iterable[Dict[str, Any]]:
        """
        Retrieve documents based on the filter criteria, applying SQL-based filtering
        where possible, or falling back to Python-based filtering for complex queries.

        Returns:
            Iterable[Dict[str, Any]]: An iterable of dictionaries representing
                                      the documents that match the filter criteria.
        """
        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params = where_result
            cmd = f"SELECT id, data FROM {self._collection.name} {where_clause}"
            db_cursor = self._collection.db.execute(cmd, params)
            return starmap(self._collection._load, db_cursor.fetchall())
        else:
            # Fallback to Python-based filtering for complex queries
            cmd = f"SELECT id, data FROM {self._collection.name}"
            db_cursor = self._collection.db.execute(cmd)
            apply = partial(self._query_helpers._apply_query, self._filter)
            all_docs = starmap(self._collection._load, db_cursor.fetchall())
            return filter(apply, all_docs)

    def _apply_sorting(
        self, docs: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Sort the documents based on the specified sorting criteria.

        Args:
            docs (Iterable[Dict[str, Any]]): The iterable of documents to sort.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the documents
                                  sorted by the specified criteria.
        """
        if not self._sort:
            return list(docs)

        sort_keys = list(self._sort.keys())
        sort_keys.reverse()
        sorted_docs = list(docs)
        for key in sort_keys:
            get_val = partial(self._collection._get_val, key=key)
            reverse = self._sort[key] == DESCENDING
            sorted_docs.sort(key=get_val, reverse=reverse)
        return sorted_docs

    def _apply_pagination(
        self, docs: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply skip and limit to the documents.

        Args:
            docs (Iterable[Dict[str, Any]]): The iterable of documents to apply pagination to.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the documents
                                  after applying skip and limit.
        """
        doc_list = list(docs)
        skipped_docs = doc_list[self._skip :]

        if self._limit is not None:
            return skipped_docs[: self._limit]
        return skipped_docs

    def _apply_projection(
        self, docs: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply projection to the documents.

        Args:
            docs (Iterable[Dict[str, Any]]): The iterable of documents to apply projection to.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the documents
                                  after applying the projection.
        """
        project = partial(
            self._query_helpers._apply_projection, self._projection
        )
        return list(map(project, docs))
