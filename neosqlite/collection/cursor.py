from __future__ import annotations
from functools import partial
from typing import Any, Dict, List, Iterator, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from . import Collection

ASCENDING = 1
DESCENDING = -1


class Cursor:
    """
    Class representing a cursor for iterating over documents in a collection with
    applied filters, projections, sorting, and pagination.
    """

    def __init__(
        self,
        collection: Collection,
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
        self._comment: str | None = None
        self._skip = 0
        self._limit: int | None = None
        self._sort: Dict[str, int] | None = None
        self._retrieved: int = 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Return an iterator over the documents in the cursor.

        Returns:
            Iterator[Dict[str, Any]]: An iterator yielding documents that match the filter,
                                      projection, sorting, and pagination criteria.
        """
        return self._execute_query()

    def limit(self, limit: int) -> Cursor:
        """
        Limit the number of documents returned by the cursor.

        Args:
            limit (int): The maximum number of documents to return.

        Returns:
            Cursor: The cursor object with the limit applied.
        """
        self._limit = limit
        return self

    def skip(self, skip: int) -> Cursor:
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
    ) -> Cursor:
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

    def batch_size(self, size: int) -> Cursor:
        """
        Set the batch size for the cursor.

        This is a placeholder method for PyMongo API compatibility.
        NeoSQLite doesn't use batch sizes in the same way as MongoDB.

        Args:
            size (int): The batch size (ignored)

        Returns:
            Cursor: The cursor object for chaining
        """
        # Placeholder for API compatibility
        return self

    def hint(self, index: str) -> Cursor:
        """
        Set the index hint for the cursor.

        Args:
            index (str): The index name to hint

        Returns:
            Cursor: The cursor object with the hint applied
        """
        self._hint = index
        return self

    def comment(self, comment: str) -> Cursor:
        """
        Add a comment to the query for debugging and profiling.

        The comment is injected as a SQL comment in the generated query,
        which can be useful for query profiling and debugging.

        Args:
            comment (str): The comment text to add to the query

        Returns:
            Cursor: The cursor object with the comment applied

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}}).comment("find adults")
        """
        self._comment = comment
        return self

    def to_list(self, length: int | None = None) -> List[Dict[str, Any]]:
        """
        Convert the cursor to a list of documents.

        This method efficiently converts the cursor contents to a list.
        If length is specified, returns at most that many documents.

        Args:
            length (int, optional): Maximum number of documents to return.
                                   If None, returns all documents.

        Returns:
            List[Dict[str, Any]]: List of documents in the cursor

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}})
            >>> adults = cursor.to_list()
            >>> first_5 = cursor.to_list(5)
        """
        results = list(self)
        if length is not None:
            return results[:length]
        return results

    def clone(self) -> Cursor:
        """
        Create a clone of this cursor with the same options.

        Returns a new cursor with the same filter, projection, hint,
        sort, skip, and limit settings. The clone is unevaluated and
        can be iterated independently.

        Returns:
            Cursor: A new cursor with the same settings

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}}).limit(10)
            >>> clone = cursor.clone()
            >>> results1 = list(cursor)
            >>> results2 = list(clone)
        """
        from copy import deepcopy

        cloned = Cursor(
            self._collection,
            filter=deepcopy(self._filter),
            projection=self._projection,
            hint=self._hint,
        )
        cloned._skip = self._skip
        cloned._limit = self._limit
        cloned._sort = deepcopy(self._sort) if self._sort else None
        cloned._comment = self._comment
        cloned._retrieved = 0  # Clone starts fresh
        return cloned

    @property
    def retrieved(self) -> int:
        """
        Return the number of documents retrieved from the cursor.

        This property tracks how many documents have been iterated over
        since the cursor was created or last reset.

        Returns:
            int: The number of documents retrieved so far

        Example:
            >>> cursor = collection.find({}).limit(10)
            >>> docs = list(cursor)
            >>> cursor.retrieved
            10
        """
        return self._retrieved

    def explain(self, verbosity: str = "executionStats") -> Dict[str, Any]:
        """
        Return the query execution plan.

        Uses SQLite's EXPLAIN QUERY PLAN to provide information about
        how the query will be executed, including index usage.

        Args:
            verbosity (str): Verbosity level - "executionStats" or "queryPlanner"
                           (kept for PyMongo compatibility, SQLite always returns full plan)

        Returns:
            Dict[str, Any]: Query execution plan with the following structure:
                - queryPlanner: Information about the query plan
                    - winningPlan: List of plan stages
                    - indexUsage: Information about index usage
                - executionStats: Execution statistics (if verbosity="executionStats")
                    - nReturned: Number of documents returned
                    - executionTimeMillis: Execution time in milliseconds

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}})
            >>> plan = cursor.explain()
            >>> print(plan['queryPlanner']['winningPlan'])
        """
        # Build the SQL query that would be executed
        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        if where_result is not None:
            where_clause, params = where_result
            if self._collection.query_engine._jsonb_supported:
                sql = f"SELECT id, _id, json(data) as data FROM {self._collection.name} {where_clause}"
            else:
                sql = f"SELECT id, _id, data FROM {self._collection.name} {where_clause}"
        else:
            # No filter - simple select
            if self._collection.query_engine._jsonb_supported:
                sql = f"SELECT id, _id, json(data) as data FROM {self._collection.name}"
            else:
                sql = f"SELECT id, _id, data FROM {self._collection.name}"
            params = ()

        # Get the query plan from SQLite
        try:
            plan_rows = self._collection.db.execute(
                f"EXPLAIN QUERY PLAN {sql}", params
            ).fetchall()

            # Parse the plan rows
            # Row format: (id, parent, notused, detail)
            winning_plan = []
            index_usage = []

            for row in plan_rows:
                detail = row[3] if len(row) > 3 else str(row)
                winning_plan.append({"detail": detail})

                # Extract index usage information
                if "USING INDEX" in detail or "USING COVERING INDEX" in detail:
                    index_usage.append({"detail": detail})

            result: Dict[str, Any] = {
                "queryPlanner": {
                    "winningPlan": winning_plan,
                    "indexUsage": index_usage,
                }
            }

            # Add execution stats if requested
            if verbosity == "executionStats":
                # Actually execute the query to get stats
                import time

                start_time = time.time()
                results = list(self)
                execution_time_ms: float = (time.time() - start_time) * 1000

                result["executionStats"] = {
                    "nReturned": len(results),
                    "executionTimeMillis": round(execution_time_ms, 2),
                }

            return result

        except Exception as e:
            return {
                "queryPlanner": {
                    "winningPlan": [{"detail": f"Error getting plan: {e}"}],
                    "indexUsage": [],
                },
                "error": str(e),
            }

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

        # Yield results and track count
        for doc in docs:
            self._retrieved += 1
            yield doc

    def _get_filtered_documents(self) -> Iterable[Dict[str, Any]]:
        """
        Retrieve documents based on the filter criteria, applying SQL-based filtering
        where possible, or falling back to Python-based filtering for complex queries.
        For datetime queries, use the specialized datetime query processor.

        Returns:
            Iterable[Dict[str, Any]]: An iterable of dictionaries representing
                                      the documents that match the filter criteria.
        """
        # Check if this is a datetime query that should use the specialized processor
        if self._contains_datetime_operations(self._filter):
            # Use the datetime query processor for datetime-specific queries
            try:
                from .datetime_query_processor import DateTimeQueryProcessor

                datetime_processor = DateTimeQueryProcessor(self._collection)
                return datetime_processor.process_datetime_query(self._filter)
            except Exception:
                # If datetime processor fails, fall back to normal processing
                pass

        # Special handling for $expr queries
        if "$expr" in self._filter:
            return self._handle_expr_query()

        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params = where_result
            # Use the collection's JSONB support flag to determine how to select data
            # Include _id column to support both integer id and ObjectId _id
            if self._collection.query_engine._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {self._collection.name} {where_clause}"
            else:
                cmd = f"SELECT id, _id, data FROM {self._collection.name} {where_clause}"
            # Add comment if specified
            if self._comment:
                # Sanitize comment to prevent SQL injection (remove comment delimiters)
                safe_comment = (
                    self._comment.replace("/*", "")
                    .replace("*/", "")
                    .replace("--", "")
                )
                cmd = f"/* {safe_comment} */ {cmd}"
            db_cursor = self._collection.db.execute(cmd, params)
            return self._load_documents(db_cursor.fetchall())
        else:
            # Fallback to Python-based filtering for complex queries
            # Use the collection's JSONB support flag to determine how to select data
            # Include _id column to support both integer id and ObjectId _id
            if self._collection.query_engine._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {self._collection.name}"
            else:
                cmd = f"SELECT id, _id, data FROM {self._collection.name}"
            # Add comment if specified
            if self._comment:
                safe_comment = (
                    self._comment.replace("/*", "")
                    .replace("*/", "")
                    .replace("--", "")
                )
                cmd = f"/* {safe_comment} */ {cmd}"
            db_cursor = self._collection.db.execute(cmd)
            apply = partial(self._query_helpers._apply_query, self._filter)
            all_docs = self._load_documents(db_cursor.fetchall())
            return filter(apply, all_docs)

    def _handle_expr_query(self) -> Iterable[Dict[str, Any]]:
        """
        Handle $expr queries with SQL evaluation when possible, Python fallback otherwise.

        This method uses the query helper's _build_expr_where_clause to attempt SQL
        evaluation, and falls back to Python evaluation if needed.
        """
        from .expr_evaluator import ExprEvaluator

        # Try to build SQL WHERE clause with query helper
        where_result = self._query_helpers._build_simple_where_clause(
            self._filter
        )

        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params = where_result
            if self._collection.query_engine._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {self._collection.name} {where_clause}"
            else:
                cmd = f"SELECT id, _id, data FROM {self._collection.name} {where_clause}"
            # Add comment if specified
            if self._comment:
                safe_comment = (
                    self._comment.replace("/*", "")
                    .replace("*/", "")
                    .replace("--", "")
                )
                cmd = f"/* {safe_comment} */ {cmd}"
            db_cursor = self._collection.db.execute(cmd, params)
            return self._load_documents(db_cursor.fetchall())
        else:
            # Fallback to Python evaluation
            expr = self._filter["$expr"]
            # Create evaluator with database connection for JSONB support detection
            evaluator = ExprEvaluator(db_connection=self._collection.db)

            # Get all documents
            if self._collection.query_engine._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {self._collection.name}"
            else:
                cmd = f"SELECT id, _id, data FROM {self._collection.name}"
            # Add comment if specified
            if self._comment:
                safe_comment = (
                    self._comment.replace("/*", "")
                    .replace("*/", "")
                    .replace("--", "")
                )
                cmd = f"/* {safe_comment} */ {cmd}"

            db_cursor = self._collection.db.execute(cmd)
            all_docs = self._load_documents(db_cursor.fetchall())

            # Filter documents using $expr Python evaluation
            def expr_filter(doc: Dict[str, Any]) -> bool:
                try:
                    return evaluator.evaluate_python(expr, doc)
                except Exception:
                    # If evaluation fails, exclude the document
                    return False

            return filter(expr_filter, all_docs)

    def _contains_datetime_operations(self, query: Dict[str, Any]) -> bool:
        """
        Check if a query contains datetime operations that should use the datetime processor.

        Args:
            query: MongoDB-style query dictionary

        Returns:
            True if query contains datetime operations, False otherwise
        """
        # Quick check for obvious datetime patterns in query
        if not isinstance(query, dict):
            return False

        for field, value in query.items():
            if field in ("$and", "$or", "$nor"):
                if isinstance(value, list):
                    for condition in value:
                        if isinstance(
                            condition, dict
                        ) and self._contains_datetime_operations(condition):
                            return True
            elif field == "$not":
                if isinstance(
                    value, dict
                ) and self._contains_datetime_operations(value):
                    return True
            elif isinstance(value, dict):
                # Check for datetime-related operators
                for operator, op_value in value.items():
                    if operator in ("$gte", "$gt", "$lte", "$lt", "$eq", "$ne"):
                        # Check if the value is a datetime object or datetime string
                        if self._is_datetime_value(op_value):
                            return True
                    elif operator in ("$in", "$nin"):
                        # For $in and $nin, check if any value in the list is a datetime
                        if isinstance(op_value, list):
                            if any(
                                self._is_datetime_value(item)
                                for item in op_value
                            ):
                                return True
                    elif operator == "$type":
                        # Check if looking for date type
                        if op_value in (
                            9,
                            "date",
                            "Date",
                        ):  # 9 is date type in MongoDB
                            return True
                    elif operator == "$regex":
                        # Check if it's a datetime regex pattern
                        if self._is_datetime_regex(op_value):
                            return True
        return False

    def _is_datetime_value(self, value: Any) -> bool:
        """
        Check if a value is a datetime object or datetime string.

        Args:
            value: Value to check

        Returns:
            True if value is datetime-related, False otherwise
        """
        from .datetime_utils import is_datetime_value

        return is_datetime_value(value)

    def _is_datetime_regex(self, pattern: str) -> bool:
        """
        Check if a regex pattern is likely to be for datetime matching.

        Args:
            pattern: Regex pattern string

        Returns:
            True if pattern is likely datetime-related, False otherwise
        """
        import re
        from .datetime_utils import is_datetime_value

        # If it's not a string, return False
        if not isinstance(pattern, str):
            return False

        # Check if the pattern itself looks like a datetime value
        # This handles cases where people might use exact datetime strings
        if is_datetime_value(pattern):
            return True

        # Check if the pattern contains common datetime-related regex patterns
        # Common datetime-related patterns (for regex patterns)
        datetime_indicators = [
            r"\\d{4}-\\d{2}-\\d{2}",  # Date format: \d{4}-\d{2}-\d{2}
            r"\\d{2}/\\d{2}/\\d{4}",  # US date format: \d{2}/\d{2}/\d{4}
            r"\\d{4}/\\d{2}/\\d{2}",  # Alternative date format: \d{4}/\d{2}/\d{2}
            r"\\d{2}-\\d{2}-\\d{4}",  # Common date format: \d{2}-\d{2}-\d{4}
            r"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}",  # Datetime format: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}
        ]

        for indicator in datetime_indicators:
            if re.search(indicator, pattern):
                return True

        return False

    def _load_documents(self, rows) -> Iterable[Dict[str, Any]]:
        """
        Load documents from rows returned by the database query, including handling both id and _id.

        Args:
            rows: Database result rows containing id, _id, and data

        Returns:
            Iterable[Dict[str, Any]]: An iterable of loaded documents
        """
        for row in rows:
            id_val, stored_id_val, data_val = row
            # Use the collection's _load method which now handles both id and _id
            doc = self._collection._load_with_stored_id(
                id_val, data_val, stored_id_val
            )
            yield doc

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
