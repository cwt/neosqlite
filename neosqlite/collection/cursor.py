from __future__ import annotations
from functools import partial
from typing import Any, Callable, Dict, List, Iterator, Iterable, TYPE_CHECKING

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
        self._min: Dict[str, Any] | None = None
        self._max: Dict[str, Any] | None = None
        self._collation: Dict[str, Any] | None = None
        self._where_predicate: Callable[[Dict[str, Any]], bool] | None = None
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

    def min(self, min_spec: Dict[str, Any]) -> Cursor:
        """
        Set the minimum bound for index queries.

        This method sets a lower bound on the index values to be scanned.
        Only documents with index values greater than or equal to the
        specified minimum will be returned.

        Args:
            min_spec (Dict[str, Any]): A dictionary specifying the minimum
                                       index values, e.g., {"field": value}

        Returns:
            Cursor: The cursor object with the minimum bound applied

        Example:
            >>> cursor = collection.find({"age": {"$gte": 18}}).min({"age": 18})
        """
        self._min = min_spec
        return self

    def max(self, max_spec: Dict[str, Any]) -> Cursor:
        """
        Set the maximum bound for index queries.

        This method sets an upper bound on the index values to be scanned.
        Only documents with index values less than the specified maximum
        will be returned.

        Args:
            max_spec (Dict[str, Any]): A dictionary specifying the maximum
                                       index values, e.g., {"field": value}

        Returns:
            Cursor: The cursor object with the maximum bound applied

        Example:
            >>> cursor = collection.find({"age": {"$lte": 65}}).max({"age": 65})
        """
        self._max = max_spec
        return self

    def collation(self, collation: Dict[str, Any]) -> Cursor:
        """
        Set the collation for the cursor.

        Collation allows users to specify language-specific rules for string
        comparison, such as rules for lettercase and accent marks.

        Args:
            collation (Dict[str, Any]): A dictionary specifying collation options:
                - locale (str): Language locale (e.g., "en_US", "fr_FR", "de_DE")
                - caseLevel (bool): Whether to include case comparison
                - caseFirst (str): "upper", "lower", or "off"
                - strength (int): Comparison strength (1-5)
                - numericOrdering (bool): Compare numbers numerically
                - alternate (str): "shifted" or "non-ignorable"
                - backwards (bool): Sort backwards (for French)

        Returns:
            Cursor: The cursor object with the collation applied

        Example:
            >>> cursor = collection.find({"name": {"$gte": "A"}}).collation(
            ...     {"locale": "fr_FR", "strength": 2}
            ... )

        Note:
            NeoSQLite maps common locales to SQLite collations:
            - Default/unknown: BINARY (case-sensitive)
            - Locales with case-insensitive: NOCASE
            - Custom collations can be registered via Connection tokenizers
        """
        self._collation = collation
        return self

    def where(self, predicate: Callable[[Dict[str, Any]], bool]) -> Cursor:
        """
        Filter cursor results using a Python predicate function.

        This is a Tier-3 (Python fallback) method that applies a Python
        function to filter documents after they are retrieved from the database.

        Args:
            predicate (Callable[[Dict[str, Any]], bool]): A function that takes a document and returns
                                 True if the document should be included.

        Returns:
            Cursor: The cursor object with the predicate applied

        Example:
            >>> cursor = collection.find({}).where(
            ...     lambda doc: doc.get('value', 0) > 10
            ... )

        Note:
            This method uses Python-based filtering (Tier-3), which means all
            matching documents are retrieved from the database first, then
            filtered in Python. For better performance, use MongoDB-style
            query operators in the find() filter when possible.
        """
        self._where_predicate = predicate
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
        cloned._min = deepcopy(self._min) if self._min else None
        cloned._max = deepcopy(self._max) if self._max else None
        cloned._collation = (
            deepcopy(self._collation) if self._collation else None
        )
        cloned._where_predicate = (
            self._where_predicate
        )  # Functions can't be deep copied
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

    @property
    def alive(self) -> bool:
        """
        Check if the cursor has more documents to iterate.

        In NeoSQLite, a cursor is considered alive if it hasn't been fully exhausted.
        This is a simplified implementation for PyMongo API compatibility.

        Returns:
            bool: True if the cursor may have more documents, False if exhausted

        Note:
            NeoSQLite cursors are re-iterable, so this property tracks whether
            the current iteration has been completed.

        Example:
            >>> cursor = collection.find({}).limit(10)
            >>> cursor.alive
            True
            >>> list(cursor)
            >>> cursor.alive
            False
        """
        # Cursor is alive if we haven't retrieved any documents yet
        # or if we haven't reached the limit
        if self._limit is not None:
            return self._retrieved < self._limit
        # Without limit, cursor is considered alive until iteration starts
        # After iteration, check if we got any results
        return self._retrieved == 0 or not hasattr(self, "_exhausted")

    @property
    def collection(self):
        """
        Return a reference to the collection this cursor is iterating over.

        Returns:
            Collection: The collection associated with this cursor

        Example:
            >>> cursor = collection.find({})
            >>> cursor.collection
            Collection(database, "collection_name")
            >>> cursor.collection.name
            'collection_name'
        """
        return self._collection

    @property
    def address(self) -> tuple | None:
        """
        Return the address of the database.

        For NeoSQLite, this returns a tuple representing the database connection.
        This is a simplified implementation for PyMongo API compatibility.

        Returns:
            tuple | None: A tuple of (database_path, 0) after iteration starts,
                         None before the cursor has been executed.
                         - For file databases: ('sqlite:///path/to/file.db', 0)
                         - For memory databases: ('sqlite::memory:', 0)

        Note:
            SQLite is an embedded database without a server, so this returns
            the database path instead of a network address. Returns None until
            the cursor has been iterated, matching PyMongo behavior.

        Example:
            >>> cursor = collection.find({})
            >>> cursor.address  # Before iteration
            None
            >>> list(cursor)
            >>> cursor.address  # After iteration
            ('sqlite::memory:', 0)  # or ('sqlite:///path/to/file.db', 0)
        """
        # Match PyMongo behavior: None before iteration, tuple after
        if self._retrieved == 0 and not hasattr(self, "_iterated"):
            return None

        # Get database path from connection
        db_name = self._collection.db.execute(
            "PRAGMA database_list"
        ).fetchone()[2]
        if db_name == ":memory:" or db_name == "":
            return ("sqlite::memory:", 0)
        else:
            return (f"sqlite://{db_name}", 0)

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

        # Mark cursor as having been iterated (for address property)
        self._iterated = True

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

            # Add min/max bounds if specified
            if self._min or self._max:
                minmax_clause, minmax_params = self._build_minmax_clause(
                    where_clause, params, self._min, self._max
                )
                where_clause = minmax_clause
                params = minmax_params

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
            docs = self._load_documents(db_cursor.fetchall())

            # Apply where predicate if specified (Tier-3 Python filtering)
            if self._where_predicate:
                return filter(self._where_predicate, docs)

            return docs
        else:
            # Fallback to Python-based filtering for complex queries
            # Use the collection's JSONB support flag to determine how to select data
            # Include _id column to support both integer id and ObjectId _id
            if self._collection.query_engine._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {self._collection.name}"
            else:
                cmd = f"SELECT id, _id, data FROM {self._collection.name}"

            # Add min/max bounds if specified (no filter case)
            if self._min or self._max:
                minmax_clause, minmax_params = self._build_minmax_clause(
                    "", (), self._min, self._max
                )
                # Remove leading " WHERE" if present and add our own
                minmax_clause = minmax_clause.lstrip().lstrip("WHERE").lstrip()
                cmd = f"{cmd} WHERE {minmax_clause}"
                params = minmax_params
            else:
                params = ()

            # Add comment if specified
            if self._comment:
                safe_comment = (
                    self._comment.replace("/*", "")
                    .replace("*/", "")
                    .replace("--", "")
                )
                cmd = f"/* {safe_comment} */ {cmd}"
            db_cursor = self._collection.db.execute(cmd, params)
            apply = partial(self._query_helpers._apply_query, self._filter)
            all_docs = self._load_documents(db_cursor.fetchall())
            filtered_docs = filter(apply, all_docs)

            # Apply where predicate if specified (Tier-3 Python filtering)
            if self._where_predicate:
                return filter(self._where_predicate, filtered_docs)

            return filtered_docs

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

    def _build_minmax_clause(
        self,
        where_clause: str,
        params: tuple,
        min_spec: Dict[str, Any] | None,
        max_spec: Dict[str, Any] | None,
    ) -> tuple:
        """
        Build SQL clause for min/max index bounds.

        Args:
            where_clause: Existing WHERE clause (may be empty)
            params: Existing parameters
            min_spec: Minimum bound specification
            max_spec: Maximum bound specification

        Returns:
            Tuple of (new WHERE clause, new parameters)
        """
        additional_conditions = []
        additional_params = list(params)

        # Add minimum bounds
        if min_spec:
            for field, value in min_spec.items():
                additional_conditions.append(
                    f"jsonb_extract(data, '$.{field}') >= ?"
                )
                additional_params.append(value)

        # Add maximum bounds (strict less than for max)
        if max_spec:
            for field, value in max_spec.items():
                additional_conditions.append(
                    f"jsonb_extract(data, '$.{field}') < ?"
                )
                additional_params.append(value)

        if additional_conditions:
            conditions_sql = " AND ".join(additional_conditions)
            # If there's an existing WHERE clause, append to it
            if where_clause and where_clause.strip():
                base_clause = where_clause.rstrip()
                new_clause = f"{base_clause} AND {conditions_sql}"
            else:
                # No existing WHERE, create new one
                new_clause = f"WHERE {conditions_sql}"
            return new_clause, tuple(additional_params)

        return where_clause, params

    def _get_collate_clause(self) -> str:
        """
        Get the SQL COLLATE clause based on collation settings.

        Returns:
            str: COLLATE clause or empty string if no collation is set

        Note:
            Maps MongoDB collation locales to SQLite collations:
            - Case-insensitive locales → NOCASE
            - Default/unknown → BINARY (case-sensitive)
            Custom collations can be registered via Connection tokenizers.

        Note: COLLATE is applied to ORDER BY clauses, not WHERE clauses.
        For WHERE clause string comparisons, use _apply_collation_to_expr().
        """
        if not self._collation:
            return ""

        self._collation.get("locale", "")
        strength = self._collation.get("strength", 3)
        case_level = self._collation.get("caseLevel", False)

        # Determine collation based on settings
        # Strength 1-2: Ignore case/diacritics → NOCASE
        # Strength 3+: Respect case → BINARY (default)
        if strength <= 2 or case_level is False:
            # Case-insensitive comparison
            return " COLLATE NOCASE"
        else:
            # Case-sensitive comparison (default SQLite behavior)
            return ""

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

        # Get collation settings for case-insensitive sorting
        case_insensitive = False
        if self._collation:
            strength = self._collation.get("strength", 3)
            case_level = self._collation.get("caseLevel", False)
            # Strength 1-2 means case-insensitive
            if strength <= 2 or case_level is False:
                case_insensitive = True

        for key in sort_keys:
            get_val = partial(self._collection._get_val, key=key)
            reverse = self._sort[key] == DESCENDING

            if case_insensitive:
                # Use case-insensitive sorting
                def make_key(get_val=get_val):
                    def key_func(doc):
                        val = get_val(doc)
                        if isinstance(val, str):
                            return val.lower()
                        return val

                    return key_func

                sorted_docs.sort(key=make_key(), reverse=reverse)
            else:
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
