"""
Query Builder Mixin for NeoSQLite.

This module contains the QueryBuilderMixin class, which provides methods for
building SQL queries from MongoDB-like query specifications.
"""

from typing import TYPE_CHECKING, Any, Dict, List

from ...sql_utils import quote_table_name
from ... import query_operators
from ...exceptions import MalformedQueryException
from ..json_helpers import neosqlite_json_dumps_for_sql
from ..json_path_utils import parse_json_path
from ..text_search import unified_text_search
from ..expr_evaluator import ExprEvaluator

if TYPE_CHECKING:
    from .. import Collection


class QueryBuilderMixin:
    """
    A mixin class that provides query building capabilities.

    This mixin assumes it will be used with a class that has:
    - self.collection (with db and name attributes)
    - self._jsonb_supported
    - self._json_function_prefix
    - self._normalize_id_query method
    - self._build_expr_where_clause method (for handling $expr queries)
    """

    collection: "Collection"
    _jsonb_supported: bool
    _json_function_prefix: str
    _normalize_id_query: Any
    _build_expr_where_clause: Any

    def _is_text_search_query(self, query: Dict[str, Any]) -> bool:
        """
        Check if the query is a text search query (contains $text operator).

        Args:
            query: The query to check.

        Returns:
            True if the query is a text search query, False otherwise.
        """
        return "$text" in query

    def _build_text_search_query(
        self, query: Dict[str, Any]
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL query for text search using FTS5.

        Args:
            query: A dictionary representing the text search query with $text operator.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL WHERE clause
                                          and a list of parameters, or None if the
                                          query is invalid or FTS index doesn't exist.
        """
        if "$text" not in query:
            return None

        text_query = query["$text"]
        if not isinstance(text_query, dict) or "$search" not in text_query:
            return None

        search_term = text_query["$search"]
        if not isinstance(search_term, str):
            return None

        # Find FTS tables for this collection
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
            (f"{quote_table_name(self.collection.name)}_%_fts",),
        )
        fts_tables = cursor.fetchall()

        if not fts_tables:
            return None

        # Build UNION query to search across ALL FTS tables
        subqueries = []
        params = []

        for (fts_table_name,) in fts_tables:
            # Extract field name from FTS table name (collection_field_fts -> field)
            index_name = fts_table_name[
                len(f"{quote_table_name(self.collection.name)}_") : -4
            ]  # Remove collection_ prefix and _fts suffix

            # Add subquery for this FTS table
            subqueries.append(
                f"SELECT rowid FROM {fts_table_name} WHERE {index_name} MATCH ?"
            )
            params.append(search_term.lower())

        # Combine all subqueries with UNION to get documents matching in ANY FTS index
        union_query = " UNION ".join(subqueries)

        # Build the FTS query
        where_clause = f"""
        WHERE id IN ({union_query})
        """
        return where_clause, params

    def _build_other_fields_clause(
        self, query: Dict[str, Any], expr: Dict[str, Any]
    ) -> tuple[str, List[Any]] | None:
        """
        Build WHERE clause for non-$expr fields.

        Args:
            query: Full query dictionary
            expr: The $expr expression

        Returns:
            Tuple of (WHERE clause, parameters) or None for Python fallback
        """
        clauses: List[str] = []
        params: List[Any] = []

        for field, value in query.items():
            if field == "$expr":
                continue

            if field in ("$and", "$or", "$nor", "$not"):
                return None

            field_result = self._build_field_clause(field, value)
            if field_result is None:
                return None
            field_clause, field_params = field_result

            clauses.append(field_clause)
            params.extend(field_params)

        if clauses:
            return " AND ".join(clauses), params
        return "", params

    def _build_field_clause(
        self, field: str, value: Any
    ) -> tuple[str, List[Any]] | None:
        """
        Build a WHERE clause for a single field.

        Args:
            field: Field name
            value: Field value or operator dict

        Returns:
            Tuple of (SQL clause, parameters) or None for Python fallback
        """
        from ...objectid import ObjectId

        if field == "_id":
            # Handle _id field specially
            if isinstance(value, ObjectId):
                return f"{quote_table_name(self.collection.name)}._id = ?", [
                    str(value)
                ]
            elif isinstance(value, str) and len(value) == 24:
                try:
                    obj_id = ObjectId(value)
                    return (
                        f"{quote_table_name(self.collection.name)}._id = ?",
                        [str(obj_id)],
                    )
                except ValueError:
                    try:
                        int_id = int(value)
                        return (
                            f"{quote_table_name(self.collection.name)}.id = ?",
                            [int_id],
                        )
                    except ValueError:
                        return (
                            f"{quote_table_name(self.collection.name)}._id = ?",
                            [value],
                        )
            elif isinstance(value, int):
                return f"{quote_table_name(self.collection.name)}.id = ?", [
                    value
                ]
            else:
                return f"{quote_table_name(self.collection.name)}._id = ?", [
                    value
                ]
        else:
            # Handle regular fields with json_extract/jsonb_extract
            # Use the correct function based on JSONB support
            json_path = f"'{parse_json_path(field)}'"

            if isinstance(value, dict):
                # Handle operators like $eq, $gt, etc.
                clause, params = self._build_operator_clause(json_path, value)
                if clause is None:
                    return None
                return f"{clause}", params
            else:
                # Simple equality
                extract_expr = (
                    f"{self._json_function_prefix}_extract(data, {json_path})"
                )
                return f"{extract_expr} = ?", [value]

    def _build_simple_where_clause(
        self,
        query: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL WHERE clause for simple queries that can be handled with json_extract.

        This method constructs a SQL WHERE clause based on the query provided.
        It handles simple equality checks and query operators like $eq, $gt, $lt,
        etc. for fields stored in JSON data. For more complex queries, it returns
        None, indicating that a Python-based method should be used instead.

        When the force fallback flag is set, this method returns None to force
        Python-based processing for benchmarking and debugging purposes.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL WHERE clause
                                          and a list of parameters, or None if the
                                          query is too complex or force fallback is enabled.
        """
        # Apply type correction to handle cases where users query 'id' with ObjectId
        # or other common type mismatches
        query = self._normalize_id_query(query)
        # Check force fallback flag
        from .utils import get_force_fallback

        if get_force_fallback():
            return None  # Force fallback to Python implementation

        # Handle text search queries separately
        if self._is_text_search_query(query):
            return self._build_text_search_query(query)

        # Handle $expr queries
        if "$expr" in query:
            return self._build_expr_where_clause(query)

        clauses: List[str] = []
        params: List[Any] = []

        for field, value in query.items():
            # Handle logical operators by falling back to Python processing
            # This is more robust than trying to build complex SQL queries
            if field in ("$and", "$or", "$nor", "$not"):
                return (
                    None  # Fall back to Python processing for logical operators
                )

            elif field == "_id":
                # Handle _id field specially since it's now stored in the dedicated _id column for new records
                # For backward compatibility, we need to check both the _id column and the auto-increment id column
                from ...objectid import ObjectId

                # Convert the value to the appropriate format for storage
                if isinstance(value, ObjectId):
                    param_value = str(value)
                    # Query the _id column
                    clauses.append(
                        f"{quote_table_name(self.collection.name)}._id = ?"
                    )
                    params.append(param_value)
                elif isinstance(value, str) and len(value) == 24:
                    try:
                        # Validate if it's a valid ObjectId string
                        obj_id = ObjectId(value)
                        param_value = str(obj_id)
                        # Query the _id column
                        clauses.append(
                            f"{quote_table_name(self.collection.name)}._id = ?"
                        )
                        params.append(param_value)
                    except ValueError:
                        # If not a valid ObjectId string, it might be an integer id
                        try:
                            int_id = int(
                                value
                            )  # Try to parse as integer for backward compatibility
                            clauses.append(
                                f"{quote_table_name(self.collection.name)}.id = ?"
                            )
                            params.append(int_id)
                        except ValueError:
                            # If not a valid integer, treat as a string _id
                            clauses.append(
                                f"{quote_table_name(self.collection.name)}._id = ?"
                            )
                            params.append(value)
                elif isinstance(value, int):
                    # Query the auto-increment id column
                    clauses.append(
                        f"{quote_table_name(self.collection.name)}.id = ?"
                    )
                    params.append(value)
                else:
                    # For other types, query the _id column
                    clauses.append(
                        f"{quote_table_name(self.collection.name)}._id = ?"
                    )
                    params.append(value)
            else:
                # Handle regular fields with json_extract
                field_result = self._build_field_clause(field, value)
                if field_result is None:
                    return None  # Fall back to Python processing
                field_clause, field_params = field_result
                if field_clause:  # Only add non-empty clauses
                    clauses.append(field_clause)
                    params.extend(field_params)

        if clauses:
            return "WHERE " + " AND ".join(clauses), params
        return "", params

    def _build_operator_clause(
        self,
        json_path: str,
        operators: Dict[str, Any],
        is_datetime_indexed: bool = False,
    ) -> tuple[str | None, List[Any]]:
        """
        Builds a SQL clause for query operators.

        This method constructs a SQL clause based on the provided operators for
        a specific JSON path. It handles various operators like $eq, $gt, $lt, etc.,
        and returns a tuple containing the SQL clause and a list of parameters.
        If an unsupported operator is encountered, it returns None, indicating
        that a fallback to Python processing is needed.

        Args:
            json_path (str): The JSON path to extract the value from.
            operators (Dict[str, Any]): A dictionary of operators and their values.
            is_datetime_indexed (bool): Whether the field has a datetime index that requires timezone normalization.

        Returns:
            tuple[str | None, List[Any]]: A tuple containing the SQL clause and
                                          parameters. If the operator is unsupported,
                                          returns (None, []).
        """
        clauses = []
        params = []

        for op, op_val in operators.items():
            # Serialize Binary objects for SQL comparisons using compact format
            if isinstance(op_val, bytes) and hasattr(
                op_val, "encode_for_storage"
            ):
                op_val = neosqlite_json_dumps_for_sql(op_val)

            match op:
                case "$eq":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the value with datetime() for proper timezone normalization
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) = datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) = ?"
                        )
                        params.append(op_val)
                case "$gt":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the value with datetime() for proper timezone normalization
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) > datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) > ?"
                        )
                        params.append(op_val)
                case "$lt":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the value with datetime() for proper timezone normalization
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) < datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) < ?"
                        )
                        params.append(op_val)
                case "$gte":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the value with datetime() for proper timezone normalization
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) >= datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) >= ?"
                        )
                        params.append(op_val)
                case "$lte":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the value with datetime() for proper timezone normalization
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) <= datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) <= ?"
                        )
                        params.append(op_val)
                case "$ne":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the value with datetime() for proper timezone normalization
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) != datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) != ?"
                        )
                        params.append(op_val)
                case "$in":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the values with datetime() for proper timezone normalization
                        placeholders = ", ".join("datetime(?)" for _ in op_val)
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) IN ({placeholders})"
                        )
                        params.extend(op_val)
                    else:
                        placeholders = ", ".join("?" for _ in op_val)
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) IN ({placeholders})"
                        )
                        params.extend(op_val)
                case "$nin":
                    if is_datetime_indexed:
                        # For datetime-indexed fields, wrap the values with datetime() for proper timezone normalization
                        placeholders = ", ".join("datetime(?)" for _ in op_val)
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) NOT IN ({placeholders})"
                        )
                        params.extend(op_val)
                    else:
                        placeholders = ", ".join("?" for _ in op_val)
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) NOT IN ({placeholders})"
                        )
                        params.extend(op_val)
                case "$exists":
                    # Handle boolean value for $exists
                    if op_val is True:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) IS NOT NULL"
                        )
                    elif op_val is False:
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) IS NULL"
                        )
                    else:
                        # Invalid value for $exists, fallback to Python
                        return None, []
                case "$mod":
                    # Handle [divisor, remainder] array
                    if isinstance(op_val, (list, tuple)) and len(op_val) == 2:
                        divisor, remainder = op_val
                        clauses.append(
                            f"{self._json_function_prefix}_extract(data, {json_path}) % ? = ?"
                        )
                        params.extend([divisor, remainder])
                    else:
                        # Invalid format for $mod, fallback to Python
                        return None, []
                case "$size":
                    # Handle array size comparison
                    if isinstance(op_val, int):
                        clauses.append(
                            f"json_array_length({self._json_function_prefix}_extract(data, {json_path})) = ?"
                        )
                        params.append(op_val)
                    else:
                        # Invalid value for $size, fallback to Python
                        return None, []
                case "$contains":
                    # Handle case-insensitive substring search
                    if isinstance(op_val, str):
                        clauses.append(
                            f"lower({self._json_function_prefix}_extract(data, {json_path})) LIKE ?"
                        )
                        params.append(f"%{op_val.lower()}%")
                    else:
                        # Invalid value for $contains, fallback to Python
                        return None, []
                case _:
                    # Unsupported operator, fallback to Python
                    return None, []

        if not clauses:
            return None, []

        # Combine all clauses with AND
        combined_clause = " AND ".join(clauses)
        return combined_clause, params

    def _search_in_value(self, value: Any, search_term: str) -> bool:
        """
        Recursively search for a term in a value (string, dict, or list).

        Args:
            value: The value to search in
            search_term: The term to search for

        Returns:
            bool: True if the search term is found, False otherwise
        """
        if isinstance(value, str):
            return search_term.lower() in value.lower()
        elif isinstance(value, dict):
            return any(
                self._search_in_value(v, search_term) for v in value.values()
            )
        elif isinstance(value, list):
            return any(
                self._search_in_value(elem, search_term) for elem in value
            )
        return False

    def _apply_query(
        self,
        query: Dict[str, Any],
        document: Dict[str, Any],
    ) -> bool:
        """
        Applies a query to a document to determine if it matches the query criteria.

        Handles logical operators ($and, $or, $nor, $not) and nested field paths.
        Processes both simple equality checks and complex query operators.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.
            document (Dict[str, Any]): The document to apply the query to.

        Returns:
            bool: True if the document matches the query, False otherwise.
        """
        if document is None:
            return False
        matches: List[bool] = []

        def reapply(q: Dict[str, Any]) -> bool:
            """
            Recursively apply the query to the document to determine if it matches
            the query criteria.

            Args:
                q (Dict[str, Any]): The query to apply.
                document (Dict[str, Any]): The document to apply the query to.

            Returns:
                bool: True if the document matches the query, False otherwise.
            """
            return self._apply_query(q, document)

        for field, value in query.items():
            match field:
                case "$expr":
                    # Handle $expr operator in Python fallback
                    evaluator = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )
                    result = evaluator._evaluate_expr_python(value, document)
                    matches.append(bool(result))
                case "$gt" | "$lt" | "$gte" | "$lte" | "$eq" | "$ne" | "$cmp":
                    # Handle direct comparison expressions (without $expr wrapper)
                    # These are expressions like {"$gt": [{"$sin": "$angle"}, 0.5]}
                    # Check if value is an array (expression form) vs dict (field operator form)
                    if isinstance(value, list) and len(value) == 2:
                        # This is a direct expression, not a field operator
                        evaluator = ExprEvaluator(
                            data_column="data", db_connection=self.collection.db
                        )
                        result = evaluator._evaluate_expr_python(
                            query, document
                        )
                        matches.append(bool(result))
                        break  # Direct expression is the entire query
                    # Otherwise, fall through to normal field operator handling
                case "$text":
                    # Handle $text operator in Python fallback
                    # This is a simplified implementation that just does basic string matching
                    if isinstance(value, dict) and "$search" in value:
                        search_term = value["$search"]
                        if isinstance(search_term, str):
                            # Find FTS tables for this collection to determine which fields are indexed
                            cursor = self.collection.db.execute(
                                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
                                (
                                    f"{quote_table_name(self.collection.name)}_%_fts",
                                ),
                            )
                            fts_tables = cursor.fetchall()

                            # Check each FTS-indexed field for matches
                            for fts_table in fts_tables:
                                fts_table_name = fts_table[0]
                                # Extract field name from FTS table name
                                # (collection_field_fts -> field)
                                index_name = fts_table_name[
                                    len(
                                        f"{quote_table_name(self.collection.name)}_"
                                    ) : -4
                                ]  # Remove collection_ prefix and _fts suffix
                                # Convert underscores back to dots for nested keys
                                field_name = index_name.replace("_", ".")
                                # Check if this field has content that matches the search term
                                try:
                                    field_value = self.collection._get_val(
                                        document, field_name
                                    )
                                except (AttributeError, TypeError):
                                    # Field path contains arrays that can't be handled in Python fallback
                                    # Skip this field - the SQL FTS index will handle it
                                    continue
                                if field_value and isinstance(field_value, str):
                                    # Simple case-insensitive substring search
                                    if (
                                        search_term.lower()
                                        in field_value.lower()
                                    ):
                                        matches.append(True)
                                        break
                                elif isinstance(field_value, list):
                                    # Field is an array - check each element
                                    for elem in field_value:
                                        if (
                                            isinstance(elem, str)
                                            and search_term.lower()
                                            in elem.lower()
                                        ):
                                            matches.append(True)
                                            break
                                        elif isinstance(elem, dict):
                                            # For object arrays, check all string values recursively
                                            if self._search_in_value(
                                                elem, search_term
                                            ):
                                                matches.append(True)
                                                break
                                    else:
                                        continue
                                    break
                            else:
                                # If no FTS indexes exist, use enhanced text search on all fields
                                # This provides better international character support and diacritic-insensitive matching
                                if unified_text_search(document, search_term):
                                    matches.append(True)
                                else:
                                    matches.append(False)
                        else:
                            matches.append(False)
                    else:
                        matches.append(False)
                case "$and":
                    matches.append(all(map(reapply, value)))
                case "$or":
                    matches.append(any(map(reapply, value)))
                case "$nor":
                    matches.append(not any(map(reapply, value)))
                case "$not":
                    matches.append(not self._apply_query(value, document))
                case _:
                    if isinstance(value, dict):
                        for operator, arg in value.items():
                            if not self._get_operator_fn(operator)(
                                field, arg, document
                            ):
                                matches.append(False)
                                break
                        else:
                            matches.append(True)
                    else:
                        doc_value: Dict[str, Any] | None = document
                        if doc_value and field in doc_value:
                            doc_value = doc_value.get(field, None)
                        else:
                            for path in field.split("."):
                                if not isinstance(doc_value, dict):
                                    break
                                doc_value = doc_value.get(path, None)
                        if value != doc_value:
                            matches.append(False)
        return all(matches)

    def _get_operator_fn(self, op: str) -> Any:
        """
        Retrieve the function associated with the given operator from the
        query_operators module.

        Args:
            op (str): The operator string, which should start with a '$' prefix.

        Returns:
            Any: The function corresponding to the operator.

        Raises:
            MalformedQueryException: If the operator does not start with '$'.
            MalformedQueryException: If the operator is not currently implemented.
        """
        if not op.startswith("$"):
            raise MalformedQueryException(
                f"Operator '{op}' is not a valid query operation"
            )
        try:
            return getattr(query_operators, op.replace("$", "_"))
        except AttributeError:
            raise MalformedQueryException(
                f"Operator '{op}' is not currently implemented"
            )
