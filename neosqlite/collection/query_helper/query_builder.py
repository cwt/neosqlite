"""
Query Builder Mixin for NeoSQLite.

This module contains the QueryBuilderMixin class, which provides methods for
building SQL queries from MongoDB-like query specifications.
"""

import re
from typing import TYPE_CHECKING, Any, Dict, List

from ... import query_operators
from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..expr_evaluator import ExprEvaluator
from ..json_helpers import neosqlite_json_dumps_for_sql
from ..json_path_utils import parse_json_path
from ..text_search import unified_text_search
from ..type_correction import normalize_id_query_for_db

if TYPE_CHECKING:
    from .. import Collection


class QueryBuilderMixin:
    """
    A mixin class that provides query building capabilities.

    This mixin assumes it will be used with a class that has:
    - self.collection (with db and name attributes)
    - self._jsonb_supported
    - self._json_function_prefix
    - self._build_expr_where_clause method (for handling $expr queries)
    """

    collection: "Collection"
    _jsonb_supported: bool
    _json_function_prefix: str
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
    ) -> tuple[str, List[Any], List[str]] | None:
        """
        Builds a SQL query for text search using FTS5.

        Args:
            query: A dictionary representing the text search query with $text operator.

        Returns:
            tuple[str, List[Any], List[str]] | None: A tuple containing the SQL WHERE clause,
                                                      a list of parameters, and an empty list of
                                                      tables to clean up, or None.
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
        return where_clause, params, []

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

        if field == "$jsonSchema":
            return None

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
                if isinstance(value, re.Pattern):
                    return None  # Fall back to Python for regex objects

                extract_expr = (
                    f"{self._json_function_prefix}_extract(data, {json_path})"
                )
                return f"{extract_expr} = ?", [value]

    def _build_simple_where_clause(
        self,
        query: Dict[str, Any],
    ) -> tuple[str, List[Any], List[str]] | None:
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
            tuple[str, List[Any], List[str]] | None: A tuple containing the SQL WHERE clause,
                                                      a list of parameters, and a list of
                                                      temporary tables to clean up, or None.
        """
        # Apply type correction to handle cases where users query 'id' with ObjectId
        # or other common type mismatches
        query = normalize_id_query_for_db(query)
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

        if "$jsonSchema" in query:
            return None

        if "$where" in query:
            raise NotImplementedError(
                "The '$where' operator (JavaScript) is not supported in NeoSQLite. "
                "Please use the '$expr' operator for field-to-field comparisons, "
                "which is fully compatible with MongoDB and highly optimized in NeoSQLite."
            )

        if "$function" in query:
            raise NotImplementedError(
                "The '$function' operator is not supported in NeoSQLite. "
                "Please use '$expr' with Python expressions, or post-process results in Python."
            )

        if "$accumulator" in query:
            raise NotImplementedError(
                "The '$accumulator' operator is not supported in NeoSQLite. "
                "Please use built-in accumulators ($sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last), "
                "or post-process results in Python."
            )

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
            return "WHERE " + " AND ".join(clauses), params, []
        return "", params, []

    def _build_sort_clause(
        self,
        sort: Dict[str, int] | None,
        collation: Dict[str, Any] | None = None,
    ) -> str:
        """
        Builds a SQL ORDER BY clause from a sort dictionary.

        Args:
            sort: A dictionary mapping fields to sort directions (1 for ASC, -1 for DESC).
            collation: Optional collation settings for case-insensitive sorting.

        Returns:
            A SQL ORDER BY clause string (including 'ORDER BY' prefix), or empty string.
        """
        if not sort:
            return ""

        clauses = []

        # Get collation settings
        collate_clause = ""
        if collation:
            strength = collation.get("strength", 3)
            case_level = collation.get("caseLevel", False)
            if strength <= 2 or case_level is False:
                collate_clause = " COLLATE NOCASE"

        for field, direction in sort.items():
            if field == "_id":
                order_field = f"{quote_table_name(self.collection.name)}._id"
            else:
                json_path = f"'{parse_json_path(field)}'"
                order_field = (
                    f"{self._json_function_prefix}_extract(data, {json_path})"
                )

            order_dir = "ASC" if direction == 1 else "DESC"
            clauses.append(f"{order_field}{collate_clause} {order_dir}")

        if clauses:
            return " ORDER BY " + ", ".join(clauses)
        return ""

    def _build_pagination_clause(
        self,
        limit: int | None,
        skip: int = 0,
    ) -> str:
        """
        Builds a SQL LIMIT and OFFSET clause.

        Args:
            limit: The maximum number of documents to return.
            skip: The number of documents to skip.

        Returns:
            A SQL LIMIT/OFFSET clause string, or empty string.
        """
        if limit is None and skip == 0:
            return ""

        clause = ""
        if limit is not None:
            clause = f" LIMIT {limit}"
            if skip > 0:
                clause += f" OFFSET {skip}"
        elif skip > 0:
            # SQLite requires a LIMIT when using OFFSET
            # Use -1 for unlimited if supported, or a very large number
            clause = f" LIMIT -1 OFFSET {skip}"

        return clause

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
                case "$elemMatch":
                    # Determine the json_each function to use
                    json_each_func = getattr(
                        self, "_json_each_function", "json_each"
                    )

                    # Build the inner WHERE clause for the subquery
                    inner_clauses = []
                    inner_params = []

                    if isinstance(op_val, dict):
                        has_operators = any(
                            k.startswith("$") for k in op_val.keys()
                        )
                        if has_operators:
                            # Case: {"field": {"$elemMatch": {"$gt": 10}}}
                            # Use "value" as path which will be retargeted below
                            c, p = self._build_operator_clause("value", op_val)
                            if c is None:
                                return None, []
                            # Retarget: replace json_extract(data, value) with value
                            # The _build_operator_clause uses f"{self._json_function_prefix}_extract(data, value)"
                            c = c.replace(
                                f"{self._json_function_prefix}_extract(data, value)",
                                "value",
                            )
                            inner_clauses.append(c)
                            inner_params.extend(p)
                        else:
                            # Case: {"field": {"$elemMatch": {"k": "v"}}}
                            for sub_field, sub_val in op_val.items():
                                if isinstance(sub_val, dict):
                                    # Operator on subfield
                                    c, p = self._build_operator_clause(
                                        f"'{parse_json_path(sub_field)}'",
                                        sub_val,
                                    )
                                else:
                                    # Equality on subfield
                                    c = (
                                        f"{self._json_function_prefix}_extract(data, "
                                        f"'{parse_json_path(sub_field)}') = ?"
                                    )
                                    p = [sub_val]

                                if c is None:
                                    return None, []
                                # Retarget from 'data' to 'value' (the row from json_each)
                                c = c.replace(
                                    f"{self._json_function_prefix}_extract(data,",
                                    f"{self._json_function_prefix}_extract(value,",
                                )
                                inner_clauses.append(c)
                                inner_params.extend(p)
                    else:
                        # Case: {"field": {"$elemMatch": "value"}}
                        # For simple values in an array
                        inner_clauses.append("value = ?")
                        inner_params.append(op_val)

                    if not inner_clauses:
                        return None, []

                    where_inner = " AND ".join(inner_clauses)
                    # EXISTS (SELECT 1 FROM json_each(data, '$.field') WHERE <inner_where>)
                    # Use json_type check to ensure it only matches arrays (MongoDB semantics)
                    clauses.append(
                        f"json_type(data, {json_path}) = 'array' AND "
                        f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) WHERE {where_inner})"
                    )
                    params.extend(inner_params)
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
                    text_match = False
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
                            if fts_tables:
                                for fts_table in fts_tables:
                                    fts_table_name = fts_table[0]
                                    index_name = fts_table_name[
                                        len(
                                            f"{quote_table_name(self.collection.name)}_"
                                        ) : -4
                                    ]
                                    field_name = index_name.replace("_", ".")
                                    try:
                                        field_value = self.collection._get_val(
                                            document, field_name
                                        )
                                    except (AttributeError, TypeError):
                                        continue

                                    if field_value and isinstance(
                                        field_value, str
                                    ):
                                        if (
                                            search_term.lower()
                                            in field_value.lower()
                                        ):
                                            text_match = True
                                            break
                                    elif isinstance(field_value, list):
                                        for elem in field_value:
                                            if (
                                                isinstance(elem, str)
                                                and search_term.lower()
                                                in elem.lower()
                                            ):
                                                text_match = True
                                                break
                                            elif isinstance(
                                                elem, dict
                                            ) and self._search_in_value(
                                                elem, search_term
                                            ):
                                                text_match = True
                                                break
                                        if text_match:
                                            break
                            else:
                                # No FTS indexes, search all fields
                                text_match = unified_text_search(
                                    document, search_term
                                )

                    matches.append(text_match)
                case "$and":
                    matches.append(all(map(reapply, value)))
                case "$or":
                    matches.append(any(map(reapply, value)))
                case "$nor":
                    matches.append(not any(map(reapply, value)))
                case "$not":
                    matches.append(not self._apply_query(value, document))
                case "$jsonSchema":
                    from .schema_validator import matches_json_schema

                    matches.append(matches_json_schema(document, value))
                case _:
                    if isinstance(value, dict):
                        # Extract $options for $regex if present
                        options = value.get("$options", "")
                        if options and "$regex" not in value:
                            raise MalformedQueryException(
                                "Can't use $options without $regex"
                            )

                        for operator, arg in value.items():
                            if operator == "$options":
                                # $options is handled together with $regex
                                continue

                            fn = self._get_operator_fn(operator)
                            # Call operator function, passing options if it's $regex
                            if operator == "$regex":
                                if not fn(
                                    field, arg, document, options=options
                                ):
                                    matches.append(False)
                                    break
                            else:
                                if not fn(field, arg, document):
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

                        if isinstance(value, re.Pattern):
                            if doc_value is None or not value.search(
                                str(doc_value)
                            ):
                                matches.append(False)
                        elif value != doc_value:
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
