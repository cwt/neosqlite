"""SQL WHERE clause and operator builder for NeoSQLite.

Extracted from query_builder.py — provides _build_simple_where_clause
and related SQL generation for MongoDB query operators.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from ...sql_utils import quote_table_name
from ..json_helpers import neosqlite_json_dumps_for_sql
from ..json_path_utils import parse_json_path
from ..type_correction import normalize_id_query_for_db

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


class SqlQueryBuilderMixin:
    """Mixin providing SQL WHERE clause building methods.

    Designed to be composed into QueryBuilderMixin.
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _build_expr_where_clause: Any

    def _is_text_search_query(self, query: dict[str, Any]) -> bool:
        """
        Check if the query is a text search query (contains $text operator).

        Args:
            query: The query to check.

        Returns:
            True if the query is a text search query, False otherwise.
        """
        return "$text" in query

    def _build_text_search_query(
        self, query: dict[str, Any]
    ) -> tuple[str, list[Any], list[str]] | None:
        """
        Builds a SQL query for text search using FTS5.

        Args:
            query: A dictionary representing the text search query with $text operator.

        Returns:
            tuple[str, list[Any], list[str]] | None: A tuple containing the SQL WHERE clause,
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

    def _id_column_ref(self) -> str:
        """Return the quoted SQL column reference that stores the logical _id.

        Modern collections keep the logical _id in a dedicated ``_id`` column.
        Legacy collections that predate that column store it in the autoincrement
        ``id`` surrogate, which is retained only as a deprecated fallback. All
        _id queries must target this column so MongoDB-compatible _id semantics
        are preserved (integer _id values are NOT redirected to ``id``).
        """
        return (
            f"{quote_table_name(self.collection.name)}."
            f"{self.collection._id_column}"
        )

    def _build_id_operator_clause(
        self, value: dict
    ) -> tuple[str, list[Any]] | None:
        """
        Build a WHERE clause for _id field with operators like $in, $nin, $ne, etc.

        Args:
            value: Dictionary containing operators like $in, $nin, $ne, etc.

        Returns:
            Tuple of (SQL clause, parameters) or None for Python fallback
        """
        id_col = self._id_column_ref()

        from ...objectid import ObjectId

        def _normalize_id_value(v: Any) -> Any:
            if isinstance(v, ObjectId):
                return str(v)
            if isinstance(v, str):
                try:
                    return str(ObjectId(v))
                except ValueError:
                    return v
            return v

        clauses: list[str] = []
        params: list[Any] = []

        for op, op_val in value.items():
            if op == "$in" or op == "$nin":
                if not isinstance(op_val, list) or len(op_val) == 0:
                    return None
                ph = ", ".join("?" for _ in op_val)
                if op == "$in":
                    clauses.append(f"{id_col} IN ({ph})")
                else:
                    clauses.append(f"{id_col} NOT IN ({ph})")
                params.extend(_normalize_id_value(v) for v in op_val)

            elif op == "$ne":
                if isinstance(op_val, list):
                    return None
                clauses.append(f"{id_col} != ?")
                params.append(_normalize_id_value(op_val))
            elif op in ("$gt", "$gte", "$lt", "$lte"):
                # Fall back to Python for range comparisons on _id.
                # The _id column may contain mixed types (int, str, ObjectId).
                # SQLite JSONB ordering (numbers < strings) differs from MongoDB
                # BSON ordering, which only compares within the same type.
                # Python evaluation handles these correctly.
                return None
            elif op == "$eq":
                clauses.append(f"{id_col} = ?")
                params.append(_normalize_id_value(op_val))
            else:
                return None  # Fall back to Python for unsupported operators

        if clauses:
            return " AND ".join(clauses), params
        return None  # Fall back to Python if no clauses generated

    # _categorize_id_value removed: _id queries now use _id_column_ref() for
    # strict MongoDB-like semantics (integer _id targets the _id column).
    def _build_field_clause(
        self, field: str, value: Any
    ) -> tuple[str, list[Any]] | None:
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
            # Strict MongoDB-like _id handling: the logical _id lives in the
            # dedicated `_id` column (or the deprecated `id` surrogate for legacy
            # collections). Integer _id values are NOT redirected to `id`.
            id_col = self._id_column_ref()
            if isinstance(value, dict):
                return self._build_id_operator_clause(value)
            elif isinstance(value, ObjectId):
                return f"{id_col} = ?", [str(value)]
            elif isinstance(value, str) and len(value) == 24:
                try:
                    obj_id = ObjectId(value)
                    return f"{id_col} = ?", [str(obj_id)]
                except ValueError:
                    # Not a valid ObjectId hex string: treat as a plain string _id.
                    return f"{id_col} = ?", [value]
            elif isinstance(value, int):
                return f"{id_col} = ?", [value]
            else:
                return f"{id_col} = ?", [value]
        else:
            # Handle regular fields with json_extract/jsonb_extract
            # Use the correct function based on JSONB support
            json_path = f"'{parse_json_path(field)}'"

            if isinstance(value, dict):
                # Handle operators like $eq, $gt, etc.
                # Extract $options for $regex if present
                options = value.get("$options", "")
                clause, params = self._build_operator_clause(
                    json_path, value, regex_options=options
                )
                if clause is None:
                    return None
                return f"{clause}", params
            else:
                # Simple equality
                if isinstance(value, re.Pattern):
                    return None  # Fall back to Python for regex objects

                extract_expr = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"
                return f"{extract_expr} = ?", [value]

    def _build_simple_where_clause(
        self,
        query: dict[str, Any],
    ) -> tuple[str, list[Any], list[str]] | None:
        """
        Builds a SQL WHERE clause for simple queries that can be handled with json_extract.

        This method constructs a SQL WHERE clause based on the query provided.
        It handles simple equality checks and query operators like $eq, $gt, $lt,
        etc. for fields stored in JSON data. For more complex queries, it returns
        None, indicating that a Python-based method should be used instead.

        When the force fallback flag is set, this method returns None to force
        Python-based processing for benchmarking and debugging purposes.

        Args:
            query (dict[str, Any]): A dictionary representing the query criteria.

        Returns:
            tuple[str, list[Any], list[str]] | None: A tuple containing the SQL WHERE clause,
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

        clauses: list[str] = []
        params: list[Any] = []

        for field, value in query.items():
            # Handle logical operators by falling back to Python processing
            # This is more robust than trying to build complex SQL queries
            if field in ("$and", "$or", "$nor", "$not"):
                return (
                    None  # Fall back to Python processing for logical operators
                )

            elif field == "_id":
                # Strict MongoDB-like _id handling: the logical _id lives in the
                # dedicated `_id` column (or the deprecated `id` surrogate for
                # legacy collections). Integer _id values are NOT redirected to `id`.
                from ...objectid import ObjectId

                id_col = self._id_column_ref()

                # Handle operator-based queries on _id using the dedicated method
                if isinstance(value, dict):
                    result = self._build_id_operator_clause(value)
                    if result is None:
                        return None
                    clause, field_params = result
                    clauses.append(clause)
                    params.extend(field_params)
                elif isinstance(value, ObjectId):
                    param_value = str(value)
                    clauses.append(f"{id_col} = ?")
                    params.append(param_value)
                elif isinstance(value, str) and len(value) == 24:
                    try:
                        obj_id = ObjectId(value)
                        param_value = str(obj_id)
                        clauses.append(f"{id_col} = ?")
                        params.append(param_value)
                    except ValueError:
                        # Not a valid ObjectId hex string: treat as a plain string _id.
                        clauses.append(f"{id_col} = ?")
                        params.append(value)
                elif isinstance(value, int):
                    clauses.append(f"{id_col} = ?")
                    params.append(value)
                else:
                    clauses.append(f"{id_col} = ?")
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
        sort: dict[str, int] | None,
        collation: dict[str, Any] | None = None,
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
            if strength <= 2 or not case_level:
                collate_clause = " COLLATE NOCASE"

        for field, direction in sort.items():
            if field == "_id":
                order_field = f"{quote_table_name(self.collection.name)}._id"
            else:
                json_path = f"'{parse_json_path(field)}'"
                order_field = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"

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
        operators: dict[str, Any],
        is_datetime_indexed: bool = False,
        regex_options: str = "",
    ) -> tuple[str | None, list[Any]]:
        """
        Builds a SQL clause for query operators.

        This method constructs a SQL clause based on the provided operators for
        a specific JSON path. It handles various operators like $eq, $gt, $lt, etc.,
        and returns a tuple containing the SQL clause and a list of parameters.
        If an unsupported operator is encountered, it returns None, indicating
        that a fallback to Python processing is needed.

        Args:
            json_path (str): The JSON path to extract the value from.
            operators (dict[str, Any]): A dictionary of operators and their values.
            is_datetime_indexed (bool): Whether the field has a datetime index that requires timezone normalization.

        Returns:
            tuple[str | None, list[Any]]: A tuple containing the SQL clause and
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
                    # Array values need Python for correct semantics
                    if isinstance(op_val, (list, tuple)):
                        return None, []
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) = datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) = ?"
                        )
                        params.append(op_val)
                case "$gt":
                    # Array values need Python for correct semantics
                    if isinstance(op_val, (list, tuple)):
                        return None, []
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) > datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) > ?"
                        )
                        params.append(op_val)
                case "$lt":
                    # Array values need Python for correct semantics
                    if isinstance(op_val, (list, tuple)):
                        return None, []
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) < datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) < ?"
                        )
                        params.append(op_val)
                case "$gte":
                    if isinstance(op_val, (list, tuple)):
                        return None, []
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) >= datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) >= ?"
                        )
                        params.append(op_val)
                case "$lte":
                    if isinstance(op_val, (list, tuple)):
                        return None, []
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) <= datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) <= ?"
                        )
                        params.append(op_val)
                case "$ne":
                    # Array values need Python for correct semantics
                    if isinstance(op_val, (list, tuple)):
                        return None, []
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) != datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) != ?"
                        )
                        params.append(op_val)
                case "$in":
                    json_each_func = self.jsonb.json_each_function
                    if isinstance(op_val, (list, tuple)):
                        placeholders = ", ".join("?" for _ in op_val)
                        if json_path == "value":
                            clauses.append(f"value IN ({placeholders})")
                        else:
                            clauses.append(
                                f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each WHERE json_each.value IN ({placeholders}))"
                            )
                        params.extend(op_val)
                    else:
                        return None, []
                case "$nin":
                    json_each_func = self.jsonb.json_each_function
                    if isinstance(op_val, (list, tuple)):
                        placeholders = ", ".join("?" for _ in op_val)
                        if json_path == "value":
                            clauses.append(f"value NOT IN ({placeholders})")
                        else:
                            clauses.append(
                                f"NOT EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each WHERE json_each.value IN ({placeholders}))"
                            )
                        params.extend(op_val)
                    else:
                        return None, []
                case "$all":
                    json_each_func = getattr(
                        self, "_json_each_function", "json_each"
                    )
                    if isinstance(op_val, (list, tuple)):
                        if len(op_val) == 0:
                            return None, []
                        for v in op_val:
                            clauses.append(
                                f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each WHERE json_each.value = ?)"
                            )
                            params.append(v)
                    else:
                        return None, []
                case "$exists":
                    # Handle boolean value for $exists
                    if isinstance(op_val, bool):
                        if op_val:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NOT NULL"
                            )
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NULL"
                            )
                    else:
                        # Invalid value for $exists, fallback to Python
                        return None, []
                case "$mod":
                    # Handle [divisor, remainder] array
                    if isinstance(op_val, (list, tuple)) and len(op_val) == 2:
                        divisor, remainder = op_val
                        clauses.append(
                            f"json_type(data, {json_path}) IN ('integer', 'real') AND "
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) % ? = ?"
                        )
                        params.extend([divisor, remainder])
                    else:
                        # Invalid format for $mod, fallback to Python
                        return None, []
                case "$size":
                    # Handle array size comparison
                    if isinstance(op_val, int):
                        clauses.append(
                            f"json_array_length({self.jsonb.json_function_prefix}_extract(data, {json_path})) = ?"
                        )
                        params.append(op_val)
                    else:
                        # Invalid value for $size, fallback to Python
                        return None, []
                case "$contains":
                    # Handle case-insensitive substring search
                    if isinstance(op_val, str):
                        clauses.append(
                            f"lower({self.jsonb.json_function_prefix}_extract(data, {json_path})) LIKE ?"
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
                            # The _build_operator_clause uses f"{self.jsonb.json_function_prefix}_extract(data, value)"
                            c = c.replace(
                                f"{self.jsonb.json_function_prefix}_extract(data, value)",
                                "value",
                            )
                            # Also fix json_each(data, value) -> json_each(data, "value")
                            # This handles $in and $nin inside $elemMatch
                            c = c.replace(
                                "json_each(data, value)",
                                'json_each(data, "value")',
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
                                        f"{self.jsonb.json_function_prefix}_extract(data, "
                                        f"'{parse_json_path(sub_field)}') = ?"
                                    )
                                    p = [sub_val]

                                if c is None:
                                    return None, []
                                # Retarget from 'data' to 'value' (the row from json_each)
                                c = c.replace(
                                    f"{self.jsonb.json_function_prefix}_extract(data,",
                                    f"{self.jsonb.json_function_prefix}_extract(value,",
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
                        f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each WHERE {where_inner})"
                    )
                    params.extend(inner_params)
                case "$regex":
                    # Handle regex with optional options
                    if not isinstance(op_val, str):
                        return None, []

                    # Build pattern with inline regex flags for SQLite REGEXP
                    # Convert MongoDB options to Python inline regex flags
                    if regex_options:
                        flag_str = ""
                        if "i" in regex_options.lower():
                            flag_str += "i"
                        if "m" in regex_options.lower():
                            flag_str += "m"
                        if "s" in regex_options.lower():
                            flag_str += "s"
                        if "x" in regex_options.lower():
                            flag_str += "x"
                        pattern = f"(?{flag_str}){op_val}"
                    else:
                        pattern = op_val

                    clauses.append(
                        f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) REGEXP ?"
                    )
                    params.append(pattern)
                case _:
                    # Unsupported operator, fallback to Python
                    return None, []

        if not clauses:
            return None, []

        # Combine all clauses with AND
        combined_clause = " AND ".join(clauses)
        return combined_clause, params
