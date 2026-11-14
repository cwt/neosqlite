"""SQL WHERE clause and operator builder for NeoSQLite.

Extracted from query_builder.py — provides _build_simple_where_clause
and related SQL generation for MongoDB query operators.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from ..jsonb_support import json_data_column
from ..type_correction import normalize_id_query_for_db
from .utils import sanitize_fts_query

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


def _normalize_id_value(v: Any) -> Any:
    """Normalize an ID value for database queries."""
    from ...objectid import ObjectId

    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, str):
        try:
            return str(ObjectId(v))
        except ValueError:
            return v
    return v


class SqlQueryBuilderMixin:
    """Mixin providing SQL WHERE clause building methods.

    Designed to be composed into QueryBuilderMixin.
    """

    collection: Collection
    jsonb: JSONBContext
    _build_expr_where_clause: Any
    _combine_expr_with_other_fields: Any

    def _is_text_search_query(self, query: dict[str, Any]) -> bool:
        """
        Check if the query is a text search query (contains $text operator).

        Args:
            query: The query to check.

        Returns:
            True if the query is a text search query, False otherwise.
        """
        return "$text" in query

    def _build_fts_subquery(
        self, query: dict[str, Any]
    ) -> tuple[str, list[Any]] | None:
        """
        Build FTS subquery that calculates BM25 text relevance score across all
        search indexes of this collection.
        """
        if "$text" not in query:
            return None

        from .utils import get_force_fallback

        if get_force_fallback():
            return None

        text_query = query["$text"]
        if not isinstance(text_query, dict) or "$search" not in text_query:
            return None

        search_term = text_query["$search"]
        if not isinstance(search_term, str):
            return None

        sanitized_term = sanitize_fts_query(search_term)
        if not sanitized_term:
            return None

        indexed_fields = self.collection.indexes.list_search_indexes()
        if not indexed_fields:
            return None

        subqueries = []
        params = []

        for field in indexed_fields:
            index_name = field.replace(".", "_")
            fts_table_name = quote_table_name(
                f"{self.collection.name}_{index_name}_fts"
            )
            subqueries.append(
                f"SELECT rowid AS rid, -bm25({fts_table_name}) AS s FROM {fts_table_name} WHERE {fts_table_name} MATCH ?"
            )
            params.append(sanitized_term.lower())

        if not subqueries:
            return None

        if len(subqueries) == 1:
            field0 = indexed_fields[0].replace(".", "_")
            table0 = quote_table_name(f"{self.collection.name}_{field0}_fts")
            fts_subquery = f"SELECT rowid AS fts_rid, -bm25({table0}) AS text_score FROM {table0} WHERE {table0} MATCH ?"
            return fts_subquery, params

        union_all = " UNION ALL ".join(subqueries)
        fts_subquery = f"SELECT f.rid AS fts_rid, SUM(f.s) AS text_score FROM ({union_all}) f GROUP BY f.rid"
        return fts_subquery, params

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

        from .utils import get_force_fallback

        if get_force_fallback():
            return None

        text_query = query["$text"]
        if not isinstance(text_query, dict) or "$search" not in text_query:
            return None

        search_term = text_query["$search"]
        if not isinstance(search_term, str):
            return None

        sanitized_term = sanitize_fts_query(search_term)
        if not sanitized_term:
            return "WHERE 0", [], []

        fts_res = self._build_fts_subquery(query)
        if fts_res is None:
            return None

        fts_subquery, params = fts_res
        where_clause = f"WHERE id IN (SELECT fts_rid FROM ({fts_subquery}))"
        return where_clause, params, []

    def _build_fts_sort_clause(
        self,
        sort: dict[str, Any] | None,
        collation: dict[str, Any] | None = None,
    ) -> str:
        """
        Build ORDER BY clause for FTS queries, prioritizing textScore if requested.
        """
        if not sort:
            return ""

        clauses = []
        collate_clause = ""
        if collation:
            strength = collation.get("strength", 3)
            case_level = collation.get("caseLevel", False)
            if strength <= 2 or not case_level:
                collate_clause = " COLLATE NOCASE"

        for field, direction in sort.items():
            is_text_score_sort = (
                direction == {"$meta": "textScore"}
                or (
                    isinstance(direction, dict)
                    and direction.get("$meta") == "textScore"
                )
                or (
                    field == "score" and direction in (-1, "desc", "DESCENDING")
                )
            )
            if is_text_score_sort:
                clauses.append("fts.text_score DESC")
            elif field == "_id":
                order_dir = "ASC" if direction == 1 else "DESC"
                clauses.append(f"c._id {order_dir}")
            else:
                json_path = f"'{parse_json_path(field)}'"
                order_field = f"{self.jsonb.json_function_prefix}_extract(c.data, {json_path})"
                order_dir = "ASC" if direction == 1 else "DESC"
                clauses.append(f"{order_field}{collate_clause} {order_dir}")

        if clauses:
            return " ORDER BY " + ", ".join(clauses)
        return ""

    def _build_full_fts_query(
        self,
        query: dict[str, Any],
        sort: dict[str, Any] | None,
        collation: dict[str, Any] | None,
        limit: int | None = None,
        skip: int | None = None,
    ) -> tuple[str, list[Any]] | None:
        """
        Build a complete FTS query with BM25 score, JOIN, sorting, and pagination.
        """
        if "$text" not in query:
            return None

        from .utils import get_force_fallback

        if get_force_fallback():
            return None

        text_query = query["$text"]
        if not isinstance(text_query, dict) or "$search" not in text_query:
            return None

        search_term = text_query["$search"]
        if not isinstance(search_term, str):
            return None

        sanitized_term = sanitize_fts_query(search_term)
        jsonb = self.collection.query_engine.jsonb.jsonb_supported
        json_col = json_data_column(jsonb)
        table_name = quote_table_name(self.collection.name)

        if not sanitized_term:
            return (
                f"SELECT id, _id, {json_col} as data, 0.0 as text_score FROM {table_name} WHERE 0",
                [],
            )

        fts_res = self._build_fts_subquery(query)
        if fts_res is None:
            return None

        fts_subquery, fts_params = fts_res

        # Compile rest of query (conditions other than $text)
        rest_query = {k: v for k, v in query.items() if k != "$text"}
        rest_where = ""
        rest_params: list[Any] = []
        if rest_query:
            rest_result = self._build_simple_where_clause(rest_query)
            if rest_result is None:
                return None
            rw, rp, _ = rest_result
            if rw:
                cond = rw[len("WHERE ") :].strip()
                rest_where = f"WHERE ({cond}) "
                rest_params = rp

        sort_clause = self._build_fts_sort_clause(sort, collation)
        pagination_clause = self._build_pagination_clause(limit, skip or 0)

        cmd = (
            f"WITH fts AS ({fts_subquery}) "
            f"SELECT c.id, c._id, {json_col} as data, fts.text_score "
            f"FROM {table_name} c "
            f"JOIN fts ON fts.fts_rid = c.id "
            f"{rest_where}{sort_clause}{pagination_clause}"
        ).strip()

        all_params = fts_params + rest_params
        return cmd, all_params

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

    def _regex_to_sql_pattern(
        self, value: Any, options: str = ""
    ) -> str | None:
        """Convert a regex string (+ $options) or compiled re.Pattern to a validated SQLite REGEXP pattern."""
        from ..type_utils import regex_to_sql_pattern

        return regex_to_sql_pattern(value, options)

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

        clauses: list[str] = []
        params: list[Any] = []

        for op, op_val in value.items():
            if op == "$in" or op == "$nin":
                if not isinstance(op_val, list) or not op_val:
                    return None
                has_null = any(v is None for v in op_val)
                non_null = [v for v in op_val if v is not None]
                if op == "$in":
                    if has_null:
                        if non_null:
                            ph = ", ".join("?" for _ in non_null)
                            clauses.append(
                                f"({id_col} IN ({ph}) OR {id_col} IS NULL)"
                            )
                            params.extend(
                                _normalize_id_value(v) for v in non_null
                            )
                        else:
                            clauses.append(f"{id_col} IS NULL")
                    else:
                        ph = ", ".join("?" for _ in op_val)
                        clauses.append(f"{id_col} IN ({ph})")
                        params.extend(_normalize_id_value(v) for v in op_val)
                else:
                    if has_null:
                        if non_null:
                            ph = ", ".join("?" for _ in non_null)
                            clauses.append(
                                f"({id_col} NOT IN ({ph}) AND {id_col} IS NOT NULL)"
                            )
                            params.extend(
                                _normalize_id_value(v) for v in non_null
                            )
                        else:
                            clauses.append(f"{id_col} IS NOT NULL")
                    else:
                        ph = ", ".join("?" for _ in op_val)
                        clauses.append(f"{id_col} NOT IN ({ph})")
                        params.extend(_normalize_id_value(v) for v in op_val)

            elif op == "$ne":
                if isinstance(op_val, (list, tuple, re.Pattern, dict)):
                    return None
                if op_val is None:
                    clauses.append(f"{id_col} IS NOT NULL")
                else:
                    clauses.append(f"{id_col} != ?")
                    params.append(_normalize_id_value(op_val))
            elif op == "$eq":
                if isinstance(op_val, (list, tuple, re.Pattern, dict)):
                    return None
                if op_val is None:
                    clauses.append(f"{id_col} IS NULL")
                else:
                    clauses.append(f"{id_col} = ?")
                    params.append(_normalize_id_value(op_val))
            elif op == "$options":
                if "$regex" not in value or not isinstance(op_val, str):
                    return None
                continue
            elif op == "$regex":
                opts = value.get("$options", "")
                if opts and not isinstance(opts, str):
                    return None
                pattern = self._regex_to_sql_pattern(
                    op_val, opts if isinstance(op_val, str) else ""
                )
                if pattern is None:
                    return None
                clauses.append(f"{id_col} REGEXP ?")
                params.append(pattern)
            elif op in ("$gt", "$gte", "$lt", "$lte"):
                # Fall back to Python for range comparisons on _id.
                # The _id column may contain mixed types (int, str, ObjectId).
                # SQLite JSONB ordering (numbers < strings) differs from MongoDB
                # BSON ordering, which only compares within the same type.
                # Python evaluation handles these correctly.
                return None
            else:
                return None  # Fall back to Python for unsupported operators

        if clauses:
            return " AND ".join(clauses), params
        return None  # Fall back to Python if no clauses generated

    # _categorize_id_value removed: _id queries now use _id_column_ref() for
    # strict MongoDB-like semantics (integer _id targets the _id column).

    def _build_containment_clause(
        self, field: str, extract_expr: str, value: Any
    ) -> tuple[str, list[Any]]:
        """Array-containment for plain equality (#99).

        The array may sit at any depth along the dotted path ("a.scores"
        is a whole array; "students.name" names a leaf inside each element
        of $.students), so one EXISTS arm is emitted per candidate depth.
        """
        from ..type_utils import serialize_sql_query_value

        json_each_func = self.jsonb.json_each_function
        scalar_val, scalar_ph = serialize_sql_query_value(
            value, self.jsonb.jsonb_supported
        )
        each_val, each_ph = serialize_sql_query_value(
            value, json_each_func == "jsonb_each"
        )
        text_val, _ = serialize_sql_query_value(value, False)
        segs = field.split(".")
        arms: list[str] = []
        params: list[Any] = [scalar_val]
        for k in range(1, len(segs) + 1):
            arr_path = "'$." + ".".join(segs[:k]) + "'"
            arr_expr = "json_extract(data, " + arr_path + ")"
            rest = segs[k:]
            if rest:
                leaf_path = "$." + ".".join(rest)
                elem_pred = "json_extract(value, '" + leaf_path + "') = ?"
                arm_param = text_val
            else:
                elem_pred = f"value = {each_ph}"
                arm_param = each_val
            arms.append(
                "EXISTS (SELECT 1 FROM "
                + json_each_func
                + "("
                + "CASE WHEN json_type(data, "
                + arr_path
                + ") = 'array' "
                + "THEN "
                + arr_expr
                + " END"
                + ") WHERE "
                + elem_pred
                + ")"
            )
            params.append(arm_param)
        clause = " OR ".join([f"{extract_expr} = {scalar_ph}"] + arms)
        return "(" + clause + ")", params

    def _field_has_index(self, field: str) -> bool:
        """True when an index exists whose name encodes this field (#99).

        Indexed scalar equalities keep a sargable predicate; adding the
        array-containment OR would otherwise force a full scan.
        """
        try:
            suffix = f"_{field.replace('.', '_')}"
            for name in self.collection.list_indexes():
                if isinstance(name, str) and name.endswith(suffix):
                    return True
        except Exception:
            pass
        return False

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
            elif isinstance(value, re.Pattern):
                pattern = self._regex_to_sql_pattern(value)
                if pattern is None:
                    return None
                return f"{id_col} REGEXP ?", [pattern]
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
                if "$options" in value and (
                    "$regex" not in value or not isinstance(options, str)
                ):
                    return None
                clause, params = self._build_operator_clause(
                    json_path, value, regex_options=options
                )
                if clause is None:
                    return None
                return f"{clause}", params
            else:
                # Simple equality
                if isinstance(value, re.Pattern):
                    pattern = self._regex_to_sql_pattern(value)
                    if pattern is None:
                        return None
                    return (
                        f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) REGEXP ?",
                        [pattern],
                    )

                extract_expr = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"
                if value is None:
                    # MongoDB: {f: null} matches docs where f is JSON null
                    # *or absent*; json_extract yields SQL NULL for both (#90)
                    return f"{extract_expr} IS NULL", []
                if isinstance(value, (dict, list)):
                    return f"{extract_expr} = ?", [value]
                # MongoDB: {arr: v} also matches when the array CONTAINS
                # v at any depth (#99). Skipped for indexed fields so the
                # OR-form cannot disable the expression index.
                if not self._field_has_index(field):
                    return self._build_containment_clause(
                        field, extract_expr, value
                    )
                from ..type_utils import serialize_sql_query_value

                param_val, ph = serialize_sql_query_value(
                    value, self.jsonb.jsonb_supported
                )
                return f"{extract_expr} = {ph}", [param_val]

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

        # Handle text search queries. When $text is combined with other
        # conditions, the FTS match is AND-ed with the remaining compiled
        # conditions instead of replacing them (#91).
        if self._is_text_search_query(query):
            text_result = self._build_text_search_query(query)
            if text_result is None or len(query) == 1:
                return text_result

            fts_where, fts_params, _ = text_result
            rest_query = {k: v for k, v in query.items() if k != "$text"}
            rest_result = None
            if rest_query:
                rest_result = self._build_simple_where_clause(rest_query)
            if rest_result is None:
                # Remaining conditions need the Python tier; a combined SQL
                # clause would silently drop them, so fall back entirely.
                return None
            rest_where, rest_params, _ = rest_result
            if not rest_where:
                return text_result
            rest_conditions = rest_where[len("WHERE ") :]
            return (
                f"{fts_where.strip()} AND ({rest_conditions})",
                fts_params + rest_params,
                [],
            )

        # Handle $expr queries
        if "$expr" in query:
            return self._build_expr_where_clause(query)

        condition_result = self._build_query_condition(query)
        if condition_result is None:
            return None
        condition_sql, params = condition_result
        if condition_sql == "1 = 1":
            return "", params, []
        if condition_sql:
            return f"WHERE {condition_sql}", params, []
        return "", params, []

    def _build_logical_field_clause(
        self, operator: str, value: Any
    ) -> tuple[str, list[Any]] | None:
        """Build SQL condition for top-level logical operators ($and, $or, $nor, $not)."""
        if operator == "$not":
            if not isinstance(value, dict):
                return None
            inner_result = self._build_query_condition(value)
            if inner_result is None:
                return None
            inner_sql, inner_params = inner_result
            if not inner_sql:
                inner_sql = "1 = 1"
            return f"NOT (COALESCE(({inner_sql}), 0))", inner_params

        if not isinstance(value, list) or not value:
            return None

        branch_clauses: list[str] = []
        all_params: list[Any] = []
        for subquery in value:
            if not isinstance(subquery, dict):
                return None
            sub_result = self._build_query_condition(subquery)
            if sub_result is None:
                return None
            sub_sql, sub_params = sub_result
            if not sub_sql:
                sub_sql = "1 = 1"
            branch_clauses.append(sub_sql)
            all_params.extend(sub_params)

        if operator == "$and":
            if len(branch_clauses) == 1:
                return branch_clauses[0], all_params
            return (
                " AND ".join(f"({c})" for c in branch_clauses),
                all_params,
            )
        if operator == "$or":
            return (
                " OR ".join(f"COALESCE(({c}), 0)" for c in branch_clauses),
                all_params,
            )
        if operator == "$nor":
            disjunction = " OR ".join(
                f"COALESCE(({c}), 0)" for c in branch_clauses
            )
            return f"NOT ({disjunction})", all_params
        return None

    def _build_query_condition(
        self, query: dict[str, Any]
    ) -> tuple[str, list[Any]] | None:
        """Build a composable SQL boolean condition (without leading WHERE) for a query dict."""
        if not isinstance(query, dict):
            return None

        if "$text" in query:
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

        if "$expr" in query:
            expr = query["$expr"]
            if not isinstance(expr, dict):
                return None
            from ..expr_evaluator import ExprEvaluator

            tier1_evaluator = ExprEvaluator(
                (
                    self.collection.query_engine._data_column
                    if hasattr(self.collection.query_engine, "_data_column")
                    else "data"
                ),
                self.collection.db,
            )
            sql_expr, expr_params = tier1_evaluator.evaluate(
                expr, tier=1, force_python=False
            )
            if sql_expr is None:
                return None
            combined = self._combine_expr_with_other_fields(
                sql_expr, expr_params, query, expr
            )
            if combined is None:
                return None
            where_sql, combined_params, _ = combined
            if where_sql.startswith("WHERE "):
                return where_sql[len("WHERE ") :], combined_params
            return where_sql or "1 = 1", combined_params

        if not query:
            return "1 = 1", []

        clauses: list[str] = []
        params: list[Any] = []

        for field, value in query.items():
            if field == "$jsonSchema":
                from .schema_compiler import compile_query_schema_to_sql

                schema_res = compile_query_schema_to_sql(
                    value,
                    data_column="data",
                    jsonb=self.jsonb.jsonb_supported,
                )
                if schema_res is None:
                    return None
                schema_clause, schema_params = schema_res
                clauses.append(schema_clause)
                params.extend(schema_params)
            elif field in ("$and", "$or", "$nor", "$not"):
                logical_result = self._build_logical_field_clause(field, value)
                if logical_result is None:
                    return None
                logical_clause, logical_params = logical_result
                if logical_clause:
                    clauses.append(logical_clause)
                    params.extend(logical_params)
            else:
                field_result = self._build_field_clause(field, value)
                if field_result is None:
                    return None  # Fall back to Python processing
                field_clause, field_params = field_result
                if field_clause:  # Only add non-empty clauses
                    clauses.append(field_clause)
                    params.extend(field_params)

        if len(clauses) == 1:
            return clauses[0], params
        if clauses:
            return " AND ".join(f"({c})" for c in clauses), params
        return "1 = 1", params

    def _build_sort_clause(
        self,
        sort: dict[str, int] | None,
        collation: dict[str, Any] | None = None,
        table_name: str | None = None,
    ) -> str:
        """
        Builds a SQL ORDER BY clause from a sort dictionary.

        Args:
            sort: A dictionary mapping fields to sort directions (1 for ASC, -1 for DESC).
            collation: Optional collation settings for case-insensitive sorting.
            table_name: Optional table qualifier when sorting a JOIN query.

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

        data_col = (
            f"{quote_table_name(table_name)}.data" if table_name else "data"
        )
        for field, direction in sort.items():
            if field == "_id":
                order_field = f"{quote_table_name(self.collection.name)}._id"
            else:
                json_path = f"'{parse_json_path(field)}'"
                order_field = f"{self.jsonb.json_function_prefix}_extract({data_col}, {json_path})"

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
        if limit == 0:
            limit = None

        if limit is None and skip == 0:
            return ""

        clause = ""
        if limit is not None:
            actual_limit = abs(limit)
            clause = f" LIMIT {actual_limit}"
            if skip > 0:
                clause += f" OFFSET {skip}"
        elif skip > 0:
            # SQLite requires a LIMIT when using OFFSET
            # Use -1 for unlimited if supported, or a very large number
            clause = f" LIMIT -1 OFFSET {skip}"

        return clause

    def _json_type_guard_sql(self, json_path: str, value: Any) -> str:
        """Return a SQL fragment restricting json_type to the value's BSON type."""
        from ..type_utils import json_type_guard_sql

        return json_type_guard_sql(json_path, value)

    def _json_type_list_sql(self, value: Any) -> str:
        """Return a SQL list of json_type/json_each.type values for the value's BSON type."""
        from ..type_utils import json_type_list_sql

        return json_type_list_sql(value)

    def _build_operator_clause(
        self,
        json_path: str,
        operators: dict[str, Any],
        is_datetime_indexed: bool = False,
        regex_options: str = "",
        allow_array_existential: bool = True,
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
        from ..type_utils import (
            build_json_in_clause,
            build_json_nin_clause,
            is_encoded_json_value,
            json_type_list_sql,
            serialize_sql_query_value,
        )

        clauses = []
        params = []

        for op, op_val in operators.items():
            match op:
                case "$eq":
                    # Array values and regex patterns need Python for correct semantics
                    if isinstance(op_val, (list, tuple, re.Pattern)):
                        return None, []
                    if op_val is None:
                        # MongoDB: {$eq: null} matches null-or-missing (#90)
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NULL"
                        )
                    elif is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) = datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        json_each_func = self.jsonb.json_each_function
                        param_val, scalar_ph = serialize_sql_query_value(
                            op_val, self.jsonb.jsonb_supported
                        )
                        each_val, each_ph = serialize_sql_query_value(
                            op_val, json_each_func == "jsonb_each"
                        )
                        if allow_array_existential and json_path.startswith(
                            "'"
                        ):
                            guard = self._json_type_guard_sql(json_path, op_val)
                            clauses.append(
                                f"({guard} AND "
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) = {scalar_ph}) OR "
                                f"(json_type(data, {json_path}) = 'array' AND EXISTS "
                                f"(SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each "
                                f"WHERE json_each.value = {each_ph}))"
                            )
                            params.extend([param_val, each_val])
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) = {scalar_ph}"
                            )
                            params.append(param_val)
                case "$gt":
                    # Array and ObjectId/Binary values need Python for correct semantics
                    if isinstance(
                        op_val, (list, tuple)
                    ) or is_encoded_json_value(op_val):
                        return None, []
                    if op_val is None:
                        # MongoDB: nothing is greater than null.
                        clauses.append("0")
                        continue
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) > datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        json_each_func = self.jsonb.json_each_function
                        if allow_array_existential and json_path.startswith(
                            "'"
                        ):
                            clauses.append(
                                f"((json_type(data, {json_path}) != 'array' AND "
                                f"{self._json_type_guard_sql(json_path, op_val)} AND "
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) > ?) OR "
                                f"(json_type(data, {json_path}) = 'array' AND EXISTS "
                                f"(SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each "
                                f"WHERE json_each.value > ?)))"
                            )
                            params.extend([op_val, op_val])
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) > ?"
                            )
                            params.append(op_val)
                case "$lt":
                    # Array and ObjectId/Binary values need Python for correct semantics
                    if isinstance(
                        op_val, (list, tuple)
                    ) or is_encoded_json_value(op_val):
                        return None, []
                    if op_val is None:
                        # MongoDB: nothing is less than null.
                        clauses.append("0")
                        continue
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) < datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        json_each_func = self.jsonb.json_each_function
                        if allow_array_existential and json_path.startswith(
                            "'"
                        ):
                            clauses.append(
                                f"((json_type(data, {json_path}) != 'array' AND "
                                f"{self._json_type_guard_sql(json_path, op_val)} AND "
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) < ?) OR "
                                f"(json_type(data, {json_path}) = 'array' AND EXISTS "
                                f"(SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each "
                                f"WHERE json_each.value < ?)))"
                            )
                            params.extend([op_val, op_val])
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) < ?"
                            )
                            params.append(op_val)
                case "$gte":
                    if isinstance(
                        op_val, (list, tuple)
                    ) or is_encoded_json_value(op_val):
                        return None, []
                    if op_val is None:
                        # MongoDB: only null/missing values are >= null.
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NULL"
                        )
                        continue
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) >= datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        json_each_func = self.jsonb.json_each_function
                        if allow_array_existential and json_path.startswith(
                            "'"
                        ):
                            clauses.append(
                                f"((json_type(data, {json_path}) != 'array' AND "
                                f"{self._json_type_guard_sql(json_path, op_val)} AND "
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) >= ?) OR "
                                f"(json_type(data, {json_path}) = 'array' AND EXISTS "
                                f"(SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each "
                                f"WHERE json_each.value >= ?)))"
                            )
                            params.extend([op_val, op_val])
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) >= ?"
                            )
                            params.append(op_val)
                case "$lte":
                    if isinstance(
                        op_val, (list, tuple)
                    ) or is_encoded_json_value(op_val):
                        return None, []
                    if op_val is None:
                        # MongoDB: only null/missing values are <= null.
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NULL"
                        )
                        continue
                    if is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) <= datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        json_each_func = self.jsonb.json_each_function
                        if allow_array_existential and json_path.startswith(
                            "'"
                        ):
                            clauses.append(
                                f"((json_type(data, {json_path}) != 'array' AND "
                                f"{self._json_type_guard_sql(json_path, op_val)} AND "
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) <= ?) OR "
                                f"(json_type(data, {json_path}) = 'array' AND EXISTS "
                                f"(SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each "
                                f"WHERE json_each.value <= ?)))"
                            )
                            params.extend([op_val, op_val])
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) <= ?"
                            )
                            params.append(op_val)
                case "$ne":
                    # Array values and regex patterns need Python for correct semantics
                    if isinstance(op_val, (list, tuple, re.Pattern)):
                        return None, []
                    if op_val is None:
                        # MongoDB: {$ne: null} matches only documents where the field
                        # exists and is not null (missing and null fields excluded).
                        clauses.append(
                            f"(json_type(data, {json_path}) IS NOT NULL AND "
                            f"json_type(data, {json_path}) IS NOT 'null')"
                        )
                    elif is_datetime_indexed:
                        clauses.append(
                            f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NOT datetime(?)"
                        )
                        params.append(op_val)
                    else:
                        types = self._json_type_list_sql(op_val)
                        json_each_func = self.jsonb.json_each_function
                        param_val, scalar_ph = serialize_sql_query_value(
                            op_val, self.jsonb.jsonb_supported
                        )
                        each_val, each_ph = serialize_sql_query_value(
                            op_val, json_each_func == "jsonb_each"
                        )
                        if json_path.startswith("'"):
                            clauses.append(
                                f"({self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NULL OR "
                                f"(json_type(data, {json_path}) != 'array' AND json_type(data, {json_path}) NOT IN {types}) OR "
                                f"(json_type(data, {json_path}) IN {types} AND "
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NOT {scalar_ph}) OR "
                                f"(json_type(data, {json_path}) = 'array' AND NOT EXISTS ("
                                f"SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each "
                                f"WHERE json_each.type IN {types} AND json_each.value = {each_ph})))"
                            )
                            params.extend([param_val, each_val])
                        else:
                            clauses.append(
                                f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) IS NOT {scalar_ph}"
                            )
                            params.append(param_val)
                case "$in":
                    if isinstance(op_val, (list, tuple)):
                        in_clause, in_params = build_json_in_clause(
                            "data",
                            json_path,
                            op_val,
                            self.jsonb.json_function_prefix,
                            self.jsonb.jsonb_supported,
                            self.jsonb.json_each_function,
                        )
                        clauses.append(in_clause)
                        params.extend(in_params)
                    else:
                        return None, []
                case "$nin":
                    if isinstance(op_val, (list, tuple)):
                        nin_clause, nin_params = build_json_nin_clause(
                            "data",
                            json_path,
                            op_val,
                            self.jsonb.json_function_prefix,
                            self.jsonb.jsonb_supported,
                            self.jsonb.json_each_function,
                        )
                        clauses.append(nin_clause)
                        params.extend(nin_params)
                    else:
                        return None, []
                case "$all":
                    json_each_func = self.jsonb.json_each_function
                    if isinstance(op_val, (list, tuple)):
                        if not op_val:
                            return None, []
                        clauses.append(
                            f"json_type(data, {json_path}) = 'array'"
                        )
                        for v in op_val:
                            if v is None:
                                clauses.append(
                                    f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each WHERE json_each.value IS NULL)"
                                )
                            else:
                                p_val, ph = serialize_sql_query_value(
                                    v, json_each_func == "jsonb_each"
                                )
                                elem_types = json_type_list_sql(v)
                                clauses.append(
                                    f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path}) AS json_each WHERE json_each.value = {ph} AND json_each.type IN {elem_types})"
                                )
                                params.append(p_val)
                    else:
                        return None, []
                case "$exists":
                    # Handle boolean value for $exists
                    if isinstance(op_val, bool):
                        if op_val:
                            # Field present, including explicit null values.
                            clauses.append(
                                f"json_type(data, {json_path}) IS NOT NULL"
                            )
                        else:
                            # Field missing only (not present-but-null).
                            clauses.append(
                                f"json_type(data, {json_path}) IS NULL"
                            )
                    else:
                        # Invalid value for $exists, fallback to Python
                        return None, []
                case "$mod":
                    # Handle [divisor, remainder] array
                    if isinstance(op_val, (list, tuple)) and len(op_val) == 2:
                        divisor, remainder = op_val
                        if divisor == 0:
                            # Zero divisor: let the Python tier raise
                            # MalformedQueryException (#160)
                            return None, []
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
                    if isinstance(op_val, int) and not isinstance(op_val, bool):
                        clauses.append(
                            f"json_type(data, {json_path}) = 'array' AND "
                            f"json_array_length({self.jsonb.json_function_prefix}_extract(data, {json_path})) = ?"
                        )
                        params.append(op_val)
                    else:
                        # Invalid value for $size, fallback to Python
                        return None, []
                case "$not":
                    # MongoDB field-level $not: {<field>: {"$not": <op_dict | re.Pattern>}}
                    # Matches documents where the negated predicate does not match
                    # (including null/missing fields).
                    if isinstance(op_val, re.Pattern):
                        pattern = self._regex_to_sql_pattern(op_val)
                        if pattern is None:
                            return None, []
                        clauses.append(
                            f"NOT COALESCE(({self.jsonb.json_function_prefix}_extract(data, {json_path}) REGEXP ?), 0)"
                        )
                        params.append(pattern)
                    elif (
                        isinstance(op_val, dict)
                        and op_val
                        and "$not" not in op_val
                    ):
                        inner_options = op_val.get("$options", "")
                        if "$options" in op_val and (
                            "$regex" not in op_val
                            or not isinstance(inner_options, str)
                        ):
                            return None, []
                        inner_c, inner_p = self._build_operator_clause(
                            json_path,
                            op_val,
                            is_datetime_indexed=is_datetime_indexed,
                            regex_options=inner_options,
                            allow_array_existential=allow_array_existential,
                        )
                        if inner_c is None:
                            return None, []
                        clauses.append(f"NOT COALESCE(({inner_c}), 0)")
                        params.extend(inner_p)
                    else:
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
                    json_each_func = self.jsonb.json_each_function

                    # Build the inner WHERE clause for the subquery
                    inner_clauses = []
                    inner_params = []

                    if isinstance(op_val, dict) and not is_encoded_json_value(
                        op_val
                    ):
                        has_operators = any(
                            k.startswith("$") for k in op_val.keys()
                        )
                        if has_operators:
                            # Case: {"field": {"$elemMatch": {"$gt": 10}}}
                            # Use "value" as path which will be retargeted below
                            inner_options = op_val.get("$options", "")
                            if "$options" in op_val and (
                                "$regex" not in op_val
                                or not isinstance(inner_options, str)
                            ):
                                return None, []
                            c, p = self._build_operator_clause(
                                "value",
                                op_val,
                                regex_options=inner_options,
                                allow_array_existential=False,
                            )
                            if c is None:
                                return None, []
                            # Retarget: replace json_extract(data, value) with value
                            # The _build_operator_clause uses f"{self.jsonb.json_function_prefix}_extract(data, value)"
                            c = c.replace(
                                f"{self.jsonb.json_function_prefix}_extract(data, value)",
                                "value",
                            )
                            if "(data," in c:
                                return None, []
                            inner_clauses.append(c)
                            inner_params.extend(p)
                        else:
                            # Case: {"field": {"$elemMatch": {"k": "v"}}}
                            if not op_val:
                                return None, []
                            inner_clauses.append("json_each.type = 'object'")
                            for sub_field, sub_val in op_val.items():
                                if isinstance(
                                    sub_val, dict
                                ) and not is_encoded_json_value(sub_val):
                                    # Operator on subfield
                                    sub_options = sub_val.get("$options", "")
                                    if "$options" in sub_val and (
                                        "$regex" not in sub_val
                                        or not isinstance(sub_options, str)
                                    ):
                                        return None, []
                                    c, p = self._build_operator_clause(
                                        f"'{parse_json_path(sub_field)}'",
                                        sub_val,
                                        regex_options=sub_options,
                                        allow_array_existential=False,
                                    )
                                elif isinstance(sub_val, (list, tuple)) or (
                                    isinstance(sub_val, dict)
                                    and is_encoded_json_value(sub_val)
                                ):
                                    return None, []
                                elif isinstance(sub_val, re.Pattern):
                                    pat = self._regex_to_sql_pattern(sub_val)
                                    if pat is None:
                                        return None, []
                                    c = (
                                        f"{self.jsonb.json_function_prefix}_extract(data, "
                                        f"'{parse_json_path(sub_field)}') REGEXP ?"
                                    )
                                    p = [pat]
                                elif sub_val is None:
                                    c = (
                                        f"{self.jsonb.json_function_prefix}_extract(data, "
                                        f"'{parse_json_path(sub_field)}') IS NULL"
                                    )
                                    p = []
                                else:
                                    # Equality on subfield (supports ObjectId/Binary/scalars)
                                    param_val, ph = serialize_sql_query_value(
                                        sub_val, self.jsonb.jsonb_supported
                                    )
                                    c = (
                                        f"{self.jsonb.json_function_prefix}_extract(data, "
                                        f"'{parse_json_path(sub_field)}') = {ph}"
                                    )
                                    p = [param_val]

                                if c is None:
                                    return None, []
                                # Retarget from 'data' to 'value' (the row from json_each)
                                c = c.replace("(data,", "(value,")
                                inner_clauses.append(c)
                                inner_params.extend(p)
                    else:
                        # Case: {"field": {"$elemMatch": "value"}}
                        # For simple values in an array
                        if isinstance(op_val, (dict, list, tuple, re.Pattern)):
                            return None, []
                        if op_val is None:
                            inner_clauses.append("value IS NULL")
                        else:
                            param_val, ph = serialize_sql_query_value(
                                op_val, json_each_func == "jsonb_each"
                            )
                            inner_clauses.append(f"value = {ph}")
                            inner_params.append(param_val)

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
                case "$options":
                    if "$regex" not in operators or not isinstance(op_val, str):
                        return None, []
                    continue
                case "$regex":
                    # Handle regex with optional options or compiled re.Pattern
                    effective_options = regex_options or operators.get(
                        "$options", ""
                    )
                    if effective_options and not isinstance(
                        effective_options, str
                    ):
                        return None, []
                    pattern = self._regex_to_sql_pattern(
                        op_val,
                        effective_options if isinstance(op_val, str) else "",
                    )
                    if pattern is None:
                        return None, []

                    clauses.append(
                        f"{self.jsonb.json_function_prefix}_extract(data, {json_path}) REGEXP ?"
                    )
                    params.append(pattern)
                case "$type":
                    from ..type_utils import build_bson_type_sql

                    if json_path == "value":
                        type_sql = build_bson_type_sql(
                            "value",
                            "'$'",
                            op_val,
                            type_expr="json_each.type",
                            val_expr="value",
                        )
                    else:
                        type_sql = build_bson_type_sql(
                            "data", json_path, op_val
                        )
                    if type_sql is None:
                        return None, []
                    clauses.append(type_sql)
                case _:
                    # Unsupported operator, fallback to Python
                    return None, []

        if not clauses:
            return None, []

        # Combine all clauses with AND
        combined_clause = " AND ".join(clauses)
        return combined_clause, params
