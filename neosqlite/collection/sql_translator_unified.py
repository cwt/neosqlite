"""Unified SQL translation framework for NeoSQLite.

Provides a unified framework for translating MongoDB-style queries into SQL
clauses usable both for direct query execution and temporary table pipelines.
"""

import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

from ..query_operators import _convert_to_bitmask
from .cursor import DESCENDING
from .json_path_utils import parse_json_path
from .jsonb_support import (
    JSONBContext,
    json_function,
    should_use_json_functions,
)


def _empty_result() -> tuple[str, list[Any]]:
    """Return an empty SQL clause result tuple for fallback cases.

    Returns:
        tuple[str, list[Any]]: Tuple of an empty string and an empty list.
    """
    return "", []


def _text_search_fallback() -> tuple[None, list[Any]]:
    """Return a sentinel fallback result indicating Python execution is needed.

    Returns:
        tuple[None, list[Any]]: Tuple of ``None`` and an empty parameter list.
    """
    return None, []


class SQLFieldAccessor:
    """Generates SQL expressions for accessing document fields by context.

    Maps ``_id`` to the dedicated ``_id`` column when ``allow_id_column`` is
    enabled and translates nested JSON fields to ``json_extract`` or
    ``jsonb_extract`` calls depending on JSONB and text-search rules.
    """

    def __init__(
        self,
        data_column: str = "data",
        id_column: str = "id",
        jsonb_supported: bool = False,
        allow_id_column: bool = True,
    ):
        """Initialize the SQLFieldAccessor with column and JSONB settings.

        Args:
            data_column: Name of the column storing JSON data. Defaults to
                ``"data"``.
            id_column: Name of the column storing integer document IDs.
                Defaults to ``"id"``.
            jsonb_supported: Whether SQLite JSONB functions are supported.
                Defaults to ``False``.
            allow_id_column: Whether ``"_id"`` maps to the ``_id`` column
                instead of extracting ``$._id`` from JSON. Defaults to
                ``True``.
        """
        self.data_column = data_column
        self.id_column = id_column
        self.jsonb_supported = jsonb_supported
        self.allow_id_column = allow_id_column

    def get_field_access(
        self,
        field: str,
        context: str = "direct",
        query: dict | None = None,
    ) -> str:
        """Generate a SQL expression for accessing a document field.

        Args:
            field: MongoDB dot-notation field name to access.
            context: Execution context identifier (reserved for callers).
                Defaults to ``"direct"``.
            query: Optional full query dictionary inspected to decide whether
                ``json_*`` functions are required over ``jsonb_*``.

        Returns:
            str: SQL expression referencing ``_id`` or calling
                ``json_extract`` / ``jsonb_extract``.
        """
        if field == "_id" and self.allow_id_column:
            # Special handling for _id field - access the _id column directly
            return "_id"
        else:
            # Use enhanced JSON path parsing; force $._id when inside embedded JSON objects
            json_path = "$._id" if field == "_id" else parse_json_path(field)

            # Determine whether to use json_* or jsonb_* functions
            use_json = should_use_json_functions(query, self.jsonb_supported)
            function_name = json_function("extract", not use_json)

            return f"{function_name}({self.data_column}, '{json_path}')"


def _parse_extract_expr(
    field_access: str,
) -> tuple[str, str, str, bool] | None:
    """Parse a ``json_extract`` or ``jsonb_extract`` call expression.

    Args:
        field_access: SQL field access expression to inspect.

    Returns:
        tuple[str, str, str, bool] | None: Tuple containing
            ``(prefix, col, path_sql, is_jsonb)`` if ``field_access`` has the
            form ``json[b]_extract(col, '$.path')``, or ``None`` otherwise.
    """
    for prefix, is_jsonb in (("jsonb", True), ("json", False)):
        fn_prefix = f"{prefix}_extract("
        if field_access.startswith(fn_prefix) and field_access.endswith(")"):
            inner = field_access[len(fn_prefix) : -1]
            col, sep, path_sql = inner.partition(", ")
            if sep and path_sql.startswith("'") and path_sql.endswith("'"):
                return prefix, col, path_sql, is_jsonb
    return None


class SQLOperatorTranslator:
    """Translates MongoDB query operators into SQLite SQL expressions.

    Converts comparison, array, existence, bitwise, modulo, regex, BSON type,
    and ``$elemMatch`` operators into parameterized SQL predicates using an
    ``SQLFieldAccessor`` and ``json_each`` / ``jsonb_each`` support.
    """

    def __init__(
        self,
        field_accessor: SQLFieldAccessor | None = None,
        json_each_function: str | None = None,
        db_connection: Any = None,
    ):
        """Initialize the SQLOperatorTranslator.

        Args:
            field_accessor: ``SQLFieldAccessor`` used for field access
                expressions. Creates a default instance when ``None``.
            json_each_function: Table-valued JSON iteration function name
                (``"jsonb_each"`` or ``"json_each"``). Defaults to
                ``"json_each"``.
            db_connection: Optional SQLite connection retained for nested
                ``$expr`` evaluation.
        """
        self.field_accessor = field_accessor or SQLFieldAccessor()
        self._json_each_function = json_each_function or "json_each"
        self._db_connection = db_connection

    def translate_operator(
        self,
        field_access: str,
        operator: str,
        value: Any,
        regex_options: str = "",
    ) -> tuple[str | None, list[Any]]:
        """Translate a MongoDB query operator to a SQL predicate.

        Handles comparison (``$eq``, ``$gt``, ``$lt``, ``$gte``, ``$lte``,
        ``$ne``), array (``$in``, ``$nin``, ``$all``, ``$size``,
        ``$elemMatch``), existence (``$exists``), modulo (``$mod``), substring
        (``$contains``), bitwise (``$bitsAllClear``, ``$bitsAllSet``,
        ``$bitsAnyClear``, ``$bitsAnySet``), regex (``$regex``), and BSON type
        (``$type``) operators, including ``datetime()`` normalization for
        datetime values. Returns ``(None, [])`` when Python fallback is needed.

        Args:
            field_access: SQL expression for accessing the target field.
            operator: MongoDB operator name (e.g., ``"$eq"``, ``"$in"``).
            value: Comparison value or operator specification.
            regex_options: Regex flag string (e.g., ``"i"``) used with
                ``$regex``. Defaults to ``""``.

        Returns:
            tuple[str | None, list[Any]]: ``(sql_expr, params)`` tuple, or
                ``(None, [])`` when the operator must fall back to Python.
        """
        from ..objectid import ObjectId
        from .type_utils import (
            build_json_in_clause,
            build_json_nin_clause,
            is_encoded_json_value,
            json_type_list_sql,
            regex_to_sql_pattern,
            serialize_sql_query_value,
        )

        # default `sql` and `params`
        sql: str | None = None
        params: list[Any] = []

        # Range comparisons on ObjectId or Binary fall back to Python for strict BSON semantics
        if operator in ("$gt", "$gte", "$lt", "$lte") and is_encoded_json_value(
            value
        ):
            return None, []

        # _id ranges need BSON type guards that this translator does not
        # build. Leave them to the normal SQL query builder (or Python) so
        # mixed-type collections do not produce spurious matches.
        if field_access == "_id" and operator in (
            "$gt",
            "$gte",
            "$lt",
            "$lte",
        ):
            return None, []

        extracted = _parse_extract_expr(field_access)
        is_jsonb = extracted[3] if extracted else False
        each_is_jsonb = self._json_each_function == "jsonb_each"

        def _normalize_for_field(val: Any) -> tuple[Any, str]:
            """Serialize a comparison value and choose its SQL placeholder.

            Args:
                val: Python value to normalize for the target SQL expression.

            Returns:
                tuple[Any, str]: ``(serialized_value, placeholder)`` pair.
            """
            if field_access == "_id":
                return (str(val) if isinstance(val, ObjectId) else val), "?"
            if field_access == "json_each.value":
                return serialize_sql_query_value(val, each_is_jsonb)
            return serialize_sql_query_value(val, is_jsonb)

        # Check if this is a datetime comparison that should be wrapped with datetime() function
        def to_iso(v: Any) -> str:
            """Convert a datetime instance to an ISO-8601 string.

            Args:
                v: Datetime object or pre-formatted string value.

            Returns:
                str: ISO-8601 formatted string if ``v`` is a ``datetime``,
                    otherwise ``v`` unchanged.
            """
            if isinstance(v, datetime):
                return v.isoformat()
            return v

        # Check if this is a datetime comparison that should be wrapped with datetime() function
        # This is needed when datetime fields are indexed using datetime(json_extract(...)) for timezone normalization
        is_datetime_comparison = (
            operator in ("$eq", "$gt", "$lt", "$gte", "$lte", "$ne")
            and isinstance(value, (str, datetime))
            and self._is_datetime_value(value)
        ) or (
            operator in ("$in", "$nin")
            and isinstance(value, (list, tuple))
            and value  # Only if there are elements to check
            and all(
                isinstance(v, (str, datetime)) and self._is_datetime_value(v)
                for v in value
            )
        )

        # If it's a datetime comparison, wrap both field and value with datetime() to match indexing strategy
        if is_datetime_comparison:
            # Wrap the field access with datetime() function for consistency with datetime indexes.
            # Handles both {"$date": "..."} objects and plain ISO date strings.
            if field_access.endswith("')"):
                date_path_access = field_access[:-2] + '."$date"\')'
                field_for_datetime = (
                    f"COALESCE({date_path_access}, {field_access})"
                )
            else:
                field_for_datetime = field_access
            datetime_field_access = f"datetime({field_for_datetime})"
            match operator:
                case "$eq":
                    sql = f"{datetime_field_access} = datetime(?)"
                    params = [to_iso(value)]
                case "$gt":
                    sql = f"{datetime_field_access} > datetime(?)"
                    params = [to_iso(value)]
                case "$lt":
                    sql = f"{datetime_field_access} < datetime(?)"
                    params = [to_iso(value)]
                case "$gte":
                    sql = f"{datetime_field_access} >= datetime(?)"
                    params = [to_iso(value)]
                case "$lte":
                    sql = f"{datetime_field_access} <= datetime(?)"
                    params = [to_iso(value)]
                case "$ne":
                    sql = f"{datetime_field_access} != datetime(?)"
                    params = [to_iso(value)]
                case "$in":
                    if isinstance(value, (list, tuple)):
                        if field_access == "_id":
                            placeholders = ", ".join(
                                "datetime(?)" for _ in value
                            )
                            sql = f"{datetime_field_access} IN ({placeholders})"
                            params = [to_iso(v) for v in value]
                        else:
                            # For array fields with datetime values, use json_each
                            placeholders = ", ".join(
                                "datetime(?)" for _ in value
                            )
                            sql = f"EXISTS (SELECT 1 FROM {self._json_each_function}({datetime_field_access}) WHERE json_each.value IN ({placeholders}))"
                            params = [to_iso(v) for v in value]
                case "$nin":
                    if isinstance(value, (list, tuple)):
                        if field_access == "_id":
                            placeholders = ", ".join(
                                "datetime(?)" for _ in value
                            )
                            sql = f"{datetime_field_access} NOT IN ({placeholders})"
                            params = [to_iso(v) for v in value]
                        else:
                            # For array fields with datetime values, use json_each
                            placeholders = ", ".join(
                                "datetime(?)" for _ in value
                            )
                            sql = f"NOT EXISTS (SELECT 1 FROM {self._json_each_function}({datetime_field_access}) WHERE json_each.value IN ({placeholders}))"
                            params = [to_iso(v) for v in value]
                case _:
                    # For unsupported operators with datetime values, fall back to regular processing
                    is_datetime_comparison = False
                    # Use regular processing below
        else:
            # Regular processing for non-datetime values
            match operator:
                case "$eq":
                    if isinstance(value, re.Pattern):
                        return None, []
                    # Array values need Python for correct semantics
                    if isinstance(value, (list, tuple)):
                        return None, []
                    if value is None:
                        # MongoDB: null matches null-or-missing (#90)
                        sql = f"{field_access} IS NULL"
                        params = []
                    else:
                        param_val, ph = _normalize_for_field(value)
                        sql = f"{field_access} = {ph}"
                        params = [param_val]
                case "$gt":
                    # Array values need Python for correct semantics
                    if isinstance(value, (list, tuple)):
                        return None, []
                    sql = f"{field_access} > ?"
                    params = [value]
                case "$lt":
                    # Array values need Python for correct semantics
                    if isinstance(value, (list, tuple)):
                        return None, []
                    sql = f"{field_access} < ?"
                    params = [value]
                case "$gte":
                    # Array values need Python for correct semantics
                    if isinstance(value, (list, tuple)):
                        return None, []
                    if value is None:
                        sql = f"{field_access} IS NULL"
                        params = []
                    else:
                        sql = f"{field_access} >= ?"
                        params = [value]
                case "$lte":
                    # Array values need Python for correct semantics
                    if isinstance(value, (list, tuple)):
                        return None, []
                    if value is None:
                        sql = f"{field_access} IS NULL"
                        params = []
                    else:
                        sql = f"{field_access} <= ?"
                        params = [value]
                case "$ne":
                    # Array values and regex Pattern values need Python fallback
                    if isinstance(value, (list, tuple, re.Pattern)):
                        return None, []
                    if value is None:
                        if field_access == "_id":
                            sql = f"{field_access} IS NOT NULL"
                        elif "extract(" in field_access:
                            type_field_access = field_access.replace(
                                "jsonb_extract(", "json_type("
                            ).replace("json_extract(", "json_type(")
                            sql = (
                                f"({type_field_access} IS NOT NULL AND "
                                f"{type_field_access} IS NOT 'null')"
                            )
                        else:
                            sql = f"{field_access} IS NOT NULL"
                    else:
                        param_val, ph = _normalize_for_field(value)
                        if extracted is not None:
                            _, data_expr, json_path, _ = extracted
                            type_field_access = (
                                f"json_type({data_expr}, {json_path})"
                            )
                            types = json_type_list_sql(value)
                            each_val, each_ph = serialize_sql_query_value(
                                value, self._json_each_function == "jsonb_each"
                            )
                            sql = (
                                f"({type_field_access} IS NULL OR "
                                f"({type_field_access} = 'array' AND NOT EXISTS ("
                                f"SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path}) AS json_each "
                                f"WHERE json_each.value = {each_ph} AND json_each.type IN {types})) OR "
                                f"({type_field_access} != 'array' AND ("
                                f"{type_field_access} NOT IN {types} OR "
                                f"({type_field_access} IN {types} AND "
                                f"{field_access} IS NOT {ph}))))"
                            )
                            params = [each_val, param_val]
                        elif "extract(" in field_access:
                            type_field_access = field_access.replace(
                                "jsonb_extract(", "json_type("
                            ).replace("json_extract(", "json_type(")
                            types = json_type_list_sql(value)
                            sql = (
                                f"({type_field_access} IS NULL OR "
                                f"{type_field_access} NOT IN {types} OR "
                                f"({type_field_access} IN {types} AND "
                                f"{field_access} IS NOT {ph}))"
                            )
                            params = [param_val]
                        else:
                            sql = f"{field_access} IS NOT {ph}"
                            params = [param_val]
                case "$in":
                    if isinstance(value, list):
                        if field_access == "_id":
                            if not value:
                                return "0", []
                            has_null = any(v is None for v in value)
                            non_null = [v for v in value if v is not None]
                            norm_non_null = [
                                str(v) if isinstance(v, ObjectId) else v
                                for v in non_null
                            ]
                            if value and all(
                                isinstance(v, (str, datetime))
                                and self._is_datetime_value(v)
                                for v in value
                            ):
                                placeholders = ", ".join(
                                    "datetime(?)" for _ in value
                                )
                                sql = f"datetime({field_access}) IN ({placeholders})"
                                params = [to_iso(v) for v in value]
                            elif has_null:
                                if norm_non_null:
                                    placeholders = ", ".join(
                                        "?" for _ in norm_non_null
                                    )
                                    sql = f"({field_access} IN ({placeholders}) OR {field_access} IS NULL)"
                                    params = norm_non_null
                                else:
                                    sql = f"{field_access} IS NULL"
                                    params = []
                            else:
                                placeholders = ", ".join(
                                    "?" for _ in norm_non_null
                                )
                                sql = f"{field_access} IN ({placeholders})"
                                params = norm_non_null
                        elif extracted is not None:
                            func_prefix, data_expr, json_path, _ = extracted
                            sql, params = build_json_in_clause(
                                data_expr,
                                json_path,
                                value,
                                func_prefix,
                                is_jsonb,
                                self._json_each_function,
                            )
                        else:
                            return None, []
                    else:
                        return None, []
                case "$nin":
                    if isinstance(value, list):
                        if field_access == "_id":
                            if not value:
                                return "1", []
                            has_null = any(v is None for v in value)
                            non_null = [v for v in value if v is not None]
                            norm_non_null = [
                                str(v) if isinstance(v, ObjectId) else v
                                for v in non_null
                            ]
                            if value and all(
                                isinstance(v, (str, datetime))
                                and self._is_datetime_value(v)
                                for v in value
                            ):
                                placeholders = ", ".join(
                                    "datetime(?)" for _ in value
                                )
                                sql = f"datetime({field_access}) NOT IN ({placeholders})"
                                params = [to_iso(v) for v in value]
                            elif has_null:
                                if norm_non_null:
                                    placeholders = ", ".join(
                                        "?" for _ in norm_non_null
                                    )
                                    sql = f"({field_access} NOT IN ({placeholders}) AND {field_access} IS NOT NULL)"
                                    params = norm_non_null
                                else:
                                    sql = f"{field_access} IS NOT NULL"
                                    params = []
                            else:
                                placeholders = ", ".join(
                                    "?" for _ in norm_non_null
                                )
                                sql = f"{field_access} NOT IN ({placeholders})"
                                params = norm_non_null
                        elif extracted is not None:
                            func_prefix, data_expr, json_path, _ = extracted
                            sql, params = build_json_nin_clause(
                                data_expr,
                                json_path,
                                value,
                                func_prefix,
                                is_jsonb,
                                self._json_each_function,
                            )
                        else:
                            return None, []
                    else:
                        return None, []
                case "$all":
                    if (
                        isinstance(value, list)
                        and value
                        and extracted is not None
                    ):
                        _, data_expr, json_path, _ = extracted
                        all_clauses = [
                            f"json_type({data_expr}, {json_path}) = 'array'"
                        ]
                        all_params: list[Any] = []
                        for item in value:
                            if item is None:
                                all_clauses.append(
                                    f"EXISTS (SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path}) AS json_each WHERE json_each.value IS NULL)"
                                )
                            else:
                                p_val, ph = serialize_sql_query_value(
                                    item, each_is_jsonb
                                )
                                elem_types = json_type_list_sql(item)
                                all_clauses.append(
                                    f"EXISTS (SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path}) AS json_each WHERE json_each.value = {ph} AND json_each.type IN {elem_types})"
                                )
                                all_params.append(p_val)
                        sql = "(" + " AND ".join(all_clauses) + ")"
                        params = all_params
                    else:
                        return None, []
                case "$exists":
                    # Handle boolean value for $exists
                    if isinstance(value, bool):
                        if field_access == "_id":
                            if value:
                                sql = f"{field_access} IS NOT NULL"
                            else:
                                sql = f"{field_access} IS NULL"
                        elif "extract(" in field_access:
                            type_field_access = field_access.replace(
                                "jsonb_extract(", "json_type("
                            ).replace("json_extract(", "json_type(")
                            if value:
                                sql = f"{type_field_access} IS NOT NULL"
                            else:
                                sql = f"{type_field_access} IS NULL"
                        else:
                            if value:
                                sql = f"{field_access} IS NOT NULL"
                            else:
                                sql = f"{field_access} IS NULL"
                        params = []
                    else:
                        # Invalid value for $exists, fallback to Python
                        return None, []
                case "$mod":
                    # Handle [divisor, remainder] array
                    if isinstance(value, (list, tuple)) and len(value) == 2:
                        divisor, remainder = value
                        if divisor == 0:
                            return None, []  # Python tier raises (#160)
                        sql = f"{field_access} % ? = ?"
                        params = [divisor, remainder]
                case "$size":
                    # Handle array size comparison
                    if isinstance(value, int) and not isinstance(value, bool):
                        if "extract(" in field_access:
                            type_field_access = field_access.replace(
                                "jsonb_extract(", "json_type("
                            ).replace("json_extract(", "json_type(")
                            sql = (
                                f"{type_field_access} = 'array' AND "
                                f"json_array_length({field_access}) = ?"
                            )
                        else:
                            sql = f"json_array_length({field_access}) = ?"
                        params = [value]
                case "$contains":
                    # Handle case-insensitive substring search
                    # Convert value to string to match Python implementation behavior
                    str_value = str(value)
                    sql = f"lower({field_access}) LIKE ?"
                    params = [f"%{str_value.lower()}%"]
                case "$bitsAllClear":
                    # Handle bitwise all clear operator
                    # Check if all specified bits are clear (0)
                    bitmask = _convert_to_bitmask(value)
                    if bitmask is not None:
                        sql = f"({field_access} & ?) = 0"
                        params = [bitmask]
                case "$bitsAllSet":
                    # Handle bitwise all set operator
                    # Check if all specified bits are set (1)
                    bitmask = _convert_to_bitmask(value)
                    if bitmask is not None:
                        sql = f"({field_access} & ?) = ?"
                        params = [bitmask, bitmask]
                case "$bitsAnyClear":
                    # Handle bitwise any clear operator
                    # Check if any of the specified bits are clear (0)
                    bitmask = _convert_to_bitmask(value)
                    if bitmask is not None:
                        sql = f"((~{field_access}) & ?) != 0"
                        params = [bitmask]
                case "$bitsAnySet":
                    # Handle bitwise any set operator
                    # Check if any of the specified bits are set (1)
                    bitmask = _convert_to_bitmask(value)
                    if bitmask is not None:
                        sql = f"({field_access} & ?) != 0"
                        params = [bitmask]
                case "$regex":
                    pattern = regex_to_sql_pattern(value, regex_options)
                    if pattern is None:
                        return None, []
                    if extracted is not None:
                        _, data_expr, json_path, _ = extracted
                        sql = (
                            f"(json_type({data_expr}, {json_path}) IS NOT NULL AND "
                            f"json_type({data_expr}, {json_path}) != 'null' AND "
                            f"{field_access} REGEXP ?)"
                        )
                    else:
                        sql = f"({field_access} IS NOT NULL AND {field_access} REGEXP ?)"
                    params = [pattern]
                case "$options":
                    # Standalone $options without $regex must fall back so Python raises
                    return None, []
                case "$elemMatch":
                    if extracted is not None:
                        _, data_expr, json_path, _ = extracted
                        sql, params = self._translate_elem_match(
                            data_expr, json_path, value
                        )
                    else:
                        return None, []
                case "$type":
                    from .type_utils import build_bson_type_sql

                    if extracted is not None:
                        _, data_expr, json_path, _ = extracted
                        sql = build_bson_type_sql(data_expr, json_path, value)
                    elif field_access == "json_each.value":
                        sql = build_bson_type_sql(
                            "json_each.value",
                            "'$'",
                            value,
                            type_expr="json_each.type",
                            val_expr="json_each.value",
                        )
                    else:
                        return None, []
                case _:
                    # Unsupported operator
                    pass

        return sql, params

    def _translate_elem_match(
        self, data_expr: str, json_path: str, elem_query: Any
    ) -> tuple[str | None, list[Any]]:
        """Translate ``$elemMatch`` on a JSON array field to SQLite ``EXISTS``.

        Supports scalar criteria, empty criteria dictionaries, element-level
        operator dictionaries (``$eq``, ``$gt``, ``$lt``, ``$gte``, ``$lte``,
        ``$ne``, ``$in``, ``$nin``, ``$regex``/``$options``, ``$type``), and
        subdocument queries over array objects.

        Args:
            data_expr: SQL column or expression holding the parent JSON value.
            json_path: Quoted JSON path string for the target array field.
            elem_query: Scalar value or criteria dictionary for array elements.

        Returns:
            tuple[str | None, list[Any]]: ``(sql_expr, params)`` tuple, or
                ``(None, [])`` when the query requires Python evaluation.
        """
        from .type_utils import (
            is_encoded_json_value,
            json_type_list_sql,
            json_types_for_values,
            regex_to_sql_pattern,
            serialize_in_values,
            serialize_sql_query_value,
        )

        if not isinstance(elem_query, dict):
            if isinstance(elem_query, (list, tuple, re.Pattern)):
                return None, []
            each_is_jsonb = self._json_each_function == "jsonb_each"
            if elem_query is None:
                cond = "json_each.type = 'null'"
                params_out: list[Any] = []
            else:
                p_val, ph = serialize_sql_query_value(elem_query, each_is_jsonb)
                elem_types = json_type_list_sql(elem_query)
                cond = f"(json_each.type IN {elem_types} AND json_each.value = {ph})"
                params_out = [p_val]
            return (
                f"(json_type({data_expr}, {json_path}) = 'array' AND EXISTS ("
                f"SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path}) AS json_each "
                f"WHERE {cond}))",
                params_out,
            )

        if not elem_query:
            return (
                f"(json_type({data_expr}, {json_path}) = 'array' AND EXISTS ("
                f"SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path})))",
                [],
            )

        has_ops = any(k.startswith("$") for k in elem_query)
        has_non_ops = any(not k.startswith("$") for k in elem_query)
        if has_ops and has_non_ops:
            return None, []

        if has_ops:
            options = elem_query.get("$options", "")
            if "$options" in elem_query and (
                "$regex" not in elem_query or not isinstance(options, str)
            ):
                return None, []

            each_is_jsonb = self._json_each_function == "jsonb_each"
            sub_clauses: list[str] = []
            sub_params: list[Any] = []
            cmp_map = {
                "$eq": "=",
                "$gt": ">",
                "$lt": "<",
                "$gte": ">=",
                "$lte": "<=",
            }
            for sub_op, sub_val in elem_query.items():
                if sub_op == "$options":
                    continue
                if sub_op == "$regex":
                    pattern = regex_to_sql_pattern(sub_val, options)
                    if pattern is None:
                        return None, []
                    sub_clauses.append(
                        "(json_each.value IS NOT NULL AND json_each.type != 'null' AND json_each.value REGEXP ?)"
                    )
                    sub_params.append(pattern)
                elif sub_op in ("$in", "$nin") and isinstance(sub_val, list):
                    if not sub_val:
                        sub_clauses.append("0" if sub_op == "$in" else "1")
                        continue
                    has_null = any(v is None for v in sub_val)
                    non_null = [v for v in sub_val if v is not None]
                    if not non_null:
                        sub_clauses.append(
                            "json_each.type = 'null'"
                            if sub_op == "$in"
                            else "json_each.type != 'null'"
                        )
                        continue
                    types = json_types_for_values(non_null)
                    types_sql = "(" + ", ".join(repr(t) for t in types) + ")"
                    phs, p_list = serialize_in_values(non_null, each_is_jsonb)
                    in_match = f"(json_each.value IN ({phs}) AND json_each.type IN {types_sql})"
                    if has_null:
                        in_match = f"({in_match} OR json_each.type = 'null')"
                    sub_clauses.append(
                        in_match if sub_op == "$in" else f"NOT ({in_match})"
                    )
                    sub_params.extend(p_list)
                elif sub_op == "$ne":
                    if isinstance(sub_val, (list, tuple, re.Pattern)):
                        return None, []
                    if sub_val is None:
                        sub_clauses.append("json_each.type != 'null'")
                    else:
                        p_val, ph = serialize_sql_query_value(
                            sub_val, each_is_jsonb
                        )
                        val_types = json_type_list_sql(sub_val)
                        sub_clauses.append(
                            f"(json_each.type NOT IN {val_types} OR json_each.value IS NOT {ph})"
                        )
                        sub_params.append(p_val)
                elif sub_op in cmp_map:
                    if isinstance(sub_val, (list, tuple, re.Pattern)) or (
                        sub_op != "$eq" and is_encoded_json_value(sub_val)
                    ):
                        return None, []
                    if sub_val is None:
                        if sub_op in ("$eq", "$gte", "$lte"):
                            sub_clauses.append("json_each.type = 'null'")
                        else:
                            sub_clauses.append("0")
                    else:
                        p_val, ph = serialize_sql_query_value(
                            sub_val, each_is_jsonb
                        )
                        val_types = json_type_list_sql(sub_val)
                        sub_clauses.append(
                            f"(json_each.type IN {val_types} AND json_each.value {cmp_map[sub_op]} {ph})"
                        )
                        sub_params.append(p_val)
                elif sub_op == "$type":
                    from .type_utils import build_bson_type_sql

                    t_sql = build_bson_type_sql(
                        "json_each.value",
                        "'$'",
                        sub_val,
                        type_expr="json_each.type",
                        val_expr="json_each.value",
                    )
                    if t_sql is None:
                        return None, []
                    sub_clauses.append(t_sql)
                else:
                    return None, []

            return (
                f"(json_type({data_expr}, {json_path}) = 'array' AND EXISTS ("
                f"SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path}) AS json_each "
                f"WHERE {' AND '.join(sub_clauses)}))",
                sub_params,
            )

        # Subdocument criteria inside an array of objects
        sub_accessor = SQLFieldAccessor(
            "json_each.value",
            self.field_accessor.id_column,
            self.field_accessor.jsonb_supported,
            allow_id_column=False,
        )
        sub_builder = SQLClauseBuilder(
            sub_accessor,
            json_each_function=self._json_each_function,
            db_connection=self._db_connection,
        )
        sub_sql, sub_params = sub_builder.build_where_clause(
            elem_query, context="direct", is_nested=True
        )
        if not sub_sql:
            return None, []
        return (
            f"(json_type({data_expr}, {json_path}) = 'array' AND EXISTS ("
            f"SELECT 1 FROM {self._json_each_function}({data_expr}, {json_path}) AS json_each "
            f"WHERE json_each.type = 'object' AND ({sub_sql})))",
            sub_params,
        )

    def _is_datetime_value(self, value: Any) -> bool:
        """Check whether a value represents a datetime or ISO date string.

        Args:
            value: Value to inspect for datetime representation.

        Returns:
            bool: ``True`` if ``value`` is a recognized datetime value,
                ``False`` otherwise.
        """
        from .datetime_utils import is_datetime_value

        return is_datetime_value(value)


class SQLClauseBuilder:
    """Builds SQL WHERE, ORDER BY, and LIMIT/OFFSET clauses from queries.

    Combines ``SQLFieldAccessor`` and ``SQLOperatorTranslator`` to translate
    MongoDB filter, sort, skip, and limit specifications into SQLite clauses.
    """

    def __init__(
        self,
        field_accessor: SQLFieldAccessor | None = None,
        operator_translator: SQLOperatorTranslator | None = None,
        json_each_function: str | None = None,
        db_connection: Any = None,
    ):
        """Initialize the SQLClauseBuilder.

        Args:
            field_accessor: ``SQLFieldAccessor`` for generating field access
                SQL expressions. Creates a default instance when ``None``.
            operator_translator: ``SQLOperatorTranslator`` for translating
                field operators. Creates a default or configured instance when
                ``None``.
            json_each_function: Table-valued iteration function name
                (``"jsonb_each"`` or ``"json_each"``). Used when constructing
                a default translator and array containment predicates.
            db_connection: Optional SQLite connection for Tier-1 ``$expr``
                evaluation.
        """
        self.field_accessor = field_accessor or SQLFieldAccessor()
        # Retained so clause builders can emit array-containment checks (#99)
        self._json_each_function = json_each_function or "json_each"
        self.data_column = self.field_accessor.data_column
        self.db_connection = db_connection
        if operator_translator is not None:
            self.operator_translator = operator_translator
        elif json_each_function is not None or db_connection is not None:
            self.operator_translator = SQLOperatorTranslator(
                field_accessor, self._json_each_function, db_connection
            )
        else:
            self.operator_translator = SQLOperatorTranslator()

    def _build_logical_condition(
        self,
        operator: str,
        conditions: list[dict[str, Any]],
        context: str = "direct",
    ) -> tuple[str | None, list[Any]]:
        """Build a SQL logical condition for ``$and``, ``$or``, or ``$nor``.

        Recursively translates each sub-condition dictionary via
        ``build_where_clause()`` and combines them using ``AND``, ``OR``, or
        ``NOT (... OR ...)`` with ``COALESCE(..., 0)`` null-safety for
        disjunctions.

        Args:
            operator: Logical operator name (``"$and"``, ``"$or"``, or
                ``"$nor"``).
            conditions: List of sub-condition dictionaries.
            context: Execution context passed to ``build_where_clause()``.
                Defaults to ``"direct"``.

        Returns:
            tuple[str | None, list[Any]]: ``(sql_expr, params)``, ``("", [])``
                when no clauses were produced, or ``(None, [])`` when any
                sub-condition is unsupported or non-dict.
        """
        # default `sql` and `params`
        sql: str | None = None
        params: list[Any] = []

        if not isinstance(conditions, list):
            return sql, params

        clauses: list[str] = []

        for condition in conditions:
            if isinstance(condition, dict):
                # Recursively build condition
                clause, clause_params = self.build_where_clause(
                    condition, context, is_nested=True
                )
                if clause is None:
                    # Unsupported condition - return None to signal Python fallback
                    return None, []
                elif clause:  # Only add non-empty clauses
                    # Remove "WHERE " prefix if present
                    clause = clause.removeprefix("WHERE ")
                    clauses.append(f"({clause})")
                    params.extend(clause_params)
            else:
                # Non-dict condition - return None to signal Python fallback
                return None, []

        if not clauses:
            return _empty_result()

        match operator:
            case "$and":
                sql = " AND ".join(clauses)
            case "$or":
                sql = (
                    "("
                    + " OR ".join(f"COALESCE(({c}), 0)" for c in clauses)
                    + ")"
                )
            case "$nor":
                sql = (
                    "NOT ("
                    + " OR ".join(f"COALESCE(({c}), 0)" for c in clauses)
                    + ")"
                )
            case _:
                # Unsupported logical operator
                pass

        return sql, params

    def build_where_clause(
        self,
        query: dict[str, Any],
        context: str = "direct",
        is_nested: bool = False,
        query_param: dict | None = None,
    ) -> tuple[str | None, list[Any]]:
        """Build a SQL WHERE clause from a MongoDB query dictionary.

        Translates field comparisons, operator dicts, logical operators
        (``$and``, ``$or``, ``$nor``, ``$not``), ``$jsonSchema``, ``$expr``,
        regex patterns, datetimes, nulls, and array containment checks into a
        parameterized SQL predicate.

        Args:
            query: MongoDB-style query dictionary.
            context: Execution context passed to ``get_field_access()``.
                Defaults to ``"direct"``.
            is_nested: When ``True``, omits the leading ``"WHERE "`` prefix.
                Defaults to ``False``.
            query_param: Optional outer query dictionary used when deciding
                between ``json_*`` and ``jsonb_*`` functions.

        Returns:
            tuple[str | None, list[Any]]: ``(where_clause, params)`` tuple,
                ``("", [])`` for an empty query, or ``(None, [])`` when Python
                fallback is required.

        Raises:
            NotImplementedError: If ``query`` contains unsupported JavaScript
                or custom aggregation operators (``$where``, ``$function``, or
                ``$accumulator``).
        """
        # default `sql` and `params`
        sql: str | None = None
        params: list[Any] = []
        # default `clauses`
        clauses: list[str] = []

        for field, value in query.items():
            if field == "$where":
                raise NotImplementedError(
                    "The '$where' operator (JavaScript) is not supported in NeoSQLite. "
                    "Please use the '$expr' operator for field-to-field comparisons, "
                    "which is fully compatible with MongoDB and highly optimized in NeoSQLite."
                )

            if field == "$function":
                raise NotImplementedError(
                    "The '$function' operator is not supported in NeoSQLite. "
                    "Please use '$expr' with Python expressions, or post-process results in Python."
                )

            if field == "$accumulator":
                raise NotImplementedError(
                    "The '$accumulator' operator is not supported in NeoSQLite. "
                    "Please use built-in accumulators ($sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last), "
                    "or post-process results in Python."
                )

            if field == "$jsonSchema":
                from .query_helper.schema_compiler import (
                    compile_query_schema_to_sql,
                )

                schema_res = compile_query_schema_to_sql(
                    value,
                    data_column=self.data_column,
                    jsonb=self.field_accessor.jsonb_supported,
                    json_each_function=self._json_each_function,
                )
                if schema_res is None:
                    return None, []
                schema_clause, schema_params = schema_res
                clauses.append(schema_clause)
                params.extend(schema_params)
                continue

            if field == "$expr":
                if not isinstance(value, dict):
                    return None, []
                from .expr_evaluator import ExprEvaluator
                from .type_utils import expr_truthiness_sql

                evaluator = ExprEvaluator(
                    data_column=self.data_column,
                    db_connection=self.db_connection,
                )
                expr_sql, expr_params = evaluator.evaluate(
                    value, tier=1, force_python=False
                )
                if expr_sql is None:
                    return None, []
                clauses.append(expr_truthiness_sql(expr_sql))
                params.extend(expr_params)
            elif field in ("$and", "$or", "$nor"):
                # Handle logical operators directly
                sql, clause_params = self._build_logical_condition(
                    field, value, context
                )
                if sql is None:
                    return None, []  # Unsupported condition, fallback to Python
                else:  # Only add if not empty
                    clauses.append(sql)
                    params.extend(clause_params)
            elif field == "$not":
                # Handle $not logical operator (applies to single condition)
                if isinstance(value, dict):
                    not_clause, not_params = self.build_where_clause(
                        value, context, is_nested=True, query_param=query_param
                    )
                    if not_clause is None:
                        return (
                            None,
                            [],
                        )  # Unsupported condition, fallback to Python
                    if not_clause:
                        # Remove "WHERE " prefix if present
                        not_clause = not_clause.removeprefix("WHERE ")
                        clauses.append(f"NOT COALESCE(({not_clause}), 0)")
                        params.extend(not_params)
                    else:
                        return _empty_result()  # Empty condition
                else:
                    return _empty_result()  # Invalid format for $not
            else:
                # Regular field condition
                # Get field access expression
                field_access = self.field_accessor.get_field_access(
                    field, context, query_param
                )

                if isinstance(value, dict):
                    # Handle query operators like $eq, $gt, $lt, etc.
                    options = value.get("$options", "")
                    if "$options" in value and (
                        "$regex" not in value or not isinstance(options, str)
                    ):
                        return None, []
                    for operator, op_val in value.items():
                        if operator == "$options":
                            continue
                        if operator == "$not":
                            if not isinstance(op_val, (dict, re.Pattern)) or (
                                isinstance(op_val, dict) and "$not" in op_val
                            ):
                                return None, []
                            sub_clause, sub_params = self.build_where_clause(
                                {field: op_val},
                                context,
                                is_nested=True,
                                query_param=query_param,
                            )
                            if not sub_clause:
                                return None, []
                            sub_clause = sub_clause.removeprefix("WHERE ")
                            clauses.append(f"NOT COALESCE(({sub_clause}), 0)")
                            params.extend(sub_params)
                            continue
                        if operator == "$regex" and options:
                            sql, clause_params = (
                                self.operator_translator.translate_operator(
                                    field_access,
                                    operator,
                                    op_val,
                                    regex_options=options,
                                )
                            )
                        else:
                            sql, clause_params = (
                                self.operator_translator.translate_operator(
                                    field_access, operator, op_val
                                )
                            )
                        if sql is None:
                            # Unsupported operator, fallback to Python
                            return None, []
                        clauses.append(sql)
                        params.extend(clause_params)
                elif isinstance(value, re.Pattern):
                    from .type_utils import regex_to_sql_pattern

                    pattern = regex_to_sql_pattern(value)
                    if pattern is None:
                        return None, []
                    clauses.append(
                        f"({field_access} IS NOT NULL AND {field_access} REGEXP ?)"
                    )
                    params.append(pattern)
                elif self.operator_translator._is_datetime_value(value):
                    # For simple datetime equality, translate it as an $eq operator
                    sql, clause_params = (
                        self.operator_translator.translate_operator(
                            field_access, "$eq", value
                        )
                    )
                    if sql is None:
                        return None, []
                    clauses.append(sql)
                    params.extend(clause_params)
                elif value is None:
                    clauses.append(f"{field_access} IS NULL")
                elif isinstance(value, (dict, list)) or (
                    field == "_id" and self.field_accessor.allow_id_column
                ):
                    # _id column stores hex strings for ObjectId; no containment applies (#99)
                    from ..objectid import ObjectId

                    bound_val = (
                        str(value)
                        if field == "_id" and isinstance(value, ObjectId)
                        else value
                    )
                    clauses.append(f"{field_access} = ?")
                    params.append(bound_val)
                else:
                    # Simple equality with array containment at any depth (#99)
                    from .type_utils import build_json_containment_clause

                    containment_clause, params_arm = (
                        build_json_containment_clause(
                            self.data_column,
                            field,
                            field_access,
                            value,
                            self._json_each_function,
                        )
                    )
                    clauses.append(containment_clause)
                    params.extend(params_arm)

        if not clauses:
            return _empty_result()

        # Build the final WHERE clause
        where_clause = "WHERE " + " AND ".join(clauses)
        if is_nested:
            # Remove "WHERE " prefix for nested conditions
            where_clause = where_clause[6:]

        return where_clause, params

    def build_order_by_clause(
        self, sort_spec: dict[str, Any], context: str = "direct"
    ) -> str:
        """Build a SQL ``ORDER BY`` clause from a MongoDB sort specification.

        Args:
            sort_spec: Mapping of field names to sort directions (``1`` for
                ascending, ``DESCENDING`` / ``-1`` for descending).
            context: Field access context identifier. Defaults to
                ``"direct"``.

        Returns:
            str: SQL ``ORDER BY ...`` clause, or ``""`` when ``sort_spec`` is
                empty.
        """
        if not sort_spec:
            return ""

        order_parts = []
        for field, direction in sort_spec.items():
            field_access = self.field_accessor.get_field_access(field, context)
            order_parts.append(
                f"{field_access} {'DESC' if direction == DESCENDING else 'ASC'}"
            )

        return "ORDER BY " + ", ".join(order_parts)

    def build_limit_offset_clause(
        self, limit_value: int | None = None, skip_value: int = 0
    ) -> str:
        """Build SQL ``LIMIT`` and ``OFFSET`` clauses.

        Treats ``limit_value == 0`` as unlimited and emits ``LIMIT -1 OFFSET``
        when ``skip_value > 0`` without a limit, as required by SQLite syntax.

        Args:
            limit_value: Maximum number of documents to return (absolute value
                is used), or ``None`` / ``0`` for no limit.
            skip_value: Number of leading documents to skip. Defaults to ``0``.

        Returns:
            str: SQL ``LIMIT``/``OFFSET`` clause string, or ``""`` if neither
                limit nor skip applies.
        """
        if limit_value == 0:
            limit_value = None

        limit_clause = ""
        if limit_value is not None:
            actual_limit = abs(limit_value)
            if skip_value > 0:
                limit_clause = f"LIMIT {actual_limit} OFFSET {skip_value}"
            else:
                limit_clause = f"LIMIT {actual_limit}"
        elif skip_value > 0:
            # SQLite requires LIMIT when using OFFSET
            limit_clause = f"LIMIT -1 OFFSET {skip_value}"

        return limit_clause


class SQLTranslator:
    """High-level unified SQL translator for direct and temp-table queries.

    Coordinates ``SQLFieldAccessor``, ``SQLOperatorTranslator``, and
    ``SQLClauseBuilder`` to translate ``$match``, ``$sort``, ``$skip``,
    ``$limit``, and field access expressions into SQLite SQL clauses.
    """

    def __init__(
        self,
        table_name: str | None = None,
        data_column: str = "data",
        id_column: str = "id",
        jsonb_supported: bool = False,
        json_each_function: str | None = None,
        db_connection: Any = None,
        jsonb_context: JSONBContext | None = None,
    ):
        """Initialize the SQLTranslator with table, column, and JSONB config.

        Args:
            table_name: Target table name. Defaults to ``"collection"`` when
                ``None`` or empty.
            data_column: Column name storing JSON documents. Defaults to
                ``"data"``.
            id_column: Column name storing document row IDs. Defaults to
                ``"id"``.
            jsonb_supported: Whether SQLite JSONB functions are supported.
                Defaults to ``False``.
            json_each_function: Name of the ``json_each`` function
                (``"jsonb_each"`` or ``"json_each"``); defaults to
                ``"json_each"``.
            db_connection: Optional SQLite connection for Tier-1 ``$expr``
                evaluation.
            jsonb_context: Optional ``JSONBContext`` whose flags override
                ``jsonb_supported`` and ``json_each_function`` when provided.
        """
        if jsonb_context is not None:
            jsonb_supported = jsonb_context.jsonb_supported
            json_each_function = jsonb_context.json_each_function
        self.table_name = table_name or "collection"
        self.data_column = data_column
        self.id_column = id_column
        self.jsonb_supported = jsonb_supported
        self._json_each_function = json_each_function or "json_each"
        self.db_connection = db_connection

        # Initialize components
        self.field_accessor = SQLFieldAccessor(
            data_column, id_column, jsonb_supported
        )
        self.operator_translator = SQLOperatorTranslator(
            self.field_accessor, self._json_each_function, db_connection
        )
        self.clause_builder = SQLClauseBuilder(
            self.field_accessor,
            self.operator_translator,
            self._json_each_function,
            db_connection,
        )

    def translate_match(
        self, match_spec: dict[str, Any], context: str = "direct"
    ) -> tuple[str | None, list[Any]]:
        """Translate a ``$match`` stage specification into a SQL WHERE clause.

        Returns ``(None, [])`` when the force-fallback flag is active, when
        ``match_spec`` is not a dict, or when any ``$text`` operator is present
        so the caller can run FTS or Python fallback handling.

        Args:
            match_spec: MongoDB ``$match`` query specification dictionary.
            context: Field access context identifier. Defaults to
                ``"direct"``.

        Returns:
            tuple[str | None, list[Any]]: ``(where_clause, params)`` tuple, or
                ``(None, [])`` if fallback handling is required.
        """
        from .query_helper.utils import get_force_fallback

        if get_force_fallback():
            return _text_search_fallback()

        # Handle text search queries separately
        if not isinstance(match_spec, dict):
            return _text_search_fallback()  # Fallback for non-dict queries

        if "$text" in match_spec:
            return (
                _text_search_fallback()
            )  # Special handling required, return None to fallback

        # Check for nested $text operators in logical operators
        if self._contains_text_operator(match_spec):
            return (
                _text_search_fallback()
            )  # Special handling required, return None to fallback

        # Pass the query to the clause builder so it can be used for field access decisions
        return self.clause_builder.build_where_clause(
            match_spec, context, query_param=match_spec
        )

    def _contains_text_operator(self, query: dict[str, Any]) -> bool:
        """Check whether a query contains any ``$text`` operators.

        Delegates to ``jsonb_support._contains_text_operator`` so top-level and
        nested logical-operator ``$text`` checks stay consistent.

        Args:
            query: MongoDB query dictionary to inspect.

        Returns:
            bool: ``True`` if ``query`` contains any ``$text`` operator,
                ``False`` otherwise.
        """
        from .jsonb_support import _contains_text_operator

        return _contains_text_operator(query)

    def translate_sort(
        self, sort_spec: dict[str, Any], context: str = "direct"
    ) -> str:
        """Translate a ``$sort`` stage specification to a SQL ORDER BY clause.

        Args:
            sort_spec: Mapping of field names to sort directions.
            context: Field access context identifier. Defaults to
                ``"direct"``.

        Returns:
            str: SQL ``ORDER BY`` clause string, or ``""`` if empty.
        """
        return self.clause_builder.build_order_by_clause(sort_spec, context)

    def translate_skip_limit(
        self, limit_value: int | None = None, skip_value: int = 0
    ) -> str:
        """Translate ``$skip`` and ``$limit`` values to LIMIT/OFFSET clauses.

        Args:
            limit_value: Maximum number of rows to return, or ``None``/``0``
                for unlimited.
            skip_value: Number of leading rows to skip. Defaults to ``0``.

        Returns:
            str: SQL ``LIMIT``/``OFFSET`` clause string, or ``""`` if unused.
        """
        return self.clause_builder.build_limit_offset_clause(
            limit_value, skip_value
        )

    def translate_field_access(
        self, field: str, context: str = "direct"
    ) -> str:
        """Translate a field name to its SQL access expression.

        Args:
            field: MongoDB dot-notation field path.
            context: Field access context identifier. Defaults to
                ``"direct"``.

        Returns:
            str: SQL expression for reading ``field``.
        """
        return self.field_accessor.get_field_access(field, context)

    def translate_sort_skip_limit(
        self,
        sort_spec: dict[str, Any] | None,
        skip_value: int = 0,
        limit_value: int | None = None,
        context: str = "direct",
    ) -> tuple[str, str, str]:
        """Translate sort, skip, and limit stages to SQL clause strings.

        Combines ``translate_sort()`` and ``translate_skip_limit()``; the third
        tuple element is always ``""`` because ``LIMIT`` and ``OFFSET`` are
        returned together in the second element.

        Args:
            sort_spec: Optional ``$sort`` mapping of field names to directions.
            skip_value: Number of leading rows to skip. Defaults to ``0``.
            limit_value: Maximum number of rows to return, or ``None``.
            context: Field access context identifier. Defaults to
                ``"direct"``.

        Returns:
            tuple[str, str, str]: Three-element tuple containing
                ``(order_by_clause, limit_offset_clause, "")``.
        """
        # Build ORDER BY clause
        order_by = self.translate_sort(sort_spec, context) if sort_spec else ""

        # Build LIMIT/OFFSET clause
        limit_offset = self.translate_skip_limit(limit_value, skip_value)

        return order_by, limit_offset, ""
