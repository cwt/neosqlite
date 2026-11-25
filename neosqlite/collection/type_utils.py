"""Shared type checking and conversion utilities for the collection package.

This module consolidates type-related utility functions used across multiple
submodules (``expr_evaluator``, ``query_helper``, etc.) to avoid duplication
and provide a single source of truth for type operations.
"""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from typing import Any

from ..exceptions import InvalidOperation
from .jsonb_support import json_constructor

# =============================================================================
# Type Conversion Functions
# =============================================================================


def _convert_to_int(value: Any) -> Any:
    """Convert a value to a Python ``int``.

    Args:
        value: Input value convertible by ``int()``.

    Returns:
        Any: Converted integer value.
    """
    return int(value)


def _convert_to_long(value: Any) -> Any:
    """Convert a value to a 64-bit integer (Python ``int``).

    Args:
        value: Input value convertible by ``int()``.

    Returns:
        Any: Converted integer value.
    """
    return int(value)


def _convert_to_double(value: Any) -> Any:
    """Convert a value to a double-precision ``float``.

    Args:
        value: Input value convertible by ``float()``.

    Returns:
        Any: Converted floating-point value.
    """
    return float(value)


def _convert_to_decimal(value: Any) -> Any:
    """Convert a value to a numeric ``float`` (SQLite lacks Decimal128).

    Args:
        value: Input value convertible by ``float()``.

    Returns:
        Any: Converted floating-point value.
    """
    return float(value)


def _convert_to_string(value: Any) -> Any:
    """Convert a value to a ``str``.

    Args:
        value: Input value to stringify.

    Returns:
        Any: String representation of ``value``.
    """
    return str(value)


def _convert_to_bool(value: Any) -> Any:
    """Convert a value to a ``bool``.

    Args:
        value: Input value evaluated for Python truthiness.

    Returns:
        Any: Boolean truth value of ``value``.
    """
    return bool(value)


def _convert_to_objectid(value: Any) -> Any:
    """Convert a truthy value to an ``ObjectId``, or return ``None``.

    Args:
        value: Hex string, ``ObjectId``, or falsy value.

    Returns:
        Any: ``ObjectId(str(value))`` when ``value`` is truthy, else ``None``.
    """
    from neosqlite.objectid import ObjectId

    return ObjectId(str(value)) if value else None


def _convert_to_bindata(value: Any) -> Any:
    """Convert a value to a ``Binary`` (``binData``) instance.

    Encodes ``str`` inputs as UTF-8 bytes before wrapping in ``Binary``;
    returns ``None`` unchanged when ``value`` is ``None``.

    Args:
        value: Bytes-like value, ``str``, or ``None``.

    Returns:
        Any: ``Binary`` wrapper instance, or ``None`` if ``value`` is
        ``None``.
    """
    from neosqlite.binary import Binary

    if value is None:
        return None
    if isinstance(value, str):
        return Binary(value.encode("utf-8"))
    return Binary(value)


def _convert_to_bsonbindata(value: Any) -> Any:
    """Convert a value to a ``Binary`` (``bsonBinData``) instance.

    Encodes ``str`` inputs as UTF-8 bytes before wrapping in ``Binary``;
    returns ``None`` unchanged when ``value`` is ``None``.

    Args:
        value: Bytes-like value, ``str``, or ``None``.

    Returns:
        Any: ``Binary`` wrapper instance, or ``None`` if ``value`` is
        ``None``.
    """
    from neosqlite.binary import Binary

    if value is None:
        return None
    if isinstance(value, str):
        return Binary(value.encode("utf-8"))
    return Binary(value)


def _convert_to_regex(value: Any) -> Any:
    """Convert a truthy pattern value to a compiled ``re.Pattern``.

    Args:
        value: Regex pattern string or falsy value.

    Returns:
        Any: Compiled ``re.Pattern`` when ``value`` is truthy, else ``None``.
    """
    return re.compile(str(value)) if value else None


def _convert_to_bsonregex(value: Any) -> Any:
    """Convert a truthy pattern value to a compiled ``re.Pattern`` (bsonRegex).

    Args:
        value: Regex pattern string or falsy value.

    Returns:
        Any: Compiled ``re.Pattern`` when ``value`` is truthy, else ``None``.
    """
    return re.compile(str(value)) if value else None


def _convert_to_date(value: Any) -> Any:
    """Pass through a date value (full conversion requires parsing).

    Args:
        value: Input date-like value.

    Returns:
        Any: The input ``value`` unchanged.
    """
    return value


def _convert_to_null(value: Any) -> None:
    """Convert any value to ``None`` (BSON null).

    Args:
        value: Input value (ignored).
    """
    return None


def get_bson_type(value: Any) -> str:
    """Get the BSON type name for a Python value.

    Args:
        value: The value to inspect.

    Returns:
        str: BSON type name (``"null"``, ``"bool"``, ``"int"``, ``"double"``,
        ``"string"``, ``"array"``, ``"object"``, or ``"unknown"``).
    """
    match value:
        case None:
            return "null"
        case bool():
            return "bool"
        case int():
            return "int"
        case float():
            return "double"
        case str():
            return "string"
        case list():
            return "array"
        case dict():
            return "object"
        case _:
            return "unknown"


def bson_sort_key(value: Any) -> tuple:
    """Build a total-order sort key following BSON type ordering.

    Missing/None values sort before numbers, then strings, objects, arrays,
    booleans, and datetimes. Values of the same class compare naturally;
    values of different classes are ordered by class rank so mixed
    populations never raise ``TypeError`` (MongoDB orders by BSON type
    first).

    Use with ``sort(reverse=True)`` for descending order; missing/null then
    correctly sort last, as in MongoDB.

    Args:
        value: The value to build a key from (``None`` represents missing).

    Returns:
        tuple: A tuple that compares safely against keys of any other value.
    """
    if value is None:
        return (0, 0)
    if isinstance(value, bool):
        # bool must be tested before int (bool subclasses int)
        return (5, value)
    if isinstance(value, (int, float)):
        return (1, value)
    if isinstance(value, str):
        return (2, value)
    if isinstance(value, dict):
        canonical = tuple(
            sorted((k, bson_sort_key(v)) for k, v in value.items())
        )
        return (3, canonical)
    if isinstance(value, (list, tuple)):
        return (4, tuple(bson_sort_key(v) for v in value))
    if isinstance(value, datetime):
        aware = (
            value.astimezone(UTC)
            if value.tzinfo is not None
            else value.replace(tzinfo=UTC)
        )
        return (6, aware)
    # Unknown types: order by class name, then repr — always comparable
    return (9, type(value).__name__, repr(value))


def is_case_insensitive_collation(collation: dict[str, Any] | None) -> bool:
    """Return True when a MongoDB collation requires case-insensitive order.

    In MongoDB collation semantics, strengths ``1`` and ``2`` compare case
    insensitively unless ``caseLevel`` is ``True``. Strength ``3`` (the default)
    and higher always preserve case.

    Args:
        collation: Collation specification dictionary, or ``None``.

    Returns:
        bool: ``True`` if strings should be compared case-insensitively.
    """
    if not collation:
        return False
    strength = collation.get("strength", 3)
    case_level = collation.get("caseLevel", False)
    return bool(strength <= 2 and not case_level)


# =============================================================================
# Type Checking Helpers
# =============================================================================


def _is_expression(value: Any) -> bool:
    """Check if value is an aggregation expression.

    An expression is a dict with exactly one key starting with ``$`` that is
    not a reserved field name (``$field`` or ``$index``).

    Args:
        value: Value to check.

    Returns:
        bool: True if ``value`` is an expression, False otherwise.

    Examples:
        >>> _is_expression({"$sin": "$angle"})
        True
        >>> _is_expression({"$field": "value"})  # Reserved
        False
        >>> _is_expression("$field")
        False
        >>> _is_expression(42)
        False
    """
    # Reserved field names that are NOT operators (copied from expr_evaluator.constants
    # to avoid circular import)
    RESERVED_FIELDS = {"$field", "$index"}

    if not isinstance(value, dict):
        return False
    if len(value) != 1:
        return False  # Could be a literal dict
    key = next(iter(value.keys()))
    return key.startswith("$") and key not in RESERVED_FIELDS


def _is_field_reference(value: Any) -> bool:
    """Check if value is a field reference.

    Field references start with ``$`` but are not system variables (``$$``) or
    expression dicts (e.g. ``"$field"`` or ``"$nested.field"``).

    Args:
        value: Value to check.

    Returns:
        bool: True if ``value`` is a field reference, False otherwise.

    Examples:
        >>> _is_field_reference("$field")
        True
        >>> _is_field_reference("$nested.field")
        True
        >>> _is_field_reference("$$ROOT")
        False
        >>> _is_field_reference({"$sin": "$angle"})
        False
    """
    return (
        isinstance(value, str)
        and value.startswith("$")
        and not value.startswith("$$")
    )


def _is_literal(value: Any) -> bool:
    """Check if value is a literal rather than a field reference or variable.

    Literals include numbers, strings not starting with ``$``, booleans,
    ``None``, arrays, and plain dicts.

    Args:
        value: Value to check.

    Returns:
        bool: True if ``value`` is a literal, False otherwise.

    Examples:
        >>> _is_literal(42)
        True
        >>> _is_literal("string")
        True
        >>> _is_literal(True)
        True
        >>> _is_literal(None)
        True
        >>> _is_literal([1, 2, 3])
        True
        >>> _is_literal("$field")
        False
    """
    if isinstance(value, str):
        # Strings starting with $ are field refs or variables, not literals
        return not value.startswith("$")
    # All other types are literals
    return True


def _is_numeric_value(value: Any) -> bool:
    """Check if a value is a finite numeric int or float.

    Determines whether ``value`` can be used in arithmetic updates such as
    ``$inc`` and ``$mul``. Treats ``int`` and finite ``float`` as numeric while
    excluding ``bool``, ``NaN``, ``inf``, ``None``, and numeric strings.

    Args:
        value: The value to check.

    Returns:
        bool: True if the value is a finite non-bool number, False otherwise.
    """
    # Explicitly exclude boolean values (even though bool is subclass of int in Python)
    if isinstance(value, bool):
        return False

    # Check for actual numeric types
    if isinstance(value, (int, float)):
        # Special case: check for NaN and infinity
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return False
        return True

    # Everything else is considered non-numeric for MongoDB compatibility
    return False


def validate_session(session: Any | None, connection: Any) -> None:
    """Validate that the session is active and belongs to the given connection.

    Args:
        session: ``ClientSession`` instance or ``None``.
        connection: Parent ``Connection`` or ``sqlite3.Connection`` to check.

    Raises:
        InvalidOperation: If the session has already ended.
        ValueError: If the session belongs to a different connection.
    """
    if session is not None:
        if getattr(session, "has_ended", False):
            raise InvalidOperation("Cannot use ended ClientSession")
        # connection could be neosqlite.Connection or sqlite3.Connection
        # session.client is neosqlite.Connection
        # neosqlite.Connection.db is sqlite3.Connection
        if session.client != connection and session.client.db != connection:
            raise ValueError("Session belongs to a different Connection")


__all__ = [
    # Type conversion functions
    "_convert_to_int",
    "_convert_to_long",
    "_convert_to_double",
    "_convert_to_decimal",
    "_convert_to_string",
    "_convert_to_bool",
    "_convert_to_objectid",
    "_convert_to_bindata",
    "_convert_to_bsonbindata",
    "_convert_to_regex",
    "_convert_to_bsonregex",
    "_convert_to_date",
    "_convert_to_null",
    "get_bson_type",
    "bson_sort_key",
    "_hashable_group_key",
    # SQL query helpers
    "is_encoded_json_value",
    "serialize_sql_query_value",
    "serialize_in_values",
    "json_type_guard_sql",
    "json_types_for_values",
    "json_type_list_sql",
    "regex_to_sql_pattern",
    "build_json_in_clause",
    "build_json_nin_clause",
    "build_json_containment_clause",
    "expr_truthiness_sql",
    # Type checking helpers
    "_is_expression",
    "_is_field_reference",
    "_is_literal",
    "_is_numeric_value",
    "validate_session",
]


def is_encoded_json_value(value: Any) -> bool:
    """Return True if value is stored as an encoded JSON wrapper object.

    Detects ``ObjectId`` instances and ``bytes`` subclasses exposing an
    ``encode_for_storage`` method (such as ``Binary``).

    Args:
        value: Python query or document value to check.

    Returns:
        bool: True if ``value`` is serialized as a JSON wrapper object.
    """
    from ..objectid import ObjectId

    return isinstance(value, ObjectId) or (
        isinstance(value, bytes) and hasattr(value, "encode_for_storage")
    )


def serialize_sql_query_value(
    value: Any, jsonb_supported: bool = False
) -> tuple[Any, str]:
    """Serialize a Python query value and return ``(sql_param, placeholder)``.

    ``ObjectId`` and ``Binary`` values are stored as JSON objects in ``data``;
    under JSONB mode, ``jsonb(?)`` lets SQLite compare JSONB-extracted or
    iterated values directly against compact JSON text.

    Args:
        value: Python query value to bind in a SQL condition.
        jsonb_supported: Whether SQLite JSONB comparisons are active.

    Returns:
        tuple[Any, str]: Bound SQL parameter and SQL placeholder (``"?"`` or
        ``"jsonb(?)"``).
    """
    if is_encoded_json_value(value):
        from .json_helpers import neosqlite_json_dumps_for_sql

        placeholder = (
            json_constructor("?", jsonb_supported) if jsonb_supported else "?"
        )
        return neosqlite_json_dumps_for_sql(value), placeholder
    return value, "?"


def serialize_in_values(
    values: list[Any] | tuple[Any, ...], jsonb_supported: bool = False
) -> tuple[str, list[Any]]:
    """Serialize values for a SQL ``IN (...)`` list.

    Calls ``serialize_sql_query_value`` on each element and joins the
    resulting placeholders into a comma-separated SQL fragment.

    Args:
        values: Sequence of Python query values to serialize.
        jsonb_supported: Whether SQLite JSONB comparisons are active.

    Returns:
        tuple[str, list[Any]]: Tuple of ``(placeholders_sql, params)``.
    """
    pairs = [serialize_sql_query_value(v, jsonb_supported) for v in values]
    return ", ".join(ph for _, ph in pairs), [p for p, _ in pairs]


def _bson_json_types(value: Any) -> list[str]:
    """Return SQLite ``json_type`` / ``json_each.type`` names for a value.

    Args:
        value: Python value whose compatible SQLite JSON types are needed.

    Returns:
        list[str]: Matching SQLite JSON type names, or ``[]`` if unknown.
    """
    if isinstance(value, bool):
        return ["true", "false"]
    if isinstance(value, (int, float)):
        return ["integer", "real"]
    if isinstance(value, str):
        return ["text"]
    if isinstance(value, list):
        return ["array"]
    if isinstance(value, dict) or is_encoded_json_value(value):
        return ["object"]
    if value is None:
        return ["null"]
    return []


def json_type_guard_sql(
    json_path: str, value: Any, data_expr: str = "data"
) -> str:
    """Return a SQL predicate restricting ``json_type`` to a value's BSON type.

    Args:
        json_path: Quoted SQL JSON path expression (e.g. ``"'$.a'"``).
        value: Python value whose SQLite JSON type should be matched.
        data_expr: SQL expression yielding the JSON column. Defaults to ``"data"``.

    Returns:
        str: SQL predicate guarding ``json_type(data_expr, json_path)``.
    """
    if value is None:
        return f"json_type({data_expr}, {json_path}) IS NULL"
    types = _bson_json_types(value)
    if not types:
        return "1"
    if len(types) == 1:
        return f"json_type({data_expr}, {json_path}) = '{types[0]}'"
    joined = ", ".join(repr(t) for t in types)
    return f"json_type({data_expr}, {json_path}) IN ({joined})"


def json_types_for_values(values: list[Any] | tuple[Any, ...]) -> list[str]:
    """Collect sorted unique SQLite ``json_type`` names for a sequence.

    Args:
        values: Sequence of Python values to inspect.

    Returns:
        list[str]: Sorted unique SQLite JSON type strings across ``values``.
    """
    types: set[str] = set()
    for val in values:
        types.update(_bson_json_types(val))
    return sorted(types)


def json_type_list_sql(value: Any) -> str:
    """Return a SQL ``IN``-list tuple of ``json_type`` names for a value.

    Args:
        value: Python value whose SQLite JSON type names are formatted.

    Returns:
        str: SQL tuple literal such as ``"('integer', 'real')"``, defaulting
        to ``"('null',)"`` when no known JSON types match.
    """
    types = _bson_json_types(value)
    if not types:
        return "('null',)"
    return "(" + ", ".join(repr(t) for t in types) + ")"


def regex_to_sql_pattern(value: Any, options: str = "") -> str | None:
    """Convert a regex string or ``re.Pattern`` to a SQLite REGEXP pattern.

    Supports compiled ``re.Pattern`` instances with string patterns and
    flags within ``IGNORECASE``, ``MULTILINE``, ``DOTALL``, ``VERBOSE``,
    ``UNICODE``, and ``ASCII``, as well as string patterns with ``options``
    characters (``i``, ``m``, ``s``, ``x``).

    Args:
        value: Compiled ``re.Pattern`` or regex pattern string.
        options: Optional MongoDB regex options string when ``value`` is a
            ``str``.

    Returns:
        str | None: SQLite-compatible pattern string with inline flags, or
        ``None`` if ``value`` uses unsupported flags/types or fails to compile.
    """
    if isinstance(value, re.Pattern):
        raw_pattern = value.pattern
        if not isinstance(raw_pattern, str):
            return None
        supported_mask = (
            re.IGNORECASE
            | re.MULTILINE
            | re.DOTALL
            | re.VERBOSE
            | re.UNICODE
            | re.ASCII
        )
        if value.flags & ~supported_mask:
            return None
        flag_str = ""
        if value.flags & re.ASCII:
            flag_str += "a"
        if value.flags & re.IGNORECASE:
            flag_str += "i"
        if value.flags & re.MULTILINE:
            flag_str += "m"
        if value.flags & re.DOTALL:
            flag_str += "s"
        if value.flags & re.VERBOSE:
            flag_str += "x"
        pattern = f"(?{flag_str}:{raw_pattern})" if flag_str else raw_pattern
    elif isinstance(value, str):
        flag_str = ""
        if options:
            opts_lower = options.lower()
            if "i" in opts_lower:
                flag_str += "i"
            if "m" in opts_lower:
                flag_str += "m"
            if "s" in opts_lower:
                flag_str += "s"
            if "x" in opts_lower:
                flag_str += "x"
        pattern = f"(?{flag_str}:{value})" if flag_str else value
    else:
        return None

    try:
        re.compile(pattern)
    except re.error:
        return None
    return pattern


def _format_types_in_sql(values: list[Any] | tuple[Any, ...]) -> str:
    """Format SQLite ``json_type`` names for values into a SQL ``IN`` tuple.

    Args:
        values: Sequence of Python values to inspect.

    Returns:
        str: SQL tuple literal of unique JSON type names, defaulting to
        ``"('null',)"`` when empty.
    """
    types = json_types_for_values(values)
    if not types:
        return "('null',)"
    return "(" + ", ".join(repr(t) for t in types) + ")"


def build_json_in_clause(
    data_expr: str,
    json_path: str,
    op_val: list[Any] | tuple[Any, ...],
    json_function_prefix: str,
    jsonb_supported: bool,
    json_each_function: str,
) -> tuple[str, list[Any]]:
    """Build SQL WHERE predicate and params for MongoDB ``$in`` on JSON fields.

    Handles empty lists (always false ``"0"``), unwrapped ``value`` array
    paths, ``None`` elements (matching missing/null fields or null array
    items), non-object scalar/array checks via ``json_each``/``jsonb_each``,
    and object values (including ``ObjectId`` and ``Binary``) via
    ``<prefix>_extract``.

    Args:
        data_expr: SQL expression yielding the JSON/JSONB document column.
        json_path: Quoted JSON path expression or ``"value"`` inside
            ``json_each``.
        op_val: Sequence of candidate values for ``$in``.
        json_function_prefix: Prefix for scalar JSON functions (``"json"`` or
            ``"jsonb"``).
        jsonb_supported: Whether ``jsonb_*`` comparison syntax is enabled.
        json_each_function: Table-valued function name (``"json_each"`` or
            ``"jsonb_each"``).

    Returns:
        tuple[str, list[Any]]: Tuple of ``(sql_clause, params)``.
    """
    if not op_val:
        return "0", []
    has_null = any(v is None for v in op_val)
    non_null = [v for v in op_val if v is not None]
    types = json_types_for_values(non_null)
    in_types_sql = _format_types_in_sql(non_null)
    each_is_jsonb = json_each_function == "jsonb_each"

    if json_path == "value":
        each_phs, each_params = serialize_in_values(non_null, each_is_jsonb)
        if has_null:
            if non_null:
                return f"(value IN ({each_phs}) OR value IS NULL)", each_params
            return "value IS NULL", []
        return f"value IN ({each_phs})", each_params

    params: list[Any] = []
    if not non_null:
        clause = (
            f"(EXISTS (SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
            f"WHERE json_each.value IS NULL AND json_type({data_expr}, {json_path}) = 'array') "
            f"OR json_type({data_expr}, {json_path}) IS NULL "
            f"OR json_type({data_expr}, {json_path}) = 'null')"
        )
        return clause, params

    each_phs, each_params = serialize_in_values(non_null, each_is_jsonb)
    if "object" not in types:
        if has_null:
            clause = (
                f"((json_type({data_expr}, {json_path}) != 'object' AND "
                f"EXISTS (SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
                f"WHERE (json_each.value IN ({each_phs}) AND json_each.type IN {in_types_sql}) "
                f"OR (json_type({data_expr}, {json_path}) = 'array' AND json_each.value IS NULL))) "
                f"OR json_type({data_expr}, {json_path}) IS NULL "
                f"OR json_type({data_expr}, {json_path}) = 'null')"
            )
        else:
            clause = (
                f"(json_type({data_expr}, {json_path}) != 'object' AND "
                f"EXISTS (SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
                f"WHERE json_each.value IN ({each_phs}) AND json_each.type IN {in_types_sql}))"
            )
        params.extend(each_params)
        return clause, params

    # Object values (ObjectId, Binary, dicts) cannot use json_each directly on scalar objects
    # because json_each iterates object keys/values rather than treating the object as one element.
    scalar_phs, scalar_params = serialize_in_values(non_null, jsonb_supported)
    extract_expr = f"{json_function_prefix}_extract({data_expr}, {json_path})"
    array_null = " OR json_each.value IS NULL" if has_null else ""
    null_pred = (
        f" OR json_type({data_expr}, {json_path}) IS NULL "
        f"OR json_type({data_expr}, {json_path}) = 'null'"
        if has_null
        else ""
    )
    non_obj_check = (
        f"json_type({data_expr}, {json_path}) != 'object'"
        if len(types) > 1
        else f"json_type({data_expr}, {json_path}) = 'array'"
    )
    clause = (
        f"((json_type({data_expr}, {json_path}) = 'object' AND "
        f"{extract_expr} IN ({scalar_phs})) OR "
        f"({non_obj_check} AND EXISTS ("
        f"SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
        f"WHERE (json_each.value IN ({each_phs}) AND json_each.type IN {in_types_sql}){array_null}))"
        f"{null_pred})"
    )
    params.extend(scalar_params)
    params.extend(each_params)
    return clause, params


def build_json_nin_clause(
    data_expr: str,
    json_path: str,
    op_val: list[Any] | tuple[Any, ...],
    json_function_prefix: str,
    jsonb_supported: bool,
    json_each_function: str,
) -> tuple[str, list[Any]]:
    """Build SQL WHERE clause and params for MongoDB ``$nin`` on JSON fields.

    Handles empty lists (always true ``"1"``), unwrapped ``value`` array
    paths, null exclusion (requiring non-missing, non-null fields and no null
    array elements when ``None`` is present), non-object fields, and object
    values (including ``ObjectId`` and ``Binary``).

    Args:
        data_expr: SQL expression yielding the JSON/JSONB document column.
        json_path: Quoted JSON path expression or ``"value"`` inside
            ``json_each``.
        op_val: Sequence of excluded values for ``$nin``.
        json_function_prefix: Prefix for scalar JSON functions (``"json"`` or
            ``"jsonb"``).
        jsonb_supported: Whether ``jsonb_*`` comparison syntax is enabled.
        json_each_function: Table-valued function name (``"json_each"`` or
            ``"jsonb_each"``).

    Returns:
        tuple[str, list[Any]]: Tuple of ``(sql_clause, params)``.
    """
    if not op_val:
        return "1", []
    has_null = any(v is None for v in op_val)
    non_null = [v for v in op_val if v is not None]
    types = json_types_for_values(non_null)
    in_types_sql = _format_types_in_sql(non_null)
    each_is_jsonb = json_each_function == "jsonb_each"

    if json_path == "value":
        each_phs, each_params = serialize_in_values(non_null, each_is_jsonb)
        if has_null:
            if non_null:
                return (
                    f"(value NOT IN ({each_phs}) AND value IS NOT NULL)",
                    each_params,
                )
            return "value IS NOT NULL", []
        return f"value NOT IN ({each_phs})", each_params

    params: list[Any] = []
    if not non_null:
        clause = (
            f"(NOT (json_type({data_expr}, {json_path}) = 'array' AND EXISTS ("
            f"SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
            f"WHERE json_each.value IS NULL)) AND "
            f"json_type({data_expr}, {json_path}) IS NOT NULL AND "
            f"json_type({data_expr}, {json_path}) != 'null')"
        )
        return clause, params

    each_phs, each_params = serialize_in_values(non_null, each_is_jsonb)
    if "object" not in types:
        if has_null:
            clause = (
                f"((json_type({data_expr}, {json_path}) = 'object' OR NOT EXISTS ("
                f"SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
                f"WHERE (json_each.value IN ({each_phs}) AND json_each.type IN {in_types_sql}) "
                f"OR json_each.value IS NULL)) AND "
                f"json_type({data_expr}, {json_path}) IS NOT NULL AND "
                f"json_type({data_expr}, {json_path}) != 'null')"
            )
        else:
            clause = (
                f"(json_type({data_expr}, {json_path}) = 'object' OR NOT EXISTS ("
                f"SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
                f"WHERE json_each.value IN ({each_phs}) AND json_each.type IN {in_types_sql}))"
            )
        params.extend(each_params)
        return clause, params

    scalar_phs, scalar_params = serialize_in_values(non_null, jsonb_supported)
    extract_expr = f"{json_function_prefix}_extract({data_expr}, {json_path})"
    array_null = " OR json_each.value IS NULL" if has_null else ""
    obj_mismatch = (
        f"NOT COALESCE((json_type({data_expr}, {json_path}) = 'object' AND "
        f"{extract_expr} IN ({scalar_phs})), 0)"
    )
    non_obj_no_match = (
        f"(json_type({data_expr}, {json_path}) = 'object' OR NOT EXISTS ("
        f"SELECT 1 FROM {json_each_function}({data_expr}, {json_path}) AS json_each "
        f"WHERE (json_each.value IN ({each_phs}) AND json_each.type IN {in_types_sql}){array_null}))"
    )
    params.extend(scalar_params)
    params.extend(each_params)
    if has_null:
        clause = (
            f"({obj_mismatch} AND {non_obj_no_match} AND "
            f"json_type({data_expr}, {json_path}) IS NOT NULL AND "
            f"json_type({data_expr}, {json_path}) != 'null')"
        )
    else:
        clause = f"({obj_mismatch} AND {non_obj_no_match})"
    return clause, params


def expr_truthiness_sql(sql_expr: str) -> str:
    """Wrap a Tier-1 ``$expr`` SQL expression in MongoDB truthiness logic.

    Args:
        sql_expr: Translated SQL expression representing an ``$expr`` value.

    Returns:
        str: SQL boolean predicate matching MongoDB truthiness semantics.
    """
    return (
        f"(WITH _et(v) AS (SELECT ({sql_expr})) "
        f"SELECT COALESCE(CASE WHEN v = json('true') THEN 1 "
        f"WHEN v = json('false') THEN 0 "
        f"ELSE COALESCE(v, 0) != 0 END, 0) FROM _et)"
    )


def build_json_containment_parts(
    data_expr: str,
    field: str,
    field_access: str,
    value: Any,
    json_each_function: str,
    *,
    jsonb_supported: bool | None = None,
) -> tuple[str, list[Any], str, list[Any]]:
    """Split scalar equality and array containment SQL arms for a field.

    Returns separate ``(scalar_sql, scalar_params, array_sql, array_params)``
    components so callers can combine them with ``OR`` or indexed ``UNION``.

    Args:
        data_expr: SQL expression yielding the JSON/JSONB document column.
        field: Dotted MongoDB field path (e.g., ``"items.tag"``).
        field_access: SQL expression extracting the field directly.
        value: Target scalar or encoded JSON value to match.
        json_each_function: Table-valued function name (``"json_each"`` or
            ``"jsonb_each"``).
        jsonb_supported: Optional explicit JSONB flag for scalar comparison
            encoding; inferred from ``field_access`` when omitted.

    Returns:
        tuple[str, list[Any], str, list[Any]]: Tuple of
        ``(scalar_sql, scalar_params, array_sql, array_params)``.
    """
    from .json_path_utils import parse_json_path

    is_field_jsonb = (
        jsonb_supported
        if jsonb_supported is not None
        else field_access.startswith("jsonb_extract(")
    )
    each_is_jsonb = json_each_function == "jsonb_each"
    scalar_val, scalar_ph = serialize_sql_query_value(value, is_field_jsonb)
    each_val, each_ph = serialize_sql_query_value(value, each_is_jsonb)
    text_val, _ = serialize_sql_query_value(value, False)

    segs = field.split(".")
    scalar_cmp = f"{field_access} = {scalar_ph}"
    if isinstance(value, (bool, int, float)):
        full_path = f"'{parse_json_path(field, preserve_id=False)}'"
        scalar_guard = json_type_guard_sql(
            full_path, value, data_expr=data_expr
        )
        scalar_cmp = f"({scalar_guard} AND {scalar_cmp})"
    arr_arms: list[str] = []
    arr_params: list[Any] = []
    for k in range(1, len(segs) + 1):
        arr_path = f"'{parse_json_path('.'.join(segs[:k]), preserve_id=False)}'"
        arr_expr = "json_extract(" + data_expr + ", " + arr_path + ")"
        rest = segs[k:]
        if rest:
            leaf_path = parse_json_path(".".join(rest), preserve_id=False)
            elem_cmp = f"json_extract(value, '{leaf_path}') = ?"
            if isinstance(value, (bool, int, float)):
                elem_cmp = f"(json_type(value, '{leaf_path}') IN {json_type_list_sql(value)} AND {elem_cmp})"
            elem_pred = f"CASE WHEN type = 'object' THEN {elem_cmp} ELSE 0 END"
            arm_param = text_val
        else:
            if isinstance(value, (bool, int, float)):
                elem_pred = (
                    f"value = {each_ph} AND type IN {json_type_list_sql(value)}"
                )
            else:
                elem_pred = f"value = {each_ph}"
            arm_param = each_val
        arr_arms.append(
            "EXISTS (SELECT 1 FROM "
            + json_each_function
            + "(CASE WHEN "
            + "json_type("
            + data_expr
            + ", "
            + arr_path
            + ") = 'array' THEN "
            + arr_expr
            + " END) WHERE "
            + elem_pred
            + ")"
        )
        arr_params.append(arm_param)
    return scalar_cmp, [scalar_val], " OR ".join(arr_arms), arr_params


def build_json_containment_clause(
    data_expr: str,
    field: str,
    field_access: str,
    value: Any,
    json_each_function: str,
    *,
    jsonb_supported: bool | None = None,
) -> tuple[str, list[Any]]:
    """Build scalar equality and array containment SQL clause for a field.

    Combines direct scalar comparison (``field_access = ?``) with ``EXISTS``
    checks over ``json_each_function`` across every dotted prefix of
    ``field`` to mirror MongoDB array traversal semantics (#99).

    Args:
        data_expr: SQL expression yielding the JSON/JSONB document column.
        field: Dotted MongoDB field path (e.g., ``"items.tag"``).
        field_access: SQL expression extracting the field directly.
        value: Target scalar or encoded JSON value to match.
        json_each_function: Table-valued function name (``"json_each"`` or
            ``"jsonb_each"``).
        jsonb_supported: Optional explicit JSONB flag for scalar comparison
            encoding; inferred from ``field_access`` when omitted.

    Returns:
        tuple[str, list[Any]]: Tuple of ``(sql_clause, params)``.
    """
    scalar_sql, scalar_params, arr_sql, arr_params = (
        build_json_containment_parts(
            data_expr,
            field,
            field_access,
            value,
            json_each_function,
            jsonb_supported=jsonb_supported,
        )
    )
    return f"({scalar_sql} OR {arr_sql})", scalar_params + arr_params


def _hashable_group_key(value: Any) -> tuple:
    """Build a type-tagged, hashable key for a ``$group`` ``_id`` value.

    MongoDB groups by array and document values as well as scalars; Python
    dicts require hashable keys, so nested structures are converted into
    canonical type-tagged tuples while keeping the original value as ``_id``
    (#103).

    Args:
        value: Any Python value representing a ``$group`` ``_id`` key.

    Returns:
        tuple: Canonical hashable type-tagged tuple representation.
    """
    match value:
        case bool():
            return ("b", value)
        case int():
            return ("i", value)
        case float():
            return ("f", repr(value))
        case str():
            return ("s", value)
        case None:
            return ("z",)
        case dict():
            return (
                "d",
                tuple(
                    sorted(
                        (k, _hashable_group_key(v)) for k, v in value.items()
                    )
                ),
            )
        case list() | tuple():
            return ("l", tuple(_hashable_group_key(v) for v in value))
        case bytes():
            return ("y", value)
        case _:
            return ("o", type(value).__name__, str(value))


_BSON_TYPE_ALIASES: dict[str, int] = {
    "double": 1,
    "string": 2,
    "object": 3,
    "array": 4,
    "bindata": 5,
    "objectid": 7,
    "bool": 8,
    "boolean": 8,
    "null": 10,
    "int": 16,
    "integer": 16,
    "long": 18,
    "decimal": 19,
    "number": 99,
}


def build_bson_type_sql(
    data_expr: str,
    json_path: str,
    type_spec: Any,
    type_expr: str | None = None,
    val_expr: str | None = None,
) -> str | None:
    """Build a SQLite predicate matching MongoDB ``$type`` semantics.

    Supports single type specifications (BSON integer code, alias string from
    ``_BSON_TYPE_ALIASES``, or Python type) as well as lists/tuples of types
    combined with ``OR``. Distinguishes plain objects from encoded
    ``ObjectId``, ``Binary``, and ``$date`` documents.

    Args:
        data_expr: SQL expression yielding the JSON/JSONB document column.
        json_path: Quoted JSON path expression (ignored if ``val_expr`` set).
        type_spec: BSON type code, alias string, Python type, or list/tuple.
        type_expr: Optional precomputed SQL ``json_type`` expression.
        val_expr: Optional SQL expression yielding an already-extracted value.

    Returns:
        str | None: SQLite predicate string, or ``None`` when any requested
        type requires Python evaluation (e.g., date, regex, unknown type) or
        when ``json_path`` is not a quoted literal without ``val_expr``.
    """
    from ..binary import Binary
    from ..objectid import ObjectId

    if isinstance(type_spec, (list, tuple)):
        if not type_spec:
            return "0"
        parts: list[str] = []
        for item in type_spec:
            part = build_bson_type_sql(
                data_expr,
                json_path,
                item,
                type_expr=type_expr,
                val_expr=val_expr,
            )
            if part is None:
                return None
            parts.append(f"({part})")
        return (
            "(" + " OR ".join(parts) + ")" if len(parts) > 1 else parts[0][1:-1]
        )

    code: int | None = None
    if isinstance(type_spec, int) and not isinstance(type_spec, bool):
        code = type_spec
    elif isinstance(type_spec, str):
        code = _BSON_TYPE_ALIASES.get(type_spec.lower())
    elif isinstance(type_spec, type):
        if type_spec is float:
            code = 1
        elif type_spec is str:
            code = 2
        elif type_spec is dict:
            code = 3
        elif type_spec is list:
            code = 4
        elif type_spec is Binary or type_spec is bytes:
            code = 5
        elif type_spec is ObjectId:
            code = 7
        elif type_spec is bool:
            code = 8
        elif type_spec is type(None):
            code = 10
        elif type_spec is int:
            code = 16

    if code is None:
        return None

    if val_expr is not None:
        eff_type_expr = type_expr or f"json_type({val_expr})"
        oid_check = f"json_extract({val_expr}, '$.__neosqlite_objectid__') = 1"
        bin_check = f"json_extract({val_expr}, '$.__neosqlite_binary__') = 1"
        date_type = f"json_type({val_expr}, '$.$date')"
    else:
        if not (json_path.startswith("'") and json_path.endswith("'")):
            return None
        inner_path = json_path[1:-1]
        eff_type_expr = type_expr or f"json_type({data_expr}, {json_path})"
        oid_check = f"json_extract({data_expr}, '{inner_path}.__neosqlite_objectid__') = 1"
        bin_check = f"json_extract({data_expr}, '{inner_path}.__neosqlite_binary__') = 1"
        date_type = f"json_type({data_expr}, '{inner_path}.$date')"

    match code:
        case 1:
            return f"{eff_type_expr} = 'real'"
        case 2:
            return f"{eff_type_expr} = 'text'"
        case 3:
            return (
                f"({eff_type_expr} = 'object' AND "
                f"COALESCE({oid_check}, 0) != 1 AND "
                f"COALESCE({bin_check}, 0) != 1 AND "
                f"{date_type} IS NULL)"
            )
        case 4:
            return f"{eff_type_expr} = 'array'"
        case 5:
            return f"({eff_type_expr} = 'object' AND {bin_check})"
        case 7:
            return f"({eff_type_expr} = 'object' AND {oid_check})"
        case 8:
            return f"{eff_type_expr} IN ('true', 'false')"
        case 10:
            return f"{eff_type_expr} = 'null'"
        case 16 | 18:
            return f"{eff_type_expr} = 'integer'"
        case 19 | 99:
            return f"{eff_type_expr} IN ('integer', 'real')"
        case _:
            return None
