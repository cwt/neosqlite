"""SQL Compiler for JSON Schema validation.

Translates MongoDB $jsonSchema rules into native SQLite SQL expressions.
"""

import re
from typing import Any

from ..jsonb_support import json_function


def _format_schema_segment(part: str) -> str:
    """Format one schema property key segment safely as a SQLite JSON path part."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part):
        return part
    escaped = part.replace("\\", "\\\\").replace('"', '\\"').replace("'", "''")
    return f'"{escaped}"'


def _schema_path_from_parts(parts: tuple[str, ...]) -> str:
    """Build a SQLite JSON path string from literal property path segments."""
    if not parts:
        return "$"
    return "$." + ".".join(_format_schema_segment(p) for p in parts)


def _safe_json_path(path: str) -> str:
    """Escape a dotted JSON path for embedding inside a SQL string literal.

    Strips a leading ``"$."`` prefix and delegates to ``parse_json_path`` so
    single quotes in field names are doubled for SQLite safety (#123).

    Args:
        path: JSON path string (e.g., ``"$"`` or ``"$.user.name"``).

    Returns:
        str: SQL-safe JSON path string starting with ``"$"``.
    """
    from ..json_path_utils import parse_json_path

    rel = path.removeprefix("$.")
    return parse_json_path(rel) if rel else "$"


def _numeric_literal(value: Any, name: str) -> str:
    """Validate and format a numeric JSON Schema bound as a SQL float literal.

    Args:
        value: Candidate bound value from a schema keyword.
        name: Schema keyword name used in error reporting.

    Returns:
        str: ``repr(float(value))`` suitable for embedding in SQL.

    Raises:
        ValueError: If ``value`` is a ``bool`` or not an ``int`` or ``float``.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"JSON schema '{name}' must be a number, got {value!r}"
        )
    return repr(float(value))


def compile_schema_to_sql(
    schema: dict[str, Any], data_column: str = "data", jsonb: bool = False
) -> str:
    """Compile a JSON Schema into a SQLite ``CHECK`` constraint expression.

    Args:
        schema: JSON Schema dictionary describing validation rules.
        data_column: Name of the SQLite JSON column to validate. Defaults to
            ``"data"``.
        jsonb: Whether to emit ``jsonb_extract`` instead of ``json_extract``.
            Defaults to ``False``.

    Returns:
        str: SQL boolean expression (or ``"1"`` when unconstrained).
    """
    json_func = json_function("extract", jsonb)
    sql_expr, _ = _compile_node_with_status(
        schema, data_column, json_func, path_parts=(), is_root=True
    )
    return sql_expr


def schema_needs_python_validation(schema: Any) -> bool:
    """Return True if ``schema`` contains constraints SQLite ``CHECK`` cannot enforce."""
    if not isinstance(schema, dict) or not schema:
        return False
    _, complete = _compile_node_with_status(
        schema, "data", "json_extract", path_parts=(), is_root=True
    )
    return not complete


def _compile_check_enum_literal(
    enum_vals: Any,
    type_expr: str,
    val_expr: str,
) -> str | None:
    """Compile scalar ``enum`` items into SQL literal predicates for ``CHECK`` constraints."""
    if not isinstance(enum_vals, list):
        return None
    if not enum_vals:
        return "0"

    arms: list[str] = []
    for item in enum_vals:
        if isinstance(item, bool):
            arms.append(f"{type_expr} = '{'true' if item else 'false'}'")
        elif isinstance(item, (int, float)):
            arms.append(
                f"({type_expr} IN ('integer', 'real') AND {val_expr} = {repr(item)})"
            )
        elif isinstance(item, str):
            if "\x00" in item:
                return None
            escaped = item.replace("'", "''")
            arms.append(f"({type_expr} = 'text' AND {val_expr} = '{escaped}')")
        elif item is None:
            arms.append(f"{type_expr} = 'null'")
        else:
            return None

    return "(" + " OR ".join(arms) + ")"


def _compile_node(
    schema: Any, data_column: str, json_func: str, path: str = "$"
) -> str:
    """Compile a ``CHECK``-constraint schema node into SQL."""
    rel = path.removeprefix("$.")
    parts = tuple(rel.split(".")) if rel and rel != "$" else ()
    sql_expr, _ = _compile_node_with_status(
        schema, data_column, json_func, path_parts=parts, is_root=(not parts)
    )
    return sql_expr


_CHECK_COMPILABLE_KEYS = frozenset(
    {
        "bsonType",
        "type",
        "required",
        "properties",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "enum",
        "anyOf",
        "allOf",
        "oneOf",
        "not",
        "title",
        "description",
    }
)


def _compile_node_with_status(
    schema: Any,
    data_column: str,
    json_func: str,
    path_parts: tuple[str, ...] = (),
    is_root: bool = True,
) -> tuple[str, bool]:
    """Recursively compile a ``CHECK``-constraint schema node into SQL.

    Returns ``(sql_expr, is_complete)`` where ``is_complete`` is ``False`` if
    any keyword or constraint could not be faithfully expressed in SQLite
    ``CHECK`` (such as ``pattern``, ``items``, ``objectId``, or non-scalar
    ``enum`` values).
    """
    if not isinstance(schema, dict):
        return "1", True

    complete = not any(k not in _CHECK_COMPILABLE_KEYS for k in schema)
    safe_path = _schema_path_from_parts(path_parts)
    type_expr = f"json_type({data_column}, '{safe_path}')"
    val_expr = f"{json_func}({data_column}, '{safe_path}')"
    clauses: list[str] = []

    # 1. Handle required fields (json_type IS NOT NULL distinguishes present null from missing)
    if "required" in schema:
        req_fields = schema["required"]
        if not isinstance(req_fields, list) or not all(
            isinstance(f, str) for f in req_fields
        ):
            complete = False
        elif req_fields:
            req_parts = []
            for field in req_fields:
                if is_root and field == "_id":
                    complete = False
                field_safe = _schema_path_from_parts((*path_parts, field))
                req_parts.append(
                    f"json_type({data_column}, '{field_safe}') IS NOT NULL"
                )
            req_sql = " AND ".join(req_parts)
            if is_root:
                clauses.append(req_sql)
            else:
                clauses.append(f"({type_expr} != 'object' OR ({req_sql}))")

    # 2. Handle properties (presence checked via json_type so explicit null validates against schema)
    if "properties" in schema:
        props = schema["properties"]
        if not isinstance(props, dict):
            complete = False
        elif props:
            prop_clauses: list[str] = []
            for field, prop_schema in props.items():
                if not isinstance(field, str):
                    complete = False
                    continue
                if is_root and field == "_id":
                    complete = False
                field_parts = (*path_parts, field)
                field_safe = _schema_path_from_parts(field_parts)
                prop_sql, prop_complete = _compile_node_with_status(
                    prop_schema,
                    data_column,
                    json_func,
                    path_parts=field_parts,
                    is_root=False,
                )
                if not prop_complete:
                    complete = False
                if prop_sql != "1":
                    prop_clauses.append(
                        f"(json_type({data_column}, '{field_safe}') IS NULL OR ({prop_sql}))"
                    )
            if prop_clauses:
                all_props_sql = " AND ".join(prop_clauses)
                if is_root:
                    clauses.append(all_props_sql)
                else:
                    clauses.append(
                        f"({type_expr} != 'object' OR ({all_props_sql}))"
                    )

    # 3. Handle type/bsonType
    if "bsonType" in schema:
        target_type = schema["bsonType"]
    elif "type" in schema:
        target_type = schema["type"]
    else:
        target_type = None
    if target_type is not None:
        type_clause = _compile_type_check(
            data_column, safe_path, target_type, json_func
        )
        if type_clause is not None:
            clauses.append(type_clause)
        else:
            complete = False

    # 4. Handle numeric constraints (guarded so strings/bools are not coerced)
    for kw, sql_op in (
        ("minimum", ">="),
        ("maximum", "<="),
        ("exclusiveMinimum", ">"),
        ("exclusiveMaximum", "<"),
    ):
        if kw in schema:
            lit = _numeric_literal(schema[kw], kw)
            clauses.append(
                f"({type_expr} NOT IN ('integer', 'real') OR {val_expr} {sql_op} {lit})"
            )

    # 5. Handle string length constraints (guarded to text values)
    for kw, sql_op in (("minLength", ">="), ("maxLength", "<=")):
        if kw in schema:
            bound = schema[kw]
            if isinstance(bound, bool) or not isinstance(bound, int):
                complete = False
            else:
                clauses.append(
                    f"({type_expr} != 'text' OR length({val_expr}) {sql_op} {bound})"
                )

    # 6. Handle array length constraints (guarded to array values)
    for kw, sql_op in (("minItems", ">="), ("maxItems", "<=")):
        if kw in schema:
            bound = schema[kw]
            if isinstance(bound, bool) or not isinstance(bound, int):
                complete = False
            else:
                clauses.append(
                    f"({type_expr} != 'array' OR json_array_length(CASE WHEN {type_expr} = 'array' THEN {val_expr} ELSE NULL END) {sql_op} {bound})"
                )

    # 7. Handle scalar enum constraints
    if "enum" in schema:
        enum_sql = _compile_check_enum_literal(
            schema["enum"], type_expr, val_expr
        )
        if enum_sql is not None:
            clauses.append(enum_sql)
        else:
            complete = False

    # 8. Handle logical combinators
    if "allOf" in schema:
        subs = schema["allOf"]
        if not isinstance(subs, list):
            complete = False
        else:
            for sub in subs:
                sub_sql, sub_complete = _compile_node_with_status(
                    sub,
                    data_column,
                    json_func,
                    path_parts=path_parts,
                    is_root=is_root,
                )
                if not sub_complete:
                    complete = False
                if sub_sql != "1":
                    clauses.append(f"({sub_sql})")

    if "anyOf" in schema:
        subs = schema["anyOf"]
        if not isinstance(subs, list):
            complete = False
        elif not subs:
            clauses.append("0")
        else:
            sub_sqls: list[str] = []
            all_ok = True
            for sub in subs:
                s_sql, s_comp = _compile_node_with_status(
                    sub,
                    data_column,
                    json_func,
                    path_parts=path_parts,
                    is_root=is_root,
                )
                if not s_comp:
                    all_ok = False
                sub_sqls.append(f"({s_sql})")
            if all_ok:
                clauses.append("(" + " OR ".join(sub_sqls) + ")")
            else:
                complete = False

    if "oneOf" in schema:
        subs = schema["oneOf"]
        if not isinstance(subs, list):
            complete = False
        elif not subs:
            clauses.append("0")
        else:
            sum_terms: list[str] = []
            all_ok = True
            for sub in subs:
                s_sql, s_comp = _compile_node_with_status(
                    sub,
                    data_column,
                    json_func,
                    path_parts=path_parts,
                    is_root=is_root,
                )
                if not s_comp:
                    all_ok = False
                sum_terms.append(f"(CASE WHEN ({s_sql}) THEN 1 ELSE 0 END)")
            if all_ok:
                clauses.append("(" + " + ".join(sum_terms) + ") = 1")
            else:
                complete = False

    if "not" in schema:
        n_sql, n_comp = _compile_node_with_status(
            schema["not"],
            data_column,
            json_func,
            path_parts=path_parts,
            is_root=is_root,
        )
        if n_comp:
            clauses.append(f"NOT COALESCE(({n_sql}), 0)")
        else:
            complete = False

    if not clauses:
        return "1", complete

    return " AND ".join(clauses), complete


def _compile_type_check(
    data_column: str, path: str, target_type: Any, json_func: str
) -> str | None:
    """Compile a ``CHECK``-constraint type check using ``json_type``."""
    safe_path = path if path.startswith("$") else _safe_json_path(path)
    type_expr = f"json_type({data_column}, '{safe_path}')"

    types = target_type if isinstance(target_type, list) else [target_type]
    if not types:
        return "0"

    arms: list[str] = []
    for t in types:
        match t:
            case "string":
                arms.append(f"{type_expr} = 'text'")
            case "number":
                arms.append(f"{type_expr} IN ('integer', 'real')")
            case "integer" | "int" | "long":
                arms.append(f"{type_expr} = 'integer'")
            case "double" | "decimal":
                arms.append(f"{type_expr} IN ('integer', 'real')")
            case "object":
                oid_ext = f"{json_func}({data_column}, '{safe_path}.__neosqlite_objectid__')"
                bin_ext = f"{json_func}({data_column}, '{safe_path}.__neosqlite_binary__')"
                date_tp = f"json_type({data_column}, '{safe_path}.$date')"
                arms.append(
                    f"(CASE WHEN {type_expr} = 'object' THEN "
                    f"(COALESCE({oid_ext}, 0) != 1 AND "
                    f"COALESCE({bin_ext}, 0) != 1 AND "
                    f"{date_tp} IS NULL) ELSE 0 END)"
                )
            case "array":
                arms.append(f"{type_expr} = 'array'")
            case "bool" | "boolean":
                arms.append(f"{type_expr} IN ('true', 'false')")
            case "null":
                arms.append(f"{type_expr} = 'null'")
            case "binData":
                bin_ext = f"{json_func}({data_column}, '{safe_path}.__neosqlite_binary__')"
                arms.append(
                    f"(CASE WHEN {type_expr} = 'object' THEN COALESCE({bin_ext}, 0) = 1 ELSE 0 END)"
                )
            case "date":
                date_tp = f"json_type({data_column}, '{safe_path}.$date')"
                arms.append(
                    f"(CASE WHEN {type_expr} = 'object' THEN {date_tp} IS NOT NULL ELSE 0 END)"
                )
            case _:
                return None

    return "(" + " OR ".join(arms) + ")" if len(arms) > 1 else arms[0]


_SUPPORTED_QUERY_SCHEMA_KEYS = frozenset(
    {
        "bsonType",
        "type",
        "required",
        "properties",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "items",
        "enum",
        "anyOf",
        "allOf",
        "oneOf",
        "not",
        "title",
        "description",
    }
)


def compile_query_schema_to_sql(
    schema: Any,
    data_column: str = "data",
    jsonb: bool = False,
    json_each_function: str = "json_each",
) -> tuple[str, list[Any]] | None:
    """Compile a MongoDB ``$jsonSchema`` query dict into parameterized SQL.

    Returns ``(sql_condition, params)`` when every constraint in ``schema`` has
    an exact SQLite equivalent matching Python ``matches_json_schema``.
    Returns ``None`` for empty schemas, non-dicts, trivial ``"1"`` predicates,
    ``_id`` rules, or any unsupported keyword/type so callers fall back to
    Python evaluation.

    Args:
        schema: MongoDB ``$jsonSchema`` specification object.
        data_column: SQLite column or expression containing the JSON document.
            Defaults to ``"data"``.
        jsonb: Whether to use ``jsonb_extract`` instead of ``json_extract``.
            Defaults to ``False``.
        json_each_function: Table-valued JSON array function name (e.g.,
            ``"json_each"`` or ``"jsonb_each"``). Defaults to ``"json_each"``.

    Returns:
        tuple[str, list[Any]] | None: Parameterized ``(sql_condition, params)``
            tuple, or ``None`` when SQL compilation is not possible or trivial.
    """
    if not isinstance(schema, dict) or not schema:
        return None
    json_func = json_function("extract", jsonb)
    # jsonb_each() only exists in SQLite 3.51+; json_each() works with both
    # TEXT JSON and JSONB, so callers on 3.45-3.50 must use it.
    each_func = json_each_function
    compiled = _compile_query_node(
        schema,
        data_column,
        (),
        json_func,
        each_func,
        is_root=True,
        aux_counter=[0],
        type_override=None,
        val_override=None,
    )
    if compiled is None:
        return None
    sql, params = compiled
    if sql == "1":
        return None
    return sql, params


def _compile_query_type_check(
    data_expr: str,
    safe_path: str,
    target_type: Any,
    type_expr_override: str | None = None,
    val_expr_override: str | None = None,
) -> str | None:
    """Compile schema ``type``/``bsonType`` into a SQLite JSON type predicate.

    Handles ``string``, ``integer``/``int``/``long``,
    ``number``/``double``/``decimal``, ``array``, ``bool``/``boolean``,
    ``null``, ``object`` (excluding encoded NeoSQLite ObjectId/Binary/$date
    wrappers), and ``objectId`` (sentinel objects or 24-character hex strings).

    Args:
        data_expr: SQL expression yielding the JSON document or sub-value.
        safe_path: SQL-safe JSON path within ``data_expr``.
        target_type: Expected type name or list of type names.
        type_expr_override: Optional precomputed SQL ``json_type`` expression.
            Defaults to ``None``.
        val_expr_override: Optional precomputed SQL value expression used when
            checking iterated array elements. Defaults to ``None``.

    Returns:
        str | None: SQL predicate expression, ``"0"`` for an empty type list,
            or ``None`` if any entry is non-string or unsupported.
    """
    type_expr = type_expr_override or f"json_type({data_expr}, '{safe_path}')"
    types = target_type if isinstance(target_type, list) else [target_type]
    if not types:
        return "0"

    arms: list[str] = []
    for t in types:
        if not isinstance(t, str):
            return None
        match t:
            case "string":
                arms.append(f"{type_expr} = 'text'")
            case "integer" | "int" | "long":
                arms.append(f"{type_expr} = 'integer'")
            case "number" | "double" | "decimal":
                arms.append(f"{type_expr} IN ('integer', 'real')")
            case "array":
                arms.append(f"{type_expr} = 'array'")
            case "bool" | "boolean":
                arms.append(f"{type_expr} IN ('true', 'false')")
            case "null":
                arms.append(f"{type_expr} = 'null'")
            case "object":
                if val_expr_override is not None:
                    safe_obj = (
                        f"(CASE WHEN {type_expr} = 'object' "
                        f"THEN {val_expr_override} ELSE NULL END)"
                    )
                    oid_ext = (
                        f"json_extract({safe_obj}, "
                        f"'$.__neosqlite_objectid__')"
                    )
                    bin_ext = (
                        f"json_extract({safe_obj}, "
                        f"'$.__neosqlite_binary__')"
                    )
                    date_tp = f"json_type({safe_obj}, '$.$date')"
                else:
                    prefix = safe_path
                    oid_ext = (
                        f"json_extract({data_expr}, "
                        f"'{prefix}.__neosqlite_objectid__')"
                    )
                    bin_ext = (
                        f"json_extract({data_expr}, "
                        f"'{prefix}.__neosqlite_binary__')"
                    )
                    date_tp = f"json_type({data_expr}, '{prefix}.$date')"
                arms.append(
                    f"(CASE WHEN {type_expr} = 'object' THEN "
                    f"(COALESCE({oid_ext}, 0) != 1 AND "
                    f"COALESCE({bin_ext}, 0) != 1 AND "
                    f"{date_tp} IS NULL) ELSE 0 END)"
                )
            case "objectId":
                if val_expr_override is not None:
                    safe_obj = (
                        f"(CASE WHEN {type_expr} = 'object' "
                        f"THEN {val_expr_override} ELSE NULL END)"
                    )
                    oid_ext = (
                        f"json_extract({safe_obj}, "
                        f"'$.__neosqlite_objectid__')"
                    )
                    text_val = val_expr_override
                else:
                    prefix = safe_path
                    oid_ext = (
                        f"json_extract({data_expr}, "
                        f"'{prefix}.__neosqlite_objectid__')"
                    )
                    text_val = f"json_extract({data_expr}, '{safe_path}')"
                arms.append(
                    f"(CASE WHEN {type_expr} = 'object' THEN ({oid_ext} = 1) "
                    f"WHEN {type_expr} = 'text' THEN "
                    f"(length({text_val}) = 24 AND "
                    f"{text_val} REGEXP '^[0-9a-fA-F]{{24}}$') ELSE 0 END)"
                )
            case _:
                return None

    return "(" + " OR ".join(arms) + ")" if len(arms) > 1 else arms[0]


def _compile_query_enum_check(
    enum_vals: Any,
    type_expr: str,
    val_expr: str,
) -> tuple[str, list[Any]] | None:
    """Compile scalar schema ``enum`` checks into typed SQL predicates.

    Supports boolean, numeric (matching Python ``1 == 1.0`` equality while
    excluding booleans), string, and ``None`` enum members.

    Args:
        enum_vals: Candidate list of allowed enum values.
        type_expr: SQL expression evaluating to the JSON value's ``json_type``.
        val_expr: SQL expression evaluating to the extracted JSON value.

    Returns:
        tuple[str, list[Any]] | None: ``(sql_predicate, params)``,
            ``("0", [])`` for an empty list, or ``None`` if ``enum_vals`` is
            not a list or contains non-scalar items.
    """
    if not isinstance(enum_vals, list):
        return None
    if not enum_vals:
        return "0", []

    arms: list[str] = []
    params: list[Any] = []
    for item in enum_vals:
        if isinstance(item, bool):
            arms.append(f"{type_expr} = '{'true' if item else 'false'}'")
        elif isinstance(item, (int, float)):
            # Match Python's 1 == 1.0 enum equality while excluding booleans.
            arms.append(
                f"({type_expr} IN ('integer', 'real') AND {val_expr} = ?)"
            )
            params.append(item)
        elif isinstance(item, str):
            arms.append(f"({type_expr} = 'text' AND {val_expr} = ?)")
            params.append(item)
        elif item is None:
            arms.append(f"{type_expr} = 'null'")
        else:
            return None

    return "(" + " OR ".join(arms) + ")", params


def _compile_query_node(
    schema: Any,
    data_expr: str,
    path_parts: tuple[str, ...],
    json_func: str,
    each_func: str,
    is_root: bool,
    aux_counter: list[int],
    type_override: str | None,
    val_override: str | None,
) -> tuple[str, list[Any]] | None:
    """Recursively compile a ``$jsonSchema`` node into parameterized SQL.

    Compiles supported keywords in ``_SUPPORTED_QUERY_SCHEMA_KEYS`` and falls
    back by returning ``None`` on unsupported keywords, invalid operands,
    non-compilable regexes, or root ``_id`` constraints.

    Args:
        schema: Current ``$jsonSchema`` sub-schema object.
        data_expr: SQL expression producing the JSON document or element.
        path_parts: Tuple of property segments relative to ``data_expr``.
        json_func: SQLite JSON extract function name.
        each_func: Table-valued JSON iteration function name.
        is_root: Whether ``schema`` sits at the document root.
        aux_counter: Single-element mutable list used to allocate unique
            table aliases for nested ``items`` subqueries.
        type_override: Optional SQL expression supplying the node's JSON type
            when at root (used for ``items`` element checks).
        val_override: Optional SQL expression supplying the node's value when
            at root (used for ``items`` element checks).

    Returns:
        tuple[str, list[Any]] | None: Parameterized ``(sql, params)`` pair
            (``("1", [])`` when unconstrained), or ``None`` on fallback.
    """
    if not isinstance(schema, dict):
        return "1", []

    if any(k not in _SUPPORTED_QUERY_SCHEMA_KEYS for k in schema):
        return None

    at_self = not path_parts
    safe_path = _schema_path_from_parts(path_parts)
    type_expr = (
        type_override
        if (type_override is not None and at_self)
        else f"json_type({data_expr}, '{safe_path}')"
    )
    val_expr = (
        val_override
        if (val_override is not None and at_self)
        else f"{json_func}({data_expr}, '{safe_path}')"
    )

    clauses: list[str] = []
    params: list[Any] = []

    if "bsonType" in schema or "type" in schema:
        target_type = (
            schema["bsonType"] if "bsonType" in schema else schema["type"]
        )
        type_sql = _compile_query_type_check(
            data_expr,
            safe_path,
            target_type,
            type_expr_override=type_expr,
            val_expr_override=(
                val_expr if (val_override and at_self) else None
            ),
        )
        if type_sql is None:
            return None
        clauses.append(type_sql)

    if "required" in schema:
        req_fields = schema["required"]
        if not isinstance(req_fields, list) or not all(
            isinstance(f, str) for f in req_fields
        ):
            return None
        if req_fields:
            req_parts: list[str] = []
            for req_field in req_fields:
                if is_root and req_field == "_id":
                    return None
                req_safe = _schema_path_from_parts((*path_parts, req_field))
                req_parts.append(
                    f"json_type({data_expr}, '{req_safe}') IS NOT NULL"
                )
            req_sql = " AND ".join(req_parts)
            if is_root:
                clauses.append(req_sql)
            else:
                clauses.append(f"({type_expr} != 'object' OR ({req_sql}))")

    if "properties" in schema:
        props = schema["properties"]
        if not isinstance(props, dict):
            return None
        if props:
            prop_parts: list[str] = []
            prop_params: list[Any] = []
            for field, prop_schema in props.items():
                if not isinstance(field, str) or (is_root and field == "_id"):
                    return None
                field_parts = (*path_parts, field)
                field_safe = _schema_path_from_parts(field_parts)
                compiled_prop = _compile_query_node(
                    prop_schema,
                    data_expr,
                    field_parts,
                    json_func,
                    each_func,
                    is_root=False,
                    aux_counter=aux_counter,
                    type_override=None,
                    val_override=None,
                )
                if compiled_prop is None:
                    return None
                prop_sql, p_params = compiled_prop
                if prop_sql != "1":
                    prop_parts.append(
                        f"(json_type({data_expr}, '{field_safe}') IS NULL OR ({prop_sql}))"
                    )
                    prop_params.extend(p_params)
            if prop_parts:
                all_props_sql = " AND ".join(prop_parts)
                if is_root:
                    clauses.append(all_props_sql)
                else:
                    clauses.append(
                        f"({type_expr} != 'object' OR ({all_props_sql}))"
                    )
                params.extend(prop_params)

    for kw, sql_op in (
        ("minimum", ">="),
        ("maximum", "<="),
        ("exclusiveMinimum", ">"),
        ("exclusiveMaximum", "<"),
    ):
        if kw in schema:
            bound = schema[kw]
            if isinstance(bound, bool) or not isinstance(bound, (int, float)):
                return None
            clauses.append(
                f"(CASE WHEN {type_expr} IN ('integer', 'real') THEN {val_expr} {sql_op} ? ELSE 1 END)"
            )
            params.append(float(bound))

    for kw, sql_op in (("minLength", ">="), ("maxLength", "<=")):
        if kw in schema:
            bound = schema[kw]
            if isinstance(bound, bool) or not isinstance(bound, int):
                return None
            clauses.append(
                f"(CASE WHEN {type_expr} = 'text' THEN length({val_expr}) {sql_op} ? ELSE 1 END)"
            )
            params.append(bound)

    if "pattern" in schema:
        pattern = schema["pattern"]
        if not isinstance(pattern, str):
            return None
        try:
            re.compile(pattern)
        except re.error:
            return None
        clauses.append(
            f"(CASE WHEN {type_expr} = 'text' THEN ({val_expr} REGEXP ?) ELSE 1 END)"
        )
        params.append(pattern)

    for kw, sql_op in (("minItems", ">="), ("maxItems", "<=")):
        if kw in schema:
            bound = schema[kw]
            if isinstance(bound, bool) or not isinstance(bound, int):
                return None
            clauses.append(
                f"(CASE WHEN {type_expr} = 'array' THEN json_array_length(CASE WHEN {type_expr} = 'array' THEN {val_expr} ELSE NULL END) {sql_op} ? ELSE 1 END)"
            )
            params.append(bound)

    if "items" in schema:
        items_schema = schema["items"]
        if isinstance(items_schema, dict):
            alias = f"_js{aux_counter[0]}"
            aux_counter[0] += 1
            compiled_item = _compile_query_node(
                items_schema,
                f"(CASE WHEN {alias}.type IN ('object', 'array') THEN {alias}.value ELSE NULL END)",
                (),
                "json_extract",
                each_func,
                is_root=False,
                aux_counter=aux_counter,
                type_override=f"{alias}.type",
                val_override=f"{alias}.value",
            )
            if compiled_item is None:
                return None
            item_sql, item_params = compiled_item
            if item_sql != "1":
                each_source = (
                    f"{each_func}((CASE WHEN {type_expr} = 'array' THEN {val_override} ELSE '[]' END))"
                    if (val_override is not None and at_self)
                    else f"{each_func}({data_expr}, '{safe_path}')"
                )
                clauses.append(
                    f"(CASE WHEN {type_expr} = 'array' THEN "
                    f"NOT EXISTS (SELECT 1 FROM {each_source} AS {alias} "
                    f"WHERE NOT COALESCE(({item_sql}), 0)) ELSE 1 END)"
                )
                params.extend(item_params)

    if "enum" in schema:
        compiled_enum = _compile_query_enum_check(
            schema["enum"], type_expr, val_expr
        )
        if compiled_enum is None:
            return None
        enum_sql, enum_params = compiled_enum
        clauses.append(enum_sql)
        params.extend(enum_params)

    if "anyOf" in schema:
        subschemas = schema["anyOf"]
        if not isinstance(subschemas, list):
            return None
        if not subschemas:
            clauses.append("0")
        else:
            sub_parts: list[str] = []
            for sub in subschemas:
                compiled_sub = _compile_query_node(
                    sub,
                    data_expr,
                    path_parts,
                    json_func,
                    each_func,
                    is_root=is_root,
                    aux_counter=aux_counter,
                    type_override=type_override,
                    val_override=val_override,
                )
                if compiled_sub is None:
                    return None
                s_sql, s_params = compiled_sub
                sub_parts.append(f"({s_sql})")
                params.extend(s_params)
            clauses.append("(" + " OR ".join(sub_parts) + ")")

    if "allOf" in schema:
        subschemas = schema["allOf"]
        if not isinstance(subschemas, list):
            return None
        for sub in subschemas:
            compiled_sub = _compile_query_node(
                sub,
                data_expr,
                path_parts,
                json_func,
                each_func,
                is_root=is_root,
                aux_counter=aux_counter,
                type_override=type_override,
                val_override=val_override,
            )
            if compiled_sub is None:
                return None
            s_sql, s_params = compiled_sub
            if s_sql != "1":
                clauses.append(f"({s_sql})")
                params.extend(s_params)

    if "oneOf" in schema:
        subschemas = schema["oneOf"]
        if not isinstance(subschemas, list):
            return None
        if not subschemas:
            clauses.append("0")
        else:
            sum_terms: list[str] = []
            for sub in subschemas:
                compiled_sub = _compile_query_node(
                    sub,
                    data_expr,
                    path_parts,
                    json_func,
                    each_func,
                    is_root=is_root,
                    aux_counter=aux_counter,
                    type_override=type_override,
                    val_override=val_override,
                )
                if compiled_sub is None:
                    return None
                s_sql, s_params = compiled_sub
                sum_terms.append(f"(CASE WHEN ({s_sql}) THEN 1 ELSE 0 END)")
                params.extend(s_params)
            clauses.append("(" + " + ".join(sum_terms) + ") = 1")

    if "not" in schema:
        compiled_not = _compile_query_node(
            schema["not"],
            data_expr,
            path_parts,
            json_func,
            each_func,
            is_root=is_root,
            aux_counter=aux_counter,
            type_override=type_override,
            val_override=val_override,
        )
        if compiled_not is None:
            return None
        n_sql, n_params = compiled_not
        clauses.append(f"NOT COALESCE(({n_sql}), 0)")
        params.extend(n_params)

    if not clauses:
        return "1", []
    if len(clauses) == 1:
        return clauses[0], params
    return "(" + " AND ".join(f"({c})" for c in clauses) + ")", params


def match_has_uncompilable_schema(match_spec: Any) -> bool:
    """Return whether ``match_spec`` contains an uncompilable ``$jsonSchema``.

    Recursively inspects ``$jsonSchema`` keys at top level and within logical
    combinators ``$and``, ``$or``, ``$nor``, and ``$not``.

    Args:
        match_spec: ``$match`` stage filter dictionary or arbitrary value.

    Returns:
        bool: ``True`` if any embedded ``$jsonSchema`` cannot be compiled to
            SQL by ``compile_query_schema_to_sql``, else ``False``.
    """
    if not isinstance(match_spec, dict):
        return False
    if "$jsonSchema" in match_spec:
        if compile_query_schema_to_sql(match_spec["$jsonSchema"]) is None:
            return True
    for op in ("$and", "$or", "$nor"):
        if op in match_spec and isinstance(match_spec[op], list):
            if any(match_has_uncompilable_schema(s) for s in match_spec[op]):
                return True
    if "$not" in match_spec and isinstance(match_spec["$not"], dict):
        if match_has_uncompilable_schema(match_spec["$not"]):
            return True
    return False
