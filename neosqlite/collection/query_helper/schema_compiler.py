"""
SQL Compiler for JSON Schema validation.
Translates MongoDB $jsonSchema rules into native SQLite SQL expressions.
"""

from typing import Any


def compile_schema_to_sql(
    schema: dict[str, Any], data_column: str = "data", jsonb: bool = False
) -> str:
    """
    Compile a JSON Schema into a SQL expression for use in a CHECK constraint.
    """
    json_func = "jsonb_extract" if jsonb else "json_extract"
    return _compile_node(schema, data_column, json_func)


def _safe_json_path(path: str) -> str:
    """Escape a dotted path for embedding in a SQL string literal (#123)."""
    from ..json_path_utils import parse_json_path

    rel = path[2:] if path.startswith("$.") else path
    return parse_json_path(rel) if rel else "$"


def _numeric_literal(value: Any, name: str) -> str:
    """Validate and render a numeric schema bound (#123)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"JSON schema '{name}' must be a number, got {value!r}"
        )
    return repr(float(value))


def _compile_node(
    schema: Any, data_column: str, json_func: str, path: str = "$"
) -> str:
    """Recursively compile schema nodes into SQL."""
    if not isinstance(schema, dict):
        return "1"  # Always true

    clauses = []

    # 1. Handle required fields
    if "required" in schema:
        req_fields = schema["required"]
        for field in req_fields:
            field_path = f"{path}.{field}"
            clauses.append(
                f"{json_func}({data_column}, "
                f"'{_safe_json_path(field_path)}') IS NOT NULL"
            )

    # 2. Handle properties
    if "properties" in schema:
        for field, prop_schema in schema["properties"].items():
            field_path = f"{path}.{field}"
            # Only validate if the field exists (JSON Schema standard behavior)
            prop_sql = _compile_node(
                prop_schema, data_column, json_func, field_path
            )
            if prop_sql != "1":
                clauses.append(
                    f"({json_func}({data_column}, "
                    f"'{_safe_json_path(field_path)}') IS NULL OR ({prop_sql}))"
                )

    # 3. Handle type/bsonType
    # SQLite json_type returns: 'null', 'integer', 'real', 'text', 'array', 'object'
    target_type = schema.get("bsonType") or schema.get("type")
    if target_type:
        type_clause = _compile_type_check(
            data_column, path, target_type, json_func
        )
        if type_clause:
            clauses.append(type_clause)

    # 4. Handle numeric constraints
    val_expr = f"{json_func}({data_column}, '{_safe_json_path(path)}')"
    if "minimum" in schema:
        clauses.append(
            f"{val_expr} >= {_numeric_literal(schema['minimum'], 'minimum')}"
        )
    if "maximum" in schema:
        clauses.append(
            f"{val_expr} <= {_numeric_literal(schema['maximum'], 'maximum')}"
        )
    if "exclusiveMinimum" in schema:
        clauses.append(
            f"{val_expr} > {_numeric_literal(schema['exclusiveMinimum'], 'exclusiveMinimum')}"
        )
    if "exclusiveMaximum" in schema:
        clauses.append(
            f"{val_expr} < {_numeric_literal(schema['exclusiveMaximum'], 'exclusiveMaximum')}"
        )

    if not clauses:
        return "1"

    return " AND ".join(clauses)


def _compile_type_check(
    data_column: str, path: str, target_type: Any, json_func: str
) -> str | None:
    """Compile type checks into SQL using json_type."""
    # json_type(data, path)
    type_expr = f"json_type({data_column}, '{_safe_json_path(path)}')"

    types = target_type if isinstance(target_type, list) else [target_type]
    sql_types = []

    for t in types:
        match t:
            case "string":
                sql_types.append("'text'")
            case "number":
                sql_types.extend(["'integer'", "'real'"])
            case "integer" | "int" | "long":
                sql_types.append("'integer'")
            case "double" | "decimal":
                sql_types.append("'real'")
            case "object":
                sql_types.append("'object'")
            case "array":
                sql_types.append("'array'")
            case "bool" | "boolean":
                # SQLite stores bools as 0/1, json_type might not distinguish well
                # but usually it's handled.
                pass
            case "null":
                sql_types.append("'null'")

    if not sql_types:
        return None

    return f"{type_expr} IN ({', '.join(sql_types)})"


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
    schema: Any, data_column: str = "data", jsonb: bool = False
) -> tuple[str, list[Any]] | None:
    """Compile a MongoDB ``$jsonSchema`` query dict into parameterized SQL.

    Returns ``(sql_condition, params)`` when every constraint in ``schema`` has
    an exact SQLite equivalent matching Python ``matches_json_schema``.
    Returns ``None`` for empty schemas, non-dicts, ``_id`` rules, or any
    unsupported keyword/type so callers fall back to Python evaluation.
    """
    if not isinstance(schema, dict) or not schema:
        return None
    json_func = "jsonb_extract" if jsonb else "json_extract"
    each_func = "jsonb_each" if jsonb else "json_each"
    compiled = _compile_query_node(
        schema,
        data_column,
        "$",
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
    """Compile schema type/bsonType check into SQLite JSON type predicate."""
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
                    oid_ext = (
                        f"json_extract({val_expr_override}, "
                        f"'$.__neosqlite_objectid__')"
                    )
                    bin_ext = (
                        f"json_extract({val_expr_override}, "
                        f"'$.__neosqlite_binary__')"
                    )
                    date_tp = f"json_type({val_expr_override}, '$.$date')"
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
                    f"({type_expr} = 'object' AND "
                    f"COALESCE({oid_ext}, 0) != 1 AND "
                    f"COALESCE({bin_ext}, 0) != 1 AND "
                    f"{date_tp} IS NULL)"
                )
            case "objectId":
                if val_expr_override is not None:
                    oid_ext = (
                        f"json_extract({val_expr_override}, "
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
                    f"(({type_expr} = 'object' AND {oid_ext} = 1) OR "
                    f"({type_expr} = 'text' AND length({text_val}) = 24 AND "
                    f"{text_val} REGEXP '^[0-9a-fA-F]{{24}}$'))"
                )
            case _:
                return None

    return "(" + " OR ".join(arms) + ")" if len(arms) > 1 else arms[0]


def _compile_query_enum_check(
    enum_vals: Any,
    type_expr: str,
    val_expr: str,
) -> tuple[str, list[Any]] | None:
    """Compile scalar schema ``enum`` checks into typed SQL predicates."""
    if not isinstance(enum_vals, list):
        return None
    if not enum_vals:
        return "0", []

    arms: list[str] = []
    params: list[Any] = []
    for item in enum_vals:
        if isinstance(item, bool):
            arms.append(
                f"{type_expr} = '{'true' if item else 'false'}'"
            )
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
    path: str,
    json_func: str,
    each_func: str,
    is_root: bool,
    aux_counter: list[int],
    type_override: str | None,
    val_override: str | None,
) -> tuple[str, list[Any]] | None:
    """Recursively compile a ``$jsonSchema`` node into parameterized SQL."""
    import re

    if not isinstance(schema, dict):
        return "1", []

    if any(k not in _SUPPORTED_QUERY_SCHEMA_KEYS for k in schema):
        return None

    safe_path = _safe_json_path(path)
    type_expr = (
        type_override
        if (type_override is not None and path == "$")
        else f"json_type({data_expr}, '{safe_path}')"
    )
    val_expr = (
        val_override
        if (val_override is not None and path == "$")
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
            val_expr_override=val_expr if (val_override and path == "$") else None,
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
                req_safe = _safe_json_path(f"{path}.{req_field}")
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
                field_path = f"{path}.{field}"
                field_safe = _safe_json_path(field_path)
                compiled_prop = _compile_query_node(
                    prop_schema,
                    data_expr,
                    field_path,
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
                f"(CASE WHEN {type_expr} = 'array' THEN json_array_length({val_expr}) {sql_op} ? ELSE 1 END)"
            )
            params.append(bound)

    if "items" in schema:
        items_schema = schema["items"]
        if isinstance(items_schema, dict):
            alias = f"_js{aux_counter[0]}"
            aux_counter[0] += 1
            compiled_item = _compile_query_node(
                items_schema,
                f"{alias}.value",
                "$",
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
                    f"{each_func}({val_override})"
                    if (val_override is not None and path == "$")
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
                    path,
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
                path,
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
                    path,
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
            path,
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
    """Return True if ``match_spec`` has any ``$jsonSchema`` SQL cannot compile."""
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
