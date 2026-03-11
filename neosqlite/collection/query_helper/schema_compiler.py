"""
SQL Compiler for JSON Schema validation.
Translates MongoDB $jsonSchema rules into native SQLite SQL expressions.
"""

from typing import Any, Dict, Optional


def compile_schema_to_sql(
    schema: Dict[str, Any], data_column: str = "data", jsonb: bool = False
) -> str:
    """
    Compile a JSON Schema into a SQL expression for use in a CHECK constraint.
    """
    json_func = "jsonb_extract" if jsonb else "json_extract"
    return _compile_node(schema, data_column, json_func)


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
                f"{json_func}({data_column}, '{field_path}') IS NOT NULL"
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
                    f"({json_func}({data_column}, '{field_path}') IS NULL OR ({prop_sql}))"
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
    val_expr = f"{json_func}({data_column}, '{path}')"
    if "minimum" in schema:
        clauses.append(f"{val_expr} >= {schema['minimum']}")
    if "maximum" in schema:
        clauses.append(f"{val_expr} <= {schema['maximum']}")
    if "exclusiveMinimum" in schema:
        clauses.append(f"{val_expr} > {schema['exclusiveMinimum']}")
    if "exclusiveMaximum" in schema:
        clauses.append(f"{val_expr} < {schema['exclusiveMaximum']}")

    if not clauses:
        return "1"

    return " AND ".join(clauses)


def _compile_type_check(
    data_column: str, path: str, target_type: Any, json_func: str
) -> Optional[str]:
    """Compile type checks into SQL using json_type."""
    # json_type(data, path)
    type_expr = f"json_type({data_column}, '{path}')"

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
