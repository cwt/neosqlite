"""Shared utility module for JSON path parsing functionality.

Provides common JSON path parsing and SQLite JSON/JSONB extraction helpers to
avoid code duplication across modules, converting dot notation with optional
array indexing into SQLite JSON path syntax.
"""


def parse_json_path(field: str) -> str:
    """Convert dot notation with array indexing to SQLite JSON path syntax.

    Supports:

    - Simple fields: ``"name"`` -> ``"$.name"``
    - Nested fields: ``"address.street"`` -> ``"$.address.street"``
    - Array indexing: ``"tags[0]"`` -> ``"$.tags[0]"``
    - Nested array access: ``"orders.items[2].name"`` ->
      ``"$.orders.items[2].name"``
    - Complex paths: ``"a.b[0].c[1].d"`` -> ``"$.a.b[0].c[1].d"``

    Returns ``"_id"`` unchanged and escapes single quotes in path segments by
    doubling them for SQLite string safety.

    Args:
        field: Field path in dot notation with optional array indices.

    Returns:
        str: Properly formatted SQLite JSON path (or ``"_id"``).
    """
    # Handle special case for _id
    if field == "_id":
        return field

    # Split the field path by dots while preserving array indices
    parts = []
    current_part = ""

    i = 0
    while i < len(field):
        if field[i] == ".":
            if current_part:
                parts.append(current_part)
                current_part = ""
        elif field[i] == "[":
            # Find the closing bracket
            bracket_end = field.find("]", i)
            if bracket_end != -1:
                # Add the array index to current part
                current_part += field[i : bracket_end + 1]
                i = bracket_end
            else:
                # Malformed array index, treat as regular character
                current_part += field[i]
        else:
            current_part += field[i]
        i += 1

    # Add the last part
    if current_part:
        parts.append(current_part)

    # Convert each part to JSON path format
    json_parts = []
    for part in parts:
        # Escape single quotes in the field name by doubling them for SQLite
        part = part.replace("'", "''")

        # Check if part contains array indices
        if "[" in part:
            # Split field name from array indices
            field_name_end = part.find("[")
            field_name = part[:field_name_end]
            array_indices = part[field_name_end:]
            json_parts.append(f"{field_name}{array_indices}")
        else:
            json_parts.append(part)

    return f"$.{'.'.join(json_parts)}"


def build_json_extract_expression(data_column: str, field_path: str) -> str:
    """Build a ``json_extract`` SQL expression with a formatted JSON path.

    Args:
        data_column: Name of the JSON data column (e.g., ``"data"``).
        field_path: Field path in dot notation (e.g., ``"name"`` or
            ``"address.street[0]"``).

    Returns:
        str: Complete ``json_extract`` expression, e.g.,
            ``"json_extract(data, '$.field')"``.
    """
    json_path = parse_json_path(field_path)
    return f"json_extract({data_column}, '{json_path}')"


def build_jsonb_extract_expression(data_column: str, field_path: str) -> str:
    """Build a ``jsonb_extract`` SQL expression with a formatted JSON path.

    Args:
        data_column: Name of the JSON data column (e.g., ``"data"``).
        field_path: Field path in dot notation (e.g., ``"name"`` or
            ``"address.street[0]"``).

    Returns:
        str: Complete ``jsonb_extract`` expression, e.g.,
            ``"jsonb_extract(data, '$.field')"``.
    """
    json_path = parse_json_path(field_path)
    return f"jsonb_extract({data_column}, '{json_path}')"
