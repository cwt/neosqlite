"""
Shared utility module for JSON path parsing functionality.

This module provides common JSON path parsing functions to avoid code duplication
across multiple modules. It handles conversion of dot notation with optional
array indexing to JSON path syntax.
"""


def parse_json_path(field: str) -> str:
    """
    Convert dot notation with array indexing to JSON path syntax.

    Supports:
    - Simple fields: "name" -> "$.name"
    - Nested fields: "address.street" -> "$.address.street"
    - Array indexing: "tags[0]" -> "$.tags[0]"
    - Nested array access: "orders.items[2].name" -> "$.orders.items[2].name"
    - Complex paths: "a.b[0].c[1].d" -> "$.a.b[0].c[1].d"

    Args:
        field (str): The field path in dot notation with optional array indices

    Returns:
        str: Properly formatted JSON path
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
    """
    Build a complete json_extract SQL expression with properly formatted JSON path.

    Args:
        data_column (str): The name of the JSON data column (e.g., "data")
        field_path (str): The field path in dot notation (e.g., "name", "address.street[0]")

    Returns:
        str: Complete json_extract expression, e.g., "json_extract(data, '$.field')"
    """
    json_path = parse_json_path(field_path)
    return f"json_extract({data_column}, '{json_path}')"


def build_jsonb_extract_expression(data_column: str, field_path: str) -> str:
    """
    Build a complete jsonb_extract SQL expression with properly formatted JSON path.

    Args:
        data_column (str): The name of the JSON data column (e.g., "data")
        field_path (str): The field path in dot notation (e.g., "name", "address.street[0]")

    Returns:
        str: Complete jsonb_extract expression, e.g., "jsonb_extract(data, '$.field')"
    """
    json_path = parse_json_path(field_path)
    return f"jsonb_extract({data_column}, '{json_path}')"
