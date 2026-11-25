"""Shared utility module for JSON path parsing functionality.

Provides common JSON path parsing and SQLite JSON/JSONB extraction helpers to
avoid code duplication across modules, converting dot notation with optional
array indexing into SQLite JSON path syntax.
"""

from __future__ import annotations

import re

_ARRAY_SUFFIX_RE = re.compile(r"^(.*?)((?:\[(?:\d+|#(?:-\d+)?)\])+)$")


def _needs_json_key_quotes(field_name: str) -> bool:
    """Return True if a JSON key segment requires double-quoting in SQLite."""
    if not field_name:
        return False
    return any(ch in field_name for ch in ("'", '"', "[", "]", "\\")) or any(
        ch.isspace() for ch in field_name
    )


def _format_json_segment(part: str) -> str:
    """Format one dot-separated JSON path segment safely for SQLite SQL literals."""
    match = _ARRAY_SUFFIX_RE.match(part)
    if match:
        field_name, array_indices = match.group(1), match.group(2)
    else:
        field_name, array_indices = part, ""

    if _needs_json_key_quotes(field_name):
        # Escape backslash and double-quote for SQLite JSON path double-quoted keys,
        # and double single quotes for enclosing SQL single-quoted string literals.
        escaped = (
            field_name.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("'", "''")
        )
        return f'"{escaped}"{array_indices}'

    escaped = field_name.replace("'", "''")
    return f"{escaped}{array_indices}"


def parse_json_path(field: str, *, preserve_id: bool = True) -> str:
    """Convert dot notation with array indexing to SQLite JSON path syntax.

    Supports:

    - Simple fields: ``"name"`` -> ``"$.name"``
    - Nested fields: ``"address.street"`` -> ``"$.address.street"``
    - Array indexing: ``"tags[0]"`` -> ``"$.tags[0]"``
    - Nested array access: ``"orders.items[2].name"`` ->
      ``"$.orders.items[2].name"``
    - Complex paths: ``"a.b[0].c[1].d"`` -> ``"$.a.b[0].c[1].d"``

    When ``preserve_id`` is ``True`` (default), returns ``"_id"`` unchanged.
    Quotes path segments containing quotes, brackets, backslashes, or
    whitespace as ``"segment"`` and doubles single quotes for SQL string safety.

    Args:
        field: Field path in dot notation with optional array indices.
        preserve_id: Whether to return ``"_id"`` as bare ``"_id"`` rather than
            ``"$._id"``.

    Returns:
        str: Properly formatted SQLite JSON path (or ``"_id"``).
    """
    # Handle special case for _id
    if preserve_id and field == "_id":
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
            # Find the closing bracket only if it looks like a valid array subscript
            bracket_end = field.find("]", i)
            candidate = field[i : bracket_end + 1] if bracket_end != -1 else ""
            if bracket_end != -1 and _ARRAY_SUFFIX_RE.match("x" + candidate):
                current_part += candidate
                i = bracket_end
            else:
                # Literal bracket (unmatched '[' or non-index '[...]')
                current_part += field[i]
        else:
            current_part += field[i]
        i += 1

    # Add the last part
    if current_part:
        parts.append(current_part)

    # Convert each part to JSON path format
    json_parts = [_format_json_segment(part) for part in parts]

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
