"""Utility functions for query helper operations."""

import logging
import os
import re
from typing import Any

from ..._sqlite import sqlite3
from ...binary import Binary
from ...exceptions import MalformedQueryException

# Import JSON function helpers from shared module to avoid duplication
from ..jsonb_support import (
    _get_json_function_prefix as _get_json_function_prefix,
)
from ..jsonb_support import (
    json_function as _json_function,
)

# Import type checking helpers from shared module to avoid duplication
from ..type_utils import _is_numeric_value as _is_numeric_value

logger = logging.getLogger(__name__)

# Global cache for SQLite features
_SQLITE_FEATURES: dict[str, bool | None] = {
    "relative_indexing": None,
    "returning_clause": None,
    "heap_parser_stack": None,
}


def _check_sqlite_version(min_version: str) -> bool:
    """Check if the current SQLite version meets the minimum requirement.

    Parses dot-separated version strings, pads both to at least three
    components, and compares them lexicographically as integer tuples.

    Args:
        min_version: Minimum required version string (e.g., ``"3.42.0"``).

    Returns:
        bool: True if the current SQLite version is greater than or equal to
        ``min_version``, False otherwise.
    """
    current = [int(x) for x in sqlite3.sqlite_version.split(".")]
    required = [int(x) for x in min_version.split(".")]

    # Pad versions to same length
    while len(current) < 3:
        current.append(0)
    while len(required) < 3:
        required.append(0)

    return tuple(current) >= tuple(required)


def _supports_relative_json_indexing() -> bool:
    """Check if SQLite supports ``[#-N]`` relative indexing in JSON paths.

    Relative JSON array indexing was added in SQLite 3.42.0 (2023-05-16).
    The result is cached in ``_SQLITE_FEATURES``.

    Returns:
        bool: True if relative JSON indexing is supported, False otherwise.
    """
    val = _SQLITE_FEATURES["relative_indexing"]
    if val is None:
        val = _check_sqlite_version("3.42.0")
        _SQLITE_FEATURES["relative_indexing"] = val
    return val


def _supports_heap_parser_stack() -> bool:
    """Check whether SQLite grows its expression parser stack from the heap.

    SQLite before 3.46.0 uses a fixed-size parser stack (100 entries). Deeply
    nested JSON update expressions can raise ``parser stack overflow`` on
    those versions; 3.46.0+ allocates the stack dynamically, removing the
    limit. The result is cached in ``_SQLITE_FEATURES``.

    Returns:
        bool: True when the parser stack is heap-grown (SQLite >= 3.46.0).
    """
    val = _SQLITE_FEATURES["heap_parser_stack"]
    if val is None:
        val = _check_sqlite_version("3.46.0")
        _SQLITE_FEATURES["heap_parser_stack"] = val
    return val


def _supports_returning_clause() -> bool:
    """Check if SQLite supports the ``RETURNING`` clause in DML statements.

    Tests ``DELETE ... RETURNING`` at runtime against an in-memory database
    rather than relying solely on version checks, since some SQLite builds may
    disable ``RETURNING`` (introduced in SQLite 3.35.0). Results are cached
    in ``_SQLITE_FEATURES``.

    Returns:
        bool: True if ``RETURNING`` is supported, False otherwise.
    """
    val = _SQLITE_FEATURES["returning_clause"]
    if val is None:
        try:
            # Create a temporary table to test RETURNING
            from ..._sqlite import sqlite3 as sqlite_module

            # Use the same connection type as the collection
            # We'll test with a simple in-memory database
            test_conn = sqlite_module.connect(":memory:")
            test_conn.execute(
                "CREATE TABLE test_returning (id INTEGER, data TEXT)"
            )
            test_conn.execute("INSERT INTO test_returning VALUES (1, 'test')")
            # Try DELETE with RETURNING
            cursor = test_conn.execute(
                "DELETE FROM test_returning WHERE id = 1 RETURNING data"
            )
            result = cursor.fetchone()
            test_conn.close()
            val = result is not None and result[0] == "test"
        except Exception as e:
            logger.debug(f"SQLite RETURNING clause check failed: {e}")
            val = False
        _SQLITE_FEATURES["returning_clause"] = val
    return val


def _get_json_function(name: str, jsonb_supported: bool) -> str:
    """Get the appropriate JSON function name based on JSONB support.

    Args:
        name: Base function name without the ``json_`` or ``jsonb_`` prefix.
        jsonb_supported: Whether JSONB functions are supported.

    Returns:
        str: Full SQLite function name with the appropriate prefix.
    """
    return _json_function(name, jsonb_supported)


# Global flag to force fallback - for benchmarking and debugging
_FORCE_FALLBACK = os.environ.get(
    "NEOSQLITE_FORCE_FALLBACK", ""
).strip().lower() in ("1", "true", "yes", "on")


def _convert_bytes_to_binary(obj: Any) -> Any:
    """Recursively convert ``bytes`` objects to ``Binary`` in a document.

    Traverses dictionaries and lists and converts raw ``bytes`` instances to
    ``Binary`` objects so they can be serialized to JSON. Existing ``Binary``
    instances are returned unchanged to preserve their subtype information.

    Args:
        obj: Object to process (dict, list, bytes, Binary, or any scalar).

    Returns:
        Any: Processed object with all raw ``bytes`` converted to ``Binary``.
    """
    match obj:
        case Binary():
            return obj
        case bytes():
            return Binary(obj)
        case dict():
            return {
                key: _convert_bytes_to_binary(value)
                for key, value in obj.items()
            }
        case list():
            return [_convert_bytes_to_binary(item) for item in obj]
        case _:
            return obj


def set_force_fallback(force: bool = True) -> None:
    """Set global flag to force aggregation queries to use Python fallback.

    Useful for benchmarking and debugging to compare behavior and performance
    between the SQL optimization tiers and Python fallback.

    Args:
        force: If True, forces aggregation queries to use Python fallback.
            If False, allows normal SQL optimization. Defaults to True.
    """
    global _FORCE_FALLBACK
    _FORCE_FALLBACK = force


def get_force_fallback() -> bool:
    """Get the current state of the force fallback flag.

    Returns:
        bool: True if fallback is forced, False otherwise.
    """
    global _FORCE_FALLBACK
    return _FORCE_FALLBACK


# Note: _is_numeric_value is now imported from ..type_utils above to avoid
# code duplication. It is re-exported from this module for backward compatibility.


def _validate_inc_mul_field_value(
    field_name: str, field_value: Any, operation: str
) -> None:
    """Validate that a field value is valid for ``$inc`` or ``$mul``.

    ``None`` values are accepted because missing fields are treated as ``0``
    during update operations. Non-numeric values raise
    ``MalformedQueryException``.

    Args:
        field_name: Name of the field being validated.
        field_value: Current value of the field.
        operation: Update operator being applied (``"$inc"`` or ``"$mul"``).

    Raises:
        MalformedQueryException: If ``field_value`` is non-None and not
            numeric.
    """
    # If the field doesn't exist, it's acceptable as it will be treated as 0
    if field_value is None:
        return

    # Check if the field value is numeric
    if not _is_numeric_value(field_value):
        raise MalformedQueryException(
            f"Cannot apply {operation} to a value of non-numeric type. "
            f"Field '{field_name}' has non-numeric type {type(field_value).__name__} "
            f"with value {repr(field_value)}"
        )


def _validate_bit_field_value(field_name: str, field_value: Any) -> None:
    """Validate that a field value is an integer (or missing) for ``$bit``.

    Args:
        field_name: Name of the field being validated.
        field_value: Current field value, or ``None`` if absent.

    Raises:
        MalformedQueryException: If ``field_value`` is non-None and not a
            non-boolean ``int``.
    """
    if field_value is None:
        return
    if isinstance(field_value, bool) or not isinstance(field_value, int):
        raise MalformedQueryException(
            f"Cannot apply $bit to a value of non-integral type. "
            f"Field '{field_name}' has type {type(field_value).__name__} "
            f"with value {repr(field_value)}"
        )


def _validate_bit_operand(bit_op: Any) -> None:
    """Validate a ``$bit`` operator payload and integer bitmasks."""
    if not isinstance(bit_op, dict):
        raise MalformedQueryException(
            "$bit operator requires a dict with 'and', 'or', or 'xor'"
        )
    for op_name, mask in bit_op.items():
        if op_name not in ("and", "or", "xor"):
            raise MalformedQueryException(
                f"Unsupported $bit sub-operator: '{op_name}'"
            )
        if isinstance(mask, bool) or not isinstance(mask, int):
            raise MalformedQueryException(
                f"$bit '{op_name}' mask must be an integer, not {type(mask).__name__}"
            )


def _validate_push_spec(push_spec: Any) -> None:
    """Validate ``$push`` modifiers (``$slice``, ``$position``, ``$sort`` require ``$each``)."""
    if not isinstance(push_spec, dict):
        return
    for field, push_val in push_spec.items():
        if isinstance(push_val, dict) and "$each" not in push_val:
            for modifier in ("$slice", "$position", "$sort"):
                if modifier in push_val:
                    raise MalformedQueryException(
                        f"{modifier} modifier in $push for '{field}' requires $each"
                    )


def build_upsert_base_document(filter_doc: dict[str, Any]) -> dict[str, Any]:
    """Extract equality predicates from a query filter for upserts (#101).

    MongoDB builds an upserted document from equality conditions only,
    ignoring range or pattern operators. Supported forms are scalar or
    ``None`` equality, explicit ``$eq`` predicates, dotted field paths
    expanded into nested dicts, and top-level ``$and`` lists of those.

    Args:
        filter_doc: Query filter dictionary supplied to an upsert.

    Returns:
        dict[str, Any]: Base document constructed from equality predicates.
    """
    base: dict[str, Any] = {}

    def set_path(path: str, val: Any) -> None:
        """Assign a value into ``base`` at a dot-separated field path.

        Args:
            path: Dot-separated field path inside the base document.
            val: Value to assign at the resolved leaf key.
        """
        cur = base
        parts = path.split(".")
        for part in parts[:-1]:
            cur = cur.setdefault(part, {})
        cur[parts[-1]] = val

    def add(key: str, val: Any) -> None:
        """Add a single filter predicate to ``base`` if it is an equality.

        Skips operator keys starting with ``$``, unwraps single-key ``$eq``
        dicts, and ignores remaining dict or regex predicates.

        Args:
            key: Field name or operator key from the filter.
            val: Predicate value or operator dict associated with ``key``.
        """
        if key.startswith("$"):
            return
        if isinstance(val, dict) and len(val) == 1 and "$eq" in val:
            val = val["$eq"]
        if isinstance(val, dict) or isinstance(val, re.Pattern):
            return
        set_path(key, val)

    for key, value in filter_doc.items():
        if key == "$and" and isinstance(value, list):
            for sub in value:
                if isinstance(sub, dict):
                    for k2, v2 in sub.items():
                        add(k2, v2)
        else:
            add(key, value)
    return base


def sanitize_fts_query(search_term: str) -> str:
    """Sanitize a MongoDB ``$text`` search string for SQLite FTS5.

    SQLite FTS5 treats characters such as quotes, colons, hyphens, and
    boolean keywords (``NOT``, ``AND``, ``OR``) as query syntax. This
    function extracts quoted phrases and bare words, strips or escapes double
    quotes, and quotes each term so FTS5 evaluates them as literals.

    Args:
        search_term: Raw search string from a ``$text.$search`` query.

    Returns:
        str: Space-separated quoted FTS5 term string, or ``""`` when
        ``search_term`` is not a non-empty string.
    """
    if not isinstance(search_term, str) or not search_term.strip():
        return ""

    pattern = r'"([^"]*)"|(\S+)'
    tokens: list[str] = []
    for phrase, word in re.findall(pattern, search_term):
        if phrase:
            p = phrase.strip()
            if p:
                escaped = p.replace('"', '""')
                tokens.append('"' + escaped + '"')
        elif word:
            w = word.replace('"', "").strip()
            if w:
                escaped = w.replace('"', '""')
                tokens.append('"' + escaped + '"')

    return " ".join(tokens)
