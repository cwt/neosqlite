from __future__ import annotations

import logging
from typing import Any

from ...objectid import ObjectId
from ..jsonb_support import (
    _contains_text_operator,
)

logger = logging.getLogger(__name__)


def _sanitize_params(params: list[Any] | None) -> list[Any] | None:
    """
    Sanitize SQL parameters by converting ObjectId instances to strings.

    SQLite doesn't know how to bind ObjectId types, so we convert them to strings.

    Args:
        params: List of parameters or None

    Returns:
        Sanitized parameters with ObjectId converted to strings
    """
    if params is None:
        return None

    sanitized = []
    for param in params:
        if isinstance(param, ObjectId):
            sanitized.append(str(param))
        else:
            sanitized.append(param)
    return sanitized


def _json_extract_field_with_objectid_support(
    json_function_prefix: str,
    field_name: str,
    is_local_field: bool = True,
    data_column: str = "data",
) -> str:
    """
    Generate SQL expression to extract a field value with ObjectId support.

    When a field contains an ObjectId (stored as {"__neosqlite_objectid__":true,"id":"..."}),
    this extracts just the ID string instead of the full JSON object.

    Args:
        json_function_prefix: The JSON function prefix (json or jsonb)
        field_name: The field name to extract
        is_local_field: Whether this is a local field (True) or foreign field (False)
        data_column: The column or qualified column reference containing the JSON data

    Returns:
        SQL expression string
    """
    if field_name == "_id":
        return "_id" if is_local_field else "_id"

    from ..json_path_utils import parse_json_path

    json_extract = f"{json_function_prefix}_extract"
    json_path = parse_json_path(field_name)
    base_extract = f"{json_extract}({data_column}, '{json_path}')"
    oid_check = (
        f"{json_extract}({data_column}, '{json_path}.__neosqlite_objectid__')"
    )
    oid_id = f"{json_extract}({data_column}, '{json_path}.id')"

    # Check if the field is an ObjectId and extract the actual ID string
    # ObjectId is stored as: {"__neosqlite_objectid__":true,"id":"<oid_string>"}
    return (
        f"CASE "
        f"WHEN {base_extract} IS NULL THEN NULL "
        f"WHEN {oid_check} = 1 THEN {oid_id} "
        f"ELSE CAST({base_extract} AS TEXT) "
        f"END"
    )


def _contains_text_search(match_spec: dict[str, Any]) -> bool:
    """
    Check if a match specification contains text search operations.

    This function delegates to the centralized _contains_text_operator function
    to ensure consistent text search detection across all NeoSQLite components.

    Args:
        match_spec (dict[str, Any]): The match specification to check for text search operations

    Returns:
        bool: True if the match specification contains text search operations, False otherwise
    """
    return _contains_text_operator(match_spec)
