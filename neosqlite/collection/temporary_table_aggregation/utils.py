from __future__ import annotations

import logging
from typing import Any

from ...objectid import ObjectId
from ..jsonb_support import (
    _contains_text_operator,
)

logger = logging.getLogger(__name__)


def _sanitize_params(params: list[Any] | None) -> list[Any] | None:
    """Sanitize SQL parameters by converting ``ObjectId`` instances to strings.

    SQLite does not support binding ``ObjectId`` objects directly, so any
    ``ObjectId`` entries in ``params`` are converted via ``str()``.

    Args:
        params: Sequence of SQL bind parameters, or ``None``.

    Returns:
        list[Any] | None: Parameter list with ``ObjectId`` values converted to
            strings, or ``None`` if ``params`` is ``None``.
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
    """Generate a SQL expression extracting a field with ``ObjectId`` support.

    Returns ``"_id"`` directly when ``field_name`` is ``"_id"``. Otherwise
    builds a ``CASE`` expression that returns ``NULL`` when the field is
    absent, extracts ``.id`` when stored as
    ``{"__neosqlite_objectid__": true, "id": "..."}``, and otherwise casts the
    extracted value to ``TEXT``.

    Args:
        json_function_prefix: JSON function prefix (``"json"`` or ``"jsonb"``).
        field_name: Document field path to extract.
        is_local_field: Flag distinguishing local vs. foreign lookup fields
            (retained for signature compatibility).
        data_column: Column or qualified column expression containing JSON
            data.

    Returns:
        str: SQL expression string evaluating to the extracted field value.
    """
    if field_name == "_id":
        return "_id"

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
    """Check whether a ``$match`` specification contains a ``$text`` query.

    Delegates to ``_contains_text_operator`` from ``jsonb_support``
    for consistent detection across aggregation components.

    Args:
        match_spec: MongoDB ``$match`` query specification dictionary.

    Returns:
        bool: ``True`` if ``match_spec`` contains a ``$text`` operator,
            ``False`` otherwise.
    """
    return _contains_text_operator(match_spec)
