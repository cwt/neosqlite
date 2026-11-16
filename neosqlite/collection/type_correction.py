"""
Type correction utilities for NeoSQLite to handle automatic conversion
between integer IDs and ObjectIds in queries.
"""

import logging
from typing import Any

from ..objectid import ObjectId
from ..sql_utils import quote_table_name

logger = logging.getLogger(__name__)


def _try_convert_to_int(value: str) -> int | str:
    """Try converting a string to an integer, returning it unchanged on error.

    Args:
        value: String value to parse as a base-10 integer.

    Returns:
        int | str: Parsed integer, or the original string if parsing fails.
    """
    try:
        return int(value)
    except ValueError as e:
        logger.debug(f"{e=}")
        return value


def _is_valid_objectid_hex(value: str) -> bool:
    """Check whether a string is a valid 24-character ObjectId hex string.

    Args:
        value: Candidate hex string to validate.

    Returns:
        bool: True if ``value`` has length 24 and parses as an ``ObjectId``,
            False otherwise.
    """
    if len(value) != 24:
        return False
    try:
        ObjectId(value)
        return True
    except ValueError as e:
        logger.debug(f"Invalid ObjectId hex string '{value}': {e}")
        return False


def _convert_list_item(item: Any) -> Any:
    """Normalize a single list item inside a logical query operator.

    Recursively normalizes dictionary items via ``normalize_id_query_for_db``
    while returning non-dictionary values unchanged.

    Args:
        item: Element from a ``$and``, ``$or``, or ``$nor`` clause list.

    Returns:
        Any: Normalized query dictionary or original non-dict item.
    """
    match item:
        case dict():
            return normalize_id_query_for_db(item)
        case _:
            return item


def normalize_id_query_for_db(query: dict[str, Any]) -> dict[str, Any]:
    """Normalize ID types in a query dictionary for database lookup.

    Applies NeoSQLite's ID normalization rules:

    - ``id`` with an ``ObjectId`` or 24-char ObjectId hex string is rewritten
      to ``_id`` as a hex string.
    - ``id`` with an integer string is converted via ``_try_convert_to_int``.
    - ``_id`` with an ``ObjectId`` is converted to ``str(value)``.
    - ``_id`` with a string is preserved verbatim without integer conversion.
    - Logical operators (``$and``, ``$or``, ``$nor``, ``$not``) are processed
      recursively while preserving ``ObjectId`` values in non-ID fields.

    Args:
        query: Query dictionary to normalize. Non-dict inputs are returned
            as-is.

    Returns:
        dict[str, Any]: New query dictionary with normalized ID fields.
    """
    if not isinstance(query, dict):
        return query

    corrected_query: dict[str, Any] = {}

    for key, value in query.items():
        match key, value:
            # Case: 'id' field with ObjectId -> convert to '_id' with string
            case ("id", ObjectId()):
                corrected_query["_id"] = str(value)

            # Case: 'id' field with valid ObjectId hex string -> convert to '_id'
            case ("id", str() as s) if _is_valid_objectid_hex(s):
                corrected_query["_id"] = s

            # Case: 'id' field with string that can be converted to int
            case ("id", str() as s):
                corrected_query["id"] = _try_convert_to_int(s)

            # Case: 'id' field with any other type -> keep as-is
            case ("id", _):
                corrected_query["id"] = value

            # Case: '_id' field with ObjectId -> convert to string
            case ("_id", ObjectId()):
                corrected_query["_id"] = str(value)

            # Case: '_id' field with string
            # Strict MongoDB-like: keep the string _id verbatim. A 24-char hex
            # string is the ObjectId representation and is matched as-is against
            # the _id column (which stores ObjectIds as their hex string).
            case ("_id", str() as s):
                corrected_query["_id"] = s

            # Case: '_id' field with any other type -> keep as-is
            case ("_id", _):
                corrected_query["_id"] = value

            # Recurse into logical query operators; preserve ObjectIds in non-_id fields
            case ("$and" | "$or" | "$nor", list()):
                corrected_query[key] = [
                    _convert_list_item(item) for item in value
                ]

            case ("$not", dict()):
                corrected_query[key] = normalize_id_query_for_db(value)

            # Case: default -> keep as-is
            case _:
                corrected_query[key] = value

    return corrected_query


def get_integer_id_for_doc_id(
    db_connection: Any,
    collection_name: str,
    doc_id: Any,
) -> int:
    """Resolve a document ``_id`` value to its SQLite integer row ``id``.

    Unlike ``get_integer_id_for_oid``, integer inputs are queried against the
    ``_id`` column (trying both numeric and string forms for ``int``/``float``
    excluding ``bool``, and raw/string forms for ``bytes``) rather than
    assumed to be SQLite row IDs.

    Args:
        db_connection: SQLite database connection to query.
        collection_name: Name of the collection table.
        doc_id: Document ``_id`` value (``ObjectId``, ``int``, ``str``, etc.).

    Returns:
        int: Matching SQLite integer primary key from the ``id`` column.

    Raises:
        ValueError: If no row matches ``doc_id`` in the collection.
    """
    quoted = quote_table_name(collection_name)
    if isinstance(doc_id, (int, float)) and not isinstance(doc_id, bool):
        lookup_vals: tuple[Any, ...] = (doc_id, str(doc_id))
    elif isinstance(doc_id, bytes):
        lookup_vals = (doc_id, str(doc_id))
    elif isinstance(doc_id, str):
        lookup_vals = (doc_id,)
    else:
        lookup_vals = (str(doc_id) if hasattr(doc_id, "__str__") else doc_id,)
    for val in lookup_vals:
        cursor = db_connection.execute(
            f"SELECT id FROM {quoted} WHERE _id = ?",
            (val,),
        )
        if row := cursor.fetchone():
            return row[0]
    raise ValueError(f"Could not find integer ID for document _id: {doc_id}")


def get_integer_id_for_oid(
    db_connection: Any,
    collection_name: str,
    oid: Any,
) -> int:
    """Get the SQLite integer row ID for an ObjectId or integer ID.

    Returns integer ``oid`` values directly; otherwise looks up ``_id`` in
    the collection table using ``str(oid)`` when available.

    Args:
        db_connection: SQLite database connection to query.
        collection_name: Name of the collection table.
        oid: ID value (``int``, ``ObjectId``, ``str``, etc.).

    Returns:
        int: Integer ``id`` value from the collection table or ``oid`` itself.

    Raises:
        ValueError: If a non-integer ``oid`` cannot be found in the table.
    """
    match oid:
        case int():
            return oid
        case _:
            cursor = db_connection.execute(
                f"SELECT id FROM {quote_table_name(collection_name)} WHERE _id = ?",
                (str(oid) if hasattr(oid, "__str__") else oid,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            raise ValueError(f"Could not find integer ID for ObjectId: {oid}")


def get_integer_id_for_table(
    db_connection: Any,
    table_name: str,
    oid: Any,
) -> int:
    """Get the SQLite integer row ID for a GridFS file ID.

    Thin wrapper around ``get_integer_id_for_oid`` provided for clarity when
    resolving identifiers against GridFS tables.

    Args:
        db_connection: SQLite database connection to query.
        table_name: Name of the GridFS table.
        oid: File ID (``int``, ``ObjectId``, ``str``, etc.).

    Returns:
        int: Integer ``id`` value from the GridFS table or ``oid`` itself.

    Raises:
        ValueError: If a non-integer ``oid`` cannot be found in the table.
    """
    return get_integer_id_for_oid(db_connection, table_name, oid)
