from decimal import Decimal as PyDecimal
from typing import Any

from bson import Decimal128, Regex
from bson import ObjectId as BsonObjectId

from neosqlite.objectid import ObjectId as NeoObjectId


def convert_neo_to_bson_objectids(doc: Any) -> Any:
    """Convert NeoSQLite types to BSON-compatible types recursively.

    Converts ``NeoObjectId`` instances to ``bson.ObjectId`` and ``Decimal``
    values to ``bson.Decimal128`` (preserving precision) without mutating
    arbitrary user keys named ``"id"``.

    Args:
        doc: Document, list, or scalar value to convert.

    Returns:
        Any: Converted structure with BSON-compatible types.
    """
    if isinstance(doc, dict):
        return {
            key: convert_neo_to_bson_objectids(value)
            for key, value in doc.items()
        }
    elif isinstance(doc, list):
        return [convert_neo_to_bson_objectids(item) for item in doc]
    elif isinstance(doc, PyDecimal):
        return Decimal128(doc)
    elif isinstance(doc, NeoObjectId):
        return BsonObjectId(doc.binary)
    return doc


def convert_bson_to_neo_objectids(doc: Any) -> Any:
    """Convert incoming BSON types to NeoSQLite-compatible types recursively.

    Converts ``bson.ObjectId`` to ``neosqlite.objectid.ObjectId``,
    ``bson.Decimal128`` to ``float``, and ``bson.Regex`` to ``re.Pattern``.

    Args:
        doc: Document, list, or scalar value to convert.

    Returns:
        Any: Copy of ``doc`` with BSON types converted for NeoSQLite and SQLite.
    """
    if isinstance(doc, BsonObjectId):
        return NeoObjectId(doc.binary)
    elif isinstance(doc, Decimal128):
        return float(doc.to_decimal())
    elif isinstance(doc, Regex):
        return doc.try_compile()
    elif isinstance(doc, list):
        return [convert_bson_to_neo_objectids(item) for item in doc]
    elif isinstance(doc, dict):
        return {k: convert_bson_to_neo_objectids(v) for k, v in doc.items()}
    return doc


def convert_json_to_neo_objectids(doc: Any) -> Any:
    """Recursively convert extended-JSON ``$oid`` dicts to NeoObjectIds.

    Replaces single-key ``{"$oid": ...}`` dictionaries whose value is a
    ``str`` or ``bytes`` with ``neosqlite.objectid.ObjectId`` instances.

    Args:
        doc: Parsed JSON document, list, or scalar value.

    Returns:
        Any: Converted structure with ``$oid`` wrappers replaced by
            ``ObjectId`` instances.
    """
    if isinstance(doc, dict):
        if len(doc) == 1 and "$oid" in doc:
            val = doc["$oid"]
            return NeoObjectId(val) if isinstance(val, (str, bytes)) else val
        return {k: convert_json_to_neo_objectids(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [convert_json_to_neo_objectids(item) for item in doc]
    return doc
