from decimal import Decimal as PyDecimal
from typing import Any

from bson import ObjectId as BsonObjectId
from bson.int64 import Int64

from neosqlite.objectid import ObjectId as NeoObjectId


def convert_neo_to_bson_objectids(doc: Any) -> Any:
    """Convert NeoSQLite ObjectIds to BSON ObjectIds, and Decimal to float."""

    def _convert_value(value: Any) -> Any:
        if isinstance(value, NeoObjectId):
            return BsonObjectId(value.binary)
        elif isinstance(value, PyDecimal):
            return float(value)
        elif isinstance(value, list):
            return [_convert_value(item) for item in value]
        elif isinstance(value, dict):
            return convert_neo_to_bson_objectids(value)
        return value

    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            if key == "id" and value == 0:
                result[key] = Int64(0)
            else:
                result[key] = _convert_value(value)
        return result
    elif isinstance(doc, list):
        return [_convert_value(item) for item in doc]
    elif isinstance(doc, PyDecimal):
        return float(doc)
    elif isinstance(doc, NeoObjectId):
        return BsonObjectId(doc.binary)
    return doc


def convert_bson_to_neo_objectids(doc: Any) -> Any:
    """Convert PyMongo/BSON ObjectIds to NeoSQLite ObjectIds recursively."""
    if isinstance(doc, BsonObjectId):
        return NeoObjectId(doc.binary)
    elif isinstance(doc, list):
        return [convert_bson_to_neo_objectids(item) for item in doc]
    elif isinstance(doc, dict):
        return {k: convert_bson_to_neo_objectids(v) for k, v in doc.items()}
    return doc


def convert_json_to_neo_objectids(doc: Any) -> Any:
    """Recursively convert JSON-like $oid dicts to NeoSQLite ObjectIds."""
    if isinstance(doc, dict):
        if len(doc) == 1 and "$oid" in doc:
            val = doc["$oid"]
            return NeoObjectId(val) if isinstance(val, (str, bytes)) else val
        return {k: convert_json_to_neo_objectids(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [convert_json_to_neo_objectids(item) for item in doc]
    return doc
