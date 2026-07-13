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

    def _convert_value(value: Any) -> Any:
        if isinstance(value, dict):
            return convert_bson_to_neo_objectids(value)
        elif isinstance(value, list):
            return [_convert_value(item) for item in value]
        elif isinstance(value, BsonObjectId):
            return NeoObjectId(value.binary)
        return value

    if not isinstance(doc, dict):
        return doc

    result = {}
    for key, value in doc.items():
        result[key] = _convert_value(value)
    return result


def convert_json_to_neo_objectids(doc: Any) -> Any:
    """Recursively convert JSON-like $oid dicts to NeoSQLite ObjectIds."""
    if doc is None:
        return None

    result: dict[Any, Any] = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            if "$oid" in value:
                result[key] = NeoObjectId(value["$oid"])
            elif key == "$oid":
                result[key] = (
                    NeoObjectId(value) if isinstance(value, str) else value
                )
            else:
                result[key] = convert_json_to_neo_objectids(value)
        elif isinstance(value, list):
            result[key] = [
                (
                    NeoObjectId(v["$oid"])
                    if isinstance(v, dict) and "$oid" in v
                    else v
                )
                for v in value
            ]
        else:
            result[key] = value
    return result
