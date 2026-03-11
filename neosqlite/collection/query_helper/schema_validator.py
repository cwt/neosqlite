"""
Native JSON Schema validator for NeoSQLite.
Provides MongoDB-compatible $jsonSchema evaluation.
"""

import re
from typing import Any, Dict
from ...binary import Binary
from ...objectid import ObjectId


def matches_json_schema(
    document: Dict[str, Any], schema: Dict[str, Any]
) -> bool:
    """
    Check if a document matches the provided JSON Schema.

    Args:
        document: The document to validate
        schema: The JSON Schema specification

    Returns:
        True if the document matches the schema, False otherwise.
    """
    return _validate_node(document, schema)


def _validate_node(data: Any, schema: Any) -> bool:
    """Recursively validate a data node against a schema node."""
    if not isinstance(schema, dict):
        return True

    # Check type/bsonType
    if "bsonType" in schema:
        if not _check_type(data, schema["bsonType"]):
            return False
    elif "type" in schema:
        if not _check_type(data, schema["type"]):
            return False

    # Check required fields
    if "required" in schema and isinstance(data, dict):
        for field in schema["required"]:
            if field not in data:
                return False

    # Check properties
    if "properties" in schema and isinstance(data, dict):
        for field, prop_schema in schema["properties"].items():
            if field in data:
                if not _validate_node(data[field], prop_schema):
                    return False

    # Check minimum/maximum
    if isinstance(data, (int, float)):
        if "minimum" in schema and data < schema["minimum"]:
            return False
        if "maximum" in schema and data > schema["maximum"]:
            return False
        if "exclusiveMinimum" in schema and data <= schema["exclusiveMinimum"]:
            return False
        if "exclusiveMaximum" in schema and data >= schema["exclusiveMaximum"]:
            return False

    # Check string constraints
    if isinstance(data, str):
        if "minLength" in schema and len(data) < schema["minLength"]:
            return False
        if "maxLength" in schema and len(data) > schema["maxLength"]:
            return False
        if "pattern" in schema:
            if not re.search(schema["pattern"], data):
                return False

    # Check array constraints
    if isinstance(data, list):
        if "minItems" in schema and len(data) < schema["minItems"]:
            return False
        if "maxItems" in schema and len(data) > schema["maxItems"]:
            return False
        if "items" in schema:
            for item in data:
                if not _validate_node(item, schema["items"]):
                    return False

    # Check enum
    if "enum" in schema:
        if data not in schema["enum"]:
            return False

    # Check logical combinations
    if "anyOf" in schema:
        if not any(_validate_node(data, sub) for sub in schema["anyOf"]):
            return False
    if "allOf" in schema:
        if not all(_validate_node(data, sub) for sub in schema["allOf"]):
            return False
    if "oneOf" in schema:
        matches = [_validate_node(data, sub) for sub in schema["oneOf"]]
        if matches.count(True) != 1:
            return False
    if "not" in schema:
        if _validate_node(data, schema["not"]):
            return False

    return True


def _check_type(data: Any, type_spec: Any) -> bool:
    """Check if data matches the specified type or list of types."""
    if isinstance(type_spec, list):
        return any(_check_single_type(data, t) for t in type_spec)
    return _check_single_type(data, type_spec)


def _check_single_type(data: Any, t: str) -> bool:
    """Check a single type string (supports both JSON Schema and BSON types)."""
    # Map MongoDB/BSON types to Python types
    match t:
        case "string":
            return isinstance(data, str)
        case "number":
            return isinstance(data, (int, float))
        case "integer" | "int" | "long":
            # In Python, we treat large ints as long
            return isinstance(data, int) and not isinstance(data, bool)
        case "double" | "decimal":
            return isinstance(data, (float, int))
        case "object":
            return isinstance(data, dict)
        case "array":
            return isinstance(data, list)
        case "bool" | "boolean":
            return isinstance(data, bool)
        case "null":
            return data is None
        case "objectId":
            if isinstance(data, ObjectId):
                return True
            if isinstance(data, dict) and "__neosqlite_objectid__" in data:
                return True
            if isinstance(data, str) and len(data) == 24:
                try:
                    ObjectId(data)
                    return True
                except ValueError:
                    return False
            return False
        case "binData":
            return isinstance(data, (Binary, bytes))
        case "date":
            # We handle datetime objects in NeoSQLite
            from datetime import datetime

            return isinstance(data, datetime)
        case _:
            return True  # Unknown type, default to True or handle more?
