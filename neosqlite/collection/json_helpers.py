import json
import re
from datetime import datetime
from typing import Any, Dict

from neosqlite.binary import Binary

# Pre-compile ISO date pattern for performance
ISO_DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$"
)


class NeoSQLiteJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for NeoSQLite that handles Binary, ObjectId, and datetime objects.
    """

    def default(self, obj):
        """
        Encodes Binary, ObjectId, and datetime objects for JSON serialization.

        Args:
            obj: The object to encode.

        Returns:
            The encoded object suitable for JSON serialization.
        """
        if isinstance(obj, Binary):
            return obj.encode_for_storage()
        # Import here to avoid circular imports
        try:
            from neosqlite.objectid import ObjectId

            if isinstance(obj, ObjectId):
                return obj.encode_for_storage()
        except ImportError:
            pass  # ObjectId module not available

        # Handle datetime objects - convert to ISO format string
        if isinstance(obj, datetime):
            return obj.isoformat()

        return super().default(obj)


def neosqlite_json_dumps(obj: Any, **kwargs) -> str:
    """
    Custom JSON dumps function that handles Binary objects.

    Args:
        obj: Object to serialize
        **kwargs: Additional arguments to pass to json.dumps

    Returns:
        JSON string representation
    """
    return json.dumps(obj, cls=NeoSQLiteJSONEncoder, **kwargs)


def neosqlite_json_dumps_for_sql(obj: Any, **kwargs) -> str:
    """
    Custom JSON dumps function for SQL query parameters that handles Binary objects
    using compact formatting to match SQLite's json_extract behavior.

    Args:
        obj: Object to serialize
        **kwargs: Additional arguments to pass to json.dumps

    Returns:
        JSON string representation in compact format
    """
    # Use compact JSON formatting to match SQLite's json_extract behavior
    kwargs.setdefault("separators", (",", ":"))
    return json.dumps(obj, cls=NeoSQLiteJSONEncoder, **kwargs)


def neosqlite_json_loads(s: str, **kwargs) -> Any:
    """
    Custom JSON loads function that handles Binary objects and ISO date strings.

    For MongoDB compatibility, ISO 8601 date strings are automatically converted
    back to datetime objects, matching MongoDB's behavior where dates are stored
    as BSON Date type and returned as datetime objects.

    Args:
        s: JSON string to deserialize
        **kwargs: Additional arguments to pass to json.loads

    Returns:
        Deserialized object
    """

    def object_hook(dct: Dict[str, Any]) -> Any:
        """
        Decodes Binary objects, ObjectId objects, and ISO date strings from JSON deserialization.

        Args:
            dct: The dictionary to decode.

        Returns:
            The decoded object or the original dictionary if no Binary/ObjectId object is found.
        """
        if isinstance(dct, dict):
            if "__neosqlite_binary__" in dct:
                return Binary.decode_from_storage(dct)
            if "__neosqlite_objectid__" in dct:
                try:
                    from neosqlite.objectid import ObjectId

                    return ObjectId(dct["id"])
                except (ValueError, ImportError, KeyError):
                    pass

        # Convert ISO date strings back to datetime for MongoDB compatibility
        for key, value in dct.items():
            if isinstance(value, str) and ISO_DATE_PATTERN.match(value):
                try:
                    dct[key] = datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass  # Not a valid date string, keep as string
        return dct

    kwargs["object_hook"] = object_hook
    return json.loads(s, **kwargs)
