from neosqlite.binary import Binary
from typing import Any, Dict
import json


class NeoSQLiteJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for NeoSQLite that handles Binary objects.
    """

    def default(self, obj):
        """
        Encodes Binary objects for JSON serialization.

        Args:
            obj: The object to encode.

        Returns:
            The encoded object suitable for JSON serialization.
        """
        if isinstance(obj, Binary):
            return obj.encode_for_storage()
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
    Custom JSON loads function that handles Binary objects.

    Args:
        s: JSON string to deserialize
        **kwargs: Additional arguments to pass to json.loads

    Returns:
        Deserialized object
    """

    def object_hook(dct: Dict[str, Any]) -> Any:
        """
        Decodes Binary objects from JSON deserialization.

        Args:
            dct: The dictionary to decode.

        Returns:
            The decoded object or the original dictionary if no Binary object is found.
        """
        if isinstance(dct, dict) and "__neosqlite_binary__" in dct:
            return Binary.decode_from_storage(dct)
        return dct

    kwargs["object_hook"] = object_hook
    return json.loads(s, **kwargs)
