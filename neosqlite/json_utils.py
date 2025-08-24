import json
from typing import Any, Dict
from .binary import Binary


class NeoSQLiteJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for NeoSQLite that handles Binary objects.
    """

    def default(self, obj):
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
        if isinstance(dct, dict) and "__neosqlite_binary__" in dct:
            return Binary.decode_from_storage(dct)
        return dct

    kwargs["object_hook"] = object_hook
    return json.loads(s, **kwargs)
