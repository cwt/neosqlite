import json
import logging
import re
from datetime import date, datetime
from typing import Any

from neosqlite.binary import Binary

logger = logging.getLogger(__name__)

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
        except ImportError as e:
            logger.debug(f"ObjectId module not available for encoding: {e}")
            pass  # ObjectId module not available

        # Handle date/datetime objects.
        # datetime is encoded as BSON extended JSON {"$date": ...} to preserve type.
        # Plain date is encoded as ISO format string (#112).
        if isinstance(obj, datetime):
            return {"$date": obj.isoformat()}
        if isinstance(obj, date):
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
    Custom JSON loads function that handles Binary objects, ObjectId, and $date objects.

    Args:
        s: JSON string to deserialize
        **kwargs: Additional arguments to pass to json.loads

    Returns:
        Deserialized object
    """

    def object_hook(dct: dict[str, Any]) -> Any:
        """
        Decodes Binary objects, ObjectId objects, and $date objects from JSON deserialization.

        Args:
            dct: The dictionary to decode.

        Returns:
            The decoded object or the original dictionary.
        """
        if isinstance(dct, dict):
            if "__neosqlite_binary__" in dct:
                try:
                    return Binary.decode_from_storage(dct)
                except (ValueError, KeyError, TypeError):
                    return dct
            if "__neosqlite_objectid__" in dct:
                try:
                    from neosqlite.objectid import ObjectId

                    return ObjectId(dct["id"])
                except (ValueError, ImportError, KeyError) as e:
                    logger.debug(f"{e=}")
                    pass
            if "$date" in dct and isinstance(dct["$date"], (str, int, float)):
                try:
                    if isinstance(dct["$date"], str):
                        return datetime.fromisoformat(
                            dct["$date"].replace("Z", "+00:00")
                        )
                    from datetime import timezone

                    return datetime.fromtimestamp(
                        dct["$date"] / 1000.0, tz=timezone.utc
                    )
                except (ValueError, OSError):
                    pass

        return dct

    kwargs["object_hook"] = object_hook
    return json.loads(s, **kwargs)
