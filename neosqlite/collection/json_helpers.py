import json
import logging
import re
from datetime import UTC, date, datetime
from typing import Any, override

from neosqlite.binary import Binary

logger = logging.getLogger(__name__)

# Pre-compile ISO date pattern for performance
ISO_DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$"
)


class NeoSQLiteJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for NeoSQLite document serialization.

    Extends ``json.JSONEncoder`` to serialize ``Binary``, ``ObjectId``,
    ``datetime``, and ``date`` instances into formats compatible with
    NeoSQLite storage and BSON extended JSON conventions.
    """

    @override
    def default(self, obj):
        """Encode NeoSQLite custom types for JSON serialization.

        Handles ``Binary`` and ``ObjectId`` via ``encode_for_storage()``,
        encodes ``datetime`` as ``{"$date": ...}`` ISO strings, and encodes
        plain ``date`` values as ISO-8601 strings before falling back to the
        base encoder.

        Args:
            obj: The object to encode.

        Returns:
            Any: JSON-serializable representation of ``obj``.
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
    """Serialize an object to a JSON string using ``NeoSQLiteJSONEncoder``.

    Args:
        obj: Object to serialize.
        **kwargs: Additional keyword arguments passed to ``json.dumps``.

    Returns:
        str: JSON string representation of ``obj``.
    """
    return json.dumps(obj, cls=NeoSQLiteJSONEncoder, **kwargs)


def neosqlite_json_dumps_for_sql(obj: Any, **kwargs) -> str:
    """Serialize an object to compact JSON for SQL query parameters.

    Defaults ``separators`` to ``(",", ":")`` to match SQLite's
    ``json_extract`` output formatting while encoding NeoSQLite types via
    ``NeoSQLiteJSONEncoder``.

    Args:
        obj: Object to serialize.
        **kwargs: Additional keyword arguments passed to ``json.dumps``.

    Returns:
        str: Compact JSON string representation of ``obj``.
    """
    # Use compact JSON formatting to match SQLite's json_extract behavior
    kwargs.setdefault("separators", (",", ":"))
    return json.dumps(obj, cls=NeoSQLiteJSONEncoder, **kwargs)


def neosqlite_json_loads(s: str, **kwargs) -> Any:
    """Deserialize a JSON string into Python and NeoSQLite objects.

    Installs an ``object_hook`` on ``kwargs`` that restores ``Binary``,
    ``ObjectId``, and ``$date`` extended-JSON objects back into their
    corresponding Python instances.

    Args:
        s: JSON string to deserialize.
        **kwargs: Additional keyword arguments passed to ``json.loads``.

    Returns:
        Any: Deserialized Python object graph.
    """

    def object_hook(dct: dict[str, Any]) -> Any:
        """Decode Binary, ObjectId, and ``$date`` dictionaries from JSON.

        Args:
            dct: Dictionary parsed from a JSON object.

        Returns:
            Any: Decoded ``Binary``, ``ObjectId``, or ``datetime`` instance,
                or the original dictionary if unrecognized or invalid.
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
                        return datetime.fromisoformat(dct["$date"])
                    return datetime.fromtimestamp(dct["$date"] / 1000.0, tz=UTC)
                except (ValueError, OSError):
                    pass

        return dct

    kwargs["object_hook"] = object_hook
    return json.loads(s, **kwargs)
