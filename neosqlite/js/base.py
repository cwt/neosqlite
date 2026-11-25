"""Core types, protocols, and BSON marker codecs for JavaScript engines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol, Self, runtime_checkable

from ..binary import Binary
from ..objectid import ObjectId


class JsWorkerError(Exception):
    """Raised when a sandboxed JS worker cannot evaluate an expression."""


@dataclass(frozen=True, slots=True)
class JsEngineInfo:
    """Metadata describing an available or unavailable JS engine backend."""

    name: str
    available: bool
    version: str | None = None
    detail: str = ""
    path: str | None = None


@runtime_checkable
class JsWorker(Protocol):
    """Protocol implemented by sandboxed JavaScript execution workers."""

    engine_name: str

    def call(
        self,
        code: str,
        args: list[Any] | None = None,
        this_arg: Any = None,
    ) -> Any:
        """Evaluate a JS function with optional positional arguments and ``this``."""
        ...

    def close(self) -> None:
        """Terminate the worker and release any OS resources."""
        ...

    def __enter__(self) -> Self: ...

    def __exit__(self, *exc: object) -> None: ...


@runtime_checkable
class JsEngineBackend(Protocol):
    """Protocol implemented by pluggable JavaScript engine backends."""

    name: str

    def probe(self) -> JsEngineInfo:
        """Probe whether this backend is usable in the current environment."""
        ...

    def create_worker(
        self,
        *,
        timeout: float | None = None,
        memory_mb: int | None = None,
        heap_mb: int | None = None,
    ) -> JsWorker:
        """Create a new sandboxed worker instance for this backend."""
        ...

    def reset_cache(self) -> None:
        """Clear any cached availability probes for this backend."""
        ...


def to_marker(value: Any) -> Any:
    """Convert Python/BSON values into JSON-safe marker dictionaries.

    Args:
        value: Arbitrary Python document or scalar value.

    Returns:
        JSON-serializable value with ``ObjectId``, ``datetime``, ``date``,
        ``Binary``, and ``bytes`` encoded as marker dictionaries.
    """
    if isinstance(value, ObjectId):
        return {"$oid": str(value)}
    if isinstance(value, datetime):
        return {"$date": value.isoformat()}
    if isinstance(value, date):
        return {
            "__neosqlite_js_type__": "date",
            "isoformat": value.isoformat(),
        }
    if isinstance(value, Binary):
        return {
            "__neosqlite_js_type__": "Binary",
            "hex": bytes(value).hex(),
            "subtype": value.subtype,
        }
    if isinstance(value, (bytes, bytearray)):
        return {
            "__neosqlite_js_type__": "Binary",
            "hex": bytes(value).hex(),
            "subtype": 0,
        }
    if isinstance(value, (list, tuple)):
        return [to_marker(item) for item in value]
    if isinstance(value, dict):
        return {key: to_marker(val) for key, val in value.items()}
    return value


def from_marker(value: Any) -> Any:
    """Decode JSON-safe marker dictionaries back into Python/BSON types.

    Single-key marker objects (``$date``, ``$oid``, ``$numberLong``) and
    explicit ``__neosqlite_js_type__`` marker objects (``date``, ``Binary``)
    are decoded so ordinary user documents containing extra fields are
    preserved.

    Args:
        value: Decoded JSON value returned by a JS worker.

    Returns:
        Python value with markers restored to ``datetime``, ``date``,
        ``ObjectId``, ``Binary``, or ``int`` where valid.
    """
    if isinstance(value, dict):
        if len(value) == 1:
            if "$date" in value and isinstance(value["$date"], str):
                try:
                    return datetime.fromisoformat(value["$date"])
                except ValueError:
                    return value["$date"]
            if "$oid" in value and isinstance(value["$oid"], str):
                try:
                    return ObjectId(value["$oid"])
                except (ValueError, TypeError):
                    return value["$oid"]
            if "$numberLong" in value and isinstance(value["$numberLong"], str):
                try:
                    return int(value["$numberLong"])
                except ValueError:
                    return value["$numberLong"]
        js_type = value.get("__neosqlite_js_type__")
        if (
            js_type == "date"
            and len(value) == 2
            and isinstance(value.get("isoformat"), str)
        ):
            try:
                return date.fromisoformat(value["isoformat"])
            except ValueError:
                return value["isoformat"]
        if (
            js_type == "Binary"
            and len(value) == 3
            and isinstance(value.get("hex"), str)
            and isinstance(value.get("subtype"), int)
            and not isinstance(value.get("subtype"), bool)
        ):
            try:
                return Binary(
                    bytes.fromhex(value["hex"]),
                    subtype=value["subtype"],
                )
            except (ValueError, TypeError):
                return value
        return {key: from_marker(val) for key, val in value.items()}
    if isinstance(value, list):
        return [from_marker(item) for item in value]
    return value
