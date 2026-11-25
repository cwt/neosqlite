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


JS_MARKER_CODEC_PREAMBLE = r"""
  const _JSON_parse = JSON.parse;
  const _JSON_stringify = JSON.stringify;
  const _Promise_resolve = Promise.resolve.bind(Promise);
  const _Promise_then = Promise.prototype.then;
  const _Date = Date;
  const _Date_toISOString = Date.prototype.toISOString;
  const _Array_isArray = Array.isArray;
  const _Object_keys = Object.keys;
  const _Object_create = Object.create;
  const _Object_defineProperty = Object.defineProperty;
  const _BigInt = BigInt;
  const _BigInt_toString = BigInt.prototype.toString;
  const _Number_toString = Number.prototype.toString;
  const _Uint8Array = Uint8Array;
  const _parseInt = parseInt;

  function makeNullObj() {
    return _Object_create(null);
  }

  function setSafeProp(obj, key, val) {
    _Object_defineProperty(obj, key, {
      value: val,
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }

  function fromJSONSafe(value) {
    if (value && typeof value === 'object' && !_Array_isArray(value)) {
      const keys = _Object_keys(value);
      if (keys.length === 1 && typeof value.$date === 'string') {
        return new _Date(value.$date);
      }
      if (keys.length === 1 && typeof value.$oid === 'string') {
        const oidObj = { $oid: value.$oid };
        _Object_defineProperty(oidObj, 'toString', {
          value: function() { return this.$oid; },
          enumerable: false,
        });
        return oidObj;
      }
      if (keys.length === 1 && typeof value.$numberLong === 'string') {
        return _BigInt(value.$numberLong);
      }
      if (
        value.__neosqlite_js_type__ === 'date' &&
        typeof value.isoformat === 'string' &&
        keys.length === 2
      ) {
        return new _Date(value.isoformat + 'T00:00:00.000Z');
      }
      if (
        value.__neosqlite_js_type__ === 'Binary' &&
        typeof value.hex === 'string' &&
        keys.length === 3
      ) {
        const hex = value.hex;
        const bytes = new _Uint8Array(hex.length >> 1);
        for (let i = 0, j = 0; i < bytes.length; i++, j += 2) {
          bytes[i] = _parseInt(hex.slice(j, j + 2), 16);
        }
        _Object_defineProperty(bytes, '__neosqlite_subtype__', {
          value: typeof value.subtype === 'number' ? value.subtype : 0,
          enumerable: false,
          configurable: true,
          writable: true,
        });
        return bytes;
      }
      const out = {};
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        setSafeProp(out, k, fromJSONSafe(value[k]));
      }
      return out;
    }
    if (_Array_isArray(value)) {
      const out = [];
      for (let i = 0; i < value.length; i++) {
        setSafeProp(out, i, fromJSONSafe(value[i]));
      }
      return out;
    }
    return value;
  }

  function toJSONSafe(value) {
    if (value instanceof _Date) {
      const out = makeNullObj();
      setSafeProp(out, '$date', _Date_toISOString.call(value));
      return out;
    }
    if (value instanceof _Uint8Array) {
      let hex = '';
      for (let i = 0; i < value.length; i++) {
        const b = _Number_toString.call(value[i], 16);
        hex += b.length === 1 ? '0' + b : b;
      }
      const sub =
        typeof value.__neosqlite_subtype__ === 'number'
          ? value.__neosqlite_subtype__
          : 0;
      const out = makeNullObj();
      setSafeProp(out, '__neosqlite_js_type__', 'Binary');
      setSafeProp(out, 'hex', hex);
      setSafeProp(out, 'subtype', sub);
      return out;
    }
    if (_Array_isArray(value)) {
      const out = [];
      _Object_defineProperty(out, 'toJSON', {
        value: undefined,
        enumerable: false,
        configurable: true,
        writable: true,
      });
      for (let i = 0; i < value.length; i++) {
        setSafeProp(out, i, toJSONSafe(value[i]));
      }
      return out;
    }
    if (value && typeof value === 'object') {
      const out = makeNullObj();
      const keys = _Object_keys(value);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        setSafeProp(out, k, toJSONSafe(value[k]));
      }
      return out;
    }
    if (typeof value === 'bigint') {
      const out = makeNullObj();
      setSafeProp(out, '$numberLong', _BigInt_toString.call(value));
      return out;
    }
    return value;
  }
"""
