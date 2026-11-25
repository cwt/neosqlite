"""Isolated subprocess worker running QuickJS via ``jsbox``.

This module is executed as a standalone script inside a dedicated child Python
process (``python -I -u _jsbox_worker.py ...``) with a scrubbed environment.
It intentionally imports only the Python standard library and ``jsbox`` so the
QuickJS C/Rust extension is never loaded into the main NeoSQLite process.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TextIO

_JSBOX_HARNESS = r"""
(function() {
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

  const req = _JSON_parse(__neosqlite_req__.payload);
  delete globalThis.__neosqlite_req__;

  const fn = (0, eval)('(' + req.code + ')');
  if (typeof fn !== 'function') {
    throw new Error('JS body is not a function');
  }

  const rawArgs = req.args || [];
  const args = [];
  for (let i = 0; i < rawArgs.length; i++) {
    setSafeProp(args, i, fromJSONSafe(rawArgs[i]));
  }
  const thisArg =
    req.thisArg === undefined ? null : fromJSONSafe(req.thisArg);

  return _Promise_then.call(
    _Promise_resolve(fn.apply(thisArg, args)),
    function(result) {
      const encoded = _JSON_stringify(toJSONSafe(result));
      return encoded === undefined ? 'null' : encoded;
    }
  );
})()
"""


def _write_protocol_line(stream: TextIO, text: str) -> None:
    stream.write(text)
    stream.write("\n")
    stream.flush()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--heap-mb", type=int, default=64)
    parser.add_argument("--jsbox-path", action="append", default=[])
    return parser.parse_args(argv)


def _format_js_error(exc: Exception) -> str:
    try:
        message = getattr(exc, "message", None)
        if isinstance(message, str) and message:
            return message
        return str(exc)
    except Exception:
        return "JavaScript error (unformattable thrown value)"


def main(argv: list[str] | None = None) -> int:
    """Run the JSON-lines evaluation loop inside an isolated child process."""
    # Reserve the original stdout exclusively for protocol framing so stray
    # prints can never corrupt JSON-lines IPC.
    proto_out = sys.stdout
    sys.stdout = sys.stderr

    args = _parse_args(sys.argv[1:] if argv is None else argv)
    for path in args.jsbox_path:
        if path and path not in sys.path:
            sys.path.append(path)

    from jsbox import JavaScriptError, JsTimeoutError, Runtime

    heap_bytes = max(0, args.heap_mb) * 1024 * 1024

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        req_id = None
        timeout = None
        try:
            req = json.loads(line)
            req_id = req.get("id")
            raw_timeout = req.get("timeout")
            if isinstance(raw_timeout, (int, float)) and raw_timeout > 0:
                timeout = float(raw_timeout)

            # A fresh Runtime per call costs ~150 us, prevents cross-call
            # prototype/global pollution, and reclaims QuickJS heap memory
            # immediately after every evaluation.
            runtime = Runtime(memory_limit=heap_bytes)
            runtime.bind_object("__neosqlite_req__", {"payload": line})
            encoded_value = runtime.eval(_JSBOX_HARNESS, timeout=timeout)
            value_json = (
                encoded_value if isinstance(encoded_value, str) else "null"
            )
            _write_protocol_line(
                proto_out,
                f'{{"id":{json.dumps(req_id)},"ok":true,"value":{value_json}}}',
            )
        except JsTimeoutError:
            msg = (
                f"JavaScript evaluation timed out after {timeout}s"
                if timeout is not None
                else "JavaScript evaluation timed out"
            )
            _write_protocol_line(
                proto_out,
                json.dumps({"id": req_id, "ok": False, "error": msg}),
            )
        except JavaScriptError as exc:
            _write_protocol_line(
                proto_out,
                json.dumps(
                    {
                        "id": req_id,
                        "ok": False,
                        "error": _format_js_error(exc),
                    }
                ),
            )
        except Exception as exc:  # noqa: BLE001
            _write_protocol_line(
                proto_out,
                json.dumps({"id": req_id, "ok": False, "error": str(exc)}),
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
