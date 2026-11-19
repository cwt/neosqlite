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
  const _Object_defineProperty = Object.defineProperty;
  const _BigInt = BigInt;

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
      const out = {};
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        out[k] = fromJSONSafe(value[k]);
      }
      return out;
    }
    if (_Array_isArray(value)) {
      const out = [];
      for (let i = 0; i < value.length; i++) {
        out.push(fromJSONSafe(value[i]));
      }
      return out;
    }
    return value;
  }

  function toJSONSafe(value) {
    if (value instanceof _Date) {
      return { $date: _Date_toISOString.call(value) };
    }
    if (_Array_isArray(value)) {
      const out = [];
      for (let i = 0; i < value.length; i++) {
        out.push(toJSONSafe(value[i]));
      }
      return out;
    }
    if (value && typeof value === 'object') {
      const out = {};
      const keys = _Object_keys(value);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        out[k] = toJSONSafe(value[k]);
      }
      return out;
    }
    if (typeof value === 'bigint') {
      return { $numberLong: value.toString() };
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
    args.push(fromJSONSafe(rawArgs[i]));
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
    message = getattr(exc, "message", None)
    if isinstance(message, str) and message:
        return message
    return str(exc)


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
