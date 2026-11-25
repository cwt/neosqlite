"""Isolated subprocess worker running QuickJS via ``jsbox``.

This module is executed as a standalone script inside a dedicated child Python
process (``python -I -u _jsbox_worker.py ...``) with a scrubbed environment.
It intentionally imports only the Python standard library and ``jsbox`` so the
QuickJS C/Rust extension is never loaded into the main NeoSQLite process.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import TextIO


def _load_shared_codec_preamble() -> str:
    """Read ``JS_MARKER_CODEC_PREAMBLE`` from sibling ``../base.py`` using stdlib AST."""
    base_path = Path(__file__).resolve().parents[1] / "base.py"
    tree = ast.parse(base_path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "JS_MARKER_CODEC_PREAMBLE"
                ):
                    value = ast.literal_eval(node.value)
                    if isinstance(value, str):
                        return value
    raise RuntimeError(f"JS_MARKER_CODEC_PREAMBLE not found in {base_path}")


_JSBOX_HARNESS = "(function() {\n" + _load_shared_codec_preamble() + r"""
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
