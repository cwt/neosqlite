"""Sandboxed external Node.js backend running inside ``vm.createContext``.

Node.js serves as the fallback JS engine (or explicit user choice for V8 JIT
workloads). The child process runs with ``node --permission``, a scrubbed
environment, ``--max-old-space-size`` heap capping on all platforms,
``RLIMIT_AS`` on Linux, and per-call fresh ``vm.createContext(Object.create(null))``
sandboxes so guest code has no access to ``process``, ``require``, ``Buffer``,
or protocol internals.
"""

from __future__ import annotations

import json

from ... import js_runtime
from ..base import JsEngineInfo, JsWorkerError
from ..supervisor import DEFAULT_TIMEOUT, SubprocessJsWorker

# V8 reserves roughly 2 GB of virtual address space at startup on Linux, so the
# RLIMIT_AS cap must leave headroom even though actual RSS is much smaller.
# On macOS, processes map ~466 GiB of virtual address space (dyld shared cache /
# VM reservations), so RLIMIT_AS is applied only on Linux while V8's heap is
# capped cross-platform via --max-old-space-size.
DEFAULT_NODE_MEMORY_MB = 4096
DEFAULT_NODE_HEAP_MB = 512

_NODE_GUEST_HARNESS = r"""
(function() {
  const _JSON_parse = JSON.parse;
  const _JSON_stringify = JSON.stringify;
  const _Promise = Promise;
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
  const _String = String;
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

  function formatErr(err) {
    try {
      if (err && typeof err === 'object' && 'message' in err && err.message !== undefined) {
        return _String(err.message);
      }
      return _String(err);
    } catch (_) {
      return 'JavaScript error (unformattable thrown value)';
    }
  }

  const rawLine = globalThis.__neosqlite_payload__;
  delete globalThis.__neosqlite_payload__;

  const req = _JSON_parse(rawLine);
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

  const rawResult = fn.apply(thisArg, args);
  const settled = new _Promise(function(resolve) {
    _Promise_then.call(
      _Promise_resolve(rawResult),
      function(val) {
        try {
          const encoded = _JSON_stringify(toJSONSafe(val));
          resolve(
            '{"ok":true,"value":' +
              (encoded === undefined ? 'null' : encoded) +
              '}'
          );
        } catch (serErr) {
          resolve(_JSON_stringify({ ok: false, error: formatErr(serErr) }));
        }
      },
      function(rejErr) {
        resolve(_JSON_stringify({ ok: false, error: formatErr(rejErr) }));
      }
    );
  });
  _Object_defineProperty(settled, 'then', {
    value: _Promise_then,
    enumerable: false,
  });
  return settled;
})()
"""

_NODE_WORKER_SCRIPT = (
    "'use strict';\n"
    "const readline = require('node:readline');\n"
    "const vm = require('node:vm');\n"
    "const protoWrite = process.stdout.write.bind(process.stdout);\n"
    "const GUEST_HARNESS = " + json.dumps(_NODE_GUEST_HARNESS) + ";\n"
    "function safeFormatErr(err) {\n"
    "  try {\n"
    "    if (err && typeof err === 'object' && 'message' in err && err.message !== undefined) {\n"
    "      return String(err.message);\n"
    "    }\n"
    "    return String(err);\n"
    "  } catch (_) {\n"
    "    return 'JavaScript error (unformattable thrown value)';\n"
    "  }\n"
    "}\n"
    "process.on('unhandledRejection', (err) => {\n"
    "  process.stderr.write(\n"
    "    'Unhandled rejection: ' + safeFormatErr(err) + '\\n'\n"
    "  );\n"
    "});\n"
    "const rl = readline.createInterface({\n"
    "  input: process.stdin,\n"
    "  crlfDelay: Infinity,\n"
    "});\n"
    "rl.on('line', async (line) => {\n"
    "  if (!line.trim()) return;\n"
    "  let id = null;\n"
    "  try {\n"
    "    const meta = JSON.parse(line);\n"
    "    id = meta.id;\n"
    "    const timeoutMs =\n"
    "      typeof meta.timeout === 'number' && meta.timeout > 0\n"
    "        ? Math.max(1, Math.ceil(meta.timeout * 1000))\n"
    "        : undefined;\n"
    "    const ctx = vm.createContext(Object.create(null));\n"
    "    Object.defineProperty(ctx, '__neosqlite_payload__', {\n"
    "      value: line,\n"
    "      configurable: true,\n"
    "      writable: true,\n"
    "    });\n"
    "    const runOpts = timeoutMs !== undefined ? { timeout: timeoutMs } : {};\n"
    "    const outcome = await vm.runInContext(GUEST_HARNESS, ctx, runOpts);\n"
    "    if (typeof outcome === 'string') {\n"
    "      const parsed = JSON.parse(outcome);\n"
    "      parsed.id = id;\n"
    "      protoWrite(JSON.stringify(parsed) + '\\n');\n"
    "    } else {\n"
    "      protoWrite(JSON.stringify({ id, ok: true, value: null }) + '\\n');\n"
    "    }\n"
    "  } catch (err) {\n"
    "    protoWrite(JSON.stringify({ id, ok: false, error: safeFormatErr(err) }) + '\\n');\n"
    "  }\n"
    "});\n"
)


class NodeWorker(SubprocessJsWorker):
    """Supervised persistent Node.js worker using ``--permission`` and ``vm``."""

    engine_name = "node"

    def __init__(
        self,
        runtime: js_runtime.NodeRuntime,
        timeout: float = DEFAULT_TIMEOUT,
        memory_mb: int = DEFAULT_NODE_MEMORY_MB,
        heap_mb: int | None = None,
    ) -> None:
        self.runtime = runtime
        super().__init__(
            timeout=timeout,
            memory_mb=memory_mb,
            heap_mb=heap_mb,
            default_heap_mb=DEFAULT_NODE_HEAP_MB,
        )

    def _build_command(self) -> list[str]:
        return [
            self.runtime.path,
            "--permission",
            f"--max-old-space-size={self.heap_mb}",
            "-e",
            _NODE_WORKER_SCRIPT,
        ]


class NodeEngineBackend:
    """Pluggable ``JsEngineBackend`` backed by a patched external Node.js."""

    name = "node"

    def probe(self) -> JsEngineInfo:
        """Probe whether a vetted Node.js binary is available on ``PATH``."""
        runtime = js_runtime.find_node_runtime()
        if runtime is None:
            return JsEngineInfo(
                name="node",
                available=False,
                detail=js_runtime.node_unavailable_reason(),
            )
        return JsEngineInfo(
            name="node",
            available=True,
            version=runtime.version_string,
            detail=f"Node.js {runtime.version_string}",
            path=runtime.path,
        )

    def create_worker(
        self,
        *,
        timeout: float | None = None,
        memory_mb: int | None = None,
        heap_mb: int | None = None,
    ) -> NodeWorker:
        """Create a new sandboxed ``NodeWorker``."""
        runtime = js_runtime.find_node_runtime()
        if runtime is None:
            raise JsWorkerError(js_runtime.node_unavailable_reason())
        return NodeWorker(
            runtime,
            timeout=DEFAULT_TIMEOUT if timeout is None else timeout,
            memory_mb=(
                DEFAULT_NODE_MEMORY_MB if memory_mb is None else memory_mb
            ),
            heap_mb=heap_mb,
        )

    def reset_cache(self) -> None:
        """Clear cached Node.js probe state."""
        js_runtime.reset_node_runtime_cache()
