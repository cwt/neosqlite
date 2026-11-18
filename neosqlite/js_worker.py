"""Out-of-process JavaScript worker with sandboxing and resource limits.

A single long-lived Node.js process speaks newline-delimited JSON over
stdin/stdout. Spawning per document is ~50 ms/call; a persistent worker keeps
it under 0.1 ms/call. The worker is started with ``node --permission`` so file,
network, and child-process access are denied, and calls are bounded by a
wall-clock timeout, V8's ``--max-old-space-size`` heap cap across all
platforms, and an ``RLIMIT_AS`` address-space limit on Linux.
"""

from __future__ import annotations

import json
import logging
import selectors
import subprocess
import sys
import threading
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .objectid import ObjectId

if TYPE_CHECKING:
    from .js_runtime import NodeRuntime

logger = logging.getLogger(__name__)

_WORKER_SCRIPT = r"""
'use strict';
const readline = require('node:readline');

function toJSONSafe(value) {
  if (value instanceof Date) return {$date: value.toISOString()};
  if (Array.isArray(value)) return value.map(toJSONSafe);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value)) out[key] = toJSONSafe(value[key]);
    return out;
  }
  if (typeof value === 'bigint') return {$numberLong: value.toString()};
  return value;
}

function fromJSONSafe(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    if (typeof value.$date === 'string') return new Date(value.$date);
    if (typeof value.$oid === 'string') {
      return { $oid: value.$oid, toString() { return this.$oid; } };
    }
    const out = {};
    for (const key of Object.keys(value)) out[key] = fromJSONSafe(value[key]);
    return out;
  }
  if (Array.isArray(value)) return value.map(fromJSONSafe);
  return value;
}

const rl = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
rl.on('line', (line) => {
  if (!line.trim()) return;
  let id = null;
  try {
    const req = JSON.parse(line);
    id = req.id;
    const fn = eval('(' + req.code + ')');
    if (typeof fn !== 'function') throw new Error('JS body is not a function');
    const args = (req.args || []).map(fromJSONSafe);
    const value = fn.apply(req.thisArg === undefined ? null : req.thisArg, args);
    process.stdout.write(JSON.stringify({id, ok: true, value: toJSONSafe(value)}) + '\n');
  } catch (err) {
    const message = err && err.message ? String(err.message) : String(err);
    process.stdout.write(JSON.stringify({id, ok: false, error: message}) + '\n');
  }
});
"""

_DEFAULT_TIMEOUT = 5.0
# V8 reserves roughly 2 GB of virtual address space at startup on Linux, so the
# RLIMIT_AS cap must leave headroom even though actual RSS is much smaller.
# On macOS, processes map ~466 GiB of virtual address space (dyld shared cache /
# VM reservations), so setrlimit(RLIMIT_AS) below that footprint raises EINVAL
# (surfaced by Python as ValueError); V8's heap is capped cross-platform via
# --max-old-space-size instead.
_DEFAULT_MEMORY_MB = 4096
_DEFAULT_HEAP_MB = 512


class JsWorkerError(Exception):
    """Raised when the JS worker cannot evaluate an expression."""


class NodeWorker:
    """A supervised persistent Node.js worker."""

    def __init__(
        self,
        runtime: NodeRuntime,
        timeout: float = _DEFAULT_TIMEOUT,
        memory_mb: int = _DEFAULT_MEMORY_MB,
        heap_mb: int | None = None,
    ) -> None:
        self.runtime = runtime
        self.timeout = timeout
        self.memory_mb = memory_mb
        self.heap_mb = (
            min(_DEFAULT_HEAP_MB, memory_mb) if heap_mb is None else heap_mb
        )
        self._proc: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()
        self._next_id = 1
        self._selector: selectors.BaseSelector | None = None

    def _preexec(self) -> None:
        """Cap the worker's virtual address space on Linux."""
        if not sys.platform.startswith("linux"):
            return
        import resource

        limit = self.memory_mb * 1024 * 1024
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            limit = min(limit, hard)
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

    def _start(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            return
        self._proc = subprocess.Popen(
            [
                self.runtime.path,
                "--permission",
                f"--max-old-space-size={self.heap_mb}",
                "-e",
                _WORKER_SCRIPT,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            preexec_fn=(
                self._preexec if sys.platform.startswith("linux") else None
            ),
        )
        stdout = self._proc.stdout
        assert stdout is not None
        self._selector = selectors.DefaultSelector()
        self._selector.register(stdout, selectors.EVENT_READ)

    def _restart(self) -> None:
        self.close()
        self._start()

    def call(
        self,
        code: str,
        args: list[Any] | None = None,
        this_arg: Any = None,
    ) -> dict[str, Any]:
        """Evaluate a JS function with arguments and an optional ``this``."""
        with self._lock:
            self._start()
            assert self._proc is not None and self._proc.stdin is not None
            assert self._selector is not None and self._proc.stdout is not None
            request_id = self._next_id
            self._next_id += 1
            payload = json.dumps(
                {
                    "id": request_id,
                    "code": code,
                    "args": [self._to_marker(a) for a in (args or [])],
                    "thisArg": self._to_marker(this_arg),
                }
            )
            try:
                self._proc.stdin.write(payload + "\n")
                self._proc.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                raise JsWorkerError(f"JS worker unavailable: {exc}") from exc

            events = self._selector.select(self.timeout)
            if not events:
                self._restart()
                raise JsWorkerError(
                    f"JavaScript evaluation timed out after {self.timeout}s"
                )
            line = self._proc.stdout.readline()
            if not line:
                proc = self._proc
                stderr = ""
                try:
                    proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                if proc.stderr is not None:
                    stderr = proc.stderr.read()
                self._restart()
                raise JsWorkerError(
                    f"JS worker exited unexpectedly: {stderr.strip()}"
                )
            try:
                response = json.loads(line)
            except json.JSONDecodeError as exc:
                raise JsWorkerError(
                    f"Invalid JS worker response: {line!r}"
                ) from exc

            if response.get("id") != request_id:
                raise JsWorkerError("JS worker response id mismatch")
            if not response.get("ok"):
                raise JsWorkerError(
                    str(response.get("error", "JavaScript error"))
                )
            return self._from_marker(response.get("value"))

    def _to_marker(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return {"$oid": str(value)}
        if isinstance(value, datetime):
            return {"$date": value.isoformat()}
        if isinstance(value, (list, tuple)):
            return [self._to_marker(v) for v in value]
        if isinstance(value, dict):
            return {k: self._to_marker(v) for k, v in value.items()}
        return value

    def _from_marker(self, value: Any) -> Any:
        if isinstance(value, dict):
            marker = value.get("$date")
            if isinstance(marker, str):
                try:
                    return datetime.fromisoformat(marker)
                except ValueError:
                    return marker
            marker = value.get("$oid")
            if isinstance(marker, str):
                try:
                    return ObjectId(marker)
                except Exception:
                    return marker
            return {k: self._from_marker(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._from_marker(v) for v in value]
        return value

    def close(self) -> None:
        """Terminate the worker if running."""
        if self._selector is not None:
            try:
                self._selector.close()
            except Exception:
                pass
            self._selector = None
        proc = self._proc
        if proc is None:
            return
        self._proc = None
        if proc.poll() is None:
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except OSError:
                pass
            try:
                proc.terminate()
                proc.wait(timeout=1)
            except (subprocess.SubprocessError, OSError):
                try:
                    proc.kill()
                except OSError:
                    pass

    def __enter__(self) -> NodeWorker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


_worker: NodeWorker | None = None
_worker_lock = threading.Lock()


def get_worker(runtime: NodeRuntime | None = None) -> NodeWorker | None:
    """Return the shared worker when a supported runtime is available."""
    global _worker
    if runtime is None:
        from .js_runtime import find_node_runtime

        runtime = find_node_runtime()
    if runtime is None:
        return None
    with _worker_lock:
        if _worker is None or _worker.runtime != runtime:
            if _worker is not None:
                _worker.close()
            _worker = NodeWorker(runtime)
        return _worker


def close_worker() -> None:
    """Shut down the shared worker."""
    global _worker
    with _worker_lock:
        if _worker is not None:
            _worker.close()
            _worker = None
