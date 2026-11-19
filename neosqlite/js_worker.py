"""Backwards-compatible facade for the sandboxed Node.js worker.

New code should prefer :mod:`neosqlite.js` (which supports selectable engines
with ``jsbox`` preferred over ``node`` by default). This module preserves the
original ``NodeWorker``, ``JsWorkerError``, ``get_worker``, and
``close_worker`` API.
"""

from __future__ import annotations

import atexit
import threading
from typing import TYPE_CHECKING

from .js.base import JsWorkerError
from .js.engines.node_engine import (
    DEFAULT_NODE_HEAP_MB as _DEFAULT_HEAP_MB,
)
from .js.engines.node_engine import (
    DEFAULT_NODE_MEMORY_MB as _DEFAULT_MEMORY_MB,
)
from .js.engines.node_engine import NodeWorker
from .js.registry import close_js_workers
from .js.supervisor import DEFAULT_TIMEOUT as _DEFAULT_TIMEOUT

if TYPE_CHECKING:
    from .js_runtime import NodeRuntime

_worker: NodeWorker | None = None
_worker_lock = threading.Lock()


def get_worker(runtime: NodeRuntime | None = None) -> NodeWorker | None:
    """Return the shared Node.js worker when a supported runtime is available."""
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
    """Shut down the shared Node.js worker and any cached engine workers."""
    global _worker
    with _worker_lock:
        if _worker is not None:
            _worker.close()
            _worker = None
    close_js_workers()


atexit.register(close_worker)


__all__ = [
    "_DEFAULT_HEAP_MB",
    "_DEFAULT_MEMORY_MB",
    "_DEFAULT_TIMEOUT",
    "JsWorkerError",
    "NodeWorker",
    "close_worker",
    "get_worker",
]
