"""Backwards-compatible facade for the sandboxed Node.js worker.

New code should prefer :mod:`neosqlite.js` (which supports selectable engines
with ``jsbox`` preferred over ``node`` by default). This module preserves the
original ``NodeWorker``, ``JsWorkerError``, ``get_worker``, and
``close_worker`` API.
"""

from __future__ import annotations

import atexit
from typing import TYPE_CHECKING

from .js.base import JsEngineInfo, JsWorkerError
from .js.engines.node_engine import (
    DEFAULT_NODE_HEAP_MB as _DEFAULT_HEAP_MB,
)
from .js.engines.node_engine import (
    DEFAULT_NODE_MEMORY_MB as _DEFAULT_MEMORY_MB,
)
from .js.engines.node_engine import NodeWorker
from .js.registry import (
    _registry_lock,
    _workers,
    close_js_workers,
    get_js_worker,
)
from .js.supervisor import DEFAULT_TIMEOUT as _DEFAULT_TIMEOUT

if TYPE_CHECKING:
    from .js_runtime import NodeRuntime


def get_worker(runtime: NodeRuntime | None = None) -> NodeWorker | None:
    """Return the shared Node.js worker via ``neosqlite.js.registry``."""
    if runtime is None:
        worker = get_js_worker("node")
        return worker if isinstance(worker, NodeWorker) else None

    from . import js_runtime

    active = js_runtime.find_node_runtime()
    if active == runtime:
        worker = get_js_worker("node")
        return worker if isinstance(worker, NodeWorker) else None

    with _registry_lock:
        cached = _workers.get("node")
        if cached is not None:
            _, existing_worker = cached
            if (
                isinstance(existing_worker, NodeWorker)
                and existing_worker.runtime == runtime
            ):
                return existing_worker
            existing_worker.close()
            _workers.pop("node", None)
        new_worker = NodeWorker(runtime)
        info = JsEngineInfo(
            name="node",
            available=True,
            version=runtime.version_string,
            detail=f"Node.js {runtime.version_string}",
            path=runtime.path,
        )
        _workers["node"] = (info, new_worker)
        return new_worker


def close_worker() -> None:
    """Shut down any cached JS engine workers."""
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
