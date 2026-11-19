"""Sandboxed, pluggable JavaScript execution subsystem for NeoSQLite."""

from __future__ import annotations

from .base import (
    JsEngineBackend,
    JsEngineInfo,
    JsWorker,
    JsWorkerError,
    from_marker,
    to_marker,
)
from .engines.jsbox_engine import JsboxEngineBackend, JsboxWorker
from .engines.node_engine import NodeEngineBackend, NodeWorker
from .registry import (
    JS_ENGINE_ENV_VAR,
    close_js_workers,
    get_active_js_engine,
    get_js_engine,
    get_js_worker,
    is_js_available,
    js_unavailable_reason,
    list_js_engines,
    register_js_engine,
    reset_js_engine_cache,
    set_js_engine,
    unregister_js_engine,
    use_js_engine,
)
from .supervisor import SubprocessJsWorker, clean_subprocess_env

__all__ = [
    "JS_ENGINE_ENV_VAR",
    "JsEngineBackend",
    "JsEngineInfo",
    "JsWorker",
    "JsWorkerError",
    "JsboxEngineBackend",
    "JsboxWorker",
    "NodeEngineBackend",
    "NodeWorker",
    "SubprocessJsWorker",
    "clean_subprocess_env",
    "close_js_workers",
    "from_marker",
    "get_active_js_engine",
    "get_js_engine",
    "get_js_worker",
    "is_js_available",
    "js_unavailable_reason",
    "list_js_engines",
    "register_js_engine",
    "reset_js_engine_cache",
    "set_js_engine",
    "to_marker",
    "unregister_js_engine",
    "use_js_engine",
]
