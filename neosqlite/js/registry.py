"""Pluggable JavaScript engine registry, preference selection, and worker pool.

By default (``engine="auto"``), NeoSQLite prefers ``jsbox`` (QuickJS in an
isolated Python subprocess) over ``node`` (patched external Node.js with
``--permission`` and per-call ``vm.createContext`` sandboxes). Users can select
an engine explicitly via :func:`set_js_engine`, :func:`use_js_engine`, or the
``NEOSQLITE_JS_ENGINE`` environment variable, or register additional backends
via :func:`register_js_engine`.
"""

from __future__ import annotations

import atexit
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from .base import JsEngineBackend, JsEngineInfo, JsWorker
from .engines.jsbox_engine import JsboxEngineBackend
from .engines.node_engine import NodeEngineBackend

JS_ENGINE_ENV_VAR = "NEOSQLITE_JS_ENGINE"

_DISABLED_ALIASES = frozenset({"none", "off", "disabled", "false", "0"})

_registry_lock = threading.RLock()
_backends: list[JsEngineBackend] = [
    JsboxEngineBackend(),
    NodeEngineBackend(),
]
_selected_engine: str | None = None
_workers: dict[str, tuple[JsEngineInfo, JsWorker]] = {}


def _normalize_engine_name(engine: str) -> str:
    cleaned = engine.strip().lower()
    if cleaned in _DISABLED_ALIASES:
        return "none"
    return cleaned


def _registered_names_locked() -> list[str]:
    return [backend.name.strip().lower() for backend in _backends]


def _find_backend_locked(name: str) -> JsEngineBackend | None:
    target = name.strip().lower()
    for backend in _backends:
        if backend.name.strip().lower() == target:
            return backend
    return None


def _resolve_mode_locked(override: str | None = None) -> str:
    if override is not None:
        return _normalize_engine_name(override)
    if _selected_engine is not None:
        return _selected_engine
    env_value = os.environ.get(JS_ENGINE_ENV_VAR, "").strip()
    if env_value:
        return _normalize_engine_name(env_value)
    return "auto"


def register_js_engine(
    backend: JsEngineBackend,
    *,
    priority: int | None = None,
) -> None:
    """Register or replace a pluggable JavaScript engine backend.

    Args:
        backend: Backend implementing :class:`JsEngineBackend`. Its ``name``
            is normalized case-insensitively and cannot be ``"auto"`` or
            ``"none"``.
        priority: Optional zero-based insertion index in auto-selection order
            (``0`` means highest priority). When ``None``, appends after
            existing backends.

    Raises:
        ValueError: If ``backend.name`` is empty or reserved.
    """
    name = backend.name.strip().lower()
    if not name or name == "auto" or name in _DISABLED_ALIASES:
        raise ValueError(f"Invalid JavaScript engine name: {backend.name!r}")
    with _registry_lock:
        existing_entry = _workers.pop(name, None)
        if existing_entry is not None:
            existing_entry[1].close()
        for idx, existing in enumerate(_backends):
            if existing.name.strip().lower() == name:
                _backends.pop(idx)
                break
        if priority is None:
            _backends.append(backend)
        else:
            clamped = max(0, min(priority, len(_backends)))
            _backends.insert(clamped, backend)


def unregister_js_engine(name: str) -> bool:
    """Unregister a JavaScript engine backend by name.

    Args:
        name: Registered backend name.

    Returns:
        bool: ``True`` if a backend was removed, ``False`` otherwise.
    """
    cleaned = name.strip().lower()
    with _registry_lock:
        entry = _workers.pop(cleaned, None)
        if entry is not None:
            entry[1].close()
        for idx, backend in enumerate(_backends):
            if backend.name.strip().lower() == cleaned:
                _backends.pop(idx)
                return True
        return False


def set_js_engine(engine: str | None) -> None:
    """Select the JavaScript engine used for ``$function`` and ``$where``.

    Args:
        engine: One of:
            - ``"auto"`` or ``None``: Prefer ``jsbox`` over ``node`` (respecting
              ``NEOSQLITE_JS_ENGINE`` when ``None`` is passed).
            - ``"jsbox"``: Require the ``jsbox`` (QuickJS) backend.
            - ``"node"``: Require a patched external Node.js backend.
            - ``"none"`` (or ``"off"``/``"disabled"``): Disable JavaScript
              operators completely.
            - Any custom engine name registered via :func:`register_js_engine`.

    Raises:
        ValueError: If ``engine`` is not a recognized option.
    """
    global _selected_engine
    with _registry_lock:
        if engine is None:
            _selected_engine = None
            close_js_workers()
            return
        if not isinstance(engine, str):
            raise TypeError(
                f"JavaScript engine name must be a string or None, "
                f"got {type(engine).__name__}"
            )
        normalized = _normalize_engine_name(engine)
        valid = ["auto", "none", *_registered_names_locked()]
        if normalized not in valid:
            raise ValueError(
                f"Unknown JavaScript engine {engine!r}. "
                f"Valid options: {', '.join(valid)}"
            )
        if _selected_engine != normalized:
            _selected_engine = normalized
            close_js_workers()


def get_js_engine() -> str:
    """Return the currently configured JS engine mode (e.g. ``"auto"``)."""
    with _registry_lock:
        return _resolve_mode_locked()


@contextmanager
def use_js_engine(engine: str | None) -> Iterator[None]:
    """Temporarily select a JS engine within a ``with`` block."""
    with _registry_lock:
        previous = _selected_engine
    set_js_engine(engine)
    try:
        yield
    finally:
        set_js_engine(previous)


def list_js_engines() -> list[JsEngineInfo]:
    """Probe all registered JavaScript engines in priority order."""
    with _registry_lock:
        backends = list(_backends)
    return [backend.probe() for backend in backends]


def _select_backend_and_info(
    override: str | None = None,
) -> tuple[JsEngineBackend, JsEngineInfo] | None:
    with _registry_lock:
        mode = _resolve_mode_locked(override)
        if mode == "none":
            return None
        if mode == "auto":
            candidates = list(_backends)
        else:
            backend = _find_backend_locked(mode)
            if backend is None:
                return None
            candidates = [backend]

    for candidate in candidates:
        info = candidate.probe()
        if info.available:
            return candidate, info
    return None


def get_active_js_engine(engine: str | None = None) -> JsEngineInfo | None:
    """Return metadata for the active JS engine, or ``None`` if unavailable."""
    selected = _select_backend_and_info(engine)
    return None if selected is None else selected[1]


def is_js_available(engine: str | None = None) -> bool:
    """Return whether a sandboxed JavaScript engine is currently available."""
    return get_active_js_engine(engine) is not None


def js_unavailable_reason(engine: str | None = None) -> str:
    """Explain why JavaScript operators are unavailable (or report active)."""
    with _registry_lock:
        mode = _resolve_mode_locked(engine)
        if mode == "none":
            return "JavaScript execution is disabled (engine='none')"
        if mode == "auto":
            candidates = list(_backends)
        else:
            backend = _find_backend_locked(mode)
            if backend is None:
                valid = ", ".join(["auto", "none", *_registered_names_locked()])
                return (
                    f"Unknown JavaScript engine {mode!r} "
                    f"(valid options: {valid})"
                )
            candidates = [backend]

    reasons: list[str] = []
    for candidate in candidates:
        info = candidate.probe()
        if info.available:
            version_part = f" {info.version}" if info.version else ""
            return f"available ({info.name}{version_part})"
        reasons.append(f"{info.name}: {info.detail}")
    return "; ".join(reasons) if reasons else "no JS engines registered"


def get_js_worker(engine: str | None = None) -> JsWorker | None:
    """Return the shared sandboxed worker for the active JS engine."""
    selected = _select_backend_and_info(engine)
    if selected is None:
        return None
    backend, info = selected
    worker_key = backend.name.strip().lower()
    with _registry_lock:
        cached = _workers.get(worker_key)
        if cached is not None:
            cached_info, cached_worker = cached
            if cached_info == info:
                return cached_worker
            cached_worker.close()
            _workers.pop(worker_key, None)
        worker = backend.create_worker()
        _workers[worker_key] = (info, worker)
        return worker


def close_js_workers() -> None:
    """Terminate and clear all cached JavaScript worker subprocesses."""
    with _registry_lock:
        entries = list(_workers.values())
        _workers.clear()
    for _, worker in entries:
        worker.close()


def reset_js_engine_cache() -> None:
    """Clear cached availability probes across all registered JS backends."""
    with _registry_lock:
        backends = list(_backends)
    for backend in backends:
        backend.reset_cache()


atexit.register(close_js_workers)
