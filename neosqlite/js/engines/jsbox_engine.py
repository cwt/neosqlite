"""Sandboxed QuickJS backend powered by ``jsbox`` in a child Python process.

``jsbox`` is preferred over Node.js by default because QuickJS has zero ambient
authority (no ``process``, ``env``, ``require``, filesystem, or network APIs)
and enforces native heap, stack, value-size, and bytecode-timeout caps across
all platforms. Running ``jsbox`` inside a dedicated isolated Python subprocess
adds hard OS memory-space isolation (so C-level memory corruption or segfaults
in QuickJS cannot affect the main NeoSQLite process) and hard ``SIGKILL``
timeouts for long native operations.
"""

from __future__ import annotations

import importlib.machinery
import importlib.metadata
import importlib.util
import os
import sys
from functools import lru_cache
from pathlib import Path

from ..base import JsEngineInfo, JsWorkerError
from ..supervisor import DEFAULT_TIMEOUT, SubprocessJsWorker

DEFAULT_JSBOX_MEMORY_MB = 512
DEFAULT_JSBOX_HEAP_MB = 64

_WORKER_SCRIPT = str(Path(__file__).resolve().with_name("_jsbox_worker.py"))


def _find_jsbox_search_dir() -> str | None:
    """Locate the parent directory containing the installed ``jsbox`` package.

    Uses ``importlib.util.find_spec("jsbox")`` and filesystem checks for the
    compiled ``_jsbox`` extension so the native C/Rust module is never loaded
    into the main NeoSQLite process.
    """
    try:
        spec = importlib.util.find_spec("jsbox")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.submodule_search_locations:
        return None
    pkg_dir = os.path.abspath(next(iter(spec.submodule_search_locations)))
    has_native = any(
        os.path.isfile(os.path.join(pkg_dir, f"_jsbox{suffix}"))
        for suffix in importlib.machinery.EXTENSION_SUFFIXES
    )
    if not has_native:
        return None
    return os.path.dirname(pkg_dir)


def _get_jsbox_version() -> str:
    try:
        return importlib.metadata.version("jsbox")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


@lru_cache(maxsize=1)
def probe_jsbox() -> JsEngineInfo:
    """Probe whether ``jsbox`` is installed and usable (cached)."""
    search_dir = _find_jsbox_search_dir()
    if search_dir is None:
        return JsEngineInfo(
            name="jsbox",
            available=False,
            detail=(
                "jsbox package not installed "
                "(install with 'pip install \"neosqlite[js]\"')"
            ),
        )
    version = _get_jsbox_version()
    return JsEngineInfo(
        name="jsbox",
        available=True,
        version=version,
        detail=f"jsbox {version} (QuickJS, isolated subprocess)",
        path=os.path.join(search_dir, "jsbox"),
    )


def reset_jsbox_cache() -> None:
    """Clear the cached ``jsbox`` probe result."""
    cache_clear = getattr(probe_jsbox, "cache_clear", None)
    if cache_clear is not None:
        cache_clear()


class JsboxWorker(SubprocessJsWorker):
    """Persistent isolated Python subprocess worker executing JS via ``jsbox``."""

    engine_name = "jsbox"

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        memory_mb: int = DEFAULT_JSBOX_MEMORY_MB,
        heap_mb: int | None = None,
        jsbox_search_dir: str | None = None,
    ) -> None:
        super().__init__(
            timeout=timeout,
            memory_mb=memory_mb,
            heap_mb=heap_mb,
            default_heap_mb=DEFAULT_JSBOX_HEAP_MB,
        )
        self.jsbox_search_dir = (
            jsbox_search_dir
            if jsbox_search_dir is not None
            else _find_jsbox_search_dir()
        )

    def _build_command(self) -> list[str]:
        cmd = [
            sys.executable,
            "-I",
            "-u",
            _WORKER_SCRIPT,
            "--heap-mb",
            str(self.heap_mb),
        ]
        if self.jsbox_search_dir:
            cmd.extend(["--jsbox-path", self.jsbox_search_dir])
        return cmd


class JsboxEngineBackend:
    """Pluggable ``JsEngineBackend`` backed by ``jsbox``."""

    name = "jsbox"

    def probe(self) -> JsEngineInfo:
        """Probe whether ``jsbox`` is installed."""
        return probe_jsbox()

    def create_worker(
        self,
        *,
        timeout: float | None = None,
        memory_mb: int | None = None,
        heap_mb: int | None = None,
    ) -> JsboxWorker:
        """Create a new isolated ``JsboxWorker``."""
        info = self.probe()
        if not info.available:
            raise JsWorkerError(info.detail)
        return JsboxWorker(
            timeout=DEFAULT_TIMEOUT if timeout is None else timeout,
            memory_mb=(
                DEFAULT_JSBOX_MEMORY_MB if memory_mb is None else memory_mb
            ),
            heap_mb=heap_mb,
        )

    def reset_cache(self) -> None:
        """Clear cached ``jsbox`` probe state."""
        reset_jsbox_cache()
