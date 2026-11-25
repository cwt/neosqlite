"""Supervised persistent out-of-process worker for sandboxed JS engines.

Every JS engine runs inside a separate child OS process communicating over
JSON-lines pipes (no ``pickle`` deserialization, no ``multiprocessing``
``fork``/``spawn`` ``__main__`` re-import traps). Any C-level memory
corruption, segfault, OOM kill, or uninterruptible native operation inside the
JS engine is strictly contained to the disposable worker process and cannot
affect the main NeoSQLite process or SQLite database state.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import subprocess
import sys
import threading
from abc import ABC, abstractmethod
from collections import deque
from typing import IO, Any, Self

from .base import JsWorkerError, from_marker, to_marker

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5.0
_STDERR_TAIL_LINES = 32

# Minimal environment variables required on Windows so native CRT/system DLLs
# resolve when launching an executable by absolute path without leaking secrets.
_WINDOWS_SAFE_ENV_KEYS = ("SYSTEMROOT", "SYSTEMDRIVE", "WINDIR")


def clean_subprocess_env() -> dict[str, str]:
    """Return a scrubbed environment dictionary for sandboxed child processes.

    POSIX workers receive a strictly empty environment ``{}`` so parent-process
    secrets never enter the child's address space. Windows workers receive only
    the minimal OS system-root variables required for process initialization.
    """
    if sys.platform == "win32":
        return {
            key: os.environ[key]
            for key in _WINDOWS_SAFE_ENV_KEYS
            if key in os.environ
        }
    return {}


def _stdout_reader_loop(
    stdout: IO[str],
    line_queue: queue.Queue[str | None],
) -> None:
    """Read lines from a worker stdout pipe into ``line_queue`` until EOF."""
    try:
        while True:
            line = stdout.readline()
            if not line:
                break
            line_queue.put(line)
    except (OSError, ValueError):
        pass
    finally:
        line_queue.put(None)


def _stderr_reader_loop(
    stderr: IO[str],
    tail: deque[str],
) -> None:
    """Continuously drain worker stderr into a bounded tail buffer."""
    try:
        while True:
            line = stderr.readline()
            if not line:
                break
            tail.append(line)
    except (OSError, ValueError):
        pass


class SubprocessJsWorker(ABC):
    """Base class for persistent out-of-process JavaScript workers.

    Subclasses provide ``engine_name`` and ``_build_command()``. All IPC,
    cross-platform timeout enforcement (via dedicated reader threads so pipes
    work identically on Linux, macOS, and Windows and stderr never deadlocks),
    crash/OOM recovery, and process cleanup live here.
    """

    engine_name: str = "unknown"

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        memory_mb: int = 512,
        heap_mb: int | None = None,
        default_heap_mb: int = 64,
    ) -> None:
        self.timeout = timeout
        self.memory_mb = memory_mb
        self.heap_mb = (
            min(default_heap_mb, memory_mb) if heap_mb is None else heap_mb
        )
        self._proc: subprocess.Popen[str] | None = None
        self._line_queue: queue.Queue[str | None] | None = None
        self._reader_thread: threading.Thread | None = None
        self._stderr_tail: deque[str] = deque(maxlen=_STDERR_TAIL_LINES)
        self._stderr_thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._next_id = 1

    @abstractmethod
    def _build_command(self) -> list[str]:
        """Return the argv list used to launch the child worker process."""

    def _parent_wait_timeout(self) -> float:
        """Compute the hard-kill deadline in the parent supervisor.

        Adds a small grace window over ``self.timeout`` so engine-level
        interrupts (QuickJS bytecode timeout / V8 vm timeout) can report clean
        timeout errors before the supervisor escalates to ``SIGKILL``.
        """
        grace = min(0.5, max(0.05, self.timeout * 0.25))
        return self.timeout + grace

    def _preexec(self) -> None:
        """Cap the worker's virtual address space on Linux."""
        if not sys.platform.startswith("linux") or self.memory_mb <= 0:
            return
        import resource

        limit = self.memory_mb * 1024 * 1024
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            limit = min(limit, hard)
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

    def _start(self) -> None:
        if self._proc is not None:
            if self._proc.poll() is None:
                return
            self.close()

        preexec = (
            self._preexec
            if sys.platform.startswith("linux") and self.memory_mb > 0
            else None
        )
        proc = subprocess.Popen(
            self._build_command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env=clean_subprocess_env(),
            preexec_fn=preexec,  # noqa: PLW1509
        )
        stdout = proc.stdout
        assert stdout is not None

        line_queue: queue.Queue[str | None] = queue.Queue()
        reader = threading.Thread(
            target=_stdout_reader_loop,
            args=(stdout, line_queue),
            name=f"neosqlite-js-{self.engine_name}-reader",
            daemon=True,
        )
        reader.start()

        stderr_tail: deque[str] = deque(maxlen=_STDERR_TAIL_LINES)
        stderr_thread: threading.Thread | None = None
        if proc.stderr is not None:
            stderr_thread = threading.Thread(
                target=_stderr_reader_loop,
                args=(proc.stderr, stderr_tail),
                name=f"neosqlite-js-{self.engine_name}-stderr",
                daemon=True,
            )
            stderr_thread.start()

        self._proc = proc
        self._line_queue = line_queue
        self._reader_thread = reader
        self._stderr_tail = stderr_tail
        self._stderr_thread = stderr_thread

    def _restart(self) -> None:
        self.close()
        self._start()

    def _reap_and_read_stderr(self, proc: subprocess.Popen[str]) -> str:
        """Reap an unexpectedly exited worker and return its buffered stderr."""
        try:
            proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        stderr_thread = self._stderr_thread
        if (
            stderr_thread is not None
            and stderr_thread is not threading.current_thread()
        ):
            stderr_thread.join(timeout=0.5)
        return "".join(self._stderr_tail)

    def call(
        self,
        code: str,
        args: list[Any] | None = None,
        this_arg: Any = None,
    ) -> Any:
        """Evaluate a JS function inside the isolated worker process."""
        with self._lock:
            try:
                encoded_args = [to_marker(a) for a in (args or [])]
                encoded_this = to_marker(this_arg)
                request_id = self._next_id
                self._next_id += 1
                payload = json.dumps(
                    {
                        "id": request_id,
                        "code": code,
                        "args": encoded_args,
                        "thisArg": encoded_this,
                        "timeout": self.timeout,
                    }
                )
            except (TypeError, ValueError) as exc:
                raise JsWorkerError(
                    f"Cannot serialize JS arguments: {exc}"
                ) from exc

            self._start()
            assert self._proc is not None and self._proc.stdin is not None
            assert self._line_queue is not None

            try:
                self._proc.stdin.write(payload + "\n")
                self._proc.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                self.close()
                raise JsWorkerError(
                    f"JS worker ({self.engine_name}) unavailable: {exc}"
                ) from exc

            try:
                line = self._line_queue.get(timeout=self._parent_wait_timeout())
            except queue.Empty:
                self._restart()
                raise JsWorkerError(
                    f"JavaScript evaluation timed out after {self.timeout}s"
                ) from None

            if line is None:
                proc = self._proc
                stderr = self._reap_and_read_stderr(proc)
                self._restart()
                raise JsWorkerError(
                    f"JS worker ({self.engine_name}) exited unexpectedly: "
                    f"{stderr.strip()}"
                )

            try:
                response = json.loads(line)
            except json.JSONDecodeError as exc:
                self._restart()
                raise JsWorkerError(
                    f"Invalid JS worker response: {line!r}"
                ) from exc

            if not isinstance(response, dict) or (
                response.get("id") != request_id
            ):
                self._restart()
                raise JsWorkerError("JS worker response id mismatch")

            if not response.get("ok"):
                raise JsWorkerError(
                    str(response.get("error", "JavaScript error"))
                )
            return from_marker(response.get("value"))

    def close(self) -> None:
        """Terminate the worker process and clean up pipes and reader threads."""
        with self._lock:
            proc = self._proc
            reader = self._reader_thread
            stderr_reader = self._stderr_thread
            self._proc = None
            self._line_queue = None
            self._reader_thread = None
            self._stderr_thread = None

            if proc is not None:
                if proc.stdin is not None:
                    try:
                        proc.stdin.close()
                    except OSError:
                        pass
                if proc.poll() is None:
                    try:
                        proc.terminate()
                        proc.wait(timeout=1)
                    except (subprocess.SubprocessError, OSError):
                        try:
                            proc.kill()
                            proc.wait(timeout=1)
                        except (subprocess.SubprocessError, OSError):
                            pass

            if reader is not None and reader is not threading.current_thread():
                reader.join(timeout=1.0)
            if (
                stderr_reader is not None
                and stderr_reader is not threading.current_thread()
            ):
                stderr_reader.join(timeout=1.0)

            if proc is not None:
                for stream in (proc.stdout, proc.stderr):
                    if stream is not None:
                        try:
                            stream.close()
                        except OSError:
                            pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
