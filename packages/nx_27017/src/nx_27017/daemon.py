"""Daemon and process management for NX-27017."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from typing import Any

from nx_27017.handler import NeoSQLiteHandler
from nx_27017.server import run_server_threaded
from nx_27017.wire_protocol import SHUTDOWN_POLL_INTERVAL, SHUTDOWN_RETRY_COUNT

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

try:
    import uvloop
except ImportError:
    uvloop = None  # type: ignore[assignment]

logger = logging.getLogger("nx_27017")

# Active advisory-locked PID file descriptors held by this process on POSIX.
_PID_LOCK_FDS: dict[str, int] = {}


def _release_pid_lock(pid_file: str) -> None:
    """Release and close any held POSIX lock descriptor for ``pid_file``."""
    fd = _PID_LOCK_FDS.pop(os.path.abspath(pid_file), None)
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass


def write_pid_file(pid_file: str) -> bool:
    """Write the current process PID to ``pid_file`` safely.

    Opens ``pid_file`` with ``O_NOFOLLOW`` (refusing symlinks) and mode
    ``0o600``, acquires a non-blocking exclusive ``fcntl.flock`` lock on POSIX,
    verifies no existing daemon process is already active, and writes the
    current PID.

    Args:
        pid_file: Filesystem path to the PID file to write.

    Returns:
        bool: ``True`` if the PID was written, ``False`` if the daemon is
            already running, ``pid_file`` is a symlink, or an ``OSError``
            occurred.
    """
    abs_path = os.path.abspath(pid_file)
    if os.path.islink(pid_file):
        logger.error(f"Refusing to write PID file through symlink: {pid_file}")
        return False

    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(pid_file, flags, 0o600)
    except OSError as e:
        logger.error(f"Cannot write PID file: {e}")
        return False

    keep_fd = False
    try:
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                existing = get_pid(pid_file)
                logger.error(f"NX-27017 is already running (PID: {existing})")
                return False

        os.lseek(fd, 0, os.SEEK_SET)
        raw = os.read(fd, 64).decode("ascii", errors="ignore").strip()
        if raw:
            try:
                existing_pid = int(raw)
            except ValueError:
                existing_pid = None
            if existing_pid is not None and _is_pid_running(existing_pid):
                logger.error(
                    f"NX-27017 is already running (PID: {existing_pid})"
                )
                return False

        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, str(os.getpid()).encode("ascii"))

        if fcntl is not None:
            old_fd = _PID_LOCK_FDS.pop(abs_path, None)
            if old_fd is not None and old_fd != fd:
                try:
                    os.close(old_fd)
                except OSError:
                    pass
            _PID_LOCK_FDS[abs_path] = fd
            keep_fd = True
        return True
    except OSError as e:
        logger.error(f"Cannot write PID file: {e}")
        return False
    finally:
        if not keep_fd:
            try:
                os.close(fd)
            except OSError:
                pass


def remove_pid_file(pid_file: str):
    """Remove the daemon PID file on shutdown if it exists and is not a symlink.

    Releases any held POSIX advisory lock descriptor and suppresses ``OSError``
    if the file cannot be deleted or was already removed.

    Args:
        pid_file: Filesystem path to the PID file to remove.
    """
    _release_pid_lock(pid_file)
    try:
        if not os.path.islink(pid_file) and os.path.exists(pid_file):
            os.remove(pid_file)
    except OSError:
        pass


def _is_pid_running(pid: int) -> bool:
    """Check whether a process with ``pid`` is currently alive.

    Uses ``OpenProcess`` and ``GetExitCodeProcess`` on Windows because
    ``os.kill(pid, 0)`` sends ``CTRL_C_EVENT`` to the console process group
    on Windows rather than probing process existence. On POSIX, probes via
    ``os.kill(pid, 0)``.

    Args:
        pid: Process ID to check.

    Returns:
        bool: ``True`` if the process appears alive, ``False`` otherwise.
    """
    if pid <= 0:
        return False

    if sys.platform == "win32":
        import ctypes
        import ctypes.wintypes

        process_query_limited_information = 0x1000
        error_access_denied = 5
        still_active = 259

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [
            ctypes.wintypes.DWORD,
            ctypes.wintypes.BOOL,
            ctypes.wintypes.DWORD,
        ]
        kernel32.OpenProcess.restype = ctypes.wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [
            ctypes.wintypes.HANDLE,
            ctypes.POINTER(ctypes.wintypes.DWORD),
        ]
        kernel32.GetExitCodeProcess.restype = ctypes.wintypes.BOOL
        kernel32.CloseHandle.argtypes = [ctypes.wintypes.HANDLE]
        kernel32.CloseHandle.restype = ctypes.wintypes.BOOL

        handle = kernel32.OpenProcess(
            process_query_limited_information, False, pid
        )
        if not handle:
            return bool(ctypes.get_last_error() == error_access_denied)
        try:
            exit_code = ctypes.wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return bool(exit_code.value == still_active)
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def is_running(pid_file: str) -> bool:
    """Check whether a daemon referenced by ``pid_file`` is alive.

    Parses the PID from ``pid_file`` and probes process existence safely
    across POSIX and Windows.

    Args:
        pid_file: Filesystem path to the PID file to inspect.

    Returns:
        bool: ``True`` if the PID file exists, parses cleanly, and the
            process is alive; ``False`` otherwise.
    """
    if not os.path.exists(pid_file):
        return False

    pid = get_pid(pid_file)
    if pid is None:
        return False

    return _is_pid_running(pid)


def get_pid(pid_file: str) -> int | None:
    """Read and parse the integer PID from ``pid_file`` without following symlinks.

    Args:
        pid_file: Filesystem path to the PID file.

    Returns:
        int | None: Parsed process ID, or ``None`` if reading, symlink checks,
            or integer parsing fails.
    """
    if os.path.islink(pid_file):
        return None
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(pid_file, flags)
    except OSError:
        return None
    try:
        raw = os.read(fd, 64).decode("ascii", errors="strict").strip()
        return int(raw)
    except (OSError, ValueError, UnicodeDecodeError):
        return None
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _is_nx_27017_process(pid: int) -> bool:
    """Verify via ``/proc/<pid>/cmdline`` (on Linux) that ``pid`` is NX-27017.

    Only the invoked executable or module is considered.  A substring search
    over the whole command line would match unrelated processes whose argv
    merely mentions a path such as ``packages/nx_27017/tests/``.

    Returns ``True`` when process-cmdline inspection is unavailable on the
    current platform so non-Linux platforms continue to work cleanly.
    """
    if not os.path.exists("/proc"):
        return True
    cmdline_path = f"/proc/{pid}/cmdline"
    if not os.path.exists(cmdline_path):
        return False
    try:
        with open(cmdline_path, "rb") as fh:
            raw = fh.read()
    except OSError:
        # Unreadable process identity: fail closed rather than signal an
        # arbitrary PID referenced by a stale or malicious PID file.
        return False
    if not raw:
        return False
    argv = [
        part.decode("utf-8", errors="replace")
        for part in raw.split(b"\x00")
        if part
    ]
    if not argv:
        return False

    names = ("nx-27017", "nx_27017", "nx_27017.py")
    executable = os.path.basename(argv[0]).lower()
    if executable in names:
        return True
    if executable.startswith("python"):
        if len(argv) >= 3 and argv[1] == "-m":
            return argv[2].lower() in ("nx_27017", "nx_27017.nx_27017")
        if len(argv) >= 2:
            return os.path.basename(argv[1]).lower() in names
    return False


def stop_daemon(pid_file: str) -> int:
    """Stop a running NX-27017 daemon referenced by ``pid_file``.

    Verifies that the PID refers to a running NX-27017 process before sending
    ``SIGTERM``, polls up to ``SHUTDOWN_RETRY_COUNT`` intervals of
    ``SHUTDOWN_POLL_INTERVAL`` seconds, and escalates to ``SIGKILL`` and
    deletes ``pid_file`` if the process does not exit in time.

    Args:
        pid_file: Filesystem path to the daemon's PID file.

    Returns:
        int: ``0`` if the daemon stopped (or was forcefully killed), ``1`` if
            not running, the PID file is invalid, process identity does not
            match NX-27017, or permission is denied.
    """
    if not os.path.exists(pid_file) or os.path.islink(pid_file):
        print("NX-27017 is not running (no PID file found)")
        return 1

    pid = get_pid(pid_file)
    if pid is None:
        print("Invalid PID file")
        return 1

    if not _is_pid_running(pid):
        print("NX-27017 is not running")
        remove_pid_file(pid_file)
        return 1

    if not _is_nx_27017_process(pid):
        logger.error(
            f"Refusing to stop PID {pid}: process is not an NX-27017 instance"
        )
        print(f"PID {pid} is not an NX-27017 process; refusing to signal")
        return 1

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to NX-27017 (PID: {pid})")

        for _ in range(SHUTDOWN_RETRY_COUNT):
            if not _is_pid_running(pid):
                print("NX-27017 stopped")
                return 0
            time.sleep(SHUTDOWN_POLL_INTERVAL)

        sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
        os.kill(pid, sigkill)
        remove_pid_file(pid_file)
        print(f"Forcefully killed NX-27017 (PID: {pid})")
        return 0
    except PermissionError:
        print(f"Permission denied to send signal to PID {pid}")
        return 1
    except OSError:
        # On POSIX this catches ProcessLookupError (ESRCH); on Windows
        # os.kill() raises OSError (WinError 87) when the PID does not exist.
        print("NX-27017 is not running")
        remove_pid_file(pid_file)
        return 1


def check_status(pid_file: str) -> int:
    """Check whether the NX-27017 daemon is running and print its status.

    Args:
        pid_file: Filesystem path to the daemon's PID file.

    Returns:
        int: ``0`` if the daemon is running, ``1`` otherwise.
    """
    if is_running(pid_file):
        pid = get_pid(pid_file)
        print(f"NX-27017 is running (PID: {pid})")
        return 0
    else:
        print("NX-27017 is not running")
        return 1


def daemonize():
    """Detach the current process via Unix double-fork daemonization.

    Forks once and exits the parent, changes working directory to ``/`` and
    starts a new session via ``os.setsid()``, forks a second time so the
    daemon cannot reacquire a controlling terminal, and flushes stdio streams.
    """
    fork = getattr(os, "fork", None)
    if fork is None:
        logger.error("Daemonization is not supported on this platform")
        sys.exit(1)

    try:
        pid = fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(f"First fork failed: {e}")
        sys.exit(1)

    os.chdir("/")
    setsid = getattr(os, "setsid", None)
    if setsid is not None:
        setsid()

    try:
        pid = fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(f"Second fork failed: {e}")
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()

    try:
        with open(os.devnull, "rb") as devnull:
            os.dup2(devnull.fileno(), sys.stdin.fileno())
    except (OSError, ValueError, AttributeError):
        pass


def _signal_exit_handler(signum: int, frame: Any) -> None:
    """Handle termination signals by exiting cleanly.

    Args:
        signum: Signal number received by the process.
        frame: Current stack frame at the time the signal was delivered.
    """
    sys.exit(0)


def run_as_daemon(args: argparse.Namespace):
    """Run the NX-27017 server as a background daemon process.

    Verifies no instance is running, resolves relative database, data, log,
    PID, and FTS5 tokenizer paths to absolute paths before daemonizing,
    redirects ``stdout`` and ``stderr`` to ``args.log_file``, writes
    ``args.pid_file``, installs ``SIGTERM``/``SIGINT`` handlers, runs
    :func:`run_server_sync`, and cleans up the PID file on exit.

    Args:
        args: Parsed CLI namespace containing ``pid_file``, ``log_file``,
            ``db_path``, ``data_dir``, ``fts5_tokenizers``, and server options.
    """
    if args.pid_file and not os.path.isabs(args.pid_file):
        args.pid_file = os.path.abspath(args.pid_file)

    if args.log_file and not os.path.isabs(args.log_file):
        args.log_file = os.path.abspath(args.log_file)

    if is_running(args.pid_file):
        pid = get_pid(args.pid_file)
        logger.error(f"NX-27017 is already running (PID: {pid})")
        sys.exit(1)

    if args.db_path != "memory" and not os.path.isabs(args.db_path):
        args.db_path = os.path.abspath(args.db_path)

    data_dir = getattr(args, "data_dir", None)
    if data_dir not in (None, "memory", ":memory:") and not os.path.isabs(
        data_dir
    ):
        args.data_dir = os.path.abspath(data_dir)

    if args.fts5_tokenizers:
        args.fts5_tokenizers = [
            (name, os.path.abspath(path)) for name, path in args.fts5_tokenizers
        ]

    daemonize()

    with open(args.log_file, "a") as log_fh:
        os.dup2(log_fh.fileno(), sys.stdout.fileno())
        os.dup2(log_fh.fileno(), sys.stderr.fileno())

    if not write_pid_file(args.pid_file):
        sys.exit(1)

    signal.signal(signal.SIGTERM, _signal_exit_handler)
    signal.signal(signal.SIGINT, _signal_exit_handler)

    try:
        run_server_sync(args)
    finally:
        remove_pid_file(args.pid_file)


def run_foreground(args: argparse.Namespace):
    """Run the NX-27017 server in the foreground with file logging.

    Writes ``args.pid_file``, registers ``SIGTERM``/``SIGINT`` handlers,
    attaches a ``FileHandler`` to the root logger at ``DEBUG`` or ``INFO``
    level depending on ``args.verbose``, executes :func:`run_server_sync`,
    and removes the PID file when finished.

    Args:
        args: Parsed CLI namespace containing ``pid_file``, ``log_file``,
            ``verbose``, and server options.
    """
    if not write_pid_file(args.pid_file):
        sys.exit(1)

    signal.signal(signal.SIGTERM, _signal_exit_handler)
    signal.signal(signal.SIGINT, _signal_exit_handler)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    file_handler = logging.FileHandler(args.log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(process)d] - %(message)s"
        )
    )
    logging.getLogger().addHandler(file_handler)

    try:
        run_server_sync(args)
    finally:
        remove_pid_file(args.pid_file)


def run_server_sync(args: argparse.Namespace):
    """Configure the NeoSQLite handler and run the wire-protocol server.

    Maps ``"memory"`` to ``":memory:"``, logs startup configuration, constructs
    :class:`NeoSQLiteHandler`, and starts :func:`run_server_threaded` until
    interrupted via ``KeyboardInterrupt``.

    Args:
        args: Parsed CLI namespace containing ``db_path``, ``host``, ``port``,
            ``journal_mode``, ``fts5_tokenizers``, ``threaded``, and optional
            ``data_dir`` and ``single_db_compat`` attributes.
    """
    db_path = args.db_path
    if db_path == "memory":
        db_path = ":memory:"

    tokenizers = args.fts5_tokenizers

    async_lib = "uvloop" if uvloop is not None else "asyncio"
    logger.info(
        "Starting NX-27017 with db_path=%s, host=%s, port=%s, journal_mode=%s, tokenizers=%s (async=%s, threaded=%s)",
        db_path,
        args.host,
        args.port,
        args.journal_mode,
        tokenizers,
        async_lib,
        args.threaded,
    )

    with NeoSQLiteHandler(
        db_path,
        tokenizers=tokenizers,
        journal_mode=args.journal_mode,
        data_dir=getattr(args, "data_dir", None),
        single_db_compat=getattr(args, "single_db_compat", False),
    ) as handler:
        try:
            run_server_threaded(
                args.host, args.port, handler, use_threading=args.threaded
            )
        except KeyboardInterrupt:
            logger.info("Shutting down...")
