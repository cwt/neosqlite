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
    import uvloop
except ImportError:
    uvloop = None  # type: ignore[assignment]

logger = logging.getLogger("nx_27017")


def write_pid_file(pid_file: str) -> bool:
    """Write PID to file. Returns False if already running."""
    if is_running(pid_file):
        pid = get_pid(pid_file)
        logger.error(f"NX-27017 is already running (PID: {pid})")
        return False

    try:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))
        return True
    except OSError as e:
        logger.error(f"Cannot write PID file: {e}")
        return False


def remove_pid_file(pid_file: str):
    """Remove PID file on shutdown."""
    try:
        if os.path.exists(pid_file):
            os.remove(pid_file)
    except OSError:
        pass


def is_running(pid_file: str) -> bool:
    """Check if daemon is already running."""
    if not os.path.exists(pid_file):
        return False

    pid = get_pid(pid_file)
    if pid is None:
        return False

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_pid(pid_file: str) -> int | None:
    """Get PID from file."""
    try:
        with open(pid_file) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def stop_daemon(pid_file: str) -> int:
    """Stop the running daemon."""
    if not os.path.exists(pid_file):
        print("NX-27017 is not running (no PID file found)")
        return 1

    pid = get_pid(pid_file)
    if pid is None:
        print("Invalid PID file")
        return 1

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to NX-27017 (PID: {pid})")

        for _ in range(SHUTDOWN_RETRY_COUNT):
            try:
                os.kill(pid, 0)
            except OSError:
                print("NX-27017 stopped")
                return 0
            time.sleep(SHUTDOWN_POLL_INTERVAL)

        os.kill(pid, signal.SIGKILL)
        print(f"Forcefully killed NX-27017 (PID: {pid})")
        return 0
    except ProcessLookupError:
        print("NX-27017 is not running")
        remove_pid_file(pid_file)
        return 1
    except PermissionError:
        print(f"Permission denied to send signal to PID {pid}")
        return 1


def check_status(pid_file: str) -> int:
    """Check daemon status."""
    if is_running(pid_file):
        pid = get_pid(pid_file)
        print(f"NX-27017 is running (PID: {pid})")
        return 0
    else:
        print("NX-27017 is not running")
        return 1


def daemonize():
    """Perform Unix daemonization (double-fork)."""
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(f"First fork failed: {e}")
        sys.exit(1)

    os.chdir("/")
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(f"Second fork failed: {e}")
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()


def _signal_exit_handler(signum: int, frame: Any) -> None:
    """Handle termination signals by exiting cleanly."""
    sys.exit(0)


def run_as_daemon(args: argparse.Namespace):
    """Run the server as a background daemon."""
    if is_running(args.pid_file):
        pid = get_pid(args.pid_file)
        logger.error(f"NX-27017 is already running (PID: {pid})")
        sys.exit(1)

    if args.db_path != "memory" and not os.path.isabs(args.db_path):
        args.db_path = os.path.abspath(args.db_path)

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
    """Run the server in the foreground."""
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
    """Run the server synchronously."""
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

    handler = NeoSQLiteHandler(
        db_path, tokenizers=tokenizers, journal_mode=args.journal_mode
    )

    try:
        run_server_threaded(
            args.host, args.port, handler, use_threading=args.threaded
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
