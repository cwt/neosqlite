#!/usr/bin/env python3
"""NX-27017 - MongoDB Wire Protocol Server backed by SQLite."""

from __future__ import annotations

import argparse
import logging

from neosqlite.options import JournalMode
from nx_27017.daemon import (
    check_status,
    run_as_daemon,
    run_foreground,
    stop_daemon,
)
from nx_27017.wire_protocol import (
    LOG_FILE,
    PID_FILE,
)

logger = logging.getLogger("nx_27017")


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        prog="nx_27017",
        description=(
            "NeoSQLite Experimental Project 27017 (NX-27017) - "
            "MongoDB Wire Protocol Server"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run in foreground with default db
  %(prog)s -d                       # Run as daemon in background
  %(prog)s -d --db /data/mongo.db   # Daemon with specific database
  %(prog)s --host 0.0.0.0 -p 27018  # Listen on all interfaces, port 27018
  %(prog)s --stop                   # Stop running daemon
  %(prog)s --status                 # Check daemon status
  %(prog)s --verbose                # Enable debug logging
  %(prog)s --threaded               # Use threaded server (debugging)
        """,
    )

    parser.add_argument(
        "-d",
        "--daemon",
        action="store_true",
        help="Run as a background daemon",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="nx-27017.db",
        help=(
            "SQLite database path (default: nx-27017.db, use 'memory' for in-memory)"
        ),
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=27017,
        help="Port to listen on (default: 27017)",
    )
    parser.add_argument(
        "--log-file",
        default=LOG_FILE,
        help=f"Log file path (default: {LOG_FILE})",
    )
    parser.add_argument(
        "--pid-file",
        default=PID_FILE,
        help=f"PID file path (default: {PID_FILE})",
    )
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Stop the running daemon",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check if daemon is running",
    )
    parser.add_argument(
        "--threaded",
        action="store_true",
        help="Use threaded server instead of asyncio (for debugging)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    parser.add_argument(
        "--fts5-tokenizer",
        dest="fts5_tokenizers",
        action="append",
        default=None,
        help=(
            "FTS5 tokenizer as 'name=path' (can be specified multiple times). "
            "Example: --fts5-tokenizer icu=/path/to/libfts5_icu.so"
        ),
    )
    parser.add_argument(
        "-j",
        "--journal-mode",
        dest="journal_mode",
        default="WAL",
        choices=["WAL", "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"],
        help=(
            "SQLite journal mode (default: WAL). "
            "WAL provides best concurrency; DELETE is traditional rollback."
        ),
    )

    args = parser.parse_args()

    args.journal_mode = JournalMode.validate(args.journal_mode)

    if args.fts5_tokenizers:
        args.fts5_tokenizers = [
            tuple(t.split("=", 1)) for t in args.fts5_tokenizers
        ]
    else:
        args.fts5_tokenizers = None

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - [%(process)d] - %(message)s",
    )

    match (args.stop, args.status, args.daemon):
        case (True, _, _):
            return stop_daemon(args.pid_file)
        case (_, True, _):
            return check_status(args.pid_file)
        case (_, _, True):
            return run_as_daemon(args)
        case _:
            run_foreground(args)


if __name__ == "__main__":
    main()
