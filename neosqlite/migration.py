"""Auto-vacuum migration utilities for NeoSQLite.

Provides functionality to migrate SQLite databases to different
auto_vacuum modes while preserving all data.
"""

from __future__ import annotations

import logging
import os
import shutil
import time

from ._sqlite import sqlite3
from .options import AutoVacuumMode, JournalMode

logger = logging.getLogger(__name__)


def needs_migration(
    db: sqlite3.Connection,
    target_autovacuum: int,
) -> bool:
    """Check if the database needs auto_vacuum migration.

    Args:
        db: SQLite connection to inspect.
        target_autovacuum: Desired ``auto_vacuum`` mode integer.

    Returns:
        bool: True if current ``auto_vacuum`` differs from target, else False.
    """
    current = db.execute("PRAGMA auto_vacuum").fetchone()[0]
    return current != target_autovacuum


def get_journal_mode(db: sqlite3.Connection) -> str:
    """Get the current journal mode of a database.

    Args:
        db: SQLite connection to inspect.

    Returns:
        str: Active SQLite ``journal_mode`` pragma value.
    """
    return db.execute("PRAGMA journal_mode").fetchone()[0]


def checkpoint_and_prepare_for_migration(
    db: sqlite3.Connection,
) -> list[str]:
    """Ensure database is ready for migration by checkpointing WAL.

    Attempts ``PRAGMA wal_checkpoint(TRUNCATE)`` and commits any active
    transaction on ``db``, logging a warning and continuing if either fails.

    Args:
        db: SQLite connection to checkpoint.

    Returns:
        list[str]: Empty list after checkpoint preparation completes.
    """
    # Ensure all WAL data is written to main file
    try:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception as e:
        logger.warning(f"WAL checkpoint failed during migration: {e}")

    if db.in_transaction:
        try:
            db.execute("COMMIT")
        except Exception as e:
            logger.warning(f"COMMIT failed during migration: {e}")

    return []


def migrate_autovacuum(
    db_path: str,
    target_autovacuum: int | str,
    target_journal_mode: str = "WAL",
    extra_conn_kwargs: dict | None = None,
) -> bool:
    """Migrate a database to a different auto_vacuum mode.

    Validates target modes, checks ``needs_migration``, checkpoints WAL state,
    captures an unmutated backup via SQLite's online backup API, streams a
    consistent snapshot, rebuilds with ``VACUUM INTO`` (or ``VACUUM`` plus copy
    on SQLite older than 3.27.0), applies ``target_journal_mode``, and replaces
    ``db_path``. Restores the backup and deletes transient files on error.

    Args:
        db_path: Path to the SQLite database file.
        target_autovacuum: Desired ``auto_vacuum`` mode (0, 1, 2 or mode name).
        target_journal_mode: Desired ``journal_mode`` for output DB.
            Defaults to ``"WAL"``.
        extra_conn_kwargs: Optional extra kwargs accepted for compatibility
            (currently unused).

    Returns:
        bool: True if migration ran and succeeded, False if already at the
            requested ``auto_vacuum`` mode.

    Raises:
        ValueError: If ``target_autovacuum`` or ``target_journal_mode`` is
            invalid.
        Exception: Re-raises any error encountered after restoring backups.
    """
    validated_autovacuum = AutoVacuumMode.validate(target_autovacuum)
    validated_journal_mode = JournalMode.validate(target_journal_mode)

    timestamp = int(time.time() * 1000)

    wal_path = f"{db_path}-wal"
    shm_path = f"{db_path}-shm"

    original_backup_path = f"{db_path}.backup_{timestamp}"
    temp_new_path = f"{db_path}.temp_autovacuum_{timestamp}"
    snapshot_path = f"{db_path}.snapshot_{timestamp}"

    backup_created = False
    initial_conn = sqlite3.connect(db_path)
    try:
        if not needs_migration(initial_conn, validated_autovacuum):
            return False
        checkpoint_and_prepare_for_migration(initial_conn)
        backup_conn = sqlite3.connect(original_backup_path)
        try:
            # Capture a consistent snapshot under SQLite's shared lock instead
            # of copying the main DB and -wal/-shm sidecars separately.
            initial_conn.backup(backup_conn)
            backup_created = True
        finally:
            backup_conn.close()
    except Exception:
        if os.path.exists(original_backup_path):
            try:
                os.remove(original_backup_path)
            except OSError:
                pass
        raise
    finally:
        initial_conn.close()

    # Escape single quotes for the VACUUM INTO literal (#167); paths with
    # quotes are legal on Unix and previously aborted the migration.
    escaped_dest = temp_new_path.replace("'", "''")

    conn: sqlite3.Connection | None = None
    snap_conn: sqlite3.Connection | None = None
    out_conn: sqlite3.Connection | None = None
    replaced_via_rename = False
    try:
        if os.path.exists(temp_new_path):
            os.remove(temp_new_path)
        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)

        # Stream a clean snapshot from the unmutated backup so
        # original_backup_path never has its PRAGMAs altered.
        conn = sqlite3.connect(original_backup_path)
        snap_conn = sqlite3.connect(snapshot_path)
        try:
            conn.backup(snap_conn)
            conn.close()
            conn = None

            # auto_vacuum must be set on the snapshot BEFORE VACUUM so the
            # rebuilt output carries the target mode.
            snap_conn.execute(f"PRAGMA auto_vacuum={validated_autovacuum}")
            if sqlite3.sqlite_version_info >= (3, 27, 0):
                snap_conn.execute(f"VACUUM INTO '{escaped_dest}'")
            else:
                # VACUUM INTO was added in SQLite 3.27.0. Rebuild the
                # snapshot in place, then copy the completed file.
                snap_conn.execute("VACUUM")
                snap_conn.close()
                snap_conn = None
                shutil.copy2(snapshot_path, temp_new_path)
        finally:
            if snap_conn is not None:
                snap_conn.close()
                snap_conn = None
            if conn is not None:
                conn.close()
                conn = None

        # Honor the requested journal mode on the OUTPUT database —
        # previously this parameter was dead (#167). Applied after the
        # file exists, since journal_mode is persistent per-file.
        out_conn = sqlite3.connect(temp_new_path)
        try:
            out_conn.execute(f"PRAGMA journal_mode={validated_journal_mode}")
        finally:
            out_conn.close()
            out_conn = None

        try:
            os.replace(temp_new_path, db_path)
            replaced_via_rename = True
        except PermissionError:
            # On Windows, replacing an open database file fails while another
            # connection (e.g., an active WAL reader) holds it open. Stream the
            # rebuilt database through SQLite's online backup API instead so
            # active readers stay consistent and SQLite checkpoints the WAL
            # cleanly once they close.
            src_conn = sqlite3.connect(temp_new_path)
            dest_conn: sqlite3.Connection | None = None
            try:
                dest_conn = sqlite3.connect(db_path)
                src_conn.backup(dest_conn)
                try:
                    dest_conn.execute(
                        f"PRAGMA journal_mode={validated_journal_mode}"
                    )
                except sqlite3.OperationalError as jm_err:
                    logger.debug(
                        f"Deferred journal_mode={validated_journal_mode} while "
                        f"other connections are active: {jm_err}"
                    )
            finally:
                if dest_conn is not None:
                    dest_conn.close()
                src_conn.close()
            if os.path.exists(temp_new_path):
                os.remove(temp_new_path)

        if replaced_via_rename:
            if os.path.exists(wal_path):
                os.remove(wal_path)
            if os.path.exists(shm_path):
                os.remove(shm_path)

        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)

        if os.path.exists(original_backup_path):
            os.remove(original_backup_path)

        return True

    except Exception as e:
        if snap_conn is not None:
            try:
                snap_conn.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if out_conn is not None:
            try:
                out_conn.close()
            except Exception:
                pass
        logger.error(f"Migration failed, restoring backup: {e}")
        if replaced_via_rename:
            if os.path.exists(original_backup_path):
                os.replace(original_backup_path, db_path)
            for sidecar in (wal_path, shm_path):
                try:
                    if os.path.exists(sidecar):
                        os.remove(sidecar)
                except OSError:
                    pass
        elif backup_created and os.path.exists(original_backup_path):
            try:
                os.remove(original_backup_path)
            except OSError:
                pass
        # Remove transient artifacts so a retry starts clean (#167)
        for leftover in (snapshot_path, temp_new_path):
            try:
                if os.path.exists(leftover):
                    os.remove(leftover)
            except OSError:
                pass
        raise


def should_migrate() -> bool:
    """Check if auto_vacuum migration is enabled via environment variable.

    Returns:
        bool: True if ``AUTOVACUUM_MIGRATION=1`` is set, False otherwise.
    """
    return os.environ.get("AUTOVACUUM_MIGRATION", "0") == "1"
