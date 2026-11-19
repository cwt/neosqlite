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

    Attempts ``PRAGMA wal_checkpoint(TRUNCATE)`` and ``COMMIT``, logging a
    warning and continuing if either statement fails.

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
        pass

    try:
        db.execute("COMMIT")
    except Exception as e:
        logger.warning(f"COMMIT failed during migration: {e}")
        pass

    return []


def migrate_autovacuum(
    db_path: str,
    target_autovacuum: int,
    target_journal_mode: str = "WAL",
    extra_conn_kwargs: dict | None = None,
) -> bool:
    """Migrate a database to a different auto_vacuum mode.

    Performs an on-disk migration by checking ``needs_migration``,
    checkpointing WAL state, backing up the database (and any ``-wal``/``-shm``
    files), streaming a consistent on-disk snapshot via SQLite's backup API,
    rebuilding with ``VACUUM INTO`` (or ``VACUUM`` plus copy on SQLite older
    than 3.27.0), applying ``target_journal_mode``, and replacing ``db_path``.
    Restores backups and deletes transient files if any step raises.

    Args:
        db_path: Path to the SQLite database file.
        target_autovacuum: Desired ``auto_vacuum`` mode (0, 1, or 2).
        target_journal_mode: Desired ``journal_mode`` for output DB.
            Defaults to ``"WAL"``.
        extra_conn_kwargs: Optional extra kwargs accepted for compatibility
            (currently unused).

    Returns:
        bool: True if migration ran and succeeded, False if already at the
            requested ``auto_vacuum`` mode.

    Raises:
        Exception: Re-raises any error encountered after restoring backups.
    """
    initial_conn = sqlite3.connect(db_path)
    try:
        if not needs_migration(initial_conn, target_autovacuum):
            return False
        checkpoint_and_prepare_for_migration(initial_conn)
    finally:
        initial_conn.close()

    timestamp = int(time.time() * 1000)

    wal_path = f"{db_path}-wal"
    shm_path = f"{db_path}-shm"

    original_backup_path = f"{db_path}.backup_{timestamp}"
    backup_files: dict[str, str] = {db_path: original_backup_path}
    shutil.copy2(db_path, original_backup_path)

    wal_backup_path = f"{original_backup_path}-wal"
    if os.path.exists(wal_path):
        shutil.copy2(wal_path, wal_backup_path)
        backup_files[wal_path] = wal_backup_path

    shm_backup_path = f"{original_backup_path}-shm"
    if os.path.exists(shm_path):
        shutil.copy2(shm_path, shm_backup_path)
        backup_files[shm_path] = shm_backup_path

    temp_new_path = f"{db_path}.temp_autovacuum_{timestamp}"
    snapshot_path = f"{db_path}.snapshot_{timestamp}"

    # Escape single quotes for the VACUUM INTO literal (#167); paths with
    # quotes are legal on Unix and previously aborted the migration.
    escaped_dest = temp_new_path.replace("'", "''")

    conn: sqlite3.Connection | None = None
    snap_conn: sqlite3.Connection | None = None
    out_conn: sqlite3.Connection | None = None
    replaced_via_rename = False
    try:
        conn = sqlite3.connect(original_backup_path)
        conn.isolation_level = None
        checkpoint_and_prepare_for_migration(conn)

        old_journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if old_journal != "wal":
            conn.execute("PRAGMA journal_mode=WAL")

        conn.execute(f"PRAGMA auto_vacuum={target_autovacuum}")

        if os.path.exists(temp_new_path):
            os.remove(temp_new_path)

        # Consistent snapshot ON DISK via the sqlite backup API: it
        # streams pages incrementally (no full-RAM load, safe for
        # multi-GB databases) and cannot tear even if another connection
        # commits mid-copy (#167).
        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)

        snap_conn = sqlite3.connect(snapshot_path)
        try:
            conn.backup(snap_conn)
            # auto_vacuum must be set on the snapshot BEFORE VACUUM so the
            # rebuilt output carries the target mode.
            snap_conn.execute(f"PRAGMA auto_vacuum={target_autovacuum}")
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
        conn.close()
        conn = None

        # Honor the requested journal mode on the OUTPUT database —
        # previously this parameter was dead (#167). Applied after the
        # file exists, since journal_mode is persistent per-file.
        out_conn = sqlite3.connect(temp_new_path)
        try:
            out_conn.execute(f"PRAGMA journal_mode={target_journal_mode}")
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
                        f"PRAGMA journal_mode={target_journal_mode}"
                    )
                except sqlite3.OperationalError as jm_err:
                    logger.debug(
                        f"Deferred journal_mode={target_journal_mode} while "
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

        for backup_path in backup_files.values():
            if os.path.exists(backup_path):
                os.remove(backup_path)

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
            for original_path, backup_path in backup_files.items():
                if os.path.exists(backup_path):
                    os.replace(backup_path, original_path)
        else:
            for backup_path in backup_files.values():
                try:
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
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
