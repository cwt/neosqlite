"""
Auto-vacuum migration utilities for NeoSQLite.

Provides functionality to migrate SQLite databases to different auto_vacuum modes
while preserving all data.
"""

from __future__ import annotations

import logging
import os
import shutil

from ._sqlite import sqlite3

logger = logging.getLogger(__name__)


def needs_migration(
    db: sqlite3.Connection,
    target_autovacuum: int,
) -> bool:
    """
    Check if the database needs auto_vacuum migration.

    Args:
        db: SQLite connection
        target_autovacuum: Desired auto_vacuum mode

    Returns:
        True if migration is needed, False otherwise
    """
    current = db.execute("PRAGMA auto_vacuum").fetchone()[0]
    return current != target_autovacuum


def get_journal_mode(db: sqlite3.Connection) -> str:
    """Get the current journal mode of a database."""
    return db.execute("PRAGMA journal_mode").fetchone()[0]


def checkpoint_and_prepare_for_migration(
    db: sqlite3.Connection,
) -> list[str]:
    """
    Ensure database is ready for migration by checkpointing WAL.

    Args:
        db: SQLite connection

    Returns:
        List of files that need to be backed up (main db + wal/shm if exist)
    """
    # Ensure all WAL data is written to main file
    try:
        db.execute("PRAGMA wal_checkpoint(FULL)")
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
    """
    Migrate a database to a different auto_vacuum mode.

    This function:
    1. Check if migration is actually needed
    2. Checkpoints any WAL data
    3. Backs up all database files (main + WAL + SHM)
    4. Closes the database
    5. Opens backup and VACUUM INTO new file with desired auto_vacuum
    6. Replaces original with vacuumed file

    Args:
        db_path: Path to the database file
        target_autovacuum: Desired auto_vacuum mode (0, 1, or 2)
        target_journal_mode: Desired journal mode (default: WAL)
        extra_conn_kwargs: Extra arguments for sqlite3.connect()

    Returns:
        True if migration succeeded, False if skipped (already correct)
    """
    conn = sqlite3.connect(db_path)
    try:
        if not needs_migration(conn, target_autovacuum):
            return False
    finally:
        conn.close()

    import time

    timestamp = int(time.time() * 1000)

    wal_path = f"{db_path}-wal"
    shm_path = f"{db_path}-shm"

    files_to_backup = [db_path]
    if os.path.exists(wal_path):
        files_to_backup.append(wal_path)
    if os.path.exists(shm_path):
        files_to_backup.append(shm_path)

    backup_files: dict[str, str] = {}
    for file_path in files_to_backup:
        backup_path = f"{file_path}.backup_{timestamp}"
        shutil.copy2(file_path, backup_path)
        backup_files[file_path] = backup_path

    original_backup_path = backup_files[db_path]
    temp_new_path = f"{db_path}.temp_autovacuum_{timestamp}"

    try:
        conn = sqlite3.connect(original_backup_path)
        conn.isolation_level = None

        old_journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if old_journal != "wal":
            conn.execute("PRAGMA journal_mode=WAL")

        conn.execute(f"PRAGMA auto_vacuum={target_autovacuum}")

        if os.path.exists(temp_new_path):
            os.remove(temp_new_path)

        conn.execute(f"VACUUM INTO '{temp_new_path}'")
        conn.close()

        shutil.move(temp_new_path, db_path)

        for backup_path in backup_files.values():
            if os.path.exists(backup_path):
                os.remove(backup_path)

        if os.path.exists(wal_path):
            os.remove(wal_path)
        if os.path.exists(shm_path):
            os.remove(shm_path)

        return True

    except Exception as e:
        logger.error(f"Migration failed, restoring backup: {e}")
        for original_path, backup_path in backup_files.items():
            if os.path.exists(backup_path):
                shutil.move(backup_path, original_path)
        raise


def should_migrate() -> bool:
    """
    Check if auto_vacuum migration is enabled via environment variable.

    Returns:
        True if AUTOVACUUM_MIGRATION=1 is set, False otherwise
    """
    return os.environ.get("AUTOVACUUM_MIGRATION", "0") == "1"
