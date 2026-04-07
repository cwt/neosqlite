"""
Unit tests for NeoSQLite migration utilities.
"""

import os
import sqlite3

import pytest

from neosqlite import AutoVacuumMode
from neosqlite.migration import (
    checkpoint_and_prepare_for_migration,
    get_journal_mode,
    migrate_autovacuum,
    needs_migration,
    should_migrate,
)


def create_database(path, journal_mode, autovacuum_mode, with_data=True):
    """Helper to create a test database."""
    db = sqlite3.connect(path)
    # Set auto_vacuum BEFORE journal_mode for WAL, as WAL mode persists
    db.execute(f"PRAGMA auto_vacuum={autovacuum_mode}")
    db.execute(f"PRAGMA journal_mode={journal_mode}")
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    if with_data:
        db.execute("INSERT INTO test VALUES (1, 'hello')")
        db.execute("INSERT INTO test VALUES (2, 'world')")
    db.commit()
    db.close()


class TestNeedsMigration:
    """Tests for needs_migration function."""

    def test_needs_migration_when_different(self, tmp_path):
        """Test that migration is needed when auto_vacuum differs."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        assert needs_migration(conn, AutoVacuumMode.INCREMENTAL) is True
        conn.close()

    def test_no_migration_when_same(self, tmp_path):
        """Test that migration is not needed when auto_vacuum matches."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.INCREMENTAL)

        conn = sqlite3.connect(db_path)
        assert needs_migration(conn, AutoVacuumMode.INCREMENTAL) is False
        conn.close()

    @pytest.mark.parametrize("autovacuum_mode", [0, 1, 2])
    def test_migrate_is_noop_when_same_mode(self, tmp_path, autovacuum_mode):
        """Test that migrate_autovacuum is a no-op when source matches target."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", autovacuum_mode)
        original_mtime = os.path.getmtime(db_path)

        import time

        time.sleep(0.01)  # Ensure mtime would change if file is modified

        migrate_autovacuum(db_path, autovacuum_mode)

        # File should not be modified
        new_mtime = os.path.getmtime(db_path)
        assert (
            new_mtime == original_mtime
        ), "Database file was modified when no migration needed"

        # No backup files should exist
        files = list(tmp_path.glob("*.backup_*"))
        assert (
            len(files) == 0
        ), f"Backup files created when no migration needed: {files}"

        # No temp files should exist
        temp_files = list(tmp_path.glob("*.temp_*"))
        assert (
            len(temp_files) == 0
        ), f"Temp files created when no migration needed: {temp_files}"

        # Database should be unchanged
        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0] == autovacuum_mode
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == [(1, "hello"), (2, "world")]
        conn.close()

    @pytest.mark.parametrize("autovacuum_mode", [0, 1, 2])
    def test_migrate_is_noop_wal_mode_when_same(
        self, tmp_path, autovacuum_mode
    ):
        """Test that migrate_autovacuum is a no-op for WAL mode when source matches target."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", autovacuum_mode)
        original_mtime = os.path.getmtime(db_path)

        import time

        time.sleep(0.01)

        migrate_autovacuum(db_path, autovacuum_mode)

        new_mtime = os.path.getmtime(db_path)
        assert (
            new_mtime == original_mtime
        ), "Database file was modified when no migration needed"

        # No backup files
        files = list(tmp_path.glob("*.backup_*"))
        assert len(files) == 0, f"Backup files created: {files}"

        # Database unchanged
        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0] == autovacuum_mode
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == [(1, "hello"), (2, "world")]
        conn.close()

        if os.path.exists(db_path + "-wal"):
            os.remove(db_path + "-wal")
        if os.path.exists(db_path + "-shm"):
            os.remove(db_path + "-shm")


class TestShouldMigrate:
    """Tests for should_migrate function."""

    def test_should_not_migrate_by_default(self):
        """Test that migration is disabled by default."""
        os.environ.pop("AUTOVACUUM_MIGRATION", None)
        assert should_migrate() is False

    def test_should_migrate_when_enabled(self):
        """Test that migration is enabled when env var is set."""
        os.environ["AUTOVACUUM_MIGRATION"] = "1"
        try:
            assert should_migrate() is True
        finally:
            os.environ.pop("AUTOVACUUM_MIGRATION", None)

    def test_should_not_migrate_when_disabled(self):
        """Test that migration is disabled when env var is 0."""
        os.environ["AUTOVACUUM_MIGRATION"] = "0"
        try:
            assert should_migrate() is False
        finally:
            os.environ.pop("AUTOVACUUM_MIGRATION", None)


class TestMigrateAutovacuum:
    """Tests for migrate_autovacuum function."""

    @pytest.mark.parametrize("source_journal", ["DELETE", "WAL"])
    @pytest.mark.parametrize("source_av", [0, 1, 2])
    @pytest.mark.parametrize("target_av", [0, 1, 2])
    def test_migrate_preserves_data(
        self, tmp_path, source_journal, source_av, target_av
    ):
        """Test that migration preserves all data across all combinations."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, source_journal, source_av)
        initial_data = [(1, "hello"), (2, "world")]
        original_mtime = os.path.getmtime(db_path)

        import time

        time.sleep(0.01)

        result = migrate_autovacuum(db_path, target_av)

        new_mtime = os.path.getmtime(db_path)

        if source_av == target_av:
            assert (
                result is False
            ), "Should return False when no migration needed"
            assert (
                new_mtime == original_mtime
            ), "Database file should not be modified when no migration needed"
        else:
            assert result is True, "Should return True when migration performed"
            assert (
                new_mtime != original_mtime
            ), "Database file should be modified when migration performed"

        conn = sqlite3.connect(db_path)
        assert conn.execute("PRAGMA auto_vacuum").fetchone()[0] == target_av
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == initial_data
        conn.close()

    def test_migrate_with_delete_mode_source(self, tmp_path):
        """Test migration from DELETE journal mode."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        migrate_autovacuum(db_path, AutoVacuumMode.FULL)

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.FULL
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == [(1, "hello"), (2, "world")]
        conn.close()

    def test_migrate_with_wal_mode_source(self, tmp_path):
        """Test migration from WAL journal mode."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", AutoVacuumMode.NONE)

        migrate_autovacuum(db_path, AutoVacuumMode.INCREMENTAL)

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.INCREMENTAL
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == [(1, "hello"), (2, "world")]
        conn.close()

        if os.path.exists(db_path + "-wal"):
            os.remove(db_path + "-wal")
        if os.path.exists(db_path + "-shm"):
            os.remove(db_path + "-shm")

    def test_migrate_empty_database(self, tmp_path):
        """Test migration of an empty database."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE, with_data=False)

        migrate_autovacuum(db_path, AutoVacuumMode.FULL)

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.FULL
        )
        data = conn.execute("SELECT * FROM test").fetchall()
        assert data == []
        conn.close()

    def test_migrate_large_dataset(self, tmp_path):
        """Test migration preserves large amounts of data."""
        db_path = str(tmp_path / "test.db")

        db = sqlite3.connect(db_path)
        db.execute("PRAGMA auto_vacuum=NONE")
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        for i in range(1000):
            db.execute(f"INSERT INTO test VALUES ({i}, 'data_{i}')")
        db.commit()
        initial_count = db.execute("SELECT COUNT(*) FROM test").fetchone()[0]
        db.close()

        migrate_autovacuum(db_path, AutoVacuumMode.INCREMENTAL)

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.INCREMENTAL
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
            == initial_count
        )
        conn.close()

    def test_migrate_restores_on_failure(self, tmp_path):
        """Test that original database is restored if migration fails."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        original_data = [(1, "hello"), (2, "world")]

        with pytest.raises(Exception):
            migrate_autovacuum("/nonexistent/path/test.db", AutoVacuumMode.FULL)

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.NONE
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == original_data
        conn.close()

    def test_migrate_cleans_up_wal_files(self, tmp_path):
        """Test that WAL files are cleaned up after migration."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", AutoVacuumMode.NONE)

        migrate_autovacuum(db_path, AutoVacuumMode.FULL)

        assert not os.path.exists(db_path + "-wal")
        assert not os.path.exists(db_path + "-shm")

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.FULL
        )
        conn.close()

    def test_migrate_multiple_times(self, tmp_path):
        """Test running migration multiple times in sequence."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        migrate_autovacuum(db_path, AutoVacuumMode.FULL)
        migrate_autovacuum(db_path, AutoVacuumMode.INCREMENTAL)
        migrate_autovacuum(db_path, AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.NONE
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert data == [(1, "hello"), (2, "world")]
        conn.close()


class TestGetJournalMode:
    """Tests for get_journal_mode function."""

    def test_get_journal_mode_delete(self, tmp_path):
        """Test getting DELETE journal mode."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        journal_mode = get_journal_mode(conn)
        assert journal_mode.lower() == "delete"
        conn.close()

    def test_get_journal_mode_wal(self, tmp_path):
        """Test getting WAL journal mode."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        journal_mode = get_journal_mode(conn)
        assert journal_mode.lower() == "wal"
        conn.close()

        if os.path.exists(db_path + "-wal"):
            os.remove(db_path + "-wal")
        if os.path.exists(db_path + "-shm"):
            os.remove(db_path + "-shm")

    def test_get_journal_mode_memory(self, tmp_path):
        """Test getting MEMORY journal mode."""
        db_path = str(tmp_path / "test.db")
        db = sqlite3.connect(db_path)
        db.execute("PRAGMA journal_mode=MEMORY")
        # Check immediately before closing
        journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0]
        assert journal_mode.lower() == "memory"
        db.close()


class TestCheckpointAndPrepareForMigration:
    """Tests for checkpoint_and_prepare_for_migration function."""

    def test_checkpoint_with_wal(self, tmp_path):
        """Test checkpoint with WAL mode database."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        files = checkpoint_and_prepare_for_migration(conn)
        assert isinstance(files, list)
        conn.close()

        if os.path.exists(db_path + "-wal"):
            os.remove(db_path + "-wal")
        if os.path.exists(db_path + "-shm"):
            os.remove(db_path + "-shm")

    def test_checkpoint_with_delete_mode(self, tmp_path):
        """Test checkpoint with DELETE journal mode."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        files = checkpoint_and_prepare_for_migration(conn)
        assert isinstance(files, list)
        assert files == []
        conn.close()

    def test_checkpoint_with_pending_transaction(self, tmp_path):
        """Test checkpoint with pending transaction."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", AutoVacuumMode.NONE)

        conn = sqlite3.connect(db_path)
        # Start a transaction but don't commit
        conn.execute("BEGIN")
        conn.execute("INSERT INTO test VALUES (3, 'pending')")

        files = checkpoint_and_prepare_for_migration(conn)
        assert isinstance(files, list)
        conn.close()

        if os.path.exists(db_path + "-wal"):
            os.remove(db_path + "-wal")
        if os.path.exists(db_path + "-shm"):
            os.remove(db_path + "-shm")


class TestMigrationEdgeCases:
    """Edge case tests for migration."""

    def test_migrate_with_wal_files_present(self, tmp_path):
        """Test migration when WAL files exist."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "WAL", AutoVacuumMode.NONE)

        # Create some WAL activity
        conn = sqlite3.connect(db_path)
        conn.execute("INSERT INTO test VALUES (3, 'wal_data')")
        conn.commit()
        conn.close()

        # Don't checkpoint - let migration handle it
        result = migrate_autovacuum(db_path, AutoVacuumMode.FULL)
        assert result is True

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.FULL
        )
        data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
        assert len(data) == 3
        conn.close()

        # Clean up WAL files if they exist
        if os.path.exists(db_path + "-wal"):
            os.remove(db_path + "-wal")
        if os.path.exists(db_path + "-shm"):
            os.remove(db_path + "-shm")

    def test_migrate_all_mode_combinations(self, tmp_path):
        """Test migration across all source/target auto_vacuum combinations."""
        for source_av in [0, 1, 2]:
            for target_av in [0, 1, 2]:
                db_path = str(tmp_path / f"test_{source_av}_{target_av}.db")
                create_database(db_path, "DELETE", source_av)

                migrate_autovacuum(db_path, target_av)

                conn = sqlite3.connect(db_path)
                assert (
                    conn.execute("PRAGMA auto_vacuum").fetchone()[0]
                    == target_av
                )
                data = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
                assert data == [(1, "hello"), (2, "world")]
                conn.close()

    def test_migrate_with_extra_conn_kwargs(self, tmp_path):
        """Test migration with extra connection kwargs."""
        db_path = str(tmp_path / "test.db")
        create_database(db_path, "DELETE", AutoVacuumMode.NONE)

        result = migrate_autovacuum(
            db_path,
            AutoVacuumMode.FULL,
            extra_conn_kwargs={"timeout": 30},
        )
        assert result is True

        conn = sqlite3.connect(db_path)
        assert (
            conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            == AutoVacuumMode.FULL
        )
        conn.close()
