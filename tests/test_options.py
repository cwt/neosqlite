"""
Unit tests for NeoSQLite options classes (WriteConcern, ReadPreference, ReadConcern, CodecOptions, AutoVacuumMode).
"""

import os
import tempfile

import pytest

import neosqlite
from neosqlite import (
    AutoVacuumMode,
    CodecOptions,
    ReadConcern,
    ReadPreference,
    WriteConcern,
)


def test_write_concern_init():
    """Test WriteConcern initialization and properties."""
    wc = WriteConcern(w=1, j=True, wtimeout=1000)
    assert wc.document == {"w": 1, "j": True, "wtimeout": 1000}
    assert wc.acknowledged is True

    wc_unack = WriteConcern(w=0)
    assert wc_unack.acknowledged is False
    assert wc_unack.document == {"w": 0}


def test_write_concern_repr():
    """Test WriteConcern string representation."""
    wc = WriteConcern(w=1, j=True)
    r = repr(wc)
    assert "WriteConcern" in r
    assert "w=1" in r
    assert "j=True" in r


def test_read_preference_init():
    """Test ReadPreference initialization and properties."""
    rp = ReadPreference(ReadPreference.SECONDARY_PREFERRED)
    assert rp.mode == 3
    assert rp.document == {"mode": 3}


def test_read_concern_init():
    """Test ReadConcern initialization and properties."""
    rc = ReadConcern(level="majority")
    assert rc.level == "majority"
    assert rc.document == {"level": "majority"}

    rc_default = ReadConcern()
    assert rc_default.level is None
    assert rc_default.document == {}


def test_codec_options_init():
    """Test CodecOptions initialization."""
    co = CodecOptions(tz_aware=True, document_class=dict)
    assert co.tz_aware is True
    assert co.document_class is dict


def test_options_equality():
    """Test equality operators for options classes."""
    assert WriteConcern(w=1) == WriteConcern(w=1)
    assert WriteConcern(w=1) != WriteConcern(w=0)

    assert ReadPreference(ReadPreference.PRIMARY) == ReadPreference(
        ReadPreference.PRIMARY
    )
    assert ReadPreference(ReadPreference.PRIMARY) != ReadPreference(
        ReadPreference.SECONDARY
    )

    assert ReadConcern(level="local") == ReadConcern(level="local")
    assert ReadConcern(level="local") != ReadConcern(level="majority")

    assert CodecOptions(tz_aware=True) == CodecOptions(tz_aware=True)
    assert CodecOptions(tz_aware=True) != CodecOptions(tz_aware=False)


def test_connection_with_options_objects():
    """Test that Connection accepts options objects in constructor and with_options."""
    wc = WriteConcern(w=1, j=True)
    rp = ReadPreference(ReadPreference.PRIMARY)
    rc = ReadConcern(level="local")
    co = CodecOptions(tz_aware=True)

    # Constructor
    with neosqlite.Connection(
        ":memory:",
        write_concern=wc,
        read_preference=rp,
        read_concern=rc,
        codec_options=co,
    ) as conn:
        assert conn.write_concern == wc
        assert conn.read_preference == rp
        assert conn.read_concern == rc
        assert conn.codec_options == co

    # with_options
    with neosqlite.Connection(":memory:") as conn:
        conn2 = conn.with_options(write_concern=wc, read_preference=rp)
        assert conn2.write_concern == wc
        assert conn2.read_preference == rp
        # Original should be unchanged
        assert conn.write_concern is None


def test_collection_with_options_objects(collection):
    """Test that Collection accepts options objects in with_options."""
    wc = WriteConcern(w=1, j=True)

    coll2 = collection.with_options(write_concern=wc)
    assert coll2.write_concern == wc
    # Verify it's a different instance
    assert coll2 is not collection


def test_write_concern_pragma_mapping():
    """Test that WriteConcern objects correctly map to SQLite PRAGMAs."""
    # j=True -> synchronous=FULL (2)
    wc_j = WriteConcern(j=True)
    with neosqlite.Connection(":memory:", write_concern=wc_j) as conn:
        cursor = conn.db.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 2

    # w=0 -> synchronous=OFF (0)
    wc_w0 = WriteConcern(w=0)
    with neosqlite.Connection(":memory:", write_concern=wc_w0) as conn:
        cursor = conn.db.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 0

    # w=1 -> synchronous=NORMAL (1)
    wc_w1 = WriteConcern(w=1)
    with neosqlite.Connection(":memory:", write_concern=wc_w1) as conn:
        cursor = conn.db.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 1


# ============================================================================
# AutoVacuumMode Tests
# ============================================================================


def test_autovacuum_mode_constants():
    """Test AutoVacuumMode constant values."""
    assert AutoVacuumMode.NONE == 0
    assert AutoVacuumMode.FULL == 1
    assert AutoVacuumMode.INCREMENTAL == 2


def test_autovacuum_mode_validate_integer():
    """Test AutoVacuumMode.validate with integer values."""
    assert AutoVacuumMode.validate(0) == AutoVacuumMode.NONE
    assert AutoVacuumMode.validate(1) == AutoVacuumMode.FULL
    assert AutoVacuumMode.validate(2) == AutoVacuumMode.INCREMENTAL


def test_autovacuum_mode_validate_string():
    """Test AutoVacuumMode.validate with string values."""
    assert AutoVacuumMode.validate("NONE") == AutoVacuumMode.NONE
    assert AutoVacuumMode.validate("FULL") == AutoVacuumMode.FULL
    assert AutoVacuumMode.validate("INCREMENTAL") == AutoVacuumMode.INCREMENTAL
    assert AutoVacuumMode.validate("none") == AutoVacuumMode.NONE
    assert AutoVacuumMode.validate("full") == AutoVacuumMode.FULL
    assert AutoVacuumMode.validate("incremental") == AutoVacuumMode.INCREMENTAL


def test_autovacuum_mode_validate_invalid():
    """Test AutoVacuumMode.validate raises for invalid values."""
    with pytest.raises(ValueError):
        AutoVacuumMode.validate(3)

    with pytest.raises(ValueError):
        AutoVacuumMode.validate("INVALID")

    with pytest.raises(ValueError):
        AutoVacuumMode.validate("unknown")


def test_autovacuum_mode_to_string():
    """Test AutoVacuumMode.to_string conversion."""
    assert AutoVacuumMode.to_string(AutoVacuumMode.NONE) == "NONE"
    assert AutoVacuumMode.to_string(AutoVacuumMode.FULL) == "FULL"
    assert AutoVacuumMode.to_string(AutoVacuumMode.INCREMENTAL) == "INCREMENTAL"
    assert AutoVacuumMode.to_string(99) == "NONE"  # Default fallback


def test_connection_autovacuum_parameter():
    """Test that Connection accepts auto_vacuum parameter."""
    # Test with integer
    conn = neosqlite.Connection(":memory:", auto_vacuum=AutoVacuumMode.FULL)
    assert conn.auto_vacuum == AutoVacuumMode.FULL
    conn.close()

    # Test with string
    conn = neosqlite.Connection(":memory:", auto_vacuum="INCREMENTAL")
    assert conn.auto_vacuum == AutoVacuumMode.INCREMENTAL
    conn.close()

    # Test default is INCREMENTAL
    conn = neosqlite.Connection(":memory:")
    assert conn.auto_vacuum == AutoVacuumMode.INCREMENTAL
    conn.close()


def test_connection_autovacuum_invalid():
    """Test that Connection raises for invalid auto_vacuum values."""
    with pytest.raises(ValueError):
        neosqlite.Connection(":memory:", auto_vacuum="INVALID")

    with pytest.raises(ValueError):
        neosqlite.Connection(":memory:", auto_vacuum=99)


def test_neosqlite_created_database_has_autovacuum():
    """Test that NeoSQLite-created databases have INCREMENTAL auto_vacuum by default."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        conn = neosqlite.Connection(db_path)
        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.db.execute("INSERT INTO test VALUES (1)")
        conn.close()

        # Reopen and check
        conn = neosqlite.Connection(db_path)
        auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
        assert auto_vacuum == AutoVacuumMode.INCREMENTAL
        conn.close()
    finally:
        os.remove(db_path)


# ============================================================================
# AutoVacuum Migration Tests
# ============================================================================


@pytest.fixture(params=["DELETE", "WAL"])
def source_journal_mode(request):
    """Fixture to test with different source journal modes."""
    return request.param


@pytest.fixture(params=[0, 1, 2])
def source_autovacuum_mode(request):
    """Fixture to test with different source auto_vacuum modes."""
    return request.param


@pytest.fixture(params=[0, 1, 2])
def target_autovacuum_mode(request):
    """Fixture to test migration to different target modes."""
    return request.param


def create_database(path, journal_mode, autovacuum_mode, with_data=True):
    """Helper to create a database with specific settings."""
    import sqlite3

    db = sqlite3.connect(path)
    db.execute(f"PRAGMA auto_vacuum={autovacuum_mode}")
    db.execute(f"PRAGMA journal_mode={journal_mode}")
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    if with_data:
        db.execute("INSERT INTO test VALUES (1, 'hello')")
        db.execute("INSERT INTO test VALUES (2, 'world')")
    db.commit()
    db.close()


def test_autovacuum_no_migration_without_env_var(tmp_path):
    """Test that migration doesn't happen without AUTOVACUUM_MIGRATION env var."""
    db_path = str(tmp_path / "test.db")

    # Create database without auto_vacuum
    create_database(db_path, "DELETE", AutoVacuumMode.NONE)

    # Open with NeoSQLite (should NOT migrate)
    conn = neosqlite.Connection(db_path)
    auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
    assert auto_vacuum == AutoVacuumMode.NONE  # Unchanged
    conn.close()


def test_autovacuum_no_migration_when_already_correct(
    tmp_path, source_autovacuum_mode
):
    """Test that migration doesn't happen when auto_vacuum already matches."""
    os.environ["AUTOVACUUM_MIGRATION"] = "1"
    try:
        db_path = str(tmp_path / "test.db")

        # Create database with same auto_vacuum as NeoSQLite default
        create_database(db_path, "DELETE", AutoVacuumMode.INCREMENTAL)

        conn = neosqlite.Connection(db_path)
        auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
        assert auto_vacuum == AutoVacuumMode.INCREMENTAL
        conn.close()
    finally:
        os.environ.pop("AUTOVACUUM_MIGRATION", None)


def test_autovacuum_migration_preserves_data(
    tmp_path,
    source_journal_mode,
    source_autovacuum_mode,
    target_autovacuum_mode,
):
    """Test that migration preserves all data across all mode combinations."""
    os.environ["AUTOVACUUM_MIGRATION"] = "1"
    try:
        db_path = str(tmp_path / "test.db")

        # Create source database
        create_database(db_path, source_journal_mode, source_autovacuum_mode)

        # Verify data exists
        check_db = __import__("sqlite3").connect(db_path)
        initial_data = check_db.execute(
            "SELECT * FROM test ORDER BY id"
        ).fetchall()
        assert len(initial_data) == 2
        assert initial_data[0] == (1, "hello")
        assert initial_data[1] == (2, "world")
        check_db.close()

        # Open with NeoSQLite and migrate (or skip if same mode)
        conn = neosqlite.Connection(db_path, auto_vacuum=target_autovacuum_mode)
        auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
        assert auto_vacuum == target_autovacuum_mode

        # Verify data is preserved
        migrated_data = conn.db.execute(
            "SELECT * FROM test ORDER BY id"
        ).fetchall()
        assert migrated_data == initial_data

        conn.close()

        # Verify data is persisted correctly
        verify_db = __import__("sqlite3").connect(db_path)
        persisted_data = verify_db.execute(
            "SELECT * FROM test ORDER BY id"
        ).fetchall()
        assert persisted_data == initial_data
        verify_db.close()
    finally:
        os.environ.pop("AUTOVACUUM_MIGRATION", None)


def test_autovacuum_migration_with_empty_database(
    tmp_path, source_journal_mode
):
    """Test migration of an empty database."""
    os.environ["AUTOVACUUM_MIGRATION"] = "1"
    try:
        db_path = str(tmp_path / "test.db")

        # Create empty database
        create_database(
            db_path, source_journal_mode, AutoVacuumMode.NONE, with_data=False
        )

        # Migrate
        conn = neosqlite.Connection(db_path, auto_vacuum=AutoVacuumMode.FULL)
        auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
        assert auto_vacuum == AutoVacuumMode.FULL

        # Should still be empty
        data = conn.db.execute("SELECT * FROM test").fetchall()
        assert data == []
        conn.close()
    finally:
        os.environ.pop("AUTOVACUUM_MIGRATION", None)


def test_autovacuum_migration_with_large_data(tmp_path):
    """Test migration preserves large amounts of data."""
    os.environ["AUTOVACUUM_MIGRATION"] = "1"
    try:
        db_path = str(tmp_path / "test.db")

        import sqlite3

        db = sqlite3.connect(db_path)
        db.execute("PRAGMA auto_vacuum=NONE")
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        # Insert 1000 rows
        for i in range(1000):
            db.execute(f"INSERT INTO test VALUES ({i}, 'data_{i}')")
        db.commit()
        initial_count = db.execute("SELECT COUNT(*) FROM test").fetchone()[0]
        assert initial_count == 1000
        db.close()

        # Migrate
        conn = neosqlite.Connection(db_path, auto_vacuum=AutoVacuumMode.FULL)
        migrated_count = conn.db.execute(
            "SELECT COUNT(*) FROM test"
        ).fetchone()[0]
        assert migrated_count == initial_count

        # Verify specific rows
        row_0 = conn.db.execute("SELECT * FROM test WHERE id=0").fetchone()
        assert row_0 == (0, "data_0")
        row_999 = conn.db.execute("SELECT * FROM test WHERE id=999").fetchone()
        assert row_999 == (999, "data_999")
        conn.close()
    finally:
        os.environ.pop("AUTOVACUUM_MIGRATION", None)


def test_autovacuum_in_memory_skips_migration():
    """Test that in-memory databases skip migration checks."""
    conn = neosqlite.Connection(":memory:", auto_vacuum=AutoVacuumMode.FULL)
    auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
    assert auto_vacuum == AutoVacuumMode.FULL
    conn.close()


def test_autovacuum_migration_all_target_modes(tmp_path):
    """Test migration to all possible target auto_vacuum modes."""
    os.environ["AUTOVACUUM_MIGRATION"] = "1"
    try:
        for target_mode in [
            AutoVacuumMode.NONE,
            AutoVacuumMode.FULL,
            AutoVacuumMode.INCREMENTAL,
        ]:
            db_path = str(tmp_path / f"test_{target_mode}.db")

            # Create source database
            create_database(db_path, "DELETE", AutoVacuumMode.NONE)

            # Migrate to target mode
            conn = neosqlite.Connection(db_path, auto_vacuum=target_mode)
            auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
            assert (
                auto_vacuum == target_mode
            ), f"Expected {target_mode}, got {auto_vacuum}"

            data = conn.db.execute("SELECT * FROM test ORDER BY id").fetchall()
            assert data == [(1, "hello"), (2, "world")]
            conn.close()
    finally:
        os.environ.pop("AUTOVACUUM_MIGRATION", None)


def test_autovacuum_migration_all_journal_modes_as_source(tmp_path):
    """Test migration from all journal modes as source."""
    source_modes = ["DELETE", "WAL", "TRUNCATE", "PERSIST"]

    os.environ["AUTOVACUUM_MIGRATION"] = "1"
    try:
        for journal_mode in source_modes:
            db_path = str(tmp_path / f"test_{journal_mode}.db")

            # Create source database with this journal mode
            create_database(db_path, journal_mode, AutoVacuumMode.NONE)

            # Migrate
            conn = neosqlite.Connection(
                db_path, auto_vacuum=AutoVacuumMode.FULL
            )
            auto_vacuum = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
            assert auto_vacuum == AutoVacuumMode.FULL

            # Verify data
            data = conn.db.execute("SELECT * FROM test ORDER BY id").fetchall()
            assert data == [
                (1, "hello"),
                (2, "world"),
            ], f"Data mismatch for {journal_mode}"
            conn.close()
    finally:
        os.environ.pop("AUTOVACUUM_MIGRATION", None)
