import pytest
import os
from neosqlite import Connection

DB_PATH = "test_journal.db"


def cleanup():
    for f in [
        DB_PATH,
        f"{DB_PATH}-wal",
        f"{DB_PATH}-shm",
        f"{DB_PATH}-journal",
    ]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except OSError:
                pass


@pytest.fixture(autouse=True)
def setup_teardown():
    cleanup()
    yield
    cleanup()


def test_default_journal_mode_is_wal():
    db = Connection(DB_PATH)
    mode = db.db.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
    db.close()


def test_custom_journal_mode_delete():
    db = Connection(DB_PATH, journal_mode="delete")  # lowercase should work
    mode = db.db.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "delete"
    assert db.journal_mode == "DELETE"
    db.close()


def test_custom_journal_mode_memory():
    db = Connection(DB_PATH, journal_mode="Memory")  # mixed case should work
    mode = db.db.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "memory"
    assert db.journal_mode == "MEMORY"
    db.close()


def test_journal_mode_persists_in_clone():
    db = Connection(DB_PATH, journal_mode="delete")
    clone = db.with_options(write_concern={"w": 1})
    assert clone.journal_mode == "DELETE"
    mode = clone.db.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "delete"
    db.close()


def test_invalid_journal_mode_raises_error():
    with pytest.raises(ValueError) as excinfo:
        Connection(DB_PATH, journal_mode="INVALID")
    assert "Invalid journal_mode" in str(excinfo.value)
