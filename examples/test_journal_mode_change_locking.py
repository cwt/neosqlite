import sqlite3
import os

DB_PATH = "test_lock.db"


def cleanup():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    if os.path.exists(f"{DB_PATH}-wal"):
        os.remove(f"{DB_PATH}-wal")
    if os.path.exists(f"{DB_PATH}-shm"):
        os.remove(f"{DB_PATH}-shm")


def test_lock():
    cleanup()
    print("Initial connection setting WAL...")
    conn1 = sqlite3.connect(DB_PATH)
    conn1.execute("PRAGMA journal_mode=WAL")

    print("Second connection attempt to set WAL (redundant)...")
    conn2 = sqlite3.connect(DB_PATH)
    # This is what neosqlite currently does every time
    try:
        conn2.execute("PRAGMA journal_mode=WAL")
        print("Success: Redundant WAL set allowed with active connection.")
    except sqlite3.OperationalError as e:
        print(f"Failure: {e}")

    print("\nAttempting to change to DELETE while WAL connection is active...")
    try:
        conn2.execute("PRAGMA journal_mode=DELETE")
        print("Success: Changed to DELETE.")
    except sqlite3.OperationalError as e:
        print(f"Expected Failure: {e} (Changing mode requires exclusive lock)")

    conn1.close()
    conn2.close()
    cleanup()


if __name__ == "__main__":
    test_lock()
