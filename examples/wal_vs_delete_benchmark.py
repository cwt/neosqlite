import time
import threading
import os
import sqlite3
from neosqlite import Connection

DB_PATH = "benchmark_concurrency.db"


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


def run_concurrency_test(mode):
    cleanup()
    print(f"\nTesting concurrency for mode: {mode}")

    db = Connection(DB_PATH)
    db.db.execute(f"PRAGMA journal_mode={mode}")
    # Set a very slow synchronous mode to highlight the difference if needed,
    # but let's stick to defaults first.

    db["test"].insert_one({"init": True})

    read_results = []

    def reader_proc():
        # Use raw sqlite3 to avoid any neosqlite overhead for this specific test
        conn = sqlite3.connect(DB_PATH)
        start_read = time.time()
        try:
            # This should block in DELETE mode if a writer is active
            conn.execute("SELECT * FROM test").fetchall()
            read_results.append(time.time() - start_read)
        except Exception as e:
            print(f"Read error: {e}")
        finally:
            conn.close()

    # Start a writer that takes some time
    def writer_proc():
        conn = sqlite3.connect(DB_PATH)
        conn.execute(f"PRAGMA journal_mode={mode}")
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        time.sleep(1)  # Hold the lock for 1 second
        conn.execute("INSERT INTO test (data) VALUES ('slow')")
        conn.commit()
        conn.close()

    w = threading.Thread(target=writer_proc)
    w.start()

    time.sleep(0.2)  # Wait for writer to start and take lock

    r = threading.Thread(target=reader_proc)
    r.start()

    w.join()
    r.join()

    if read_results:
        print(f"Read latency during 1s write: {read_results[0]:.4f}s")
        if read_results[0] > 0.5:
            print("RESULT: Reader was BLOCKED")
        else:
            print("RESULT: Reader was NOT blocked")

    cleanup()


if __name__ == "__main__":
    run_concurrency_test("WAL")
    run_concurrency_test("DELETE")
