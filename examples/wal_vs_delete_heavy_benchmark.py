import time
import threading
import os
from neosqlite import Connection

DB_PATH = "heavy_benchmark.db"


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


def heavy_benchmark(mode, num_writers=1, num_readers=5, write_ops=200):
    cleanup()
    print(f"\n--- Heavy Benchmarking mode: {mode} ---")

    # Connection setup
    db = Connection(DB_PATH)
    db.db.execute(f"PRAGMA journal_mode={mode}")
    actual_mode = db.db.execute("PRAGMA journal_mode").fetchone()[0]
    print(f"Actual journal mode: {actual_mode}")

    # Initialize table
    db["test"].insert_one({"i": 0, "data": "init"})
    db.close()

    stop_event = threading.Event()
    read_counts = [0] * num_readers
    read_errors = [0] * num_readers

    def reader(idx):
        with Connection(DB_PATH) as reader_db:
            # Important: set journal mode in reader as well?
            # Actually journal mode is persistent in the file, but let's be sure
            # Actually, readers don't need to set it, but they should be aware.
            while not stop_event.is_set():
                try:
                    res = reader_db["test"].find_one()
                    if res:
                        read_counts[idx] += 1
                except Exception:
                    read_errors[idx] += 1
                # Small sleep to prevent tight loop if needed, but we want to see concurrency
                # time.sleep(0.001)

    def writer(idx):
        with Connection(DB_PATH) as writer_db:
            # If mode is not WAL, writer_db needs busy_timeout
            writer_db.db.execute("PRAGMA busy_timeout = 5000")
            for i in range(write_ops):
                try:
                    writer_db["test"].insert_one(
                        {"writer": idx, "i": i, "data": "x" * 100}
                    )
                except Exception as e:
                    print(f"Writer {idx} error: {e}")

    # Start readers
    reader_threads = []
    for i in range(num_readers):
        t = threading.Thread(target=reader, args=(i,))
        reader_threads.append(t)
        t.start()

    # Start writers
    start_time = time.time()
    writer_threads = []
    for i in range(num_writers):
        t = threading.Thread(target=writer, args=(i,))
        writer_threads.append(t)
        t.start()

    # Wait for writers to finish
    for t in writer_threads:
        t.join()

    end_time = time.time()
    stop_event.set()

    # Wait for readers to finish
    for t in reader_threads:
        t.join()

    duration = end_time - start_time
    total_reads = sum(read_counts)
    total_read_errors = sum(read_errors)

    print(f"Write duration: {duration:.4f}s")
    print(f"Total successful reads: {total_reads}")
    print(f"Total read errors (SQLITE_BUSY): {total_read_errors}")
    print(f"Read throughput: {total_reads / duration:.2f} reads/s")

    cleanup()


if __name__ == "__main__":
    # Test WAL
    heavy_benchmark("WAL", num_writers=2, num_readers=5, write_ops=100)

    # Test DELETE
    heavy_benchmark("DELETE", num_writers=2, num_readers=5, write_ops=100)
