#!/usr/bin/env python3
"""
Benchmark script to verify performance benefits of stable, predictable,
and repeatable temporary table naming system with query parameters.

This benchmark measures the end-to-end performance including SQLite
query plan caching, which is the real benefit of deterministic naming.
"""

import hashlib
import sqlite3
import statistics
import time
import uuid
from typing import Any, Dict


class DeterministicTempTableManager:
    """Manager for deterministic temporary table names."""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id

    def make_temp_table_name(self, stage: Dict[str, Any]) -> str:
        """Generate a deterministic temp table name."""
        stage_key = str(sorted(stage.items()))
        suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:6]
        stage_type = next(iter(stage.keys())).lstrip("$")
        return f"temp_{self.pipeline_id}_{stage_type}_{suffix}"


def benchmark_query_plan_caching(num_runs: int = 5000) -> Dict[str, float]:
    """Benchmark SQLite query plan caching with deterministic vs random names."""
    print(f"=== Benchmarking Query Plan Caching ({num_runs:,} runs) ===\n")

    # Test data - same query template reused
    query_template = (
        "SELECT id, name, category FROM test_data WHERE category = ? "
        "AND value > ? ORDER BY value DESC LIMIT ?"
    )
    params = ("Category 5", 250.0, 10)

    # Setup: Create a base table with data
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE test_data (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            value REAL
        )
    """)
    data = [
        (i, f"Item {i}", f"Category {i % 10}", float(i * 10))
        for i in range(1000)
    ]
    conn.executemany("INSERT INTO test_data VALUES (?, ?, ?, ?)", data)
    conn.commit()

    # Benchmark with deterministic names (same SQL template reused)
    print("Testing with deterministic naming (query plan cache friendly)...")
    deterministic_times = []
    for _ in range(num_runs):
        pipeline_id = "agg_001"  # Same pipeline ID for all runs
        manager = DeterministicTempTableManager(pipeline_id)
        table_name = manager.make_temp_table_name(
            {"$match": {"status": "active"}}
        )

        start_time = time.perf_counter()

        # Create temp table with deterministic name
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"CREATE TEMP TABLE {table_name} AS {query_template}", params
        )
        conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        end_time = time.perf_counter()
        deterministic_times.append(end_time - start_time)

    # Benchmark with random names (query plan cache unfriendly)
    print("Testing with random naming (query plan cache unfriendly)...")
    random_times = []
    for _ in range(num_runs):
        table_name = f"temp_{uuid.uuid4().hex[:12]}"

        start_time = time.perf_counter()

        # Create temp table with random name
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"CREATE TEMP TABLE {table_name} AS {query_template}", params
        )
        conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        end_time = time.perf_counter()
        random_times.append(end_time - start_time)

    conn.close()

    # Calculate statistics
    deterministic_avg = statistics.mean(deterministic_times)
    deterministic_stdev = (
        statistics.stdev(deterministic_times)
        if len(deterministic_times) > 1
        else 0.0
    )
    random_avg = statistics.mean(random_times)
    random_stdev = (
        statistics.stdev(random_times) if len(random_times) > 1 else 0.0
    )

    # Calculate speedup
    speedup = (
        random_avg / deterministic_avg
        if deterministic_avg > 0
        else float("inf")
    )

    print("\n  Deterministic naming:")
    print(f"    Average time: {deterministic_avg * 1000000:.2f} us")
    print(f"    Std dev:      {deterministic_stdev * 1000000:.2f} us")

    print("  Random naming:")
    print(f"    Average time: {random_avg * 1000000:.2f} us")
    print(f"    Std dev:      {random_stdev * 1000000:.2f} us")

    print(f"\n  Speedup: {speedup:.2f}x faster")

    return {
        "deterministic_avg": deterministic_avg,
        "deterministic_stdev": deterministic_stdev,
        "random_avg": random_avg,
        "random_stdev": random_stdev,
        "speedup": speedup,
    }


def main():
    """Run benchmarks."""
    print("NeoSQLite Deterministic Temp Table Naming Benchmark")
    print("=" * 50)
    print()

    # Run query plan caching benchmark
    results = benchmark_query_plan_caching(5000)
    print()

    # Summary
    print("=== Benchmark Summary ===")
    print(f"  Query plan caching speedup: {results['speedup']:.2f}x")
    print()
    print("Benefits of deterministic temp table naming:")
    print("  - Better SQLite query plan caching")
    print("  - Predictable execution paths")
    print("  - Easier debugging and tracing")
    print("  - Reduced query compilation overhead")
    print()
    print("Note: Name generation (SHA-256 vs UUID) is not the bottleneck.")
    print("The real benefit is SQLite reusing compiled query plans when")
    print("the same SQL statements are executed repeatedly.")


if __name__ == "__main__":
    main()
