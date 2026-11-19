#!/usr/bin/env bash
#
# NeoSQLite JavaScript Engine CPU & RAM Profiling Script
#
# Profiles every available sandboxed JS engine (e.g. jsbox vs node, plus any
# registered engines) across realistic NeoSQLite workloads on Linux and macOS:
#   1. cold_start   - Fresh worker spawn + first evaluation latency & idle RAM
#   2. where_filter - $where predicate scan across N documents
#   3. function_agg - $function aggregation transform across N documents
#   4. bson_markers - $where + $function with ObjectId, Date, and BigInt
#   5. cpu_compute  - CPU-bound JS loops (QuickJS interpreter vs V8 JIT)
#   6. mem_alloc    - Repeated temporary JS allocations (reclamation & peak RSS)
#
# Each (engine, workload) pair runs in a fresh isolated measurement process so
# kernel RUSAGE_CHILDREN peak RSS and CPU accounting never bleed across runs.
#
# Usage:
#   ./scripts/js-profiling.sh
#   ./scripts/js-profiling.sh --docs 1000 --runs 3
#   ./scripts/js-profiling.sh --engines jsbox,node --output js_profile.md
#   ./scripts/js-profiling.sh --json js_profile.json
#

set -euo pipefail

# Colors (disabled automatically when stdout is not a TTY)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    CYAN=''
    BOLD=''
    NC=''
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENGINES=""
DOCS=500
RUNS=1
OUTPUT_MD=""
OUTPUT_JSON=""
PYTHON_OVERRIDE=""

info() {
    printf "%b[INFO]%b %s\n" "$BLUE" "$NC" "$1"
}

warn() {
    printf "%b[WARN]%b %s\n" "$YELLOW" "$NC" "$1" >&2
}

error() {
    printf "%b[ERROR]%b %s\n" "$RED" "$NC" "$1" >&2
}

usage() {
    cat <<'EOF'
Usage: ./scripts/js-profiling.sh [OPTIONS]

Profile CPU and RAM usage of NeoSQLite's sandboxed JavaScript engines
(jsbox, node, or any registered backend) on Linux and macOS.

Options:
  -e, --engines LIST   Comma-separated engine names (default: all available)
  -d, --docs N         Document count for $where/$function scans (default: 500)
  -r, --runs K         Measured runs per workload, median reported (default: 1)
  -o, --output FILE    Write Markdown profiling report to FILE
  -j, --json FILE      Write raw JSON metrics to FILE
      --python CMD     Python executable to use (default: poetry run python3)
  -h, --help           Show this help message and exit

Examples:
  ./scripts/js-profiling.sh
  ./scripts/js-profiling.sh --docs 1000 --runs 3
  ./scripts/js-profiling.sh --engines jsbox,node --output js_profile.md
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -e|--engines)
            if [ $# -lt 2 ]; then
                error "Missing value for $1"
                exit 1
            fi
            ENGINES="$2"
            shift 2
            ;;
        -d|--docs)
            if [ $# -lt 2 ]; then
                error "Missing value for $1"
                exit 1
            fi
            DOCS="$2"
            shift 2
            ;;
        -r|--runs)
            if [ $# -lt 2 ]; then
                error "Missing value for $1"
                exit 1
            fi
            RUNS="$2"
            shift 2
            ;;
        -o|--output)
            if [ $# -lt 2 ]; then
                error "Missing value for $1"
                exit 1
            fi
            OUTPUT_MD="$2"
            shift 2
            ;;
        -j|--json)
            if [ $# -lt 2 ]; then
                error "Missing value for $1"
                exit 1
            fi
            OUTPUT_JSON="$2"
            shift 2
            ;;
        --python)
            if [ $# -lt 2 ]; then
                error "Missing value for $1"
                exit 1
            fi
            PYTHON_OVERRIDE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            usage >&2
            exit 1
            ;;
    esac
done

case "$DOCS" in
    ''|*[!0-9]*|0)
        error "--docs must be a positive integer (got: $DOCS)"
        exit 1
        ;;
esac

case "$RUNS" in
    ''|*[!0-9]*|0)
        error "--runs must be a positive integer (got: $RUNS)"
        exit 1
        ;;
esac

# Resolve Python runner (Poetry virtualenv preferred when available)
PYTHON_CMD=()
if [ -n "$PYTHON_OVERRIDE" ]; then
    PYTHON_CMD=("$PYTHON_OVERRIDE")
elif command -v poetry >/dev/null 2>&1 && [ -f "$PROJECT_ROOT/pyproject.toml" ]; then
    PYTHON_CMD=(poetry run python3)
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
else
    error "Neither poetry nor python3 found on PATH."
    exit 1
fi

# Collect host OS/hardware metadata portably across Linux and macOS
OS_NAME="$(uname -s)"
ARCH_NAME="$(uname -m)"
CPU_MODEL="unknown"
CPU_CORES="unknown"
RAM_GB="unknown"

case "$OS_NAME" in
    Darwin)
        OS_PRETTY="macOS $(sw_vers -productVersion 2>/dev/null || uname -r)"
        CPU_MODEL="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || sysctl -n hw.model 2>/dev/null || echo unknown)"
        CPU_CORES="$(sysctl -n hw.ncpu 2>/dev/null || echo unknown)"
        RAM_BYTES="$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
        if [ "$RAM_BYTES" -gt 0 ] 2>/dev/null; then
            RAM_GB="$(awk -v b="$RAM_BYTES" 'BEGIN { printf "%.1f GiB", b / (1024*1024*1024) }')"
        fi
        ;;
    Linux)
        if [ -r /etc/os-release ]; then
            OS_PRETTY="$(awk -F= '/^PRETTY_NAME=/{gsub(/^"|"$/, "", $2); print $2; exit}' /etc/os-release)"
        else
            OS_PRETTY="Linux $(uname -r)"
        fi
        if [ -r /proc/cpuinfo ]; then
            CPU_MODEL="$(awk -F: '/^(model name|Hardware|Processor)[[:space:]]*:/{gsub(/^[[:space:]]+/, "", $2); print $2; exit}' /proc/cpuinfo)"
        fi
        if [ -z "$CPU_MODEL" ]; then
            CPU_MODEL="unknown"
        fi
        CPU_CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo unknown)"
        if [ -r /proc/meminfo ]; then
            RAM_KB="$(awk '/^MemTotal:/{print $2; exit}' /proc/meminfo)"
            if [ -n "$RAM_KB" ] && [ "$RAM_KB" -gt 0 ] 2>/dev/null; then
                RAM_GB="$(awk -v k="$RAM_KB" 'BEGIN { printf "%.1f GiB", k / (1024*1024) }')"
            fi
        fi
        ;;
    *)
        OS_PRETTY="$OS_NAME $(uname -r)"
        CPU_CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
        ;;
esac

printf "%b========================================================================%b\n" "$CYAN" "$NC"
printf "%b  NeoSQLite JavaScript Engine Profiler (CPU & RAM)%b\n" "$BOLD" "$NC"
printf "%b========================================================================%b\n" "$CYAN" "$NC"
info "Host:     $OS_PRETTY ($ARCH_NAME)"
info "CPU:      $CPU_MODEL ($CPU_CORES logical cores)"
info "Memory:   $RAM_GB"
info "Config:   docs=$DOCS, runs=$RUNS, engines=${ENGINES:-auto (all available)}"
echo ""

# Write embedded Python profiler to a cleaned-up temporary file so the
# orchestrator can spawn itself in isolated --worker mode per (engine, workload).
PROFILER_SCRIPT="$(mktemp "${TMPDIR:-/tmp}/neosqlite_js_profiler.XXXXXX.py")"
cleanup() {
    rm -f "$PROFILER_SCRIPT"
}
trap cleanup EXIT

cat > "$PROFILER_SCRIPT" <<'PYEOF'
"""Cross-platform (Linux/macOS) CPU and RAM profiler for NeoSQLite JS engines."""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from typing import Any

import neosqlite
from neosqlite.js import (
    close_js_workers,
    get_js_worker,
    list_js_engines,
    set_js_engine,
)
from neosqlite.objectid import ObjectId


def _ru_maxrss_to_mib(raw_maxrss: int) -> float:
    """Convert ``rusage.ru_maxrss`` to MiB (bytes on macOS, KiB on Linux)."""
    if sys.platform == "darwin":
        return raw_maxrss / (1024.0 * 1024.0)
    return raw_maxrss / 1024.0


def _sample_pid_memory_mib(pid: int) -> tuple[float | None, float | None, float | None]:
    """Return ``(rss_mib, vsz_mib, hwm_mib)`` for ``pid`` on Linux or macOS.

    On Linux, reads ``/proc/<pid>/status`` directly with zero child process
    spawns. On macOS, falls back to ``ps -o rss=,vsz= -p <pid>`` (KiB).
    """
    if sys.platform.startswith("linux"):
        status_path = f"/proc/{pid}/status"
        rss_kb: int | None = None
        vsz_kb: int | None = None
        hwm_kb: int | None = None
        try:
            with open(status_path, encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                    elif line.startswith("VmSize:"):
                        vsz_kb = int(line.split()[1])
                    elif line.startswith("VmHWM:"):
                        hwm_kb = int(line.split()[1])
        except (OSError, ValueError, IndexError):
            return None, None, None
        return (
            None if rss_kb is None else rss_kb / 1024.0,
            None if vsz_kb is None else vsz_kb / 1024.0,
            None if hwm_kb is None else hwm_kb / 1024.0,
        )

    try:
        completed = subprocess.run(
            ["ps", "-o", "rss=,vsz=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None, None, None
    if completed.returncode != 0:
        return None, None, None
    parts = completed.stdout.strip().split()
    if len(parts) < 2:
        return None, None, None
    try:
        rss_kb_val = int(parts[0])
        vsz_kb_val = int(parts[1])
    except ValueError:
        return None, None, None
    return rss_kb_val / 1024.0, vsz_kb_val / 1024.0, None


def _get_worker_pid(worker: Any) -> int | None:
    proc = getattr(worker, "_proc", None)
    if proc is None:
        return None
    pid = getattr(proc, "pid", None)
    return pid if isinstance(pid, int) else None


def _seed_standard_docs(coll: Any, count: int) -> None:
    docs = [
        {
            "_id": i + 1,
            "price": (i % 50) + 5,
            "qty": (i % 10) + 1,
            "active": (i % 3) != 0,
            "name": f"item-{i + 1}",
            "items": [i % 7, (i + 1) % 7, (i + 2) % 7, (i + 3) % 7],
        }
        for i in range(count)
    ]
    coll.insert_many(docs)


def _seed_bson_docs(coll: Any, count: int) -> None:
    base_ts = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp())
    docs = [
        {
            "_id": ObjectId(f"{i + 1:024x}"),
            "created": datetime.fromtimestamp(base_ts + i * 3600, tz=UTC),
            "amount": (i + 1) * 100,
        }
        for i in range(count)
    ]
    coll.insert_many(docs)


def _run_single_workload(engine: str, workload: str, docs: int) -> dict[str, Any]:
    """Execute one isolated (engine, workload) measurement and return metrics."""
    set_js_engine(engine)
    close_js_workers()

    # Prepare SQLite data before starting timers/worker so setup cost is excluded.
    conn = neosqlite.Connection(":memory:")
    coll = conn.bench
    bson_docs = max(20, docs // 2)
    cpu_iters = max(5, min(30, docs // 20))
    mem_iters = max(10, min(40, docs // 15))

    if workload in ("where_filter", "function_agg"):
        _seed_standard_docs(coll, docs)
    elif workload == "bson_markers":
        _seed_bson_docs(coll, bson_docs)
    elif workload == "cpu_compute":
        coll.insert_many([{"_id": i + 1, "n": 6000} for i in range(cpu_iters)])
    elif workload == "mem_alloc":
        coll.insert_many(
            [{"_id": i + 1, "size": 65536} for i in range(mem_iters)]
        )

    # For non-cold-start workloads, warm up the worker first so cold start is
    # measured separately from steady-state execution.
    if workload != "cold_start":
        worker = get_js_worker()
        if worker is None:
            raise RuntimeError(f"JS engine {engine!r} is unavailable")
        worker.call("function(a, b) { return a + b; }", [1, 2])

    self_before = resource.getrusage(resource.RUSAGE_SELF)
    child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.perf_counter()

    ops = 0
    checksum: Any = None

    if workload == "cold_start":
        worker = get_js_worker()
        if worker is None:
            raise RuntimeError(f"JS engine {engine!r} is unavailable")
        checksum = worker.call("function(a, b) { return a + b; }", [20, 22])
        ops = 1
    elif workload == "where_filter":
        matched = list(
            coll.find({"$where": "this.price * this.qty > 200 && this.active"})
        )
        checksum = len(matched)
        ops = docs
    elif workload == "function_agg":
        pipeline = [
            {
                "$addFields": {
                    "score": {
                        "$function": {
                            "body": (
                                "function(price, qty, xs, name) {"
                                "  let s = 0;"
                                "  for (let i = 0; i < xs.length; i++) s += xs[i];"
                                "  return price * qty + s + name.length;"
                                "}"
                            ),
                            "args": ["$price", "$qty", "$items", "$name"],
                            "lang": "js",
                        }
                    }
                }
            }
        ]
        rows = list(coll.aggregate(pipeline))
        checksum = sum(row["score"] for row in rows)
        ops = docs
    elif workload == "bson_markers":
        matched = list(
            coll.find(
                {
                    "$where": (
                        "function() {"
                        "  return (this.created instanceof Date)"
                        "    && this._id.toString().length === 24"
                        "    && this.amount > 1000;"
                        "}"
                    )
                }
            )
        )
        agg = list(
            coll.aggregate(
                [
                    {"$limit": min(50, bson_docs)},
                    {
                        "$project": {
                            "tag": {
                                "$function": {
                                    "body": (
                                        "function(id, dt) {"
                                        "  return id.toString().slice(-4)"
                                        "    + ':' + dt.getUTCFullYear();"
                                        "}"
                                    ),
                                    "args": ["$_id", "$created"],
                                    "lang": "js",
                                }
                            }
                        }
                    },
                ]
            )
        )
        checksum = (len(matched), len(agg))
        ops = bson_docs + len(agg)
    elif workload == "cpu_compute":
        pipeline = [
            {
                "$project": {
                    "primes": {
                        "$function": {
                            "body": (
                                "function(limit) {"
                                "  let count = 0;"
                                "  for (let n = 2; n <= limit; n++) {"
                                "    let ok = true;"
                                "    for (let d = 2; d * d <= n; d++) {"
                                "      if (n % d === 0) { ok = false; break; }"
                                "    }"
                                "    if (ok) count++;"
                                "  }"
                                "  return count;"
                                "}"
                            ),
                            "args": ["$n"],
                            "lang": "js",
                        }
                    }
                }
            }
        ]
        rows = list(coll.aggregate(pipeline))
        checksum = sum(row["primes"] for row in rows)
        ops = cpu_iters
    elif workload == "mem_alloc":
        pipeline = [
            {
                "$project": {
                    "digest": {
                        "$function": {
                            "body": (
                                "function(sz) {"
                                "  const arr = new Array(sz);"
                                "  for (let i = 0; i < sz; i++) arr[i] = i & 255;"
                                "  const text = 'x'.repeat(sz * 4);"
                                "  let acc = 0;"
                                "  for (let i = 0; i < sz; i += 64) acc += arr[i];"
                                "  return acc + text.length;"
                                "}"
                            ),
                            "args": ["$size"],
                            "lang": "js",
                        }
                    }
                }
            }
        ]
        rows = list(coll.aggregate(pipeline))
        checksum = sum(row["digest"] for row in rows)
        ops = mem_iters
    else:
        raise ValueError(f"Unknown workload: {workload}")

    wall_s = time.perf_counter() - t0
    self_after = resource.getrusage(resource.RUSAGE_SELF)

    # Sample live worker RSS/VSZ/HWM before shutting down and reaping it.
    worker = get_js_worker()
    worker_pid = _get_worker_pid(worker) if worker is not None else None
    ps_cpu_delta = 0.0
    live_rss_mib: float | None = None
    live_vsz_mib: float | None = None
    live_hwm_mib: float | None = None
    if worker_pid is not None:
        ps_child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        live_rss_mib, live_vsz_mib, live_hwm_mib = _sample_pid_memory_mib(
            worker_pid
        )
        ps_child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        ps_cpu_delta = (
            (ps_child_after.ru_utime - ps_child_before.ru_utime)
            + (ps_child_after.ru_stime - ps_child_before.ru_stime)
        )

    # Reap worker process so kernel RUSAGE_CHILDREN captures worker CPU + Peak RSS.
    close_js_workers()
    child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    conn.close()

    host_cpu_s = (
        (self_after.ru_utime - self_before.ru_utime)
        + (self_after.ru_stime - self_before.ru_stime)
    )
    worker_cpu_s = max(
        0.0,
        (child_after.ru_utime - child_before.ru_utime)
        + (child_after.ru_stime - child_before.ru_stime)
        - ps_cpu_delta,
    )
    rusage_child_peak_mib = _ru_maxrss_to_mib(child_after.ru_maxrss)
    # Prefer /proc/<pid>/status VmHWM on Linux when available (exact worker HWM),
    # else fall back to kernel RUSAGE_CHILDREN.ru_maxrss.
    worker_peak_rss_mib = (
        live_hwm_mib
        if live_hwm_mib is not None and live_hwm_mib > 0
        else rusage_child_peak_mib
    )
    host_peak_rss_mib = _ru_maxrss_to_mib(self_after.ru_maxrss)

    return {
        "engine": engine,
        "workload": workload,
        "ops": ops,
        "wall_ms": wall_s * 1000.0,
        "ops_per_sec": (ops / wall_s) if wall_s > 0 else 0.0,
        "host_cpu_ms": host_cpu_s * 1000.0,
        "worker_cpu_ms": worker_cpu_s * 1000.0,
        "total_cpu_ms": (host_cpu_s + worker_cpu_s) * 1000.0,
        "worker_rss_mib": live_rss_mib,
        "worker_peak_rss_mib": worker_peak_rss_mib,
        "worker_vsz_mib": live_vsz_mib,
        "host_peak_rss_mib": host_peak_rss_mib,
        "checksum": checksum,
    }


def _median_optional(values: list[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return float(statistics.median(present))


def _aggregate_runs(samples: list[dict[str, Any]]) -> dict[str, Any]:
    base = samples[0]
    return {
        "engine": base["engine"],
        "workload": base["workload"],
        "ops": base["ops"],
        "runs": len(samples),
        "wall_ms": float(statistics.median([s["wall_ms"] for s in samples])),
        "ops_per_sec": float(
            statistics.median([s["ops_per_sec"] for s in samples])
        ),
        "host_cpu_ms": float(
            statistics.median([s["host_cpu_ms"] for s in samples])
        ),
        "worker_cpu_ms": float(
            statistics.median([s["worker_cpu_ms"] for s in samples])
        ),
        "total_cpu_ms": float(
            statistics.median([s["total_cpu_ms"] for s in samples])
        ),
        "worker_rss_mib": _median_optional(
            [s["worker_rss_mib"] for s in samples]
        ),
        "worker_peak_rss_mib": float(
            statistics.median([s["worker_peak_rss_mib"] for s in samples])
        ),
        "worker_vsz_mib": _median_optional(
            [s["worker_vsz_mib"] for s in samples]
        ),
        "host_peak_rss_mib": float(
            statistics.median([s["host_peak_rss_mib"] for s in samples])
        ),
        "checksum": base["checksum"],
    }


def _fmt_opt(value: float | None, fmt: str = "{:.1f}") -> str:
    return "n/a" if value is None else fmt.format(value)


def _render_markdown(
    engines_meta: list[dict[str, Any]],
    results: dict[str, dict[str, dict[str, Any]]],
    workloads: list[tuple[str, str]],
    host_info: dict[str, str],
    docs: int,
    runs: int,
) -> str:
    engine_names = [e["name"] for e in engines_meta]
    lines: list[str] = []
    lines.append("# NeoSQLite JavaScript Engine Profiling Report")
    lines.append("")
    lines.append(f"- **Host:** {host_info['os']} ({host_info['arch']})")
    lines.append(f"- **CPU:** {host_info['cpu']} ({host_info['cores']} cores)")
    lines.append(f"- **RAM:** {host_info['ram']}")
    lines.append(f"- **Python:** {platform.python_version()}")
    lines.append(f"- **Config:** `docs={docs}`, `runs={runs}` (median)")
    lines.append("")
    lines.append("## Detected Engines")
    lines.append("")
    lines.append("| Engine | Available | Version | Detail |")
    lines.append("|--------|-----------|---------|--------|")
    for meta in engines_meta:
        avail = "yes" if meta["available"] else "no"
        ver = meta["version"] or "-"
        lines.append(
            f"| `{meta['name']}` | {avail} | {ver} | {meta['detail']} |"
        )
    lines.append("")

    # Table 1: Performance & CPU
    lines.append("## Performance & CPU Usage")
    lines.append("")
    lines.append(
        "| Workload | Engine | Ops | Wall (ms) | Throughput (ops/s) "
        "| Host CPU (ms) | Worker CPU (ms) | Total CPU (ms) |"
    )
    lines.append(
        "|----------|--------|----:|----------:|-------------------:"
        "|--------------:|----------------:|---------------:|"
    )
    for wl_key, wl_label in workloads:
        for eng in engine_names:
            row = results.get(eng, {}).get(wl_key)
            if row is None:
                continue
            lines.append(
                f"| {wl_label} | `{eng}` | {row['ops']} "
                f"| {row['wall_ms']:.2f} | {row['ops_per_sec']:.1f} "
                f"| {row['host_cpu_ms']:.2f} | {row['worker_cpu_ms']:.2f} "
                f"| {row['total_cpu_ms']:.2f} |"
            )
    lines.append("")

    # Table 2: RAM & Virtual Address Space
    lines.append("## Memory Usage (Worker & Host)")
    lines.append("")
    lines.append(
        "| Workload | Engine | Worker Live RSS (MiB) | Worker Peak RSS (MiB) "
        "| Worker VSZ (MiB) | Host Peak RSS (MiB) |"
    )
    lines.append(
        "|----------|--------|----------------------:|----------------------:"
        "|-----------------:|--------------------:|"
    )
    for wl_key, wl_label in workloads:
        for eng in engine_names:
            row = results.get(eng, {}).get(wl_key)
            if row is None:
                continue
            lines.append(
                f"| {wl_label} | `{eng}` "
                f"| {_fmt_opt(row['worker_rss_mib'])} "
                f"| {row['worker_peak_rss_mib']:.1f} "
                f"| {_fmt_opt(row['worker_vsz_mib'])} "
                f"| {row['host_peak_rss_mib']:.1f} |"
            )
    lines.append("")

    # Table 3: Head-to-head comparison when >= 2 engines profiled
    if len(engine_names) >= 2:
        primary = engine_names[0]
        baseline = engine_names[1]
        lines.append(
            f"## Head-to-Head Comparison (`{primary}` vs `{baseline}`)"
        )
        lines.append("")
        lines.append(
            f"| Workload | Wall Speedup (`{baseline}` / `{primary}`) "
            f"| CPU Ratio (`{baseline}` / `{primary}`) "
            f"| Worker Peak RSS (`{primary}` vs `{baseline}`) "
            f"| RAM Savings |"
        )
        lines.append(
            "|----------|----------------------------------------:"
            "|---------------------------------------:"
            "|---------------------------------------------:"
            "|------------:|"
        )
        for wl_key, wl_label in workloads:
            a = results.get(primary, {}).get(wl_key)
            b = results.get(baseline, {}).get(wl_key)
            if a is None or b is None:
                continue
            wall_ratio = (
                b["wall_ms"] / a["wall_ms"] if a["wall_ms"] > 0 else 0.0
            )
            cpu_ratio = (
                b["total_cpu_ms"] / a["total_cpu_ms"]
                if a["total_cpu_ms"] > 0
                else 0.0
            )
            rss_a = a["worker_peak_rss_mib"]
            rss_b = b["worker_peak_rss_mib"]
            ram_ratio = rss_b / rss_a if rss_a > 0 else 0.0
            lines.append(
                f"| {wl_label} | {wall_ratio:.2f}x | {cpu_ratio:.2f}x "
                f"| {rss_a:.1f} vs {rss_b:.1f} MiB | {ram_ratio:.2f}x less |"
            )
        lines.append("")

    lines.append("## Measurement Notes")
    lines.append("")
    lines.append(
        "- Each `(engine, workload)` runs in its own isolated child Python "
        "process so kernel `RUSAGE_CHILDREN` peak RSS and CPU accounting "
        "cannot bleed between runs."
    )
    lines.append(
        "- **Worker CPU (ms)** is kernel-accounted `RUSAGE_CHILDREN` "
        "(user + sys across all worker threads, including background V8 "
        "JIT/GC threads and worker startup for that run); on multi-core CPUs "
        "it can exceed wall time."
    )
    lines.append(
        "- **Worker Peak RSS (MiB)** uses `/proc/<pid>/status` (`VmHWM`) on "
        "Linux and kernel `RUSAGE_CHILDREN.ru_maxrss` (converted from bytes) "
        "on macOS. **Worker Live RSS / VSZ** is sampled right before worker "
        "shutdown (`/proc/<pid>/status` on Linux, `ps -o rss=,vsz=` on macOS)."
    )
    lines.append("")

    return "\n".join(lines)


def _orchestrate(args: argparse.Namespace) -> int:
    workloads = [
        ("cold_start", "Cold start + first eval"),
        ("where_filter", f"$where scan ({args.docs} docs)"),
        ("function_agg", f"$function agg ({args.docs} docs)"),
        ("bson_markers", "BSON ObjectId + Date"),
        ("cpu_compute", "CPU-bound prime sieve"),
        ("mem_alloc", "Temp allocation + GC"),
    ]

    detected = list_js_engines()
    detected_by_name = {info.name: info for info in detected}

    if args.engines:
        requested = [
            item.strip().lower()
            for item in args.engines.split(",")
            if item.strip()
        ]
    else:
        requested = [info.name for info in detected if info.available]

    if not requested:
        print(
            "No available JS engines detected. Install jsbox "
            "(pip install 'neosqlite[js]') or a patched Node.js runtime.",
            file=sys.stderr,
        )
        for info in detected:
            print(f"  - {info.name}: {info.detail}", file=sys.stderr)
        return 1

    active_engines: list[str] = []
    for name in requested:
        info = detected_by_name.get(name)
        if info is None:
            print(f"[WARN] Unknown JS engine {name!r}; skipping.", file=sys.stderr)
        elif not info.available:
            print(
                f"[WARN] JS engine {name!r} unavailable ({info.detail}); skipping.",
                file=sys.stderr,
            )
        else:
            active_engines.append(name)

    if not active_engines:
        print("No requested JS engines are available.", file=sys.stderr)
        return 1

    print("Detected JS engines:")
    for info in detected:
        status = "AVAILABLE" if info.available else "unavailable"
        print(f"  - {info.name:8s} [{status}] {info.detail}")
    print("")

    results: dict[str, dict[str, dict[str, Any]]] = {
        eng: {} for eng in active_engines
    }
    checksums: dict[str, dict[str, Any]] = {wl: {} for wl, _ in workloads}

    script_path = os.path.abspath(__file__)
    for eng in active_engines:
        print(f"Profiling engine: {eng}")
        for wl_key, wl_label in workloads:
            samples: list[dict[str, Any]] = []
            for _ in range(args.runs):
                completed = subprocess.run(
                    [
                        sys.executable,
                        script_path,
                        "--worker",
                        "--engine",
                        eng,
                        "--workload",
                        wl_key,
                        "--docs",
                        str(args.docs),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if completed.returncode != 0:
                    print(
                        f"  [ERROR] {eng}/{wl_key} failed:\n{completed.stderr}",
                        file=sys.stderr,
                    )
                    return 1
                samples.append(json.loads(completed.stdout))
            agg = _aggregate_runs(samples)
            results[eng][wl_key] = agg
            checksums[wl_key][eng] = agg["checksum"]
            print(
                f"  - {wl_label:28s}: wall={agg['wall_ms']:8.2f} ms | "
                f"cpu={agg['total_cpu_ms']:7.2f} ms "
                f"(worker {agg['worker_cpu_ms']:6.2f} ms) | "
                f"worker_peak_rss={agg['worker_peak_rss_mib']:5.1f} MiB | "
                f"vsz={_fmt_opt(agg['worker_vsz_mib']):>6s} MiB"
            )
        print("")

    # Verify result parity across profiled engines for every workload.
    for wl_key, per_engine in checksums.items():
        unique = {json.dumps(v, sort_keys=True) for v in per_engine.values()}
        if len(unique) > 1:
            print(
                f"[WARN] Parity mismatch on workload {wl_key!r}: {per_engine}",
                file=sys.stderr,
            )

    host_info = {
        "os": args.host_os,
        "arch": args.host_arch,
        "cpu": args.host_cpu,
        "cores": args.host_cores,
        "ram": args.host_ram,
    }
    engines_meta = [
        {
            "name": info.name,
            "available": info.available,
            "version": info.version,
            "detail": info.detail,
            "path": info.path,
        }
        for info in detected
        if info.name in active_engines or not args.engines
    ]

    report_md = _render_markdown(
        engines_meta=engines_meta,
        results=results,
        workloads=workloads,
        host_info=host_info,
        docs=args.docs,
        runs=args.runs,
    )
    print(report_md)

    if args.output_md:
        with open(args.output_md, "w", encoding="utf-8") as handle:
            handle.write(report_md)
            handle.write("\n")
        print(f"[INFO] Markdown report written to {args.output_md}")

    if args.output_json:
        payload = {
            "host": host_info,
            "python": platform.python_version(),
            "docs": args.docs,
            "runs": args.runs,
            "engines": engines_meta,
            "results": results,
        }
        with open(args.output_json, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        print(f"[INFO] JSON metrics written to {args.output_json}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--engine", default="")
    parser.add_argument("--workload", default="")
    parser.add_argument("--engines", default="")
    parser.add_argument("--docs", type=int, default=500)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--output-md", default="")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--host-os", default=platform.platform())
    parser.add_argument("--host-arch", default=platform.machine())
    parser.add_argument("--host-cpu", default="unknown")
    parser.add_argument("--host-cores", default="unknown")
    parser.add_argument("--host-ram", default="unknown")
    args = parser.parse_args()

    if args.worker:
        metrics = _run_single_workload(args.engine, args.workload, args.docs)
        sys.stdout.write(json.dumps(metrics))
        sys.stdout.flush()
        return 0
    return _orchestrate(args)


if __name__ == "__main__":
    raise SystemExit(main())
PYEOF

cd "$PROJECT_ROOT"
PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}" \
    "${PYTHON_CMD[@]}" "$PROFILER_SCRIPT" \
    --engines "$ENGINES" \
    --docs "$DOCS" \
    --runs "$RUNS" \
    --output-md "$OUTPUT_MD" \
    --output-json "$OUTPUT_JSON" \
    --host-os "$OS_PRETTY" \
    --host-arch "$ARCH_NAME" \
    --host-cpu "$CPU_MODEL" \
    --host-cores "$CPU_CORES" \
    --host-ram "$RAM_GB"
