---
type: guide
title: "Force Fallback Kill Switch"
description: "Guide to using NEOSQLITE_FORCE_FALLBACK for debugging, differential benchmarking, and fallback testing."
tags:
  - debugging
  - fallback
  - kill-switch
  - benchmarks
timestamp: 2026-03-12T03:42:00Z
version: "1.15.1"
lifecycle: active
---

# Force Fallback Kill Switch

## Overview

The force fallback kill switch is a debugging, differential testing, and benchmarking feature that forces all logical collection operations (queries, filters, sorts, `min`/`max` bounds, counts, `distinct`, `$expr`, `$text`, datetime queries, CRUD matching, `find_one_and_*`, and aggregation pipelines) to bypass SQL/CTE/temporary-table optimizations (Tiers 1 and 2) and execute in Python Tier 3 as the authoritative reference of correctness.

> **Note:** GridFS operations are I/O-centric and remain SQL-only; the kill switch does not force GridFS chunk or file operations through Python fallbacks.

This is useful for:

1. **Correctness Reference**: Verifying edge-case semantics against MongoDB-compatible Python Tier 3 execution.
2. **Benchmarking**: Comparing performance between the optimized SQL/Tier 1–2 paths and the Python Tier 3 fallback.
3. **Debugging**: Isolating issues that may only appear in one of the execution paths.
4. **Regression Testing**: Ensuring SQL, temporary table, and Python Tier 3 paths produce identical results.

## Usage

The kill switch is controlled by the `NEOSQLITE_FORCE_FALLBACK` environment variable or a global runtime flag.

### Enabling via Environment Variable

Set `NEOSQLITE_FORCE_FALLBACK` to `1`, `true`, `yes`, or `on` (case-insensitive) before importing `neosqlite`:

```bash
NEOSQLITE_FORCE_FALLBACK=1 pytest
```

### Enabling/Disabling Programmatically

```python
import neosqlite.collection.query_helper

# Enable force fallback
neosqlite.collection.query_helper.set_force_fallback(True)

# Disable force fallback (default behavior)
neosqlite.collection.query_helper.set_force_fallback(False)
```

### Example: Benchmarking Performance

```python
import neosqlite
import time

# ... (setup collection and data) ...

pipeline = [
    {"$match": {"category": "Category5"}},
    {"$sort": {"value": -1}},
    {"$limit": 100}
]

# Test optimized path
neosqlite.collection.query_helper.set_force_fallback(False)
start_time = time.perf_counter()
result_optimized = collection.aggregate(pipeline)
optimized_time = time.perf_counter() - start_time

# Test fallback path
neosqlite.collection.query_helper.set_force_fallback(True)
start_time = time.perf_counter()
result_fallback = collection.aggregate(pipeline)
fallback_time = time.perf_counter() - start_time

# Reset to normal operation
neosqlite.collection.query_helper.set_force_fallback(False)

# Compare results and performance
print(f"Optimized: {optimized_time:.4f}s")
print(f"Fallback: {fallback_time:.4f}s")
print(f"Speedup: {fallback_time/optimized_time:.2f}x")
```

## How It Works

When the kill switch is active (`get_force_fallback() == True`), NeoSQLite disables Tier 1 (SQL translation, `RETURNING`, FTS SQL queries, `$expr` SQL, datetime SQL, `$count` shortcuts, SQL `COUNT`/`DISTINCT`, SQL sorting/pagination/bounds) and Tier 2 (temporary-table aggregation). Logical evaluation routes directly to Python Tier 3, using SQLite only for base document storage reads/writes (`SELECT id, _id, data FROM ...`, `_internal_insert`, `_internal_replace`, `_internal_delete`) and GridFS SQL I/O.

## Best Practices

- **Always reset the flag** after testing to avoid unintended side effects.
- Use only in controlled testing and benchmarking environments.
- Always verify that the results from both paths are identical when benchmarking.
