# Force Fallback Kill Switch

## Overview

The force fallback kill switch is a debugging and benchmarking feature that allows you to force all aggregation queries to use the Python fallback implementation instead of the SQL optimization path. This is useful for:

1.  **Benchmarking**: Comparing performance between the optimized SQL path and the Python fallback.
2.  **Debugging**: Isolating issues that may only appear in one of the execution paths.
3.  **Regression Testing**: Ensuring both paths produce identical results.

## Usage

The kill switch is controlled by a global flag.

### Enabling/Disabling the Kill Switch

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

The kill switch works by adding a check at the beginning of the `_build_aggregation_query` method. When the flag is `True`, the method immediately returns `None`, which signals the aggregation engine to use the Python implementation.

## Best Practices

-   **Always reset the flag** after testing to avoid unintended side effects.
-   Use only in controlled testing and benchmarking environments.
-   Always verify that the results from both paths are identical when benchmarking.
