# Force Fallback Kill Switch

## Overview

The force fallback kill switch is a feature that allows you to force all aggregation queries to use the Python fallback implementation instead of the SQL optimization path. This is useful for:

1. **Benchmarking** - Comparing performance between optimized and fallback paths
2. **Debugging** - Troubleshooting issues that may only appear in one path
3. **Regression testing** - Ensuring both paths produce identical results

## Usage

### Enabling/Disabling the Kill Switch

```python
import neosqlite

# Enable force fallback
neosqlite.collection.query_helper.set_force_fallback(True)

# Disable force fallback (default behavior)
neosqlite.collection.query_helper.set_force_fallback(False)

# Check current state
current_state = neosqlite.collection.query_helper.get_force_fallback()
```

### Example: Benchmarking Performance

```python
import neosqlite
import time

# Create test data
with neosqlite.Connection(":memory:") as conn:
    collection = conn["test_collection"]
    
    # Insert test data
    test_data = [
        {"category": f"Category{i % 10}", "value": i}
        for i in range(10000)
    ]
    collection.insert_many(test_data)
    
    # Define test pipeline
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

The kill switch works by adding a global flag check at the beginning of the `_build_aggregation_query` method in `query_helper.py`. When the flag is set to `True`, the method immediately returns `None`, which forces the aggregation engine to fall back to the Python implementation.

```python
# In _build_aggregation_query method
global _FORCE_FALLBACK
if _FORCE_FALLBACK:
    return None  # Force fallback to Python implementation
```

## Benefits

1. **Performance Analysis** - Quantify the performance benefits of SQL optimization
2. **Consistency Verification** - Ensure both paths produce identical results
3. **Debugging Aid** - Isolate issues to specific execution paths
4. **Regression Prevention** - Catch divergence between optimized and fallback paths

## Limitations

1. The kill switch affects all aggregation queries in the process
2. It's a global setting, not per-query
3. Should only be used for testing and benchmarking

## Best Practices

1. **Always reset the flag** after testing to avoid affecting other operations
2. **Use in controlled environments** to avoid impacting production performance
3. **Verify result consistency** when benchmarking to ensure valid comparisons