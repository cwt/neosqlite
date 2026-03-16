# Pipeline Translation Caching

This document describes the pipeline translation caching mechanism in NeoSQLite, which provides significant performance improvements for repeated aggregation queries.

## Overview

The pipeline caching system stores translated SQL templates for aggregation pipelines, avoiding repeated translation overhead for identical or similar query patterns.

### Problem

Every aggregation pipeline must be translated from MongoDB-style stages to SQL. This translation involves:
1. Parsing pipeline stages
2. Building CTE (Common Table Expression) chains
3. Converting field paths
4. Generating SQL string

For applications that repeatedly run the same pipeline structure (e.g., dashboard queries, periodic reports), this translation overhead accumulates.

### Solution

Cache the translated SQL template using the pipeline structure as the key. On cache hit, extract parameter values from the new pipeline and execute immediately.

## Parameterized SQL Caching

A key innovation in NeoSQLite's caching is **parameterized SQL templates**. Instead of embedding literal values in SQL, we use `?` placeholders that get filled at runtime.

### Why Parameterized?

Consider `$sample` with different sizes:

```python
# Naive approach: Each size = different cache key
{"$sample": {"size": 3}}   → LIMIT 3   → Key: "$sample:('size',):(('size',3),)"
{"$sample": {"size": 20}}  → LIMIT 20  → Key: "$sample:('size',):(('size',20),)"
# Result: Cache miss every time - no benefit!
```

**Parameterized approach:**

```python
# Single cache key for all sizes
{"$sample": {"size": 3}}   → SQL: "LIMIT ?" + params: [3]
{"$sample": {"size": 20}}  → SQL: "LIMIT ?" + params: [20]

# Both use same cache entry!
# Key: "$sample:('size',)"
# SQL: "LIMIT ?"
```

### Supported Parameterized Operators

| Operator | Parameter | Example |
|----------|-----------|---------|
| `$sample` | `size` | `{"$sample": {"size": 10}}` |
| `$limit` | `limit` | `{"$limit": 100}` |
| `$skip` | `skip` | `{"$skip": 50}` |

These operators' values are passed as SQL parameters rather than embedded in the SQL string.

### Nested Operator Extraction

Certain operators like `$setWindowFields` have **nested operators** that completely change the SQL generated:

```python
# These have DIFFERENT SQL but same top-level structure!
{"$setWindowFields": {"output": {"rank": {"$rank": {}}}}
{"$setWindowFields": {"output": {"runningSum": {"$sum": "$score"}}}
```

Our cache key includes **nested `$` operators** to distinguish them:

```
Pipeline: [{"$setWindowFields": {"output": {"rank": {"$rank": {}}}}]
Key:      "$setWindowFields:('$rank',)"

Pipeline: [{"$setWindowFields": {"output": {"runningSum": {"$sum": "$score"}}}}]
Key:      "$setWindowFields:('$sum',)"
```

Without this, different window functions would incorrectly share the same cache entry and return wrong results.

### Implementation

1. **SQL Generation**: Use `?` placeholder instead of embedding values

   ```python
   # Instead of: LIMIT {int(size)}
   # Generate:    LIMIT ?
   sql = f"LIMIT ?"
   return sql, [size]
   ```

2. **Parameter Extraction**: Detect `?` placeholders in SQL template

   ```python
   def _extract_param_names_from_template(sql):
       # Count ? placeholders beyond json_extract
       ...
   ```

3. **Value Mapping**: Extract values from pipeline at runtime

   ```python
   def _get_placeholder_values(pipeline):
       # Extract $sample.size, $limit, $skip from pipeline
       return {"__placeholder_0__": pipeline[0]["$sample"]["size"]}
   ```

### Benefits

- **Higher cache hit rate**: Same template reused with different values
- **Less memory**: One SQL template instead of N variants
- **Faster queries**: Translation happens once, execution is fast

## Architecture

### Cache Key Design

The cache key is based on **pipeline structure** (operator names + field names), with special handling for parameterized operators:

```
Pipeline: [{"$match": {"status": "active"}}]
Key:      "$match:('status',)"

Pipeline: [{"$match": {"age": {"$gt": 25}}}]
Key:      "$match:('age',)"

Pipeline: [{"$match": {"status": "active"}}, {"$sort": {"name": 1}}]
Key:      "$match:('status',)|$sort:('name',)"

# Parameterized operators - values NOT in key (cached as SQL params!)
Pipeline: [{"$sample": {"size": 3}}]
Pipeline: [{"$sample": {"size": 20}}]
Key:      "$sample:('size',)"  ← Same key for both!
```

**Why exclude parameterized values?**

For operators like `$sample`, `$limit`, `$skip`, the value is passed as a SQL parameter (`?`), not embedded in SQL. This means:
- Same cache entry can be reused for different values
- Only one SQL template in cache
- Runtime fills in the parameter from pipeline

This allows queries with the same structure but different parameter values to share cached SQL.

### Cache Entry

Each cache entry stores:
- **SQL template**: The translated SQL with `?` placeholders
- **Parameter names**: Field paths used in the query
- **Hit count**: Number of cache hits
- **Last hit**: Access counter for eviction decisions

### Hit-Rate-Based Eviction

When the cache reaches maximum size, entries with the **lowest hit rate** are evicted:

```
Score = hit_count / (age + 1)
```

Entries with fewer hits are prioritized for eviction, but recent entries are also protected to allow for temporal locality.

## Usage

### Configuration

```python
import neosqlite

# Default: cache enabled with 100 entries
conn = neosqlite.Connection('mydb.db')

# Custom cache size
conn = neosqlite.Connection('mydb.db', pipeline_cache=50)

# Disable cache (useful for development/debugging)
conn = neosqlite.Connection('mydb.db', pipeline_cache=0)
```

### Debug API

Access cache through the SQL tier aggregator:

```python
users = conn.users
qe = users.query_engine.sql_tier_aggregator

# Check status
qe.is_cache_enabled()          # True/False
qe.cache_size()                # Current entries

# Get statistics
qe.get_cache_stats()
# {
#     'enabled': True,
#     'size': 3,
#     'max_size': 100,
#     'hits': 42,
#     'misses': 8,
#     'hit_rate': 0.84,
#     'total_accesses': 50,
#     'entries': [
#         {'key': "$match:('status',)", 'hit_count': 20, ...},
#         ...
#     ]
# }

# Dump all entries
qe.dump_cache()

# Check specific pipeline
qe.cache_contains([{"$match": {"status": "active"}}])

# Manual operations
qe.clear_cache()                # Clear all entries
qe.evict_from_cache(pipeline)  # Evict specific
qe.resize_cache(200)           # Change max size at runtime
```

## Performance Impact

### Expected Improvements

For workloads with repeated query patterns:

| Query Pattern | Improvement |
|---------------|-------------|
| Same filter, different values | ~10-30% (avoids translation) |
| Dashboard refreshes | ~20-50% |
| Periodic reports | ~30-60% |

### Benchmarking

Run benchmarks to measure impact:

```python
import time
import neosqlite

conn = neosqlite.Connection(':memory:')
users = conn.users
users.insert_many([{'status': 'active', 'name': f'user_{i}'} for i in range(1000)])

# Warm up cache
for status in ['active', 'inactive']:
    list(users.aggregate([{'$match': {'status': status}}]))

# Benchmark with cache
start = time.perf_counter()
for _ in range(1000):
    list(users.aggregate([{'$match': {'status': 'active'}}]))
cached_time = time.perf_counter() - start

# Disable cache and benchmark
conn2 = neosqlite.Connection(':memory:', pipeline_cache=0)
users2 = conn2.users
users2.insert_many([{'status': 'active', 'name': f'user_{i}'} for i in range(1000)])

start = time.perf_counter()
for _ in range(1000):
    list(users2.aggregate([{'$match': {'status': 'active'}}]))
uncached_time = time.perf_counter() - start

print(f"Cached: {cached_time:.3f}s")
print(f"Uncached: {uncached_time:.3f}s")
print(f"Speedup: {uncached_time/cached_time:.2f}x")
```

## Configuration Recommendations

### When to Enable (Default)

- Production workloads with repeated queries
- Dashboard applications
- API endpoints with common filter patterns
- Batch processing with structured queries

### When to Disable

- Development/debugging (see actual translation)
- Highly dynamic queries (unique each time)
- Memory-constrained environments
- Testing pipeline translation correctness

### Cache Size Guidelines

| Workload | Recommended Size |
|----------|------------------|
| Simple app | 50 |
| Typical app | 100 (default) |
| Complex app | 200-500 |
| High cardinality | 1000+ |

## Implementation Details

### Files

- `neosqlite/collection/query_helper/pipeline_cache.py` - Cache implementation
- `neosqlite/collection/sql_tier_aggregator.py` - Cache integration
- `neosqlite/connection.py` - Configuration options
- `tests/test_pipeline_cache.py` - Unit tests

### Cache Flow

```
aggregate(pipeline)
    ↓
can_optimize_pipeline()  [O(n) stage check]
    ↓
cache_key = make_key(pipeline)  [O(n) key extraction]
    ↓
cached = cache.get(cache_key)
    ↓
if cached:
    # Cache hit
    params = extract_param_values(pipeline, param_names)
    execute(sql_template, params)
else:
    # Cache miss
    sql, params = build_pipeline_sql()
    cache.put(cache_key, sql, param_names)
    execute(sql, params)
```

### Limitations

1. **Simple pipelines only**: Complex expressions may not cache well
2. **Structure-based keys**: Different field order = different key
3. **No schema awareness**: Schema changes don't invalidate cache
4. **Memory usage**: Each entry stores full SQL template

## Troubleshooting

### Low Hit Rate

If hit rate is low (< 30%):

1. Check query patterns - are they truly repetitive?
2. Verify cache is enabled: `qe.is_cache_enabled()`
3. Check cache size: `qe.get_cache_stats()['max_size']`
4. Consider increasing cache size

### Unexpected Results

If cached queries return wrong results:

1. Disable cache: `conn = neosqlite.Connection(..., pipeline_cache=0)`
2. Test without cache
3. Report issue if reproducible

### Memory Issues

If cache uses too much memory:

1. Reduce cache size: `qe.resize_cache(50)`
2. Disable for specific collections
3. Monitor with `qe.get_cache_stats()`

## Future Enhancements

Potential improvements:

1. **Schema-aware invalidation**: Auto-evict on table schema changes
2. **Time-based eviction**: Expire entries after TTL
3. **Weighted scoring**: Consider query cost in eviction
4. **Persistent cache**: Save/load cache between restarts
5. **Distributed stats**: Aggregate cache stats across connections
