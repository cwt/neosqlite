# SQL Translation Caching

This document describes the translation caching mechanism in NeoSQLite, which provides significant performance improvements for repeated aggregation queries and `$expr` queries.

## Overview

The translation cache stores translated SQL templates for:
1. **Aggregation pipelines** - MongoDB-style stages translated to SQL
2. **Tier-2 $expr queries** - Complex expressions evaluated using temporary tables

This avoids repeated translation overhead for identical or similar query patterns.

### Problem

Every aggregation pipeline or `$expr` query must be translated from MongoDB-style syntax to SQL. This translation involves:
1. Parsing pipeline stages or expressions
2. Building CTE (Common Table Expression) chains or temp tables
3. Converting field paths
4. Generating SQL string

For applications that repeatedly run the same query structure (e.g., dashboard queries, periodic reports), this translation overhead accumulates.

### Solution

Cache the translated SQL template using the query structure as the key. On cache hit, extract parameter values from the new query and execute immediately.

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

```json
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

```json
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

### LRU Eviction

The cache uses **LRU (Least Recently Used)** eviction with `OrderedDict` for O(1) get/put operations:
- Most recently used entries are moved to the end
- Least recently used entries are evicted from the front when cache is full
- No complex hit-rate scoring needed

## Usage

### Configuration

```python
import neosqlite

# Default: cache enabled with 100 entries
conn = neosqlite.Connection('mydb.db')

# Custom cache size
conn = neosqlite.Connection('mydb.db', translation_cache=50)

# Disable cache (useful for development/debugging)
conn = neosqlite.Connection('mydb.db', translation_cache=0)
```

### Debug API

Access cache through the SQL tier aggregator (for pipelines) or directly (for $expr):

```python
users = conn.users

# Pipeline cache (Tier-1)
qe = users.query_engine.sql_tier_aggregator
qe.is_cache_enabled()          # True/False
qe.cache_size()                # Current entries

# $expr cache (Tier-2)
helpers = users.query_engine.helpers
tier2 = helpers.tier2_evaluator
tier2.is_cache_enabled()
tier2.cache_size()

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
tier2.dump_cache()

# Check specific pipeline
qe.cache_contains([{"$match": {"status": "active"}}])

# Manual operations
qe.clear_cache()                # Clear pipeline cache
tier2.clear_cache()             # Clear $expr cache
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
conn2 = neosqlite.Connection(':memory:', translation_cache=0)
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

### Architecture

The translation cache is used in two separate components:

1. **SQLTierAggregator** - Caches aggregation pipeline translations (Tier-1)
2. **TempTableExprEvaluator** - Caches `$expr` query translations (Tier-2)

Each component has its own independent cache instance, both configurable via the `translation_cache` connection parameter.

### Files

- `neosqlite/collection/query_helper/translation_cache.py` - Cache implementation
- `neosqlite/collection/sql_tier_aggregator.py` - Pipeline cache integration
- `neosqlite/collection/expr_temp_table.py` - $expr cache integration
- `neosqlite/connection.py` - Configuration options
- `tests/test_translation_cache.py` - Unit tests

### Cache Flow

```python
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

1. **Structure-based keys**: Different field order = different key
2. **No schema awareness**: Schema changes don't invalidate cache
3. **Memory usage**: Each entry stores full SQL template
4. **Two independent caches**: Pipeline and $expr caches are separate

## Temp Table Cleanup

Tier-2 `$expr` queries use temporary tables for evaluation. The cache tracks these tables and provides cleanup:

- Cursor tracks `tables_to_cleanup` list
- `close()` and `__del__()` methods ensure cleanup on cursor exhaustion
- Cleanup chain: Connection → Collection → QueryEngine → QueryHelper → TempTableExprEvaluator

## Troubleshooting

### Low Hit Rate

If hit rate is low (< 30%):

1. Check query patterns - are they truly repetitive?
2. Verify cache is enabled: `qe.is_cache_enabled()`
3. Check cache size: `qe.get_cache_stats()['max_size']`
4. Consider increasing cache size
5. Check both pipeline and $expr caches

### Unexpected Results

If cached queries return wrong results:

1. Disable cache: `conn = neosqlite.Connection(..., translation_cache=0)`
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
