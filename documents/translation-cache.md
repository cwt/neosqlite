---
type: design_doc
title: "SQL Translation Caching"
description: "Design and benchmarks of the LRU translation caching mechanism for repeated aggregation pipelines and $expr queries."
tags:
  - caching
  - lru
  - translation
  - performance
timestamp: 2026-11-25T00:00:00Z
version: "1.18.1"
lifecycle: active
---

# SQL Translation Caching

This document describes the translation caching mechanism in NeoSQLite, which provides significant performance improvements for repeated aggregation queries and `$expr` queries.

## Overview

The translation cache stores translated SQL templates for:
1. **Aggregation pipelines** - MongoDB-style stages translated to SQL
2. **Tier-2 $expr queries** - Complex expressions evaluated using temporary tables

This avoids repeated translation overhead for repeated query patterns.

> **v1.15.0 correctness note:** Cache keys now canonicalize **literal values**
> (type-tagged), and cache hits return the exact parameters stored with the
> template. Earlier versions keyed on structure only and re-derived parameters
> on hits, which could serve stale SQL — e.g., a descending `$sort` returned
> ascending order, or a second `{$match: {a: 2}}` pipeline reused the first
> one's bindings. See `translation_cache.py::_canonicalize`.



### Problem

Every aggregation pipeline or `$expr` query must be translated from MongoDB-style syntax to SQL. This translation involves:
1. Parsing pipeline stages or expressions
2. Building CTE (Common Table Expression) chains or temp tables
3. Converting field paths
4. Generating SQL string

For applications that repeatedly run the same query structure (e.g., dashboard queries, periodic reports), this translation overhead accumulates.

### Solution

Cache each compiled SQL template together with its exact bound parameter tuple (`CacheEntry(sql_template, payload)`), keyed by a type-tagged canonical representation of the full pipeline (`TranslationCache.make_key()`). On a cache hit, the stored SQL template and parameter tuple are returned immediately without re-running AST/SQL translation.

## Parameterized SQL Templates & Canonical Cache Keys

NeoSQLite compiles translated queries into **parameterized SQL templates** (`?` placeholders with a bound parameter tuple in `payload`).

### Canonicalizing Full Pipeline Content (`v1.15.0+`)

Previously, cache keys stripped literal values to share templates across different values and attempted to re-extract parameters on cache hits. Because sort directions, SQL literals, or operator shapes can change the compiled SQL structure—and positional re-extraction could mismatch bound values—`v1.15.0` replaced structure-only parameter re-extraction with full canonical key hashing:

```python
# In neosqlite/collection/query_helper/translation_cache.py:
key = cache.make_key(pipeline)
# Uses canonical_value_key() (type-tagged values + sorted keys, preserving
# dict insertion order inside order-sensitive keys such as $sort, $project,
# $addFields, $set, $setWindowFields, $fill, sortBy).
```

### Nested Operator & Literal Distinction

Canonicalizing both stage operators and type-tagged literal values ensures that:

- Different nested `$setWindowFields` window functions (`$rank` vs `$sum`) always get distinct cache keys.
- Type-distinct literals (`1` vs `True` vs `1.0` vs `"1"`) never collide in the cache.
- Insertion order is preserved inside `_ORDER_SENSITIVE_KEYS` (such as `$sort` or `$project`) so `{"$sort": {"a": 1, "b": -1}}` and `{"$sort": {"b": -1, "a": 1}}` never share a key.

### Implementation Flow

1. **Key Construction (`TranslationCache.make_key`)**: Formats each stage via `_canonicalize(spec, preserve_order=stage_name in _ORDER_SENSITIVE_KEYS)` using `canonical_value_key()` from `neosqlite/collection/type_utils.py`.
2. **Cache Storage (`TranslationCache.put`)**: Stores `CacheEntry(sql_template, payload)` containing both the SQL string and its matching parameter tuple.
3. **Cache Lookup (`TranslationCache.get`)**: Returns `(entry.sql_template, entry.payload)` in O(1) via `OrderedDict` and moves the entry to the MRU tail.

### Benefits

- **Repeated-query speedup**: Eliminates redundant CTE/temp-table SQL generation for repeated identical queries (e.g., paginated API polling, repeated `$match`/`$group` dashboard queries)
- **Strict correctness**: Every key maps 1:1 to an exact SQL template + bound parameter tuple
- **Bounded memory**: Fixed-size LRU cache prevents unbounded memory growth

## Architecture

### Cache Key Design

The cache key encodes every stage's canonicalized structure and type-tagged literal values:

```text
Pipeline: [{"$match": {"status": "active"}}]
Key:      "$match:(('status', ('str', 'active')),)"

Pipeline: [{"$match": {"status": "active"}}, {"$sort": {"name": 1}}]
Key:      "$match:(('status', ('str', 'active')),)|$sort:(('name', ('int', 1)),)"
```

On repeated executions of the same pipeline (such as paginated polling or dashboard refreshes), `cache.get(key)` returns the cached `(sql_template, payload)` directly.

### Cache Entry

Each `CacheEntry` stores (`__slots__`):
- **`sql_template`**: The translated SQL with `?` placeholders
- **`payload`**: Exact bound parameter `tuple[Any, ...]` corresponding 1:1 to the `?` placeholders in `sql_template`
- **`hit_count`**: Number of cache hits for this entry

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

# Setup: single connection with data
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

# Disable cache and benchmark (same connection, same data)
users.query_engine.sql_tier_aggregator._translation_cache.clear()
users.query_engine.sql_tier_aggregator._translation_cache._max_size = 0

start = time.perf_counter()
for _ in range(1000):
    list(users.aggregate([{'$match': {'status': 'active'}}]))
uncached_time = time.perf_counter() - start

print(f"Cached: {cached_time:.3f}s")
print(f"Uncached: {uncached_time:.3f}s")
print(f"Speedup: {uncached_time/cached_time:.2f}x")

# Alternative: Compare two connections (ensure identical data)
# conn_cached = neosqlite.Connection(':memory:', translation_cache=100)
# conn_uncached = neosqlite.Connection(':memory:', translation_cache=0)
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
cache_key = make_key(pipeline)  [O(n) canonical key with type-tagged literals]
    ↓
cached = cache.get(cache_key)
    ↓
if cached:
    # Cache hit: sql_template and payload correspond 1:1
    sql_template, params = cached
    execute(sql_template, params)
else:
    # Cache miss
    sql, params = build_pipeline_sql()
    cache.put(cache_key, sql, params)
    execute(sql, params)
```

### Design Characteristics

1. **Full canonical keys (`v1.15.0+`)**: Cache keys encode operator names, field names, and type-tagged literal values (`canonical_value_key`), preserving insertion order for `_ORDER_SENSITIVE_KEYS`. Because every cached entry pairs its `sql_template` with its exact `payload`, cache hits never serve mismatched SQL or parameters.

2. **Query-centric caching**: The cache is built from user queries rather than inspecting every document. If application code alters queries to match evolved document schemas, the canonical cache key changes automatically.

3. **Memory usage**: Each entry stores a SQL template and its bound parameter tuple. For most applications, 100 entries (default max size) uses negligible memory.

4. **Two independent caches**: Pipeline (Tier-1) and `$expr` (Tier-2) caches operate independently, each configurable via the `translation_cache` connection parameter.

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

1. **Check query-document alignment**: Ensure your query field names match current document structure (e.g., `status` vs `state`)
2. **Disable cache**: `conn = neosqlite.Connection(..., translation_cache=0)`
3. **Test without cache**: Verify the query works correctly
4. **Clear cache**: `qe.clear_cache()` to force fresh translation
5. **Use tier callbacks**: Add a callback to detect tier fallback:

   ```python
   def tier_callback(prev, new, pipeline):
       if prev and prev != new:
           print(f"WARNING: Fallback {prev} -> {new}")
   qe.add_tier_change_callback(tier_callback)
   ```

6. **Report issue**: If reproducible with matching query/document structure

### Memory Issues

If cache uses too much memory:

1. Reduce cache size: `qe.resize_cache(50)`
2. Disable for specific collections
3. Monitor with `qe.get_cache_stats()`

## Future Enhancements

Potential improvements:

1. **Time-based eviction**: Expire entries after TTL (useful for long-running applications with evolving query patterns)
2. **Weighted scoring**: Consider query cost or SQL template size in eviction decisions
3. **Persistent cache**: Save/load cache between connection restarts
4. **Distributed stats**: Aggregate cache stats across multiple connections
5. **Query plan hints**: Allow users to provide optimization hints for specific query patterns

## Tier Change Callbacks

NeoSQLite provides a callback system to notify when query execution tier changes during aggregation. This is useful for debugging and performance monitoring.

### Why Use Tier Callbacks?

When the translation cache has a bug (like the comparison operator bug fixed in v1.x.x), queries may silently fall back to a slower tier. The tier callback helps you detect this:

- **Normal operation**: Queries should consistently use `tier1` or `tier1_standard` (both are SQL)
- **Problem detected**: If you see `tier1` → `tier1_standard` or `tier1` → `tier2` → `tier3` changes unexpectedly, something is wrong

**Note:** "tier1_standard" was previously called "tier1_legacy" - it's the non-CTE SQL path. Both tier1 and tier1_standard are in the "SQL" category of the 3-tier model.

### Debugging Cache Bugs

Use tier callbacks to detect when cache bugs cause fallback:

```python
import neosqlite

tier_changes = []

def tier_callback(prev_tier, new_tier, pipeline):
    tier_changes.append((prev_tier, new_tier))
    if prev_tier and prev_tier != new_tier:
        print(f"WARNING: Tier fallback detected!")
        print(f"  {prev_tier} -> {new_tier}")
        print(f"  Pipeline: {pipeline}")

conn = neosqlite.Connection(':memory:', translation_cache=100)
users = conn.users
users.insert_many([{'age': i} for i in range(100)])

qe = users.query_engine
qe.add_tier_change_callback(tier_callback)

# Run queries
list(users.aggregate([{'$match': {'age': {'$gt': 50}}}]))
list(users.aggregate([{'$match': {'age': {'$gt': 25}}}]))  # Different value

# Check if any fallback happened
print(f"Tier changes: {tier_changes}")
# Expected: [(None, 'tier1')] - no fallback!
```

### Usage

```python
import neosqlite

def tier_callback(previous_tier, new_tier, pipeline):
    print(f"Tier changed: {previous_tier} -> {new_tier}")

conn = neosqlite.Connection(':memory:')
users = conn.users
users.insert_many([{'age': i} for i in range(100)])

qe = users.query_engine
qe.add_tier_change_callback(tier_callback)

# Run queries - callback will be notified of tier changes
list(users.aggregate([{'$match': {'age': {'$gt': 50}}}]))
# Output: Tier changed: None -> tier1
```

### API

- `add_tier_change_callback(callback)`: Register a callback to be notified of tier changes
- `remove_tier_change_callback(callback)`: Remove a registered callback
- `get_last_tier()`: Get the last tier that was used
- `clear_tier_callbacks()`: Remove all registered callbacks

### Callback Signature

The callback receives three arguments:
- `previous_tier`: The tier used in the previous query (or `None` for first query)
- `new_tier`: The tier used for the current query
- `pipeline`: The aggregation pipeline that was executed

### Tier Types

| Tier | Description |
|------|-------------|
| `tier1` | SQL CTE-based aggregation (new optimizer) |
| `tier1_standard` | SQL non-CTE aggregation (older SQL builder) |
| `tier2` | Temporary table for complex queries |
| `tier3` | Python fallback for unsupported operations |

Note: `tier1_standard` was previously called `tier1_legacy`. Both tier1 and tier1_standard use single SQL execution, categorized as "SQL" in the 3-tier model.

## Bug Fixes

### Fixed: Comparison Operator Parameter Extraction (v1.x.x)

Previously, when using comparison operators like `$gt`, `$lt`, `$gte`, `$lte`, `$ne` in cached queries, the cache hit would fail with an error like "Error binding parameter: type 'dict' is not supported".

**Root Cause**: The parameter extraction was returning the entire operator dict (e.g., `{"$gt": 35}`) instead of just the value (`35`).

**Fix**: Added `_extract_comparison_value()` method that properly extracts the actual value from comparison operator dicts.

**How to Detect This Bug (Historical)**:

Before the fix, running these queries would show tier fallback:

```python
list(users.aggregate([{'$match': {'age': {'$gt': 25}}}]))  # tier1
list(users.aggregate([{'$match': {'age': {'$gt': 35}}}]))  # Falls back to tier1_standard!
```

**Fix verification**: After fix, both queries should use tier1 consistently.

With tier callbacks enabled, you would see:

```text
Tier changed: None -> tier1
Tier changed: tier1 -> tier1_standard  # BUG: Should stay on tier1!
```

**After the fix**: Both queries use `tier1` with cache hits:

```text
Tier changed: None -> tier1
```

**Example**:

```python
# Before fix: Second query would fail
list(users.aggregate([{'$match': {'age': {'$gt': 25}}}))  # Cache miss
list(users.aggregate([{'$match': {'age': {'$gt': 35}}}))  # Cache hit - ERROR!

# After fix: Works correctly
list(users.aggregate([{'$match': {'age': {'$gt': 25}}}))  # Cache miss
list(users.aggregate([{'$match': {'age': {'$gt': 35}}}))  # Cache hit - SUCCESS!
```
