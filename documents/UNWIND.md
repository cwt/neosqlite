# $unwind Stage Enhancements in NeoSQLite

The `$unwind` aggregation stage in NeoSQLite has been significantly enhanced to leverage SQLite's `json_each()` function, providing substantial performance improvements over the default Python-based implementation.

## Core Optimization with json_each()

The primary enhancement is the offloading of the `$unwind` operation to the SQLite engine. Instead of fetching documents into Python and looping through arrays, NeoSQLite now generates a SQL query that uses `json_each()` to expand the array at the database level. This reduces memory usage and is significantly faster.

## Key Enhancements

### 1. Multiple Consecutive $unwind Stages
NeoSQLite can optimize multiple consecutive `$unwind` stages into a single SQL query. This is achieved by chaining `json_each()` calls in the `FROM` clause of the query, which efficiently computes the Cartesian product of the arrays.

### 2. Nested Array Unwinding
The implementation correctly handles nested arrays, such as unwinding `orders` and then `orders.items`. It generates the appropriate chained `json_each()` calls to traverse the nested structure.

### 3. $unwind + $group Optimization
Pipelines containing an `$unwind` stage followed by a `$group` stage are optimized into a single SQL query that combines `json_each()` with `GROUP BY`. This is a common and powerful aggregation pattern.

### 4. $unwind + $sort + $limit Optimization
`$sort`, `$skip`, and `$limit` stages following an `$unwind` are also pushed down to the SQL level. The generated query will include `ORDER BY`, `LIMIT`, and `OFFSET` clauses for native database sorting and pagination.

### 5. Advanced $unwind Options
NeoSQLite also supports advanced `$unwind` options for greater control:
- **`includeArrayIndex`**: Adds a new field with the array index of the unwound element.
- **`preserveNullAndEmptyArrays`**: Preserves documents where the array field is null, empty, or missing.

These advanced options are handled by the Python fallback implementation to ensure flexibility.

## Performance Benefits
- **Speed**: Native SQLite operations are orders of magnitude faster than Python loops.
- **Memory**: Intermediate results are handled by the database, drastically reducing the Python process's memory footprint.
- **Reduced Data Transfer**: Only the final, processed documents are transferred from SQLite to Python.

## Usage
The optimizations are applied automatically. Simply use the `$unwind` stage in your aggregation pipelines, and NeoSQLite will attempt to generate an optimized SQL query. If the pipeline is too complex for the optimizer, it will seamlessly fall back to the Python implementation.

**Example of an optimizable pipeline:**
```python
pipeline = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"},
    {"$unwind": "$categories"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]
```
