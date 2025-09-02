# NeoSQLite json_each() Enhancements

## Overview

This document summarizes the enhancements made to NeoSQLite to leverage SQLite's `json_each()` function for improved performance in aggregation operations. The goal is to progressively optimize more MongoDB-style operations by pushing them down to the SQLite level.

## Key Enhancements

### 1. Basic $unwind Optimization
- **Status**: ✅ Completed
- Single `$unwind` operations are optimized with `json_each()`.
- SQL queries are generated for array decomposition, which is significantly faster than Python-based iteration.

### 2. Multiple Consecutive $unwind Stages
- **Status**: ✅ Completed
- The implementation now detects and handles multiple consecutive `$unwind` stages.
- It generates SQL queries that chain multiple `json_each()` calls, creating a Cartesian product efficiently at the database level.

**Example SQL for `[{"$unwind": "$tags"}, {"$unwind": "$categories"}]`**:
```sql
SELECT collection.id, 
       json_set(
           json_set(collection.data, '$."tags"', je1.value), 
           '$."categories"', je2.value
       ) as data
FROM collection,
     json_each(json_extract(collection.data, '$.tags')) as je1,
     json_each(json_extract(collection.data, '$.categories')) as je2
```

### 3. $unwind + $group Optimization
- **Status**: ✅ Completed
- Pipelines with `$unwind` followed by `$group` are now optimized.
- A single SQL query is generated that combines `json_each()` with `GROUP BY`.
- Supported accumulators: `$sum`, `$count`, `$avg`, `$min`, `$max`, `$push`, and `$addToSet`.

**Example SQL for `[{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "count": {"$sum": 1}}}]`**:
```sql
SELECT je.value AS _id, COUNT(*) AS count
FROM collection, json_each(json_extract(collection.data, '$.tags')) as je
GROUP BY je.value
ORDER BY _id
```

### 4. Nested Array Unwinding
- **Status**: ✅ Completed
- The system now handles unwinding of nested arrays (e.g., `$unwind: "$orders.items"`).
- It generates chained `json_each()` calls where subsequent calls operate on the results of the previous ones.

### 5. Advanced $unwind Options
- **Status**: ✅ Completed
- Support for `includeArrayIndex` and `preserveNullAndEmptyArrays` options in `$unwind` operations.
- These options are implemented in Python for flexibility.

### 6. Text Search Integration with json_each()
- **Status**: ✅ Completed
- SQL-level optimization for combining array unwinding with text search operations (`$unwind` + `$match` with `$text`).
- This can be 10-100x faster than Python-based processing.

## Benefits

- **Performance**: Drastically improved performance for aggregation pipelines involving arrays by moving processing from Python to native SQLite.
- **Memory Efficiency**: Reduced Python memory footprint as intermediate results are handled within the database.
- **Scalability**: Ability to process larger datasets that would be slow or impossible with in-memory Python processing.
- **Backward Compatibility**: All enhancements maintain full backward compatibility, falling back to the Python implementation for complex or unsupported cases.
