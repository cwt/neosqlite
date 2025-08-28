# NeoSQLite $lookup Implementation

## Overview

This document describes the implementation of the `$lookup` aggregation stage in NeoSQLite, which enables join-like operations between collections using SQLite's SQL capabilities.

## Implementation Details

### Core Features

1. **Cross-Collection Joins**: Enables joining documents from different collections based on field relationships
2. **SQL Optimization**: Uses optimized SQL queries for simple `$lookup` operations
3. **Python Fallback**: Falls back to Python processing for complex pipelines
4. **Integration**: Works seamlessly with other aggregation stages

### SQL-Based Implementation

For simple `$lookup` operations (when `$lookup` is the only stage or the first stage followed by `$match`), NeoSQLite generates optimized SQL queries:

```sql
SELECT customers.id, 
       json_set(customers.data, '$."customerOrders"', 
                coalesce((
                  SELECT json_group_array(json(related.data)) 
                  FROM orders as related 
                  WHERE json_extract(related.data, '$.customerId') = 
                        json_extract(customers.data, '$.customerId') 
                ), '[]')) as data
FROM customers
```

This SQL query:
- Uses a subquery to find all matching documents in the related collection
- Uses `json_group_array` to collect the matching documents into an array
- Uses `json_set` to add the array as a new field in the result documents
- Uses `coalesce` to ensure an empty array is returned when no matches are found

### Python Fallback Implementation

For complex pipelines (when `$lookup` is followed by other stages like `$unwind`), NeoSQLite falls back to a Python implementation that:

1. Processes each document individually
2. For each document, finds matching documents in the related collection
3. Adds the matching documents as an array field
4. Continues with subsequent pipeline stages

### Supported Use Cases

1. **Basic $lookup operations**:
   ```python
   [
     {"$lookup": {
       "from": "orders",
       "localField": "customerId",
       "foreignField": "customerId",
       "as": "customerOrders"
     }}
   ]
   ```

2. **$lookup with $match**:
   ```python
   [
     {"$match": {"status": "active"}},
     {"$lookup": {
       "from": "orders",
       "localField": "customerId",
       "foreignField": "customerId",
       "as": "customerOrders"
     }}
   ]
   ```

3. **$lookup followed by $unwind**:
   ```python
   [
     {"$lookup": {
       "from": "orders",
       "localField": "customerId",
       "foreignField": "customerId",
       "as": "customerOrders"
     }},
     {"$unwind": "$customerOrders"}
   ]
   ```

### Performance Characteristics

1. **SQL-Based Approach**:
   - All processing happens at the database level
   - No intermediate Python data structures
   - Efficient use of SQLite's query optimizer
   - Reduced memory footprint

2. **Python Fallback**:
   - Processes documents one by one in Python
   - Higher memory usage for large datasets
   - Slower than SQL-based approach
   - Maintains compatibility with complex pipelines

### Limitations

1. **No Advanced $lookup Features**: 
   - Does not support the `let` and `pipeline` options for more complex lookups
   - Does not support outer joins with conditions beyond simple field matching

2. **Performance**: 
   - Complex pipelines fall back to Python processing
   - Large datasets may be slow with Python fallback

## Implementation Architecture

### QueryHelper Integration

The `$lookup` functionality is implemented in the `QueryHelper` class in `neosqlite/collection/query_helper.py`:

1. **`_build_aggregation_query` method**: Detects `$lookup` stages and generates SQL queries
2. **Pattern matching**: Identifies when SQL optimization is possible
3. **Fallback handling**: Returns `None` to trigger Python processing for complex cases

### QueryEngine Integration

The Python fallback is implemented in the `QueryEngine` class in `neosqlite/collection/query_engine.py`:

1. **Case handling**: Added `$lookup` case to the aggregation pipeline processing
2. **Collection access**: Uses the collection's database reference to access other collections
3. **Document processing**: Processes each document to find and add matching documents

## Testing

The implementation includes comprehensive tests in `tests/test_lookup_json_each.py` covering:

1. Basic `$lookup` functionality
2. `$lookup` with preceding `$match` stage
3. Handling of empty results
4. `$lookup` followed by `$unwind` operations

All tests pass, confirming the implementation works correctly and maintains backward compatibility.

## Future Enhancements

1. **Advanced $lookup Options**: Support for `let` and `pipeline` options
2. **Performance Optimization**: More sophisticated SQL generation for complex cases
3. **Index Awareness**: Leveraging indexes for improved join performance