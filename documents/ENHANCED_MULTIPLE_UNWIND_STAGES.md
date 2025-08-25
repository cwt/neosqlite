# Enhanced Multiple $unwind Stages with json_each()

## Summary

This enhancement extends the previous `$unwind` optimization to support multiple consecutive `$unwind` stages in aggregation pipelines, leveraging SQLite's `json_each()` function for improved performance.

## Implementation Details

### What Was Enhanced

1. **Multiple Consecutive $unwind Support**: The implementation now detects and handles multiple consecutive `$unwind` stages in aggregation pipelines.

2. **Chained json_each() Processing**: For multiple `$unwind` stages, the implementation generates SQL queries that chain multiple `json_each()` calls:

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

3. **Nested json_set() Calls**: Multiple `json_set()` calls are nested to properly add each unwound field to the result documents.

### Technical Approach

The enhanced implementation:

1. **Detects Consecutive $unwind Stages**: Scans the pipeline to identify sequences of `$unwind` operations
2. **Generates Chained Queries**: Creates SQL with multiple `json_each()` calls for each array field
3. **Nests json_set() Calls**: Properly nests `json_set()` calls to add all unwound fields
4. **Maintains Compatibility**: Falls back to Python implementation for complex cases

### Supported Cases

1. **Single $unwind Stage** (previously supported)
2. **Multiple Consecutive $unwind Stages** (newly enhanced)
3. **$match followed by Multiple $unwind Stages**
4. **Two, Three, or More Consecutive $unwind Stages**

### Performance Benefits

- **Cartesian Product Efficiency**: Chained `json_each()` efficiently computes Cartesian products
- **Reduced Memory Usage**: All processing happens at database level
- **Faster Execution**: No intermediate Python data structures
- **Scalable to Larger Datasets**: Handles complex unwinding operations efficiently

## Testing

Comprehensive tests were created to verify:
- Correctness of multiple `$unwind` results
- Performance improvements
- Compatibility with `$match` operations
- Backward compatibility with existing functionality

All existing tests continue to pass, ensuring no breaking changes.

## Example Usage

```python
# Multiple $unwind stages now work efficiently
pipeline = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"},
    {"$unwind": "$categories"},
    {"$unwind": "$levels"}
]
result = collection.aggregate(pipeline)
```

This pipeline now executes as a single optimized SQL query instead of multiple Python iterations.