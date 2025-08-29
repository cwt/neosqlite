# Enhanced $unwind + $group Optimization with json_each()

## Summary

This enhancement extends the previous `$unwind` optimizations to support **`$unwind` + `$group` combinations** in aggregation pipelines, leveraging SQLite's `json_each()` function for improved performance.

## Implementation Details

### What Was Enhanced

1. **$unwind + $group Optimization**: The implementation now detects and optimizes pipelines with `$unwind` followed by `$group` stages.

2. **SQL-Level Grouping**: Instead of unwinding arrays in Python and then grouping in Python, the enhancement generates a single SQL query that does both operations:

   ```sql
   SELECT je.value AS _id, COUNT(*) AS count
   FROM collection, json_each(json_extract(collection.data, '$.tags')) as je
   GROUP BY je.value
   ORDER BY _id
   ```

3. **Pattern Detection**: The implementation detects two patterns:
   - `$unwind` + `$group` ( `$unwind` as first stage)
   - `$match` + `$unwind` + `$group` ( `$unwind` as second stage)

### Technical Approach

The enhanced implementation:

1. **Pattern Detection**: Scans the pipeline to identify `$unwind` + `$group` patterns
2. **SQL Generation**: Creates optimized SQL that combines `json_each()` with `GROUP BY`
3. **Accumulator Support**: Supports `$sum` (with value 1), `$count`, `$avg`, `$min`, `$max`, `$push`, and `$addToSet` accumulators
4. **Field Grouping**: Supports grouping by the unwound field or other document fields
5. **WHERE Clause Integration**: Incorporates `$match` filters when present

### Supported Cases

1. **`$unwind` + `$group`** (newly enhanced)
2. **`$match` + `$unwind` + `$group`** (newly enhanced)
3. **Grouping by unwound field** (e.g., `{"_id": "$tags"}`)
4. **Grouping by other fields** (e.g., `{"_id": "$category"}`)
5. **Multiple accumulator functions**: `$sum` (with value 1), `$count`, `$avg`, `$min`, `$max`, `$push`, and `$addToSet`

### Performance Benefits

- **Single SQL Query**: Eliminates intermediate Python processing
- **Database-Level Processing**: All operations happen at the SQLite level
- **Reduced Memory Usage**: No need to create intermediate unwound documents in Python
- **Faster Execution**: Leverages SQLite's optimized `GROUP BY` implementation

## Testing

Comprehensive tests were created to verify:
- Correctness of `$unwind` + `$group` results
- Performance improvements
- Compatibility with `$match` operations
- Support for different grouping fields
- Backward compatibility with existing functionality

## Example Usage

```python
# These pipelines now execute as single optimized SQL queries
pipeline1 = [
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
]

pipeline2 = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
]

result = collection.aggregate(pipeline)
```

This enhancement significantly improves performance for aggregation pipelines that involve unwinding arrays and then grouping the results, while maintaining full backward compatibility.