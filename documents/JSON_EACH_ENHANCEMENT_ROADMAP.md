# NeoSQLite json_each() Enhancement Roadmap

## Overview
This document tracks the progress of enhancements to NeoSQLite that leverage SQLite's `json_each()` function for improved performance and functionality. The goal is to progressively optimize more MongoDB-style operations by pushing them down to the SQLite level.

## Completed Enhancements ✅

### 1. Basic $unwind Optimization
**Status**: ✅ Completed
- Single `$unwind` operations optimized with `json_each()`
- SQL query generation for array decomposition
- Integration with existing `$match` operations
- Performance: Significant improvement over Python-based iteration

### 2. Multiple Consecutive $unwind Stages
**Status**: ✅ Completed
- Chained `json_each()` calls for multiple `$unwind` operations
- Nested `json_set()` calls for proper field handling
- Support for 2, 3, or more consecutive `$unwind` stages
- Performance: 30,000 documents processed in 0.2 seconds

### 3. $unwind + $group Optimization
**Status**: ✅ Completed
- SQL-level optimization combining `json_each()` with `GROUP BY`
- Pattern detection for `$unwind` + `$group` combinations
- Support for `$match` + `$unwind` + `$group` pipelines
- Accumulator support: `$sum` (value 1), `$count`, `$avg`, `$min`, `$max`
- Performance: 10,000 operations in 0.0039 seconds

### 4. $unwind + $sort + $limit Optimization
**Status**: ✅ Completed
- SQL-level optimization combining `json_each()` with `ORDER BY`, `LIMIT`, and `OFFSET`
- Pattern detection for `$unwind` + `$sort` + `$limit` combinations
- Support for `$match` + `$unwind` + `$sort` + `$limit` pipelines
- Support for sorting by both unwound fields and original document fields
- Performance: Native SQLite sorting and limiting for optimized operations
- See [NESTED_ARRAY_UNWIND.md](NESTED_ARRAY_UNWIND.md) for detailed documentation

### 5. Nested Array Unwinding
**Status**: ✅ Completed
**Description**: Handle arrays of objects and deeply nested unwinding operations
**Target Use Case**:
```python
# Document structure:
{
  "name": "Alice",
  "orders": [
    {
      "items": [
        {"product": "A", "quantity": 2},
        {"product": "B", "quantity": 1}
      ]
    }
  ]
}

# Pipeline:
[
  {"$unwind": "$orders"},
  {"$unwind": "$orders.items"}
]
```
**Target SQL Pattern**:
```sql
SELECT collection.id,
       json_set(
         json_set(collection.data, '$."orders"', je1.value),
         '$."orders.items"', je2.value
       ) as data
FROM collection,
     json_each(json_extract(collection.data, '$.orders')) as je1,
     json_each(json_extract(je1.value, '$.items')) as je2
```
- See [NESTED_ARRAY_UNWIND.md](NESTED_ARRAY_UNWIND.md) for a detailed explanation.

### 6. Complex Accumulator Support
**Status**: ✅ Completed
**Description**: Extend `$unwind` + `$group` optimization to support more accumulator operations
**Implemented Accumulators**:
- `$avg`: Average calculation at SQL level
- `$min`/`$max`: Min/Max operations using SQL functions
- `$sum`: Sum operations using SQL functions
- `$count`: Count operations using SQL functions

**Example SQL Pattern**:
```sql
SELECT 
  json_extract(collection.data, '$.category') as _id,
  AVG(json_extract(collection.data, '$.price')) as avg_price,
  MIN(json_extract(collection.data, '$.price')) as min_price,
  MAX(json_extract(collection.data, '$.price')) as max_price,
  COUNT(*) as count
FROM collection, 
     json_each(json_extract(collection.data, '$.items')) as je
GROUP BY json_extract(collection.data, '$.category')
```

### 7. $lookup Operations
**Status**: ✅ Completed
**Description**: Implement join-like operations for related data across collections
**Implemented Features**:
- Basic $lookup operations between collections
- Support for $lookup followed by $unwind operations
- Integration with existing pipeline stages ($match, etc.)
- Python fallback implementation for complex cases
- SQL optimization for simple $lookup-only pipelines

**Example Usage**:
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

## Completed Enhancements ✅

### 8. Advanced $unwind Options
**Status**: ✅ Completed
**Description**: Support for additional $unwind options
**Features**:
- `includeArrayIndex`: Include the array index in the unwound documents
- `preserveNullAndEmptyArrays`: Preserve null and empty arrays in the output

**Implementation Details**:
- Supports both options individually and in combination
- Works with nested array unwinding
- Maintains backward compatibility with traditional string-based $unwind syntax
- Properly handles edge cases like null values, empty arrays, and missing fields
- See [ADVANCED_UNWIND_OPTIONS.md](ADVANCED_UNWIND_OPTIONS.md) for detailed documentation

**Example Usage**:
```python
[
  {"$unwind": {
    "path": "$scores",
    "includeArrayIndex": "scoreIndex",
    "preserveNullAndEmptyArrays": True
  }}
]
```

**Performance Notes**:
- Advanced options are implemented in Python rather than SQL for flexibility
- For large datasets, consider if advanced options are necessary or if default behavior suffices

### 9. Advanced Index-Aware Optimization
**Status**: ✅ Completed
**Description**: Leverage existing indexes in query planning for complex operations
**Features**:
- Query cost estimation based on index availability
- Automatic selection of optimal execution paths
- Integration with existing index information system
- Pipeline reordering for better performance
- Match pushdown optimization

**Implementation Details**:
- Added `_estimate_query_cost()` method to estimate query execution costs based on index availability
- Added `_get_indexed_fields()` method to identify which fields have indexes
- Added `_reorder_pipeline_for_indexes()` method to reorder pipeline stages for better index usage
- Added `_estimate_pipeline_cost()` method to estimate total pipeline costs
- Added `_optimize_match_pushdown()` method to push match filters down for early filtering
- Integrated cost estimation and optimization into the aggregation pipeline processing
- Maintains full backward compatibility with existing code

**Optimization Techniques**:
- **Pipeline Reordering**: Automatically reorders pipeline stages to put indexed `$match` operations first
- **Cost-Based Selection**: Chooses between original and reordered pipelines based on cost estimation
- **Match Pushdown**: Pushes `$match` stages earlier in the pipeline to filter data before expensive operations
- **Index-Aware Planning**: Uses index information to make better query execution decisions

**Performance Benefits**:
- Queries using indexed fields execute significantly faster due to early filtering
- Pipeline reordering reduces the amount of data processed by expensive operations
- Match pushdown optimization filters data before `$unwind`, `$group`, and `$lookup` operations
- Cost-based optimization ensures the most efficient execution path is chosen

**Example Usage**:
```python
# Create indexes on frequently queried fields
collection.create_index("category")
collection.create_index("status")

# These queries will automatically benefit from index optimization
pipeline = [
    {"$unwind": "$tags"},  # Expensive operation
    {"$match": {"category": "Category5", "status": "active"}},  // Will be moved to the front
    {"$sort": {"tags": 1}},
    {"$limit": 10}
]
result = collection.aggregate(pipeline)
# The pipeline is automatically reordered to:
# [{"$match": {...}}, {"$unwind": "$tags"}, {"$sort": {...}}, {"$limit": 10}]
```

**Testing**:
- Comprehensive test coverage for cost estimation and optimization functionality
- Integration tests with existing aggregation pipeline stages
- Performance verification tests showing significant improvements
- No regression in existing functionality

### 10. Pipeline Reordering Optimization
**Status**: ✅ Completed
**Description**: Rearrange pipeline stages for better performance when possible
**Features**:
- Query cost estimation based on pipeline stage ordering
- Automatic selection of optimal execution paths
- Integration with existing index information system
- Pipeline reordering for better performance
- Match pushdown optimization for early filtering

**Implementation Details**:
- Added `_reorder_pipeline_for_indexes()` method to reorder pipeline stages for better index usage
- Enhanced `_estimate_pipeline_cost()` method to estimate total pipeline costs with data flow awareness
- Added `_optimize_match_pushdown()` method to push match filters down for early filtering
- Integrated cost estimation and optimization into the aggregation pipeline processing
- Maintains full backward compatibility with existing code

**Optimization Techniques**:
- **Pipeline Reordering**: Automatically reorders pipeline stages to put indexed `$match` operations first
- **Cost-Based Selection**: Chooses between original and reordered pipelines based on cost estimation
- **Match Pushdown**: Pushes `$match` stages earlier in the pipeline to filter data before expensive operations
- **Index-Aware Planning**: Uses index information to make better query execution decisions

**Performance Benefits**:
- Queries using indexed fields execute significantly faster due to early filtering
- Pipeline reordering reduces the amount of data processed by expensive operations
- Match pushdown optimization filters data before `$unwind`, `$group`, and `$lookup` operations
- Cost-based optimization ensures the most efficient execution path is chosen

**Example Usage**:
```python
# Create indexes on frequently queried fields
collection.create_index("category")
collection.create_index("status")

# These queries will automatically benefit from pipeline optimization
pipeline = [
    {"$unwind": "$tags"},  // Expensive operation
    {"$match": {"category": "Category5", "status": "active"}},  // Will be moved to the front
    {"$sort": {"tags": 1}},
    {"$limit": 10}
]
result = collection.aggregate(pipeline)
// The pipeline is automatically reordered to:
// [{"$match": {...}}, {"$unwind": "$tags"}, {"$sort": {...}}, {"$limit": 10}]
```

**Testing**:
- Comprehensive test coverage for cost estimation and optimization functionality
- Integration tests with existing aggregation pipeline stages
- Performance verification tests showing significant improvements
- No regression in existing functionality
```

### 11. Additional Group Operations
**Status**: 📋 Backlog
**Description**: Support for more MongoDB-style group accumulators in SQL
**Target Accumulators**:
- `$push`: Array building at SQL level
- `$addToSet`: Unique value collection

## Future Research Opportunities 🔍

### 12. Memory-Constrained Processing
**Status**: 🔍 Research
**Description**: Handle very large datasets with memory-constrained environments
**Approach**:
- Cursor-based processing for large result sets
- Streaming results instead of loading all into memory
- Batch processing with configurable batch sizes

### 13. Text Search Integration with json_each()
**Status**: 🔍 Research
**Description**: Combine full-text search with array operations
**Use Case**:
```python
[
  {"$unwind": "$comments"},
  {"$match": {"$text": {"$search": "performance"}}},
  {"$group": {"_id": "$author", "commentCount": {"$sum": 1}}}
]
```

### 14. Recursive json_each() for Deep Nesting
**Status**: 🔍 Research
**Description**: Handle arbitrarily deep nested structures with recursive SQL
**Challenge**: SQLite's limited support for recursive CTEs with JSON

### 15. Window Function Integration
**Status**: 🔍 Research
**Description**: Leverage SQLite window functions with `json_each()` for advanced analytics
**Target Operations**:
- `$rank`, `$denseRank` with proper SQL window functions
- Running totals and moving averages
- Percentile calculations

## Performance Tracking 📊

### Current Benchmarks
| Enhancement | Operation | Dataset Size | Time | Memory Usage |
|-------------|-----------|--------------|------|--------------|
| Multiple $unwind | Cartesian Product | 1000 docs × 5×3×2 arrays | 0.2s | Database-level |
| $unwind + $group | Count Aggregation | 1000 docs × 10 tags | 0.0039s | Database-level |
| Single $unwind (baseline) | Array Decomposition | 1000 docs × 10 items | 0.015s | Database-level |
| $unwind + $sort + $limit | Sorted Limiting | 1000 docs × 10 tags, limit 5 | 0.008s | Database-level |

### Target Improvements
| Enhancement | Target Performance Goal |
|-------------|------------------------|
| $unwind + $sort + $limit | 50% faster than Python implementation |
| Nested Array Unwinding | Handle 3+ levels of nesting efficiently |
| Complex Accumulators | Support all major MongoDB accumulators |
| $lookup Operations | Join performance comparable to SQL joins |

## Implementation Priority

### Completed
1. Advanced $unwind Options
2. Additional Group Operations

### Medium Priority
3. Advanced Index-Aware Optimization
4. Pipeline Reordering

### Low Priority
5. Memory-Constrained Processing

## Testing Strategy

### Current Coverage
- **Comprehensive Tests**: Extensive test coverage for all implemented features
- **Code Coverage**: Maintains ≥85% code coverage
- **Edge Cases**: Comprehensive testing of error conditions and fallbacks

### Additional Testing Needed
1. **Performance Regression Tests**: Ensure new optimizations don't slow down existing operations
2. **Edge Case Coverage**: More tests for unusual data structures and error conditions
3. **Integration Tests**: End-to-end tests combining multiple optimizations
4. **Memory Usage Tests**: Verify memory efficiency of database-level processing
5. **Concurrency Tests**: Ensure thread safety of optimized operations
6. **Sort Direction Tests**: Verify ascending and descending sort operations work correctly

## Documentation Updates Required

1. **API Documentation**: Update for new optimized operations
2. **Performance Guide**: Document when optimizations apply and expected benefits
3. **Migration Guide**: Help users understand how to leverage new capabilities
4. **Examples**: New examples showing optimized pipeline usage

## Risk Assessment

### Technical Risks
- **Complexity**: Increasing SQL generation complexity may introduce bugs
- **Compatibility**: Maintaining backward compatibility with Python fallback
- **Edge Cases**: Unusual data structures may not optimize well

### Mitigation Strategies
- **Comprehensive Testing**: Extensive test coverage for all new features
- **Gradual Rollout**: Implement optimizations incrementally
- **Fallback Mechanisms**: Always provide Python implementation as backup
- **Performance Monitoring**: Continuous benchmarking to detect regressions

## Success Metrics

### Quantitative Metrics
- **Test Coverage**: Maintain ≥85% code coverage
- **Performance**: ≥2x improvement for optimized operations
- **Memory Efficiency**: Reduce Python memory usage by ≥50%
- **Scalability**: Handle 10x larger datasets without performance degradation
- **Query Optimization**: Handle 90% of common aggregation pipelines at SQL level

### Qualitative Metrics
- **API Compatibility**: Maintain 100% PyMongo API compatibility
- **Developer Experience**: Simplified usage of complex operations
- **Documentation Quality**: Clear guidance on optimization benefits
- **Community Adoption**: Positive feedback on performance improvements

## Next Steps

### Short-term Goals
1. Add support for advanced $unwind options
2. Implement additional group operations ($push, $addToSet)

### Long-term Vision
1. Achieve ≥90% code coverage
2. Handle 95% of common aggregation pipelines at SQL level
3. Provide 5-10x performance improvements for optimized operations
4. Become the go-to solution for MongoDB-style operations on SQLite