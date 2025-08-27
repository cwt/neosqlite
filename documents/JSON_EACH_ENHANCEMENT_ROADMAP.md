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
- Accumulator support: `$sum` (value 1) and `$count`
- Performance: 10,000 operations in 0.0039 seconds

## Completed Enhancements ✅

### 4. $unwind + $sort + $limit Optimization
**Status**: ✅ Completed
- SQL-level optimization combining `json_each()` with `ORDER BY`, `LIMIT`, and `OFFSET`
- Pattern detection for `$unwind` + `$sort` + `$limit` combinations
- Support for `$match` + `$unwind` + `$sort` + `$limit` pipelines
- Support for sorting by both unwound fields and original document fields
- Performance: Native SQLite sorting and limiting for optimized operations
- See [UNWIND_SORT_LIMIT_ENHANCEMENT.md](UNWIND_SORT_LIMIT_ENHANCEMENT.md) for detailed documentation

### 5. Nested Array Unwinding
**Status**: 🔄 Research
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

## Planned Enhancements 📋

### 6. Complex Accumulator Support
**Status**: 📋 Backlog
**Description**: Extend `$unwind` + `$group` optimization to support more accumulator operations
**Target Accumulators**:
- `$avg`: Average calculation at SQL level
- `$min`/`$max`: Min/Max operations using SQL functions
- `$push`: Array building at SQL level (using `group_concat` or similar)
- `$addToSet`: Unique value collection

**Target SQL Pattern**:
```sql
SELECT 
  json_extract(collection.data, '$.category') as _id,
  AVG(json_extract(collection.data, '$.price')) as avg_price,
  MIN(json_extract(collection.data, '$.price')) as min_price,
  MAX(json_extract(collection.data, '$.price')) as max_price
FROM collection, 
     json_each(json_extract(collection.data, '$.items')) as je
GROUP BY json_extract(collection.data, '$.category')
```

### 7. $lookup Operations with json_each()
**Status**: 📋 Backlog
**Description**: Optimize join-like operations using `json_each()` for array field matching
**Target Use Case**:
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

### 8. Advanced Index-Aware Optimization
**Status**: 📋 Backlog
**Description**: Leverage existing indexes in query planning for complex operations
**Features**:
- Query cost estimation based on index availability
- Automatic selection of optimal execution paths
- Integration with existing index information system

### 9. Pipeline Reordering Optimization
**Status**: 📋 Backlog
**Description**: Rearrange pipeline stages for better performance when possible
**Example**:
```python
# Input pipeline:
[
  {"$unwind": "$tags"},
  {"$match": {"status": "active"}},
  {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
]

# Optimized pipeline (reordered):
[
  {"$match": {"status": "active"}},  # Push filter down
  {"$unwind": "$tags"},
  {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
]
```

## Future Research Opportunities 🔍

### 10. Memory-Constrained Processing
**Status**: 🔍 Research
**Description**: Handle very large datasets with memory-constrained environments
**Approach**:
- Cursor-based processing for large result sets
- Streaming results instead of loading all into memory
- Batch processing with configurable batch sizes

### 11. Text Search Integration with json_each()
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

### 12. Recursive json_each() for Deep Nesting
**Status**: 🔍 Research
**Description**: Handle arbitrarily deep nested structures with recursive SQL
**Challenge**: SQLite's limited support for recursive CTEs with JSON

### 13. Window Function Integration
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

### High Priority (Next)
1. Nested Array Unwinding

### Medium Priority
2. Complex Accumulator Support
3. $lookup Operations with json_each()

### Low Priority
4. Advanced Index-Aware Optimization
5. Pipeline Reordering
6. Memory-Constrained Processing

## Testing Strategy

### Current Coverage
- **Total Tests**: 430 tests passing
- **Code Coverage**: 85.32%
- **Edge Cases**: Comprehensive testing of error conditions

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
- **Query Optimization**: Handle 80% of common aggregation pipelines at SQL level

### Qualitative Metrics
- **API Compatibility**: Maintain 100% PyMongo API compatibility
- **Developer Experience**: Simplified usage of complex operations
- **Documentation Quality**: Clear guidance on optimization benefits
- **Community Adoption**: Positive feedback on performance improvements

## Next Steps

### Immediate Actions (Tomorrow)
1. Implement `$unwind` + `$sort` + `$limit` optimization
2. Create test cases for the new optimization
3. Benchmark performance improvements
4. Update documentation with examples

### Short-term Goals (Next Week)
1. Complete nested array unwinding implementation
2. Add complex accumulator support
3. Implement comprehensive performance testing
4. Create migration guide for users

### Long-term Vision
1. Achieve ≥90% code coverage
2. Handle 95% of common aggregation pipelines at SQL level
3. Provide 5-10x performance improvements for optimized operations
4. Become the go-to solution for MongoDB-style operations on SQLite