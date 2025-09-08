# NeoSQLite Missing APIs Implementation Roadmap

This document provides a prioritized roadmap for implementing missing PyMongo-compatible APIs and operators in NeoSQLite.

## Priority Levels

- **P0 (Critical)**: Essential for basic functionality or widely used APIs
- **P1 (High)**: Important for compatibility and commonly requested features
- **P2 (Medium)**: Useful enhancements that improve completeness
- **P3 (Low)**: Nice-to-have features for edge cases

## P0 - Critical Priority

### ~~1. Collection.drop() Method~~
- **Status**: ✅ COMPLETED
- **Why**: Essential for collection management
- **Effort**: Low
- **Impact**: High
- **Dependencies**: None

### ~~2. Connection.create_collection() Method~~
- **Status**: ✅ COMPLETED
- **Why**: Essential for explicit collection creation with options
- **Effort**: Low
- **Impact**: High
- **Dependencies**: Collection class

### ~~3. Connection.list_collection_names() Method~~
- **Status**: ✅ COMPLETED
- **Why**: Essential for database introspection
- **Effort**: Low
- **Impact**: High
- **Dependencies**: None

## P1 - High Priority

### ~~4. Complete Logical Operator Support~~
- **Status**: ✅ COMPLETED
- **Why**: Essential for complex queries
- **Effort**: Medium
- **Impact**: High
- **Dependencies**: SQL translator enhancements
- **Operators**: `$and`, `$or`, `$not`, `$nor`

### ~~5. Missing Query Operators~~
- **Status**: ✅ COMPLETED
- **Why**: Important for query completeness
- **Effort**: Medium
- **Impact**: High
- **Operators**: 
  - `$all` - Array matching
  - `$type` - Type checking

### ~~6. Connection.list_collections() Method~~
- **Status**: ✅ COMPLETED
- **Why**: Detailed database introspection
- **Effort**: Low
- **Impact**: Medium
- **Dependencies**: None

### 7. Collection.aggregate_raw_batches() Method
- **Why**: Important for performance with large aggregation results
- **Effort**: Medium
- **Impact**: Medium
- **Dependencies**: RawBatchCursor enhancements

## P2 - Medium Priority

### 8. Search Index APIs
- **Why**: Important for text search capabilities
- **Effort**: High
- **Impact**: Medium
- **APIs**:
  - `create_search_index()`
  - `create_search_indexes()`
  - `drop_search_index()`
  - `list_search_indexes()`
  - `update_search_index()`

### 9. Evaluation Operators
- **Why**: Important for advanced queries
- **Effort**: High
- **Impact**: Medium
- **Operators**:
  - `$expr` - Expression evaluation
  - `$jsonSchema` - Schema validation

### 10. Connection.validate_collection() Method
- **Why**: Useful for database maintenance
- **Effort**: Low
- **Impact**: Low
- **Dependencies**: SQLite integrity checks

### 11. Collection.with_options() Method
- **Why**: Useful for collection cloning with different options
- **Effort**: Low
- **Impact**: Low
- **Dependencies**: None

## P3 - Low Priority

### 12. Geospatial Operators
- **Why**: Specialized use case
- **Effort**: High
- **Impact**: Low
- **Operators**:
  - `$geoIntersects`
  - `$geoWithin`
  - `$near`
  - `$nearSphere`

### 13. Enhanced Utility Methods
- **Why**: Minor usability improvements
- **Effort**: Low
- **Impact**: Low
- **Methods**:
  - Enhanced `__getitem__()` and `__getattr__()`

### 14. Array Projection Operators
- **Why**: Specialized projection use cases
- **Effort**: Medium
- **Impact**: Low
- **Operators**:
  - `$slice`

## Implementation Timeline

### Phase 1 ( Weeks 1-2): P0 Items ✅ COMPLETED
- Collection.drop()
- Connection.create_collection()
- Connection.list_collection_names()

### Phase 2 ( Weeks 3-4): P1 Items - IN PROGRESS
- Connection.list_collections()
- Collection.aggregate_raw_batches()
- Initial search index APIs

### Phase 4 (Weeks 7-8): P2 Items
- Evaluation operators
- Remaining search index APIs
- Database validation methods

### Phase 5 (Weeks 9+): P2-P3 Items
- Geospatial operators (if needed)
- Enhanced utility methods
- Array projection operators

## Success Metrics

1. **API Coverage**: Achieve 95%+ PyMongo API compatibility
2. **Performance**: Maintain or improve query performance
3. **Backward Compatibility**: 100% backward compatibility with existing code
4. **Documentation**: Complete documentation for all new APIs
5. **Testing**: 100% test coverage for new implementations

## Risk Mitigation

1. **Performance Impact**: Benchmark each new feature
2. **Compatibility Issues**: Extensive testing with existing codebases
3. **Complexity**: Implement features incrementally with thorough testing
4. **Dependencies**: Minimize external dependencies

This roadmap provides a structured approach to enhancing NeoSQLite's PyMongo compatibility while maintaining its core strengths as a SQLite-based NoSQL solution.