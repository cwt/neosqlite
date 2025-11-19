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

### ~~7. Collection.aggregate_raw_batches() Method~~
- **Status**: ✅ COMPLETED
- **Why**: Important for performance with large aggregation results
- **Effort**: Medium
- **Impact**: Medium
- **Dependencies**: RawBatchCursor enhancements

### ~~8. Enhanced JSON Functions Integration~~
- **Status**: ✅ PHASE 2 COMPLETED
- **Why**: Significant performance improvements through native JSON functions
- **Effort**: High
- **Impact**: High
- **Dependencies**: SQLite JSON1 extension support
- **Features**:
  - Enhanced update operations with `json_insert()` and `json_replace()`
  - JSONB function support with fallback to `json_*` functions
  - Enhanced aggregation with `json_group_array()` and `json_group_object()`

### ~~9. ObjectId Support~~
- **Status**: ✅ COMPLETED
- **Why**: Essential for MongoDB compatibility and document identification
- **Effort**: Medium
- **Impact**: High
- **Dependencies**: Collection schema modifications, JSON serialization updates
- **Features**:
  - MongoDB-compatible 12-byte ObjectId implementation
  - Automatic generation when no _id provided
  - Dedicated _id column with unique indexing for performance
  - Full hex interchangeability with PyMongo ObjectIds
  - Backward compatibility with existing collections
  - JSON serialization and deserialization support
  - Thread-safe implementation with proper locking

## P2 - Medium Priority

### ~~9. Evaluation Operators~~
- **Status**: ❌ NOT IMPLEMENTED
- **Why**: Important for advanced queries
- **Effort**: High
- **Impact**: Medium
- **Operators**:
  - `$expr` - Expression evaluation
  - `$jsonSchema` - Schema validation

### ~~10. Connection.validate_collection() Method~~
- **Status**: ❌ NOT IMPLEMENTED
- **Why**: Useful for database maintenance
- **Effort**: Low
- **Impact**: Low
- **Dependencies**: SQLite integrity checks

### ~~11. Collection.with_options() Method~~
- **Status**: ❌ NOT IMPLEMENTED
- **Why**: Useful for collection cloning with different options
- **Effort**: Low
- **Impact**: Low
- **Dependencies**: None

## P3 - Low Priority

### 11. Geospatial Operators
- **Why**: Specialized use case
- **Effort**: High
- **Impact**: Low
- **Operators**:
  - `$geoIntersects`
  - `$geoWithin`
  - `$near`
  - `$nearSphere`

### 12. Enhanced Utility Methods
- **Why**: Minor usability improvements
- **Effort**: Low
- **Impact**: Low
- **Methods**:
  - Enhanced `__getitem__()` and `__getattr__()`

### 13. Array Projection Operators
- **Why**: Specialized projection use cases
- **Effort**: Medium
- **Impact**: Low
- **Operators**:
  - `$slice` in projections - ❌ NOT IMPLEMENTED

### 14. Highly Feasible Aggregation Pipeline Features
- **Why**: Based on feasibility analysis of SQLite capabilities
- **Effort**: High
- **Impact**: High
- **Features**:
  - `$bucket` and `$bucketAuto` - ❌ NOT IMPLEMENTED - Can be implemented using SQL CASE statements and range functions
  - `$facet` - ❌ NOT IMPLEMENTED - Can be implemented by running multiple concurrent queries and combining results
  - `$out` and `$merge` - ❌ NOT IMPLEMENTED - Can be implemented using SQL INSERT/UPDATE statements to write aggregation results to other tables/collections
  - `$addFields` improvements - ✅ COMPLETED - Already enhanced with complex expressions

### 15. Highly Feasible Advanced Query Operators
- **Why**: Based on feasibility analysis of SQLite capabilities
- **Effort**: Medium
- **Impact**: High
- **Features**:
  - `$elemMatch` in projections - ❌ NOT IMPLEMENTED - Can be implemented using JSON path functions (Note: regular $elemMatch is implemented but not in projections)
  - `$all` operator improvements - ✅ COMPLETED - Already enhanced using JSON array functions and existence checks

### 16. Performance and Monitoring Features
- **Why**: Critical for query optimization and debugging
- **Effort**: Low
- **Impact**: High
- **Features**:
  - Explain plan functionality - ❌ NOT IMPLEMENTED - SQLite has built-in EXPLAIN QUERY PLAN functionality that can be exposed (though used internally for testing)
  - Collection statistics - ❌ NOT IMPLEMENTED - SQLite provides table statistics via PRAGMA commands
  - Index usage statistics - ❌ NOT IMPLEMENTED - SQLite provides index usage information

### 17. Session and Transaction Management
- **Why**: For consistency and reliability control
- **Effort**: Medium
- **Impact**: Medium
- **Features**:
  - Client sessions - ❌ NOT IMPLEMENTED - Can be implemented at the application level
  - Read/write concerns - ❌ NOT IMPLEMENTED - Can be implemented as configuration options
  - Retryable writes - ❌ NOT IMPLEMENTED - Can be implemented with exception handling and retry logic

### 18. Advanced Indexing Features
- **Why**: For performance optimization and specific use cases
- **Effort**: Medium
- **Impact**: Medium
- **Features**:
  - Partial indexes - ✅ COMPLETED - SQLite supports partial indexes (WHERE clause in CREATE INDEX), already implemented
  - TTL indexes simulation - ❌ NOT IMPLEMENTED - Can be implemented using triggers and background cleanup tasks
  - Advanced text index options - ✅ COMPLETED - SQLite FTS5 already provides good text search, with additional options implemented

### 19. Connection Management Features
- **Why**: For better performance in multi-threaded applications
- **Effort**: Medium
- **Impact**: Medium
- **Features**:
  - Connection pooling - ❌ NOT IMPLEMENTED - Can be implemented at the application level using connection pools
  - URI parsing - ❌ NOT IMPLEMENTED - Standard Python functionality
  - Timeout handling - ❌ NOT IMPLEMENTED - SQLite supports timeout configuration

## Not Feasible Due to SQLite Architecture

### Distributed/Multi-node Features (Not Applicable)
- **Why**: SQLite is single-file, not distributed
- **Features**:
  - Replica set support
  - Read preferences
  - Advanced security features (no user management in SQLite)

### Complex Operations (Not Applicable)
- **Why**: Would require external processing or JavaScript engine
- **Features**:
  - `$graphLookup` - Limited recursive query capabilities in SQLite
  - `$where` with JavaScript - SQLite doesn't have JavaScript engine
  - MapReduce - Would require external processing framework
  - Geospatial indexes (without spatialite extension)

### Server-level Features (Not Applicable)
- **Why**: SQLite is embedded, no server process
- **Features**:
  - Server status monitoring
  - Distributed transactions