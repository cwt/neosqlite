# NeoSQLite Missing APIs and Operators Implementation Plan

This document outlines a comprehensive plan for implementing missing PyMongo-compatible APIs and operators in NeoSQLite to improve compatibility and feature completeness.

## Table of Contents
1. [High Priority Implementations](#high-priority-implementations)
2. [Medium Priority Implementations](#medium-priority-implementations)
3. [Lower Priority Implementations](#lower-priority-implementations)
4. [Implementation Strategy](#implementation-strategy)

## High Priority Implementations

### ~~Collection Management APIs~~

#### ~~`drop()`~~
- **Status**: ✅ COMPLETED
- **Purpose**: Drop the entire collection (table in SQLite)
- **Location**: `neosqlite/collection/__init__.py`
- **Implementation**:
  ```python
  def drop(self):
      """Drop the entire collection."""
      self.db.execute(f"DROP TABLE IF EXISTS {self.name}")
  ```
- **Dependencies**: None
- **Testing**: Verify table is removed from database

#### ~~`create_collection()` (Connection level)~~
- **Status**: ✅ COMPLETED
- **Purpose**: Create a new collection with specific options
- **Location**: `neosqlite/connection.py`
- **Implementation**:
  ```python
  def create_collection(self, name, **kwargs):
      """Create a new collection with specific options."""
      if name in self._collections:
          raise CollectionInvalid(f"Collection {name} already exists")
      collection = Collection(self.db, name, create=True, database=self, **kwargs)
      self._collections[name] = collection
      return collection
  ```
- **Dependencies**: Collection class enhancements
- **Testing**: Verify collection creation with options

#### ~~`list_collection_names()` (Connection level)~~
- **Status**: ✅ COMPLETED
- **Purpose**: List all collection names in the database
- **Location**: `neosqlite/connection.py`
- **Implementation**:
  ```python
  def list_collection_names(self):
      """List all collection names in the database."""
      cursor = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
      return [row[0] for row in cursor.fetchall()]
  ```
- **Dependencies**: None
- **Testing**: Verify correct listing of tables

#### ~~`list_collections()` (Connection level)~~
- **Status**: ✅ COMPLETED
- **Purpose**: Get detailed information about collections
- **Location**: `neosqlite/connection.py`
- **Implementation**:
  ```python
  def list_collections(self):
      """Get detailed information about collections."""
      cursor = self.db.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
      return [{"name": row[0], "options": row[1]} for row in cursor.fetchall()]
  ```
- **Dependencies**: None
- **Testing**: Verify detailed collection information

### ~~Query Operators~~

#### ~~`$and`, `$or`, `$not`, `$nor` Logical Operators~~
- **Status**: ✅ COMPLETED
- **Purpose**: Complete logical operator support
- **Location**: `neosqlite/query_operators.py` and `neosqlite/collection/sql_translator_unified.py`
- **Implementation**:
  - Enhance SQL translator to fully support these operators
  - Add Python fallback implementations in query_operators.py
- **Dependencies**: Existing logical operator partial implementation
- **Testing**: Complex nested logical queries

#### ~~`$all` Array Operator~~
- **Status**: ✅ COMPLETED
- **Purpose**: Matches arrays that contain all elements specified
- **Location**: `neosqlite/query_operators.py`
- **Implementation**:
  ```python
  def _all(field, value):
      """Matches arrays that contain all elements specified."""
      # Implementation using json_array_length and json_extract
      pass
  ```
- **Dependencies**: JSON array length functions
- **Testing**: Arrays with all specified elements, partial matches, empty arrays

#### ~~`$type` Element Operator~~
- **Status**: ✅ COMPLETED
- **Purpose**: Selects documents based on field type
- **Location**: `neosqlite/query_operators.py`
- **Implementation**:
  ```python
  def _type(field, value):
      """Selects documents based on field type."""
      # Implementation using json_type function
      pass
  ```
- **Dependencies**: JSON type checking functions
- **Testing**: Different field types (string, number, array, object, null, boolean)

### ~~Advanced Aggregation~~

#### ~~`aggregate_raw_batches()`~~
- **Status**: ✅ COMPLETED
- **Purpose**: Perform aggregation and retrieve raw BSON batches
- **Location**: `neosqlite/collection/__init__.py`
- **Implementation**:
  - Add method to Collection class
  - Implement RawBatchCursor for batch retrieval
- **Dependencies**: Raw batch cursor implementation
- **Testing**: Large result sets, batch size configuration, memory usage

### ~~Search Index APIs~~

#### ~~`create_search_index()`~~
- **Status**: ✅ COMPLETED
- **Purpose**: Create a single search index
- **Location**: `neosqlite/collection/index_manager.py`
- **Implementation**:
  ```python
  def create_search_index(self, key, tokenizer=None):
      """Create a search index on the specified key."""
      # Implementation using FTS5
      pass
  ```
- **Dependencies**: FTS5 support
- **Testing**: Single field search indexes, tokenizer configuration

#### ~~`create_search_indexes()`~~
- **Status**: ✅ COMPLETED
- **Purpose**: Create multiple search indexes at once
- **Location**: `neosqlite/collection/index_manager.py`
- **Implementation**:
  ```python
  def create_search_indexes(self, indexes):
      """Create multiple search indexes."""
      # Implementation for batch search index creation
      pass
  ```
- **Dependencies**: Individual search index creation
- **Testing**: Multiple search indexes, error handling

#### ~~`drop_search_index()`~~
- **Status**: ✅ COMPLETED
- **Purpose**: Drop a search index
- **Location**: `neosqlite/collection/index_manager.py`
- **Implementation**:
  ```python
  def drop_search_index(self, index):
      """Drop the specified search index."""
      # Implementation using DROP TABLE
      pass
  ```
- **Dependencies**: None
- **Testing**: Index dropping, non-existent index handling

#### ~~`list_search_indexes()`~~
- **Status**: ✅ COMPLETED
- **Purpose**: List search indexes
- **Location**: `neosqlite/collection/index_manager.py`
- **Implementation**:
  ```python
  def list_search_indexes(self):
      """List all search indexes."""
      # Implementation querying SQLite master table
      pass
  ```
- **Dependencies**: None
- **Testing**: Empty index list, multiple indexes

### ~~Enhanced JSON Functions Integration~~

#### ~~Enhanced Update Operations~~
- **Status**: ✅ PHASE 2 COMPLETED
- **Purpose**: Leverage `json_insert()` and `json_replace()` for more efficient update operations
- **Location**: `neosqlite/collection/query_helper.py`
- **Implementation**:
  - Add `json_insert()` support for `$setOnInsert` operations
  - Add `json_replace()` support for `$replace` operations
  - Add proper fallback to existing implementations
- **Dependencies**: SQLite JSON1 extension support
- **Testing**: Update operations, fallback scenarios, performance comparison

#### ~~JSONB Function Support~~
- **Status**: ✅ PHASE 2 COMPLETED
- **Purpose**: Expand usage of `jsonb_*` functions for better performance when available
- **Location**: `neosqlite/collection/query_helper.py`
- **Implementation**:
  - Add function selection logic to use `jsonb_*` when available
  - Add fallback to `json_*` functions for older SQLite versions
- **Dependencies**: SQLite JSONB support detection
- **Testing**: JSONB vs JSON performance, fallback behavior

#### ~~Enhanced Aggregation~~
- **Status**: ✅ PHASE 2 COMPLETED
- **Purpose**: Leverage existing `json_group_array()` usage for better aggregation performance
- **Location**: `neosqlite/collection/query_helper.py`
- **Implementation**:
  - Optimize `$push` and `$addToSet` operations with `json_group_array()`
  - Optimize `$group` operations with `json_group_object()`
- **Dependencies**: SQLite JSON1 extension support
- **Testing**: Aggregation performance, correctness verification

## Medium Priority Implementations

### 8. Evaluation Operators

#### `$expr`
- **Purpose**: Allows use of aggregation expressions within query language
- **Location**: `neosqlite/query_operators.py`
- **Implementation**:
  ```python
  def _expr(field, value):
      """Allows use of aggregation expressions within query language."""
      # Implementation using aggregation expression evaluation
      pass
  ```
- **Dependencies**: Aggregation expression parser
- **Testing**: Expression evaluation, complex expressions, performance

#### `$jsonSchema`
- **Purpose**: Validate documents against a JSON Schema
- **Location**: `neosqlite/query_operators.py`
- **Implementation**:
  ```python
  def _jsonSchema(field, value):
      """Validate documents against a JSON Schema."""
      # Implementation using JSON schema validation
      pass
  ```
- **Dependencies**: JSON schema validation library
- **Testing**: Schema validation, invalid documents, performance

### 9. Connection Methods

#### `validate_collection()`
- **Purpose**: Validates collection integrity
- **Location**: `neosqlite/connection.py`
- **Implementation**:
  ```python
  def validate_collection(self, name):
      """Validates collection integrity."""
      # Implementation using SQLite integrity checks
      pass
  ```
- **Dependencies**: SQLite integrity check functions
- **Testing**: Valid collections, corrupted collections, error handling

### 10. Collection Methods

#### `with_options()`
- **Purpose**: Get a clone with different options
- **Location**: `neosqlite/collection/__init__.py`
- **Implementation**:
  ```python
  def with_options(self, **kwargs):
      """Get a clone with different options."""
      # Implementation for collection cloning with options
      pass
  ```
- **Dependencies**: None
- **Testing**: Option inheritance, method chaining, resource management

## Lower Priority Implementations

### 11. Additional Query Operators

#### `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`
- **Purpose**: Bitwise query operators
- **Location**: `neosqlite/query_operators.py`
- **Implementation**: Bitwise operations on numeric fields
- **Dependencies**: Bitwise operation support
- **Testing**: Various bit patterns, edge cases

### 12. Geospatial Operators

#### `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`
- **Purpose**: Geospatial query operators
- **Location**: `neosqlite/query_operators.py`
- **Implementation**: Geospatial calculations and indexing
- **Dependencies**: Geospatial libraries, spatial indexing
- **Testing**: Geospatial queries, performance with large datasets

## Implementation Strategy

### Development Approach

1. **Incremental Implementation**: Implement features in small, testable increments
2. **Backward Compatibility**: Maintain 100% backward compatibility at each step
3. **Comprehensive Testing**: Create thorough test suites for each feature
4. **Performance Benchmarking**: Measure performance before and after each change
5. **Documentation**: Document all new functionality with clear examples

### Risk Mitigation

1. **Modular Design**: Implement features with clear separation of concerns
2. **Error Handling**: Implement comprehensive error handling for edge cases
3. **Fallback Mechanisms**: Provide graceful fallback for unsupported operations
4. **Code Reviews**: Conduct thorough code reviews for quality assurance
5. **Continuous Integration**: Maintain existing CI pipeline with all tests passing

### Quality Standards

1. **Code Style**: Follow existing code patterns and conventions
2. **Test Coverage**: Maintain 100% test coverage for new functionality
3. **Performance**: Ensure new features meet performance targets
4. **Documentation**: Provide clear documentation for all new APIs
5. **Compatibility**: Maintain full PyMongo API compatibility

## Testing Strategy

### Unit Testing

1. **Individual Feature Tests**: Test each new API/operator in isolation
2. **Edge Case Tests**: Test boundary conditions and error scenarios
3. **Performance Tests**: Benchmark performance improvements
4. **Compatibility Tests**: Verify backward compatibility with existing code

### Integration Testing

1. **Combined Feature Tests**: Test new features working together
2. **Real-world Scenario Tests**: Test common usage patterns
3. **Migration Tests**: Verify smooth migration from existing implementations

### Performance Testing

1. **Baseline Measurements**: Establish performance baselines before implementation
2. **Improvement Verification**: Verify performance improvements meet targets
3. **Regression Testing**: Ensure no performance regressions in existing code

## Success Metrics

### Functional Completeness

- **API Coverage**: 95%+ of PyMongo Collection APIs implemented
- **Operator Coverage**: 90%+ of MongoDB query operators supported
- **Feature Parity**: Equivalent functionality for all implemented APIs

### Performance Targets

- **Speed Improvements**: 2-50x faster for JSON operations depending on use case
- **Memory Efficiency**: 50%+ reduction in Python memory usage for JSON-heavy operations
- **SQL Processing**: 95%+ of JSON operations processed at SQL level

### Quality Standards

- **Test Coverage**: 100% test coverage for new functionality
- **Code Quality**: Maintain existing code style and patterns
- **Documentation**: Comprehensive documentation for all new features
- **Reliability**: Zero regressions in existing functionality

## Timeline and Milestones

### Phase 1: Enhanced JSON Path Support and Validation (Current Focus) ✅
- **Enhanced JSON path parsing** with array indexing support
- **JSON validation** using `json_valid()` and `json_error_position()`
- **Comprehensive test coverage** for new functionality
- **Performance benchmarking** for new features

### Phase 2: Advanced JSON Operations (Completed) ✅
- **Enhanced update operations** with `json_insert()` and `json_replace()`
- **JSONB function support** with fallback to `json_*` functions
- **Enhanced aggregation** with `json_group_array()` and `json_group_object()`

### Phase 3: Feature Completeness
- **Evaluation operators** (`$expr`, `$jsonSchema`)
- **Additional query operators** (`$bitsAllClear`, etc.)
- **Geospatial operators** (`$geoWithin`, etc.)
- **Advanced indexing features**

## Dependencies and Prerequisites

### External Dependencies

1. **SQLite Version**: Minimum SQLite 3.9.0 for JSON1 support
2. **Python Version**: Compatible with current Python versions
3. **Optional Libraries**: JSON schema validation, geospatial libraries

### Internal Dependencies

1. **Existing Architecture**: Three-tier processing approach
2. **Force Fallback Mechanism**: Benchmarking and debugging support
3. **Test Infrastructure**: Existing test suite and CI pipeline

### Compatibility Requirements

1. **PyMongo API Compatibility**: Maintain full PyMongo API compatibility
2. **SQLite Version Compatibility**: Graceful degradation for older SQLite versions
3. **Python Version Compatibility**: Support for current Python versions