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
  def _all(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
      """Check if array field contains all specified elements."""
      doc_value = _get_nested_field(field, document)
      if not isinstance(doc_value, list):
          return False
      return all(item in doc_value for item in value)
  ```
- **Dependencies**: None
- **Testing**: Array matching scenarios

#### ~~`$type` Element Operator~~
- **Status**: ✅ COMPLETED
- **Purpose**: Select documents based on field type
- **Location**: `neosqlite/query_operators.py`
- **Implementation**:
  ```python
  def _type(field: str, value: Any, document: Dict[str, Any]) -> bool:
      """Check if field is of specified type."""
      doc_value = _get_nested_field(field, document)
      type_mapping = {
          1: float, 2: str, 3: dict, 4: list, 8: bool, 10: type(None), 16: int, 18: int, 19: int
      }
      expected_type = type_mapping.get(value) if isinstance(value, int) else value
      return isinstance(doc_value, expected_type)
  ```
- **Dependencies**: Type mapping definitions
- **Testing**: Type checking scenarios

## Medium Priority Implementations

### Advanced Aggregation

#### `aggregate_raw_batches()`
- **Purpose**: Perform aggregation and retrieve raw BSON batches
- **Location**: `neosqlite/collection/__init__.py`
- **Implementation**:
  ```python
  def aggregate_raw_batches(self, pipeline: List[Dict[str, Any]]) -> RawBatchCursor:
      """Perform aggregation and retrieve raw batches."""
      # Similar to find_raw_batches but for aggregation
      return RawBatchCursor(self, pipeline=pipeline, is_aggregation=True)
  ```
- **Dependencies**: RawBatchCursor enhancements
- **Testing**: Raw batch processing

### Index Management (Atlas-like features)

#### Search Index APIs
- **Purpose**: Provide search index functionality (FTS5-based)
- **Location**: `neosqlite/collection/index_manager.py`
- **Implementation**:
  - `create_search_index()` - Create FTS5-based search index
  - `create_search_indexes()` - Create multiple search indexes
  - `drop_search_index()` - Drop search index
  - `list_search_indexes()` - List search indexes
  - `update_search_index()` - Update search index
- **Dependencies**: FTS5 support
- **Testing**: Search index operations

### Database-level Operations

#### `validate_collection()`
- **Purpose**: Validate collection integrity
- **Location**: `neosqlite/connection.py`
- **Implementation**:
  ```python
  def validate_collection(self, name):
      """Validate collection integrity."""
      try:
          cursor = self.db.execute(f"PRAGMA integrity_check({name})")
          result = cursor.fetchone()
          return {"valid": result[0] == "ok", "errors": [] if result[0] == "ok" else [result[0]]}
      except Exception as e:
          return {"valid": False, "errors": [str(e)]}
  ```
- **Dependencies**: SQLite integrity check
- **Testing**: Collection validation scenarios

### Evaluation Operators

#### `$expr`
- **Purpose**: Allow aggregation expressions in queries
- **Location**: `neosqlite/query_operators.py`
- **Implementation**: Complex aggregation expression parser
- **Dependencies**: Aggregation framework
- **Testing**: Expression evaluation scenarios

#### `$jsonSchema`
- **Purpose**: Validate documents against JSON Schema
- **Location**: `neosqlite/query_operators.py`
- **Implementation**: JSON schema validation integration
- **Dependencies**: JSON schema library
- **Testing**: Schema validation scenarios

## Lower Priority Implementations

### Utility Methods

#### Enhanced `__getitem__` and `__getattr__`
- **Purpose**: Better sub-collection support
- **Location**: `neosqlite/connection.py`
- **Implementation**: Enhanced attribute access patterns
- **Dependencies**: None
- **Testing**: Sub-collection access

#### `with_options()`
- **Purpose**: Clone collection with different options
- **Location**: `neosqlite/collection/__init__.py`
- **Implementation**:
  ```python
  def with_options(self, **kwargs):
      """Get a clone with different options."""
      # Create new collection instance with modified options
      return Collection(self.db, self.name, **kwargs)
  ```
- **Dependencies**: Collection options system
- **Testing**: Option inheritance scenarios

### Geospatial Operators

#### `$geoIntersects`, `$geoWithin`, `$near`, `$nearSphere`
- **Purpose**: Geospatial query support
- **Location**: `neosqlite/query_operators.py`
- **Implementation**: Geospatial functions using SQLite extensions
- **Dependencies**: Spatialite or custom geospatial functions
- **Testing**: Geospatial query scenarios

### Array Projection Operators

#### `$slice`
- **Purpose**: Control array element projection
- **Location**: `neosqlite/collection/query_helper.py`
- **Implementation**: Array slicing in projection logic
- **Dependencies**: Projection system
- **Testing**: Array slicing scenarios

## Implementation Strategy

### Phase 1: Core Missing APIs (High Priority)
1. Implement `drop()` method in Collection class
2. Add collection management methods to Connection class
3. Complete logical operator support in query system
4. Implement missing query operators (`$all`, `$type`)

### Phase 2: Enhanced Functionality (Medium Priority)
1. Add `aggregate_raw_batches()` method
2. Implement search index APIs
3. Add database validation methods
4. Implement evaluation operators

### Phase 3: Advanced Features (Lower Priority)
1. Enhance utility methods
2. Add geospatial operator support
3. Implement array projection operators

### Testing Approach
1. Unit tests for each new API/operator
2. Integration tests with existing functionality
3. Performance benchmarks for new features
4. Backward compatibility verification

### Code Quality Considerations
1. Maintain PyMongo API compatibility
2. Follow existing code style and patterns
3. Ensure proper error handling and edge cases
4. Add comprehensive documentation
5. Maintain 100% test coverage for new code

### Dependencies and Considerations
1. Some features may require additional dependencies (e.g., JSON schema validation)
2. Geospatial features may require Spatialite extension
3. Performance impact of new operators should be evaluated
4. Memory usage for batch operations should be optimized

This implementation plan provides a roadmap for enhancing NeoSQLite's compatibility with PyMongo while maintaining its SQLite-specific advantages.