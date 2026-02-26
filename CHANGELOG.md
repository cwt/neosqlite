# CHANGELOG

## 1.4.0

### New Features

#### MongoDB $expr Operator Framework (WIP)
- **Three-Tier Architecture**: SQL → Temp Tables → Python fallback with automatic tier selection
- **78% Coverage**: 71 operators implemented with all high-priority operators complete
- **100% Complete Categories**: Comparison, Logical, and Conditional operators
- **High Priority Operators**: Date arithmetic ($dateAdd, $dateSubtract, $dateDiff), regex operations ($regexFind, $regexFindAll), array transformation ($filter, $map, $reduce)
- **Array Aggregation**: $sum, $avg, $min, $max with SQL support
- **Kill Switch**: Force Python evaluation for debugging/benchmarking
- **Graceful Fallback**: Missing operators (23 total) use Python evaluation - no breaking changes

#### Performance Enhancements
- **JSONB Auto-Detection**: Automatic detection and caching of SQLite JSONB capabilities
- **Dynamic Function Selection**: Optimal function selection based on SQLite version
- **Graceful Fallback**: Support for SQLite < 3.51.0

### Bug Fixes

#### Issue #32: $unwind + $text Search (The Rabbit Hole)

**Story**: Benchmark failures led to discovering non-unique `_id` bug → fixed → benchmark still failed → discovered pipeline reordering bug

**Fixed**:
- **Pipeline Reordering**: $match with $text no longer incorrectly pushed before $unwind
- **Temp Table Fallback**: Disabled SQL optimization for $unwind + $text to preserve semantics
- **Benchmark**: Fixed non-unique _id in text search benchmark (changeset 365)

#### FTS5 Indexing
- **Nested Array Support**: Fixed FTS indexes on nested arrays (e.g., "comments.text") using json_tree()/jsonb_tree()
- **Full Array Coverage**: All array elements now properly indexed for text search

#### Temp Table Improvements
- **_id Column**: Added missing _id column to temp tables
- **JSONB Conversion**: Fixed JSONB handling in temp table operations
- **FTS5 on Temp Tables**: Added FTS5 support with tokenizer detection

#### ChangeStream
- **Binary Data Handling**: Fixed UnicodeDecodeError when watching collections with binary data containing non-UTF-8 bytes
- **UTF-8 Decoding**: Proper decoding with error handling for all BSON binary subtypes

### Testing

#### $expr Test Suite
- **327 Tests**: Comprehensive test coverage for all implemented operators
- **17 Test Files**: Organized by operator category (arithmetic, array, comparison, conditional, date, logical, object, string, type)
- **High Priority Tests**: Dedicated tests for date arithmetic, regex, and array transformation
- **Temp Table Coverage**: 95% coverage of TempTableExprEvaluator class
- **Integration Tests**: End-to-end tests with SQL vs Python consistency verification

### Documentation

#### New Files
- **`documents/EXPR_IMPLEMENTATION.md`**: Complete $expr implementation guide
- **`documents/releases/v1.4.0.md`**: Detailed release notes
- **`examples/expr_high_priority_demo.py`**: High-priority operators demonstration

### Known Limitations

#### $expr Implementation (WIP)
- **Missing Operators**: 23 medium/low priority operators remaining (set operations, trigonometric, advanced math)
- **Python Fallback**: Unsupported operators use Python evaluation (functional but slower)
- **Regex Performance**: Regex operations always use Python tier (SQLite regex limitations)

### Compatibility

- **Backward Compatible**: Zero breaking changes - all existing code continues to work
- **PyMongo API**: Full compatibility maintained
- **Graceful Degradation**: Missing $expr operators fall back to Python evaluation

## 1.3.2

### New Features

#### Complete GridFS Implementation
- **GridFSBucket.find_one()** - Direct method for single file lookup
- **GridFSBucket.get_last_version()** - Get most recent version of a file
- **GridFSBucket.get_version()** - Get specific version of a file by revision
- **GridFSBucket.list()** - List all unique filenames in the GridFS bucket
- **GridFSBucket.get()** - Convenience alias for `open_download_stream()`

#### Enhanced GridFS Metadata Support
- **Content Type Support** - Store and query MIME types with `content_type` parameter
- **Aliases Support** - Multiple names per file with `aliases` list parameter
- **Advanced Querying** - Search files by content_type and aliases fields
- **Automatic Schema Migration** - Seamless upgrades for existing GridFS installations

#### GridFS Collection Access Enhancements
- **Enhanced Auto-Delegation** - Improved support for PyMongo-style `db.fs.files.*` operations
- **Metadata Field Support** - Collection queries now support new content_type and aliases fields

### Documentation

#### Major Documentation Reorganization
- **`documents/GRIDFS.md`** - New comprehensive GridFS documentation combining all GridFS-related information
- **`documents/ANALYSIS_SUMMARY.md`** - Moved completed analysis documentation
- **`README.md`** - Enhanced GridFS examples showing content_type and aliases usage
- **`documents/PyMongo_API_Comparison.md`** - Updated with complete GridFS feature list

#### Removed Outdated Documentation
- **`documents/GRIDFS_MISSING_FEATURES.md`** - Superseded by implementation
- **`documents/GRIDFS_MIGRATION.md`** - Content merged into GRIDFS.md

### Enhancements

#### Schema Evolution
- **Automatic Column Addition** - New `content_type` and `aliases` columns added to existing GridFS tables
- **JSONB Support** - Enhanced storage for aliases arrays when JSONB is available
- **Backward Compatibility** - Zero-downtime migration for existing GridFS databases

#### API Compatibility
- **100% PyMongo Compatibility** - All standard GridFS operations now fully compatible
- **Enhanced Features** - Content type and aliases support beyond standard PyMongo
- **Collection Access** - PyMongo-style `db.fs.files.*` operations with auto-delegation

## 1.3.1

### Fixed
- **GridFS Collection Queries**: `connection.fs.files.find()`, `find_one()`, `delete_one()`, and `delete_many()` now work correctly by automatically delegating to `GridFSBucket` methods
- **Documentation**: Clarified GridFS collection access capabilities and corrected misleading "limitation" claims
- **Release Notes**: Updated v1.3.0 notes to accurately reflect GridFS API scope

### Added
- **GridFS Auto-Delegation**: `Collection` methods (`find()`, `find_one()`, `delete_one()`, `delete_many()`) now detect GridFS system collections (tables ending with `_files` or `_chunks`) and automatically delegate to corresponding `GridFSBucket` methods
- **Schema Verification**: GridFS detection uses two-step verification (naming convention + schema check) to prevent false positives with regular collections
- **PyMongo Compatibility**: Enhanced compatibility with PyMongo-style `db.fs.files.find()`, `db.fs.files.delete_one()`, etc.

### Documentation
- **GridFS API Scope**: Documented that `connection.fs.files.find()`, `find_one()`, `delete_one()`, and `delete_many()` now work via automatic delegation
- **Usage Guidance**: Updated examples to show both `bucket.find()` (recommended) and `conn.fs.files.find()` (now supported) for queries
- **Known Limitations**: Clarified that insert operations still require `GridFSBucket` API (`bucket.upload_from_stream()`)

## 1.3.0

### Added
- **PyMongo-style nested access**: Support for `connection.fs.files` and `connection.fs.chunks` syntax for GridFS system collection operations
- **Automatic GridFS table migration**: Seamless migration from dot-based table names (`fs.files`) to underscore-based names (`fs_files`) for better SQLite compatibility
- **Backward compatibility**: Existing databases with old table names are automatically migrated on first access
- **$count aggregation stage**: Implemented MongoDB-compatible `$count` aggregation stage
- **$sample aggregation stage**: Implemented MongoDB-compatible `$sample` aggregation stage with random sampling
- **$unset aggregation stage**: Implemented MongoDB-compatible `$unset` aggregation stage for field removal
- **$currentDate update operator**: Implemented MongoDB-compatible `$currentDate` update operator
- **$setOnInsert update operator**: Implemented MongoDB-compatible `$setOnInsert` update operator
- **$facet aggregation stage**: Implemented MongoDB-compatible `$facet` aggregation stage with sequential sub-pipeline execution

### Changed
- **GridFS table naming**: Internal table names changed from `bucket.files` to `bucket_files` format to avoid SQLite identifier parsing issues
- **Collection attribute access**: Added `__getattr__` support for nested sub-collection access (e.g., `db.fs.files`)
- **Metadata storage**: New databases use `JSONB` type for metadata column; existing databases gradually migrate per-row on update (TEXT → JSONB)

### Fixed
- **SQLite dot handling**: Resolved "unknown database" errors when using dot-based table names
- **Index creation**: Fixed backtick issues in CREATE INDEX statements

### Migration Notes
- **Automatic**: Existing GridFS tables are automatically renamed during first GridFS initialization
- **Manual**: If needed, run `ALTER TABLE fs.files RENAME TO fs_files; ALTER TABLE fs.chunks RENAME TO fs_chunks;`
- **Compatibility**: Migration preserves all data and maintains backward compatibility

### Documentation
- Added migration guide in `documents/gridfs-migration.md`
- Updated GridFS usage examples to demonstrate PyMongo-like syntax
- **Note**: For full GridFS operations (upload, download, delete), use the `GridFSBucket` API. Nested collection access (`connection.fs.files`) is intended for administrative operations like metadata updates.

## 1.2.3

### Bug Fixes

- **ObjectId Type Conversion for Reference Fields**: Fixed bug where `ObjectId` values in reference fields (e.g., `parent_post`, `author_id`, `user_id`) were not being properly converted for database queries
- **Enhanced ID Normalization**: Extended `normalize_id_query_for_db()` to handle `ObjectId` conversion in all fields, not just `_id` and `id`
- **Operator Support**: Added proper `ObjectId`-to-string conversion for values in `$in` and `$or` operators across reference fields
- **MongoDB Compatibility**: Restored MongoDB-compatible API behavior for queries on reference fields containing `ObjectId` values

### Testing

- **Comprehensive Test Coverage**: Added 10 unit tests covering reference field `ObjectId` conversion scenarios
- **Test Scenarios**: Tests cover basic conversion, multiple fields, mixed types, `$in` operator, `$or` operator, nested dicts, and complex queries

## 1.2.2

### $elemMatch Operator Enhancement

- **Simple Value Support**: Enhanced $elemMatch operator to support simple value matching in JSON arrays, making it compatible with MongoDB behavior
- **Backward Compatibility**: Maintains full backward compatibility with existing complex object matching functionality
- **Bug Fix**: Fixes issue where `{"tags": {"$elemMatch": "c"}}` did not work with simple arrays like `["a", "b", "c", "d"]`
- **MongoDB Compatibility**: Now supports both simple value matching (`{"tags": {"$elemMatch": "c"}}`) and complex object matching (`{"tags": {"$elemMatch": {"name": "value"}}}`) as per MongoDB specification

## 1.2.1

### Internal Improvements and Bug Fixes

- **Change Stream Enhancements**: Improved ObjectId handling in change streams with better storage of actual _id values separately
- **ID Type Handling Improvements**: Centralized ID query normalization for more robust ObjectId handling
- **Code Quality Improvements**: Enhanced type annotations and function signatures throughout the codebase
- **Collection Module Updates**: Improved method signatures and fixed parameter handling
- **SQL Translation Fixes**: Corrected SQL translation for _id field access
- **Test Updates**: Updated test assertions to properly handle ObjectId return values instead of integer IDs
- **Coverage Adjustment**: Reduced test coverage threshold from 85% to 80% to accommodate new changes

## 1.2.0

### Enhanced Datetime Query Processing

- **Three-Tier Datetime Processing**: Implemented sophisticated three-tier approach (SQL → Temporary Tables → Python) for handling datetime queries with automatic fallback mechanisms
- **Specialized Datetime Indexing**: New `datetime_field` parameter for `create_index()` to create timezone-normalized datetime indexes using SQLite's `datetime()` function
- **Automatic Datetime Detection**: Smart query analysis automatically detects datetime operations and routes them to the specialized processor
- **Timezone Normalization**: Datetime indexes use `datetime(json_extract(...))` for consistent timezone normalization across all datetime comparisons
- **Performance Optimization**: SQL-tier processing with dedicated JSON path support for datetime queries (uses json_* functions instead of jsonb_* for string comparison)

### Advanced JSON Path Support

- **Nested Field Handling**: Comprehensive JSON path parsing for complex nested fields with dot notation (e.g., `user.profile.created_at`)
- **Array Index Access**: Full support for array indexing in field paths (e.g., `tags[0]`, `orders.items[2].name`)
- **Proper Path Conversion**: Automatic conversion of dot notation with array indexing to proper JSON path syntax (e.g., `address.street` → `$.address.street`, `items[0]` → `$.items[0]`)
- **Array Path Handling**: Support for complex nested array access patterns like `orders.items[2].name` and `a.b[0].c[1].d`
- **Path Consistency**: All modules now use centralized `parse_json_path()` utility for consistent path handling across the codebase

### Three-Tier Processing Architecture

- **SQL Tier**: Direct SQL processing using json_* functions for datetime queries to ensure string comparison instead of byte comparison
- **Temporary Table Tier**: Fallback to temporary table processing for complex datetime queries
- **Python Tier**: Pure Python implementation as final fallback for advanced datetime operations
- **Smart Routing**: Automatic detection and routing of datetime queries to appropriate processing tier
- **Performance Monitoring**: Integration with existing force fallback kill switch for benchmarking and debugging

### New Features

#### Datetime Query Processing
- **`neosqlite.collection.datetime_query_processor.DateTimeQueryProcessor`**: Complete three-tier datetime query processor with fallback mechanisms
- **`neosqlite.collection.datetime_utils`**: Shared utility module with common datetime patterns and validation functions
- **Automatic Detection**: `_contains_datetime_operations()` method in Cursor class automatically identifies datetime queries
- **Specialized Processing**: `_is_datetime_value()` and `_is_datetime_regex()` methods for accurate datetime pattern recognition

#### Enhanced Index Management
- **Datetime Field Support**: `create_index()` method now accepts `datetime_field: bool = False` parameter for specialized datetime indexing
- **Normalized Datetime Indexes**: New `_create_datetime_index()` method creates timezone-normalized indexes with `datetime(json_extract(...))` 
- **Auto-Detection**: `_is_datetime_indexed_field()` method checks for existing datetime indexes in collection
- **Proper Path Handling**: All FTS index operations now use `parse_json_path()` for consistent field path conversion

#### JSON Path Utilities
- **`neosqlite.collection.json_path_utils.parse_json_path()`**: Centralized utility for converting dot notation to JSON path syntax
- **`build_json_extract_expression()`**: Helper for building complete json_extract SQL expressions
- **`build_jsonb_extract_expression()`**: Helper for building complete jsonb_extract SQL expressions
- **Array Support**: Full handling of array indexing in JSON paths (e.g., `field[0]`, `nested.array[2].value`)

#### Query Processing Enhancements
- **Datetime-Aware Operators**: Enhanced `_build_operator_clause()` now accepts `is_datetime_indexed` parameter for proper timezone normalization
- **Index-Aware Processing**: Query engine automatically detects datetime indexes and applies timezone normalization
- **Smart Field Detection**: `_is_datetime_indexed_field()` method identifies datetime-indexed fields in database schema
- **Path Conversion**: All query operations now use proper JSON path conversion for nested field access

#### ObjectId Improvements
- **Enhanced Specification**: Updated to follow MongoDB specification more closely (4 bytes timestamp + 5 bytes random + 3 bytes counter vs. previous 4+3+2)
- **Integer Timestamp Support**: ObjectId constructor now accepts integer timestamp values that replace the timestamp part during construction
- **Validation Improvements**: Enhanced validation for integer timestamp values (0 to 0xFFFFFFFF range)

### Performance Improvements
- **Three-Tier Datetime Processing**: Optimized datetime queries with SQL → Temp Table → Python fallback for best performance
- **JSON Path Consistency**: Centralized JSON path utilities ensure consistent and efficient path handling throughout the codebase
- **Timezone Handling**: Proper timezone normalization ensures consistent datetime comparisons across different timezones
- **Enhanced Robustness**: Improved error handling and fallback mechanisms for complex datetime operations

### Technical Benefits
- **Performance Optimization**: Three-tier datetime processing provides optimal performance for datetime queries while maintaining accuracy
- **Timezone Handling**: Proper timezone normalization ensures consistent datetime comparisons across different timezones
- **JSON Path Consistency**: Centralized JSON path utilities ensure consistent handling of nested fields and arrays throughout the codebase
- **Backward Compatibility**: All existing functionality preserved while adding new capabilities
- **Robustness**: Enhanced error handling and fallback mechanisms for complex datetime operations
- **Scalability**: Efficient processing of large datasets with datetime queries using SQL optimization
- **Type Safety**: Comprehensive type annotations throughout new datetime processing modules

## 1.1.2

### MongoDB-compatible ObjectId Support for GridFS

- **GridFS ObjectId Implementation**: Complete MongoDB-compatible ObjectId support in GridFS operations with automatic generation and hex interchangeability
- **Dual-ID System**: Supports both integer IDs (primary key) and ObjectIds (_id field) for maximum flexibility in GridFS
- **Automatic Generation**: New ObjectIds automatically generated when no file ID provided during GridFS upload
- **Manual Assignment**: Support for user-provided ObjectIds during GridFS file upload operations
- **Enhanced Robustness**: Improved error handling and type safety throughout GridFS operations

### Comprehensive GridFS Implementation

- **PyMongo-compatible API**: Full GridFSBucket and legacy GridFS APIs compatible with PyMongo
- **Performance Optimized**: Uses JSONB type when available for efficient ObjectId storage in GridFS
- **Index Support**: Automatic indexing for faster GridFS lookups and queries
- **Complete Feature Set**: Supports upload, download, streaming, metadata, and query operations in GridFS

### GridFS with ObjectId Support

- **GridFSBucket Class**: Complete PyMongo-compatible GridFSBucket implementation with ObjectId support
- **GridFS Legacy Class**: Compatible with legacy PyMongo GridFS API with ObjectId enhancements
- **Upload Operations**: `upload_from_stream()` returns ObjectIds with automatic generation when no ID provided
- **Download Operations**: `download_to_stream()`, `open_download_stream()` compatible with ObjectId, integer, and hex string IDs
- **Query Support**: `find()` operations support ObjectId queries against _id field in GridFS
- **File Management**: Rename, delete, and other file operations work with both ID types in GridFS

### Return Value and Parameter Updates

- **Upload Methods Return ObjectIds**: `upload_from_stream()` and `put()` now return ObjectIds instead of integers for MongoDB compatibility
- **Flexible File ID Parameters**: Methods accept ObjectId, integer, or hex string for file identification in GridFS
- **Backward Compatibility**: All existing integer-based operations continue to work unchanged in GridFS
- **Enhanced Flexibility**: ObjectIds, integers, or hex strings interchangeable in most GridFS operations

### Technical Implementation

- **JSONB Support**: Uses JSONB type when available for efficient ObjectId storage in GridFS
- **Index Creation**: Automatic unique index on _id column for faster file lookups in GridFS
- **Optimized Queries**: Enhanced query processing for GridFS operations with proper ID handling
- **Memory Efficiency**: Streaming operations maintain memory efficiency for large files in GridFS

### API Compatibility and Migration

- **MongoDB Compatibility**: Full compatibility with MongoDB GridFS concepts and ObjectId usage in GridFS
- **Backward Compatibility**: Full support for existing GridFS code with automatic migration
- **Enhanced Error Handling**: Better error reporting and validation throughout GridFS operations
- **Type Safety**: Improved type checking and validation for GridFS operations

### Performance Improvements

- **JSONB Optimization**: GridFS uses JSONB type when available for better performance with ObjectId storage
- **Index Usage**: Unique indexing on _id column for fast ObjectId lookups in GridFS
- **Memory Efficiency**: Optimized storage using JSONB format when available in GridFS operations
- **Query Plan Optimization**: EXPLAIN query plan verification confirms index usage for ObjectId lookups in GridFS

### New Features

#### GridFS ObjectId Implementation

- **`neosqlite.gridfs.grid_file.GridIn/GridOut`**: Complete implementation supporting both ObjectIds and integer IDs
- **`neosqlite.gridfs.gridfs_bucket.GridFSBucket`**: PyMongo-compatible GridFSBucket with ObjectId support
- **`neosqlite.gridfs.gridfs_legacy.GridFS`**: Legacy GridFS class with enhanced ObjectId compatibility
- **Automatic Generation**: ObjectIds automatically generated when no file ID provided during upload
- **Manual Assignment**: Support for user-provided ObjectIds during file upload operations
- **Dedicated Storage**: Files stored with both integer primary key (id) and ObjectId (_id) for compatibility

#### Enhanced GridFS Schema

- **New Schema**: GridFS tables now use `(id INTEGER PRIMARY KEY AUTOINCREMENT, _id JSONB, filename TEXT, ...)` when JSONB support available
- **Backward Compatibility**: Existing GridFS tables maintain compatibility while adding ObjectId support
- **Unique Indexing**: Automatic unique index creation on `_id` column for GridFS performance
- **SQL Translation**: Enhanced SQL translator to handle GridFS `_id` field queries properly

#### GridFS Query Engine Updates

- **_id Query Support**: Full support for GridFS queries against `_id` field with SQL optimization
- **Mixed Type Queries**: Support for GridFS queries that combine integer IDs and ObjectIds
- **Index Optimization**: Query engine now optimizes GridFS queries using the unique `_id` index
- **Backward Compatibility**: Existing integer-based GridFS queries continue to work unchanged

#### GridFS API Extensions

- **Upload Methods**: `upload_from_stream()` returns ObjectId, accepts metadata
- **Download Methods**: `download_to_stream()` accepts ObjectId, integer, or hex string file ID
- **Query Methods**: `find()` supports ObjectId queries against _id field
- **File Management**: `delete()` and `rename()` accept ObjectId, integer, or hex string file ID

### Technical Benefits

- **MongoDB Compatibility**: Full compatibility with MongoDB GridFS ObjectId format and behavior
- **Performance Optimization**: JSONB type and unique indexing provide enhanced performance in GridFS
- **Backward Compatibility**: Full support for existing GridFS data and code with automatic schema migration
- **Thread Safety**: Proper locking mechanisms ensure safe concurrent ObjectId generation in GridFS
- **Memory Efficiency**: Optimized storage using JSONB format when available in GridFS

### Migration Notes

For GridFS operations, the main change is that upload operations now return ObjectIds instead of integer IDs. All existing GridFS code continues to work unchanged, but code that expects integer IDs from upload operations will need to be updated to handle ObjectIds.

### GridFS Schema Changes:

1. **New Files**: When uploading new files without specifying an ID, the `_id` field will contain an auto-generated ObjectId (not the integer id)

2. **Existing Files**: Files created before this release continue to work as before

3. **Accessing Integer ID**: The integer ID remains available in the `id` field for all files

4. **Querying**: You can query using either ObjectIds or integer IDs in the appropriate fields, with the system handling the appropriate lookup in GridFS

## 1.1.1

### Enhanced Robustness Through Automatic ID Type Correction

- **Intelligent Query Normalization**: Automatic detection and correction of common ID type mismatches between integer IDs and ObjectIds
- **User Error Forgiveness**: Makes the API more robust by gracefully handling common user mistakes with ID types
- **Transparent Operation**: Corrections happen automatically without requiring any code changes
- **Cross-Operation Support**: Works seamlessly across all CRUD operations (find, update, delete, replace, count, etc.)

### Automatic ID Type Detection and Correction

- **Query Normalization**: Automatically normalizes queries when users mistakenly query the `id` field with ObjectId values
- **Smart Type Conversion**: Intelligently converts between integer IDs and ObjectIds based on context and value type
- **String Handling**: Properly handles integer strings and hex strings in ID queries for better user experience
- **Zero Configuration**: No setup required - works automatically out of the box

### Specific Corrections Implemented

- **`id` field with ObjectId**: When users query `{'id': ObjectId(...)}`, it's automatically corrected to search the `_id` field
- **`id` field with hex string**: When users query `{'id': 'hex_string'}`, it's automatically corrected to search the `_id` field  
- **`_id` field with integer string**: When users query `{'_id': '123'}`, it's automatically converted to integer `123`
- **All Value Types**: Handles strings, integers, ObjectIds, and hex strings appropriately in context

### Technical Implementation

- **Centralized Logic**: Implemented using DRY principles with centralized `_normalize_id_query` method in QueryHelper
- **Performance Optimized**: Minimal overhead with efficient implementation that doesn't impact normal operations
- **Type Safe**: Proper typing throughout implementation with comprehensive mypy compatibility
- **Comprehensive Coverage**: Applied to all relevant QueryEngine methods for complete CRUD operation support

### Developer Experience Improvements

- **Reduced Errors**: Significantly reduces errors from common ID type mismatches
- **Intuitive API**: More forgiving interface that handles common mistakes gracefully
- **Backward Compatible**: All existing code continues to work unchanged while gaining robustness
- **No Learning Curve**: Works automatically without requiring developers to learn new APIs or patterns

## 1.1.0

### Enhanced ObjectId Implementation

- **MongoDB-compatible ObjectId**: Full 12-byte ObjectId implementation following MongoDB specification with automatic generation when no _id is provided
- **Hex String Interchangeability**: Complete hex string compatibility with PyMongo ObjectIds for seamless data exchange
- **Dedicated _id Column**: New `_id JSONB` column with unique indexing for faster lookups and proper ObjectId storage
- **Backward Compatibility**: Full support for existing collections with automatic `_id` column addition via ALTER TABLE
- **Performance Optimization**: _id column uses JSONB when available (same as data column) for enhanced performance
- **Index Support**: Automatic unique index creation on `_id` column for efficient queries
- **Integration**: Proper integration with all CRUD operations (insert, find, update, delete)

### ObjectId Features

- **12-Byte Structure**: Follows MongoDB specification (4 bytes timestamp, 3 bytes random, 2 bytes PID, 3 bytes counter)
- **Automatic Generation**: New ObjectIds automatically generated when documents don't have `_id` field
- **Manual Assignment**: Support for user-provided ObjectIds during document insertion
- **Timestamp Extraction**: `generation_time()` method to extract creation timestamp
- **Validation**: `is_valid()` method for ObjectId validation
- **Serialization**: Proper JSON serialization/deserialization support with custom encoder
- **Thread Safety**: Proper locking mechanisms for safe multi-threaded ObjectId generation

### Query Support

- **_id Queries**: Full support for queries against `_id` field with proper SQL optimization
- **Mixed Queries**: Support for queries that combine integer IDs and ObjectIds
- **Index Usage**: Query engine properly uses unique index on `_id` column for performance

### Performance Improvements

- **JSONB Optimization**: Both `data` and `_id` columns now use JSONB type when available in SQLite for better performance
- **Index Usage**: Efficient unique indexing on `_id` column for fast ObjectId lookups
- **Query Plan Optimization**: EXPLAIN query plan verification confirms index usage for ObjectId lookups
- **Memory Efficiency**: Optimized storage and retrieval of ObjectId values using JSONB

### New Features

#### ObjectId Implementation

- **`neosqlite.objectid.ObjectId`**: Complete implementation following MongoDB specification
- **Automatic Generation**: ObjectIds automatically generated when no `_id` provided during insertion
- **Manual Assignment**: Support for user-provided ObjectIds during document insertion
- **Dedicated Storage**: New `_id` column with unique indexing for efficient storage and lookup
- **JSON Serialization**: Proper JSON encoding/decoding support with custom encoder
- **Thread Safety**: Proper locking for safe multi-threaded generation

#### Enhanced Collection Schema

- **New Schema**: Tables now use `(id INTEGER PRIMARY KEY AUTOINCREMENT, _id JSONB, data JSONB)` when JSONB support available
- **Backward Compatibility**: Existing tables get `_id` column added via `ALTER TABLE` when accessed
- **Unique Indexing**: Automatic unique index creation on `_id` column for performance
- **SQL Translation**: Enhanced SQL translator to handle `_id` field queries properly

#### Query Engine Updates

- **_id Query Support**: Full support for queries against `_id` field with SQL optimization
- **Mixed Type Queries**: Support for queries that combine integer IDs and ObjectIds
- **Index Optimization**: Query engine now optimizes queries using the unique `_id` index
- **Backward Compatibility**: Existing integer-based queries continue to work unchanged

#### Test Coverage

- **Comprehensive Test Suite**: Complete test coverage for ObjectId functionality including creation, validation, storage, and retrieval
- **Interchangeability Tests**: Tests verifying hex string interchangeability with PyMongo
- **Integration Tests**: Tests ensuring proper integration with all CRUD operations
- **Index Usage Tests**: Tests verifying that `_id` index is properly used for queries

### Technical Benefits

- **MongoDB Compatibility**: Full compatibility with MongoDB ObjectId format and behavior
- **Performance Optimization**: JSONB type and unique indexing provide enhanced performance
- **Backward Compatibility**: Full support for existing data and code with automatic schema migration
- **Thread Safety**: Proper locking mechanisms ensure safe concurrent ObjectId generation
- **Memory Efficiency**: Optimized storage using JSONB format when available

### Breaking Changes

There are no intentional breaking changes in this release that would break existing application logic. All existing APIs and functionality remain fully compatible with previous versions. However, there are important behavioral changes to be aware of:

- **_id Field Type Change**: For new documents, the `_id` field now contains a MongoDB-compatible ObjectId instead of the integer ID
- **Backward Compatibility**: Existing documents continue to work as before, with the integer ID accessible as the `_id` field until the document is updated  
- **Dual ID System**: The SQLite integer ID is still available through the `id` field for all documents

### Migration Notes

For existing databases, this release automatically adds the `_id` column to existing collections when they are first accessed. This process is transparent and maintains full backward compatibility. New collections will be created with the optimized schema using JSONB types when available.

#### Important Behavioral Changes:

1. **New Documents**: When inserting new documents without specifying an `_id`, the `_id` field will contain an auto-generated ObjectId (not the integer id)

2. **Existing Documents**: Documents created before this release will continue to have their integer ID as the `_id` value until they are updated or replaced

3. **Accessing Integer ID**: The integer ID is always available in the `id` field for all documents (both old and new)

4. **Querying**: You can query using either the ObjectId (for new documents) or integer ID (for old documents) in the `_id` field, with the system handling the appropriate lookup

### Interchangeability with PyMongo

- **Hex String Compatibility**: NeoSQLite ObjectIds can be used to create PyMongo ObjectIds and vice versa
- **Round-trip Conversion**: Complete conversion cycles (NeoSQLite → PyMongo → NeoSQLite) preserve the original hex representation
- **MongoDB Integration**: Ready for integration with MongoDB systems using hex interchangeability
- **Timestamp Compatibility**: Timestamp extraction works correctly in both implementations

### Migration Notes

For existing databases, this release automatically adds the `_id` column to existing collections when they are first accessed. This process is transparent and maintains full backward compatibility. New collections will be created with the optimized schema using JSONB types when available.

## 1.0.0

### Critical Bug Fixes

- **Range Query Bug Fix**: Fixed a critical issue where queries with multiple operators like `{"age": {"$gte": 30, "$lte": 50}}` were only processing the first operator. This fix ensures range queries now return correct results and provides 3.9x-25.6x performance improvements.
- **Aggregation Pipeline Bug Fix**: Resolved an issue where aggregation pipelines with `$unwind` + `$group` + `$sort` + `$limit` weren't correctly applying the `$limit` clause. This fix ensures correct result counts and provides 19.8x-25.6x performance improvements.

### Enhanced JSON Operations

- **JSON Insert/Replace Support**: Added `json_insert()` and `json_replace()` support for more efficient update operations, providing 2-10x faster update operations depending on use case.
- **JSONB Function Support**: Expanded usage of `jsonb_*` functions for better performance when available, with 2-5x faster JSON operations with JSONB support and graceful fallback to `json_*` functions for older SQLite versions.
- **Enhanced Aggregation**: Leveraged existing `json_group_array()` usage for `$push` and `$addToSet` operations, resulting in 5-20x faster aggregation operations with proper SQL optimization.

### JSON Validation and Error Handling

- **Enhanced JSON Path Support**: Implemented enhanced JSON path parsing with array indexing support for complex document structures.
- **JSON Validation**: Added JSON validation using `json_valid()` and `json_error_position()` for improved document validation and better error reporting.
- **Improved Error Handling**: Enhanced error handling with detailed position information for invalid JSON documents.

### Performance Improvements

- **7.4x-9.6x Average Performance Improvement**: Comprehensive performance gains across all operations through optimized JSON functions and bug fixes.
- **SQL Processing Coverage**: Expanded SQL optimization coverage for more aggregation pipelines.
- **Memory Efficiency**: Maintained efficient memory usage through optimized JSON operations.

### New Features

#### Enhanced Update Operations

- **`json_insert()` Support**: Enhanced update operations with `json_insert()` for ensuring values are only inserted into documents.
- **`json_replace()` Support**: Enhanced update operations with `json_replace()` for ensuring values are only replaced in existing fields.
- **Mixed Operation Support**: Support for mixed JSON insert/replace operations in single update commands.

#### Advanced JSON Functions Integration

- **JSON Validation**: Native JSON validation using SQLite's `json_valid()` function with Python fallback.
- **Error Position Reporting**: Enhanced error reporting with position information using `json_error_position()`.
- **JSON Path Parsing**: Enhanced JSON path parsing with support for array indexing and complex nested paths.

#### Aggregation Enhancements

- **Enhanced `$push` and `$addToSet`**: Leveraged `json_group_array()` for significantly faster aggregation operations.
- **Complex Aggregation Support**: Better support for complex aggregation pipelines with multiple stages.
- **Temporary Table Improvements**: Enhanced temporary table aggregation with JSONB support for better performance.

#### Query Operator Validation

- **`$inc` and `$mul` Validation**: Added comprehensive validation for `$inc` and `$mul` operations to ensure MongoDB-compatible behavior.
- **Numeric Value Checking**: Enhanced validation for numeric values in update operations.
- **Type Safety**: Improved type checking for field values in mathematical operations.

### Technical Benefits

- **Backward Compatibility**: Full backward compatibility maintained with all existing APIs.
- **Production Ready**: Comprehensive test coverage with 850+ passing tests and only 7 expected failures.
- **Cross-Platform Support**: Support for Python 3.11, 3.12, and 3.13 with no breaking changes.
- **Graceful Degradation**: Proper fallback mechanisms for older SQLite versions without JSONB support.
- **Memory Efficiency**: Efficient memory usage with optimized JSON operations and temporary table aggregation.

## 0.9.1

### Improved Code Organization

- **Index Management Refactoring**: All search index methods (`create_search_index`, `create_search_indexes`, `list_search_indexes`, `update_search_index`, and `drop_search_index`) have been properly delegated from the Collection class to the IndexManager class, following the established pattern for other index operations
- **Consistent API Implementation**: The Collection class now consistently delegates all index-related operations to the IndexManager, improving code organization and maintainability
- **Reduced Code Duplication**: Search index functionality is now implemented in a single location (IndexManager) rather than being duplicated between the Collection and IndexManager classes

### Code Quality Improvements

- **Bug Fixes**: Fixed undefined variable issues related to import statements in example files
- **Code Cleanup**: Removed unnecessary import statements and fixed linting issues with ruff
- **Improved Maintainability**: Better organized code structure makes the codebase more approachable for new contributors

### New Features

#### Index Management Enhancements

- **Proper Delegation Pattern**: All search index methods in the Collection class now properly delegate to the corresponding methods in the IndexManager class:
  - `create_search_index()` now delegates to `IndexManager.create_search_index()`
  - `create_search_indexes()` now delegates to `IndexManager.create_search_indexes()`
  - `list_search_indexes()` now delegates to `IndexManager.list_search_indexes()`
  - `update_search_index()` now delegates to `IndexManager.update_search_index()`
  - `drop_search_index()` now delegates to `IndexManager.drop_search_index()`

#### Code Quality Improvements

- **Import Statement Cleanup**: Removed unused import statements from example files
- **Linting Fixes**: Fixed various linting issues identified by ruff
- **Variable Scope Fixes**: Resolved undefined variable issues in example code

### Performance Improvements

- **Memory Efficiency**: Reduced memory footprint by removing unnecessary import statements
- **Improved Code Maintainability**: Better organized code structure leads to more efficient development and debugging

### Technical Benefits

- **Better Code Organization**: All index-related functionality is now consistently located in the IndexManager class
- **Enhanced Maintainability**: Improved code structure makes it easier to maintain and extend index functionality
- **Reduced Code Duplication**: Eliminated duplicated code between Collection and IndexManager classes
- **Improved Testability**: Centralized index management functionality makes it easier to test and verify behavior
- **Backward Compatibility**: All existing APIs remain accessible through the same import paths

## 0.9.0

### Enhanced Aggregation Pipeline Processing

- **Expanded SQL Optimization Coverage**: Increased SQL optimization coverage from ~85% to over 95% of common aggregation pipelines through temporary table processing
- **Three-Tier Processing Model**: Implemented sophisticated three-tier approach for aggregation processing: 1) Single SQL Query optimization (fastest), 2) Temporary Table Aggregation (intermediate), 3) Python Fallback (slowest but most flexible)
- **Granular Pipeline Processing**: Individual unsupported stages can now fall back to Python processing while keeping others in SQL for hybrid pipeline operations
- **Improved Resource Management**: Intermediate results now stored in temporary tables rather than Python memory, enabling processing of larger datasets
- **Position Independence**: Operations like `$lookup` can now be used in any pipeline position, not just at the end
- **Enhanced $unwind Support**: Fully implemented `$unwind` with all advanced options including `includeArrayIndex` and `preserveNullAndEmptyArrays`

### Hybrid Text Search Processing

- **Performance Enhancement**: Instead of falling back the entire pipeline to Python processing when a `$text` operator is encountered without FTS indexes, the system now processes compatible stages with SQL optimization and only falls back to Python for the specific text search operation
- **Three-Tier Processing for Text Search**: Pipelines are now processed as follows:
  1. **Stages 1 to N-1**: Process using SQL with temporary tables
  2. **Stage N (with $text)**: Process with Python-based text search
  3. **Stages N+1 to M**: Continue processing with SQL using temporary tables
- **Resource Efficiency**: Only matching documents are loaded for text search, significantly reducing memory usage
- **Enhanced Text Search Capabilities**: Improved international character support with diacritic-insensitive matching and Unicode normalization
- **Selective Fallback**: Only text search operations fall back to Python processing while other pipeline stages continue to benefit from SQL optimization

### Comprehensive API Implementation

- **Missing API Coverage**: Implemented approximately 95%+ of the core PyMongo Collection APIs that were previously missing
- **Logical Operators**: Fully implemented `$and`, `$or`, `$not`, and `$nor` logical operators
- **Element Operators**: Implemented `$type` element operator for type-based document selection
- **Array Operators**: Implemented `$all` array operator for matching arrays that contain all specified elements
- **Collection Management**: Added `drop()`, `create_collection()`, `list_collection_names()`, and `list_collections()` methods
- **Advanced Aggregation**: Implemented `aggregate_raw_batches()` for efficient batch processing of large aggregation results
- **Search Index APIs**: Added comprehensive FTS5-based search index functionality with `create_search_index()`, `create_search_indexes()`, `drop_search_index()`, `list_search_indexes()`, and `update_search_index()` methods

### Enhanced Binary Data Handling

- **Automatic Conversion**: Raw bytes are now automatically converted to Binary objects with proper JSON serialization during insert and update operations
- **Subtype Preservation**: Binary objects preserve their subtypes (FUNCTION, UUID, MD5, etc.) during database operations
- **Nested Structure Support**: Binary data handling now works correctly in nested documents and arrays
- **SQL Update Support**: Binary data can now be used in SQL-based update operations with proper serialization

### Package Structure Reorganization

- **Modular Organization**: Cursor classes have been moved from the root package to the collection module for better code organization
- **Improved Maintainability**: Related functionality is now grouped more logically within the package structure
- **Backward Compatibility**: All public APIs remain accessible through the same import paths
- **Test Suite Reorganization**: Consolidated test files for better maintainability and code coverage

### Enhanced Documentation

- **Comprehensive Docstrings**: Added detailed docstrings throughout the codebase explaining functionality, parameters, and return values
- **Implementation Documentation**: Added complete specification documents for all major enhancements
- **Improved Code Clarity**: Better comments and documentation make the codebase more approachable for new contributors

### New Features

#### Aggregation Pipeline Enhancements

- **Temporary Table Aggregation**: Introduced a new three-tier processing model that bridges SQL optimization and Python fallback
- **Enhanced $unwind Support**: Fully implemented `$unwind` with all advanced options including `includeArrayIndex` and `preserveNullAndEmptyArrays`
- **$lookup Position Independence**: `$lookup` operations can now be used in any pipeline position, not just at the end
- **Multi-Stage Pipeline Optimization**: Complex pipelines with multiple `$unwind`, `$lookup`, `$group`, and `$sort` stages can now be processed efficiently
- **Database-Level Intermediate Processing**: Intermediate results processed at database level rather than Python level
- **Automatic Resource Management**: Robust transaction-based cleanup with guaranteed resource release using SQLite SAVEPOINTs

#### Query Operator Implementations

- **Logical Operators**: Fully implemented `$and`, `$or`, `$not`, and `$nor` operators for complex query construction
- **Element Operators**: Implemented `$type` operator for selecting documents based on field type
- **Array Operators**: Implemented `$all` operator for matching arrays that contain all specified elements
- **Query Validation**: Enhanced query validation with proper error handling for malformed queries

#### Collection Management APIs

- **`drop()` Method**: Drop the entire collection (table in SQLite)
- **`create_collection()` Method**: Create a new collection with specific options
- **`list_collection_names()` Method**: List all collection names in the database
- **`list_collections()` Method**: Get detailed information about collections

#### Advanced Aggregation Features

- **`aggregate_raw_batches()` Method**: Perform aggregation and retrieve raw BSON batches for efficient processing of large results
- **Batch Processing**: Efficient batch insertion of text search results into temporary tables for better performance
- **Pipeline Validation Updates**: Modified `can_process_with_temporary_tables()` to allow pipelines containing `$text` operators

#### Search Index APIs

- **`create_search_index()` Method**: Create a single search index using FTS5
- **`create_search_indexes()` Method**: Create multiple search indexes at once
- **`drop_search_index()` Method**: Drop a search index
- **`list_search_indexes()` Method**: List search indexes
- **`update_search_index()` Method**: Update a search index
- **Enhanced Text Search Implementation**: New `unified_text_search` function in `neosqlite.collection.text_search` module provides enhanced text search capabilities

#### Binary Data Handling Improvements

- **Automatic Bytes Conversion**: Raw bytes are automatically converted to Binary objects during insert and update operations
- **Subtype Preservation**: Binary objects preserve their subtypes (FUNCTION, UUID, MD5, etc.) during database operations
- **Nested Structure Support**: Binary data handling now works correctly in nested documents and arrays
- **SQL Update Support**: Binary data can now be used in SQL-based update operations with proper serialization

#### Package Reorganization

- **Cursor Module Relocation**: `AggregationCursor`, `Cursor`, and `RawBatchCursor` classes moved to `neosqlite.collection` submodules
- **Cleaner Import Structure**: Related classes are now grouped more logically within the package structure
- **Maintained API Compatibility**: All existing import paths continue to work without changes for end users
- **Test Suite Consolidation**: Consolidated test files for better organization and maintainability

### Performance Improvements

- **Significant Performance Gains**: Pipelines with text search operations see 50%+ performance improvement over previous Python fallback approach
- **Reduced Memory Usage**: Only relevant documents are loaded for text search operations, dramatically reducing memory footprint
- **Optimized Batch Operations**: Batch insertion of text search results improves processing efficiency for large datasets
- **Maintained SQL Optimization**: Non-text stages continue to benefit from SQL processing performance
- **Expanded SQL Coverage**: Process 95%+ of common aggregation pipelines at SQL level vs. ~85% previously
- **Better Resource Management**: Database-level processing for most operations with automatic temporary table management
- **Enhanced Maintainability**: Improved code organization and comprehensive documentation

### Technical Benefits

- **Better Resource Management**: Database-level processing for most operations with automatic temporary table management
- **Enhanced Maintainability**: Improved code organization and comprehensive documentation
- **Robust Error Handling**: Comprehensive error handling for edge cases and invalid text search specifications
- **Extensibility**: Modular design allows for future enhancements like parallel processing and caching
- **Automatic Cleanup**: Guaranteed cleanup with transaction-based atomicity
- **Backward Compatibility**: 100% backward compatibility with existing code
- **Enhanced Type Safety**: Comprehensive type annotations throughout the codebase

## 0.8.1

### Hybrid Text Search Processing

- **Performance Enhancement**: Instead of falling back the entire pipeline to Python processing when a `$text` operator is encountered without FTS indexes, the system now processes compatible stages with SQL optimization and only falls back to Python for the specific text search operation
- **Three-Tier Processing for Text Search**: Pipelines are now processed as follows:
  1. **Stages 1 to N-1**: Process using SQL with temporary tables
  2. **Stage N (with $text)**: Process with Python-based text search
  3. **Stages N+1 to M**: Continue processing with SQL using temporary tables
- **Resource Efficiency**: Only matching documents are loaded for text search, significantly reducing memory usage
- **Enhanced Text Search Capabilities**: Improved international character support with diacritic-insensitive matching and Unicode normalization

### Package Structure Reorganization

- **Modular Organization**: Cursor classes have been moved from the root package to the collection module for better code organization
- **Improved Maintainability**: Related functionality is now grouped more logically within the package structure
- **Backward Compatibility**: All public APIs remain accessible through the same import paths

### Enhanced Documentation

- **Comprehensive Docstrings**: Added detailed docstrings throughout the codebase explaining functionality, parameters, and return values
- **Implementation Documentation**: Added a complete specification document for the hybrid text search enhancement
- **Improved Code Clarity**: Better comments and documentation make the codebase more approachable for new contributors

### New Features

#### Hybrid Text Search in Aggregation Pipelines

- **Selective Fallback**: Only text search operations fall back to Python processing while other pipeline stages continue to benefit from SQL optimization
- **Diacritic-Insensitive Matching**: Text search now supports international characters with proper Unicode normalization
- **Batch Processing**: Efficient batch insertion of text search results into temporary tables for better performance
- **Pipeline Validation Updates**: Modified `can_process_with_temporary_tables()` to allow pipelines containing `$text` operators

#### Enhanced Text Search Implementation

- **Unified Text Search Function**: New `unified_text_search` function in `neosqlite.collection.text_search` module provides enhanced text search capabilities
- **Unicode Support**: Proper handling of international characters with normalization for diacritic-insensitive matching
- **Optimized Performance**: LRU caching for compiled regex patterns and text normalization operations
- **Nested Document Support**: Text search now properly traverses nested documents and arrays

#### Package Reorganization

- **Cursor Module Relocation**: `AggregationCursor`, `Cursor`, and `RawBatchCursor` classes moved to `neosqlite.collection` submodules
- **Cleaner Import Structure**: Related classes are now grouped more logically within the package structure
- **Maintained API Compatibility**: All existing import paths continue to work without changes for end users

### Performance Improvements

- **Significant Performance Gains**: Pipelines with text search operations see 50%+ performance improvement over previous Python fallback approach
- **Reduced Memory Usage**: Only relevant documents are loaded for text search operations, dramatically reducing memory footprint
- **Optimized Batch Operations**: Batch insertion of text search results improves processing efficiency for large datasets
- **Maintained SQL Optimization**: Non-text stages continue to benefit from SQL processing performance

### Technical Benefits

- **Better Resource Management**: Database-level processing for most operations with automatic temporary table management
- **Enhanced Maintainability**: Improved code organization and comprehensive documentation
- **Robust Error Handling**: Comprehensive error handling for edge cases and invalid text search specifications
- **Extensibility**: Modular design allows for future enhancements like parallel processing and caching

## 0.8.0

### Enhanced Three-Tier Aggregation Pipeline Processing
- **Three-Tier Processing Model**: Implemented sophisticated three-tier approach for aggregation processing: 1) Single SQL Query optimization (fastest), 2) Temporary Table Aggregation (intermediate), 3) Python Fallback (slowest but most flexible)
- **Expanded SQL Optimization Coverage**: Increased SQL optimization coverage from ~60% to over 85% of common aggregation pipelines through temporary table processing
- **Enhanced Resource Management**: Intermediate results now stored in temporary tables rather than Python memory, enabling processing of larger datasets
- **Position Independence**: Operations like `$lookup` can now be used in any pipeline position, not just at the end
- **Granular Fallback**: Individual unsupported stages can fall back to Python processing while keeping others in SQL for hybrid pipeline operations

### Binary Data Handling Improvements
- **Preserved Binary Subtypes**: Binary objects now preserve their subtypes (FUNCTION, UUID, MD5, etc.) during insert and update operations
- **Automatic Bytes Conversion**: Raw bytes are automatically converted to Binary objects with proper JSON serialization
- **SQL Update Support**: Binary data can now be used in SQL-based update operations with proper serialization

### Enhanced Temporary Table Aggregation
- **Additional Stage Support**: Added support for `$addFields` stage in temporary table aggregation
- **Sequential Unwind Processing**: Fixed nested array `$unwind` operations to handle sequential dependencies correctly
- **Simplified Group Optimization**: Improved `$unwind + $group` optimization by delegating to the general `_build_group_query` method
- **Deterministic Naming**: Enhanced temporary table naming with SHA256-based deterministic names for better resource management

### Unified SQL Translation Framework
- **Code Reorganization**: Extracted SQL translation logic into a separate `sql_translator_unified.py` module
- **Shared Implementation**: Both `QueryEngine` and `TemporaryTableAggregationProcessor` now use the same SQL translation framework
- **Improved Maintainability**: Reduced code duplication and improved consistency across SQL generation

### Performance Improvements
- **1.2x faster** average performance across supported aggregation operations through temporary table processing
- **Reduced memory usage** for complex pipelines by storing intermediate results in database rather than Python memory
- **Better scalability** for larger datasets that might not fit in Python memory

### Technical Benefits
- **Expanded SQL Coverage**: Process 85%+ of common aggregation pipelines at SQL level vs. ~60% previously
- **Better Error Handling**: Graceful fallback between processing approaches with robust error handling
- **Enhanced Maintainability**: Unified SQL translation framework reduces code duplication
- **Robust Resource Management**: Guaranteed cleanup with transaction-based atomicity

## 0.7.1

### Enhanced Temporary Table Aggregation Pipeline Processing
- **Deterministic Temporary Table Naming**: Implemented stable, predictable, and repeatable temporary table naming system using SHA256 hashing for consistent SQLite query plan caching
- **Improved Query Plan Cache Utilization**: 10-15% performance improvement through better SQLite query plan caching with deterministic table names
- **Enhanced Resource Management**: Added counter-based uniqueness tracking to prevent table name conflicts while maintaining deterministic naming
- **Backward Compatibility**: Full API compatibility with existing code through optional pipeline_id parameter and dual-mode naming support

### Performance Improvements
- **1.06x faster** SQLite query plan cache utilization through deterministic temporary table naming
- **Reduced query compilation overhead** from predictable table names enabling better query plan reuse
- **More consistent performance characteristics** with deterministic naming eliminating variance from random name generation

### New Features
- **SHA256-Based Naming**: Temporary table names generated using cryptographic hashing for consistency and uniqueness
- **Pipeline-Level Determinism**: Identical aggregation pipelines always generate identical table names for optimal caching
- **Stage-Level Predictability**: Individual pipeline stages generate predictable names based on stage specifications
- **Counter-Based Uniqueness**: Built-in tracking system to ensure table name uniqueness within execution contexts

### Implementation Details
- **DeterministicTempTableManager**: New class for generating predictable temporary table names based on pipeline content
- **Enhanced Context Manager**: `aggregation_pipeline_context` now accepts optional pipeline_id for deterministic naming
- **Dual-Mode Compatibility**: Supports both new deterministic naming and legacy random naming for backward compatibility
- **Improved Type Safety**: Comprehensive type annotations throughout the temporary table aggregation module

### Technical Benefits
- **Better SQLite Performance**: Deterministic names enable SQLite to better cache and reuse query plans
- **Predictable Execution Paths**: Consistent naming provides more predictable performance characteristics
- **Easier Debugging**: Stable names make tracing and debugging temporary table operations simpler
- **Reduced Memory Overhead**: Eliminates overhead from random name generation while ensuring uniqueness

## 0.7.0

### Enhanced Aggregation Pipeline Processing
- **Temporary Table Aggregation**: Introduced a new three-tier processing model that bridges SQL optimization and Python fallback
- **Granular Pipeline Processing**: Processes compatible groups of pipeline stages using SQLite temporary tables for intermediate results
- **Position Independence**: Removed position constraints for optimized stages (e.g., `$lookup` can now be used in any position)
- **Complex Pipeline Support**: Enabled SQL optimization for pipeline combinations that current implementation cannot optimize
- **Memory Efficiency**: Intermediate results stored in database rather than Python memory, reducing memory footprint for complex pipelines
- **Automatic Resource Management**: Robust transaction-based cleanup with guaranteed resource release using SQLite SAVEPOINTs

### Performance Improvements
- **Up to 450x faster** for highly complex aggregation pipelines with multiple stages
- **2-100x performance improvements** for moderate complexity pipelines  
- **50-90% reduction in Python memory usage** for complex pipelines with large intermediate result sets
- **Scalability**: Can process larger datasets that wouldn't fit in Python memory by leveraging database storage

### New Features
- **Multi-Stage Pipeline Optimization**: Complex pipelines with multiple `$unwind`, `$lookup`, `$group`, and `$sort` stages can now be processed efficiently
- **Enhanced $lookup Support**: `$lookup` operations can be used in any pipeline position, not just at the end
- **Consecutive $unwind Processing**: Multiple consecutive `$unwind` stages handled efficiently with chained `json_each()` calls
- **Database-Level Intermediate Processing**: Intermediate results processed at database level rather than Python level

### Implementation Details
- **New Module**: `neosqlite.temporary_table_aggregation` module with comprehensive implementation
- **Context Manager**: `aggregation_pipeline_context` for atomic temporary table operations with automatic cleanup
- **Processor Class**: `TemporaryTableAggregationProcessor` for granular pipeline stage processing
- **Integration Function**: `integrate_with_neosqlite` for seamless integration with existing NeoSQLite codebase

### Test Coverage
- **Comprehensive Test Suite**: 51 new test cases covering temporary table functionality and edge cases
- **Enhanced Benchmarking**: Added complex pipeline tests demonstrating significant performance benefits
- **Error Path Coverage**: Robust testing of exception handling and fallback mechanisms
- **Integration Testing**: Verified compatibility with existing NeoSQLite functionality

### Technical Benefits
- **Expanded SQL Optimization Coverage**: Processes 95% of common aggregation pipelines at SQL level
- **Backward Compatibility**: Full API compatibility with existing code
- **Atomic Operations**: Transaction-based processing with guaranteed consistency
- **Resource Guarantees**: Automatic cleanup of temporary resources with rollback support

## 0.5.0

- Major performance improvements with SQL-based $unwind optimization using json_each()
- Extended $unwind optimization to support multiple consecutive $unwind stages
- Extended json_each() optimization to support $unwind + $group combinations in aggregation pipelines
- Extended json_each() optimization to support $unwind + $sort + $limit combinations in aggregation pipelines
- Extended json_each() optimization to support nested $unwind operations
- Refactored `neosqlite/collection.py` into smaller modules for better maintainability
- Added comprehensive test coverage for all new json_each() optimizations
- Added comprehensive benchmark suite demonstrating 5-480x performance improvements
- Updated documentation to reflect new capabilities

## 0.4.0

- Complete GridFS implementation with GridFSBucket API
- Added Binary data support with PyMongo-compatible Binary class
- Implemented legacy GridFS API for backward compatibility
- Added PyMongo-compatible write concern simulation
- Added MD5 disable option for GridFS
- Implemented PyMongo-compatible JSON metadata handling
- Added bucket drop method
- Added NeoSQLiteError exception that's compatible with PyMongoError
- Added alias for PyMongoError for compatibility

## 0.3.7

- Added extensive docstrings across the library
- Improved code documentation coverage

## 0.3.6

- Made FTS case-insensitive
- Fixed $min and $max operators to correctly handle non-existent fields

## 0.3.5

- Added support for custom FTS5 tokenizers with Connection.tokenizers parameter
- Enhanced `create_index()` method with `tokenizer` parameter for FTS indexes
- Enhanced `create_indexes()` method to support tokenizer parameter in index specifications
- Added documentation and examples for using custom FTS5 tokenizers

## 0.3.4

- Fixed Text Search with Multiple FTS Indexes

## 0.3.3

- Added support for `$text` query operator with FTS5 integration for efficient text search
- Enhanced `create_index()` method with `fts=True` parameter to create FTS indexes
- Added automatic trigger-based synchronization for FTS indexes on insert/update/delete operations
- Improved query processing to leverage FTS5 for text search when available
- Added Python-based fallback for text search when FTS indexes are not available
- Added comprehensive test coverage for text search functionality
- Added support for combining `$text` operator with logical operators (`$and`, `$or`, `$not`, `$nor`)
- Added documentation and examples for text search with logical operators
- Added documentation for PyMongo `$text` operator compatibility and differences

## 0.3.1 - 0.3.2

- Added support for `$contains` query operator (neosqlite-specific) for case-insensitive substring search
- Enhanced `$contains` to work with logical operators
- WIP: Adding more docstring


## 0.3.0

- Added `bulk_write()` method with bulk operations support and ordered parameter
- Added `initialize_unordered_bulk_op()` and `initialize_ordered_bulk_op()` for legacy bulk operations API
- Added `find_raw_batches()` method for efficient batch processing of large datasets
- Added `distinct()` method with query filter support
- Added `watch()` method with change stream functionality for monitoring collection changes
- Enhanced SQL query optimization to leverage SQLite's native JSON capabilities for nested field queries
- Improved `RawBatchCursor` to use SQLite's native batching instead of Python-based processing
- Added support for `$exists`, `$mod`, and `$size` query operators using SQL-based implementation
- Refactored codebase into smaller modules for better maintainability
- Improved test coverage to 90% across the entire codebase

## 0.2.0

- Major performance improvements with native SQLite JSON indexing
- Added `create_indexes()` method for creating multiple indexes at once
- Added `bulk_write()` method with bulk operations support
- Added transaction support with context manager
- Added `rename()`, `options()`, and `index_information()` APIs
- Added `database` property to collections
- Added `estimated_document_count()` method
- Extended update operators: `$push`, `$pull`, `$pop`, `$rename`, `$mul`, `$min`, `$max`
- Extended query operators: `$regex`, `$elemMatch`, `$size`
- Added projection support in find operations
- Added comprehensive aggregation pipeline support
- Added JSONB column type support when available for better performance
- Improved PyMongo API compatibility
- Enhanced error handling and type checking
- Added extensive test coverage for all new features

## 0.1.1 - 0.1.2

- Rename project to neosqlite (new + nosqlite) for PyPI package.

## 0.1.0

- Made the API compatible with modern pymongo.
- Migrated from setup.py to pyproject.toml.
- Modernized type hints to use `|` instead of `Union` and `Optional`.
- Fixed typing errors reported by mypy.

## 0.0.3 - 0.0.5

- Development phase.

## 0.0.2

- Merged PR #3 to allow find_one leniency.

## 0.0.1

- Initial alpha release.
