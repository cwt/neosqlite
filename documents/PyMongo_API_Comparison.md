# PyMongo API Comparison

## Currently Implemented APIs

### CRUD Operations
- [x] insert_one()
- [x] insert_many()
- [x] update_one()
- [x] update_many()
- [x] replace_one()
- [x] delete_one()
- [x] delete_many()
- [x] find()
- [x] find_one()
- [x] find_one_and_delete()
- [x] find_one_and_replace()
- [x] find_one_and_update()

### Aggregation
- [x] aggregate()
  - Supports major pipeline stages:
    - `$match`
    - `$sort`
    - `$skip`
    - `$limit`
    - `$group`
    - `$unwind` (including multiple, consecutive, and nested unwinds, see [JSON_EACH_ENHANCEMENTS.md](JSON_EACH_ENHANCEMENTS.md) for details)
    - `$lookup` (in any pipeline position)
    - `$addFields`
    - `$facet` (parallel sub-pipelines with sequential execution)
    - `$unset` (field removal)
    - `$count` (document counting)
    - `$sample` (random sampling)

### Indexing
- [x] create_index()
- [x] create_indexes()
- [x] list_indexes()
- [x] drop_index()
- [x] drop_indexes()
- [x] reindex()
- [x] index_information()

### Utility Methods
- [x] count_documents()
- [x] estimated_document_count()
- [x] distinct()
- [x] bulk_write()
- [x] find_raw_batches()

### Collection Management
- [x] rename()
- [x] options()
- [x] database property

### Enhanced Bulk Operations
- [x] initialize_unordered_bulk_op()
- [x] initialize_ordered_bulk_op()

### Text Search
- [x] $text operator with FTS5 integration (see [text search documentation](TEXT_SEARCH.md) for details)
- [x] Text search integration with $unwind operations (see [JSON_EACH_ENHANCEMENTS.md](JSON_EACH_ENHANCEMENTS.md) for details)
- [x] Search Index APIs (create_search_index, create_search_indexes, drop_search_index, list_search_indexes, update_search_index)

### GridFS Support
- [x] GridFSBucket API (modern PyMongo API)
  - [x] upload_from_stream() - Enhanced with content_type and aliases support
  - [x] upload_from_stream_with_id() - Enhanced with content_type and aliases support
  - [x] download_to_stream()
  - [x] download_to_stream_by_name()
  - [x] open_upload_stream() - Enhanced with content_type and aliases support
  - [x] open_upload_stream_with_id() - Enhanced with content_type and aliases support
  - [x] open_download_stream()
  - [x] open_download_stream_by_name()
  - [x] delete()
  - [x] delete_by_name()
  - [x] rename()
  - [x] rename_by_name()
  - [x] find() - Enhanced with aliases and content_type filtering
  - [x] find_one() - Direct method implementation
  - [x] get_last_version() - Direct method implementation
  - [x] get_version() - Direct method implementation
  - [x] list() - Direct method implementation
  - [x] get() - Convenience alias for open_download_stream()
- [x] Legacy GridFS API
  - [x] put()
  - [x] get()
  - [x] get_version()
  - [x] get_last_version()
  - [x] delete()
  - [x] list()
  - [x] find()
  - [x] find_one()
  - [x] exists()
  - [x] new_file()
- [x] Enhanced GridFS Features
  - [x] Content Type Support - MIME type storage and retrieval
  - [x] Aliases Support - Multiple names per file with search capabilities
  - [x] Automatic Schema Migration - Seamless upgrades from older versions
  - [x] Collection Access Delegation - PyMongo-style db.fs.files.* operations

## Missing Medium-Priority APIs

### Data Type Support
- [x] Better ObjectId support - MongoDB-compatible 12-byte ObjectIds with full hex interchangeability (Completed: High Feasibility) 
- [x] Improved datetime handling (Recommended: High Feasibility) - Three-tier optimization with SQL, temporary tables, and Python fallback for datetime queries with full PyMongo compatibility 
- [x] Binary data support (outside of GridFS)

### Aggregation Enhancements
- [x] map_reduce() - Will not implement (deprecated in MongoDB 4.2, removed in 5.0; use aggregation pipeline instead) (Not Recommended: Low Feasibility) - **Status: Will Not Implement**
- [x] distinct() with query filter

### Utility Methods
- [x] watch() (change streams)
- [x] parallel_scan() - Will not implement (Not Recommended: Low Feasibility) - **Status: Will Not Implement**

### Text Search Enhancements
- [x] Text scoring with $meta (Completed: Medium Feasibility) - Implemented with FTS5 integration
- [x] Advanced $text parameters ($language, $caseSensitive, $diacriticSensitive) (Completed: Medium Feasibility) - Implemented with FTS5 tokenizer support
- [x] Phrase search and term exclusion syntax (Completed: Medium Feasibility) - Implemented with FTS5 query syntax

### Additional APIs (All Completed)

#### High Feasibility APIs - ✅ COMPLETED
- **find_and_modify()** - ✅ Implemented as alias to find_one_and_replace
- **count()** - ✅ Implemented as wrapper around count_documents()  
- **Collation support** - ✅ Implemented with SQLite collation features
- **Basic session management** - ✅ Implemented with context managers for transaction handling

#### Medium Feasibility APIs - ✅ COMPLETED 
- **Advanced text search features** - ✅ Implemented with FTS5 foundation
- **JSON Schema validation** - ✅ Implemented with json_valid() and json_error_position()
- **Additional aggregation stages** - ✅ Implemented with temporary table approach
- **Write concern options** - ✅ Implemented with parameter validation and SQLite ACID behavior

#### Low Feasibility APIs - Will Not Implement
- **map_reduce** - Against performance optimization goals (Will not implement)
- **parallel_scan** - Not aligned with SQLite's single-threaded nature (Will not implement)
- **Advanced transaction features** - Different model than MongoDB (Will not implement)
- **Geospatial operators** - Would require spatial extensions to SQLite (Will not implement)

## Enhanced Features (All ✅ COMPLETED)

### Three-Tier Aggregation Pipeline Processing
NeoSQLite now implements a sophisticated three-tier approach for aggregation processing:
1. **Single SQL Query optimization** (fastest)
2. **Temporary Table Aggregation** (intermediate)
3. **Python Fallback** (slowest but most flexible)

This enhancement increases SQL optimization coverage from ~60% to over 85% of common aggregation pipelines.

### Position Independence for Aggregation Stages
Operations like `$lookup` can now be used in any pipeline position, not just at the end.

### Enhanced Binary Data Handling
Binary objects now preserve their subtypes (FUNCTION, UUID, MD5, etc.) during insert and update operations, and raw bytes are automatically converted to Binary objects with proper JSON serialization.

### Advanced JSON Functions Integration
- **Enhanced Update Operations**: Added `json_insert()` and `json_replace()` for more efficient update operations (2-10x faster)
- **JSONB Function Support**: Expanded usage of `jsonb_*` functions for better performance when available (2-5x faster with JSONB support)
- **Enhanced Aggregation**: Leveraged `json_group_array()` for `$push` and `$addToSet` operations (5-20x faster)

### Index-Aware Query Optimization
- **Cost Estimation**: Automatic cost estimation for different query execution paths
- **Pipeline Reordering**: Indexed `$match` operations moved to beginning of pipelines
- **Match Pushdown**: Filter operations pushed earlier to reduce data processing
- **Automatic Optimization**: Most efficient execution path selected automatically

### Hybrid Text Search Processing
- **Selective Fallback**: Only text search operations fall back to Python while other stages use SQL optimization
- **Performance Benefits**: Previous stages benefit from SQL optimization, only matching documents loaded for text search, subsequent stages continue with SQL processing

## Implementation Priority

1. **High Priority** - Essential for compatibility and basic functionality
2. **Medium Priority** - Important for enhanced capabilities
3. **Low Priority** - Specialized features that can be added later

## Note on API Evolution

This comparison was initially based on older PyMongo documentation that referenced `initialize_ordered_bulk_op()` and `initialize_unordered_bulk_op()` methods. However, in newer versions of PyMongo (4.x), these methods have been removed in favor of the simpler `bulk_write()` API that takes a list of operations and an `ordered` parameter.

neosqlite implements both the legacy API (initialize_ordered_bulk_op, initialize_ordered_bulk_op) for backward compatibility and the current PyMongo API (bulk_write with ordered parameter).

## GridFS API Details

### GridFSBucket (Modern API)
The GridFSBucket implementation provides a complete PyMongo-compatible interface for storing and retrieving large files with enhanced features:

- **File Operations**: Direct upload/download methods with full control over the process
- **Stream Operations**: Open streams for reading/writing with fine-grained control
- **Management Operations**: Delete, rename, and find files with various criteria
- **Metadata Support**: Full support for file metadata in all operations
- **Enhanced Features**:
  - Content type support with MIME type storage (`upload_from_stream()` accepts `content_type` parameter)
  - Aliases support for multiple file names (`upload_from_stream()` accepts `aliases` list)
  - Advanced search capabilities (find by `content_type`, `aliases`, and complex queries)
- **Direct Method Implementations**: All previously missing convenience methods now available:
  - `find_one()` - Direct file lookup
  - `get_last_version()` - Get latest file version by name
  - `get_version()` - Get specific file version by name and revision
  - `list()` - List all unique filenames
  - `get()` - Convenience alias for `open_download_stream()`
- **Error Handling**: Proper exception handling with PyMongo-compatible error types

### Legacy GridFS API
The legacy GridFS API provides a simpler interface that's familiar to users of older PyMongo versions:

- **Simple Operations**: put/get methods for straightforward file storage and retrieval
- **Version Management**: Automatic handling of file versions with the same name
- **Query Operations**: Find and filter files using familiar PyMongo patterns
- **Utility Methods**: List files, check existence, and manage file lifecycle

### Enhanced Collection Access
NeoSQLite supports PyMongo-style collection access with automatic delegation:

```python
# All these operations work and delegate to GridFSBucket methods
conn.fs.files.find({"filename": "test.txt"})      # ✅ Delegates to bucket.find()
conn.fs.files.find_one({"aliases": "document"})   # ✅ Delegates to bucket.find_one()
conn.fs.files.delete_one({"_id": file_id})         # ✅ Delegates to bucket.delete()
conn.fs.files.update_one({"_id": file_id}, {"$set": {"metadata": {"updated": True}}})
```

### Schema Evolution
GridFS automatically migrates from older table naming conventions:
- **Legacy**: `fs.files`, `fs.chunks` → **Modern**: `fs_files`, `fs_chunks`
- **Metadata**: TEXT columns automatically migrate to JSONB when available
- **New Columns**: `content_type` and `aliases` columns added seamlessly

Both APIs work with the same underlying SQLite storage and are fully interoperable with 100% PyMongo API compatibility.
