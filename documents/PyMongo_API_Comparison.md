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
  - [x] upload_from_stream()
  - [x] upload_from_stream_with_id()
  - [x] download_to_stream()
  - [x] download_to_stream_by_name()
  - [x] open_upload_stream()
  - [x] open_upload_stream_with_id()
  - [x] open_download_stream()
  - [x] open_download_stream_by_name()
  - [x] delete()
  - [x] delete_by_name()
  - [x] rename()
  - [x] rename_by_name()
  - [x] find()
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

## Missing Medium-Priority APIs

### Data Type Support
- [ ] Better ObjectId support
- [ ] Improved datetime handling
- [x] Binary data support (outside of GridFS)

### Aggregation Enhancements
- [ ] map_reduce() - Will not implement (deprecated in MongoDB 4.2, removed in 5.0; use aggregation pipeline instead)
- [x] distinct() with query filter

### Utility Methods
- [x] watch() (change streams)
- [ ] parallel_scan()

### Text Search Enhancements
- [ ] Text scoring with $meta
- [ ] Advanced $text parameters ($language, $caseSensitive, $diacriticSensitive)
- [ ] Phrase search and term exclusion syntax

## Enhanced Features

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

## Implementation Priority

1. **High Priority** - Essential for compatibility and basic functionality
2. **Medium Priority** - Important for enhanced capabilities
3. **Low Priority** - Specialized features that can be added later

## Note on API Evolution

This comparison was initially based on older PyMongo documentation that referenced `initialize_ordered_bulk_op()` and `initialize_unordered_bulk_op()` methods. However, in newer versions of PyMongo (4.x), these methods have been removed in favor of the simpler `bulk_write()` API that takes a list of operations and an `ordered` parameter.

neosqlite implements both the legacy API (initialize_ordered_bulk_op, initialize_ordered_bulk_op) for backward compatibility and the current PyMongo API (bulk_write with ordered parameter).

## GridFS API Details

### GridFSBucket (Modern API)
The GridFSBucket implementation provides a complete PyMongo-compatible interface for storing and retrieving large files:

- **File Operations**: Direct upload/download methods with full control over the process
- **Stream Operations**: Open streams for reading/writing with fine-grained control
- **Management Operations**: Delete, rename, and find files with various criteria
- **Metadata Support**: Full support for file metadata in all operations
- **Error Handling**: Proper exception handling with PyMongo-compatible error types

### Legacy GridFS API
The legacy GridFS API provides a simpler interface that's familiar to users of older PyMongo versions:

- **Simple Operations**: put/get methods for straightforward file storage and retrieval
- **Version Management**: Automatic handling of file versions with the same name
- **Query Operations**: Find and filter files using familiar PyMongo patterns
- **Utility Methods**: List files, check existence, and manage file lifecycle

Both APIs work with the same underlying SQLite storage and are fully interoperable.
