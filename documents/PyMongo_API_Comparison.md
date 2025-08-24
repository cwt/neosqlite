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

## Missing Medium-Priority APIs

### Data Type Support
- [ ] Better ObjectId support
- [ ] Improved datetime handling
- [x] Binary data support (GridFS) - Partially implemented (GridFSBucket)

### Aggregation Enhancements
- [ ] map_reduce()
- [x] distinct() with query filter

### Utility Methods
- [x] watch() (change streams)
- [ ] parallel_scan()

### Text Search Enhancements
- [ ] Text scoring with $meta
- [ ] Advanced $text parameters ($language, $caseSensitive, $diacriticSensitive)
- [ ] Phrase search and term exclusion syntax

## Implementation Priority

1. **High Priority** - Essential for compatibility and basic functionality
2. **Medium Priority** - Important for enhanced capabilities
3. **Low Priority** - Specialized features that can be added later

## Note on API Evolution

This comparison was initially based on older PyMongo documentation that referenced `initialize_ordered_bulk_op()` and `initialize_unordered_bulk_op()` methods. However, in newer versions of PyMongo (4.x), these methods have been removed in favor of the simpler `bulk_write()` API that takes a list of operations and an `ordered` parameter.

neosqlite implements both the legacy API (initialize_ordered_bulk_op, initialize_ordered_bulk_op) for backward compatibility and the current PyMongo API (bulk_write with ordered parameter).
