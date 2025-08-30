# CHANGELOG

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
