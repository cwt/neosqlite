# NeoSQLite - NoSQL for SQLite with PyMongo-like API

[![PyPI Version](https://img.shields.io/pypi/v/neosqlite.svg)](https://pypi.org/project/neosqlite/)

`NeoSQLite` (new + nosqlite) is a pure Python library that provides a schemaless, `PyMongo`-like wrapper for interacting with SQLite databases. The API is designed to be familiar to those who have worked with `PyMongo`, providing a simple and intuitive way to work with document-based data in a relational database.

NeoSQLite brings NoSQL capabilities to SQLite, offering a NoSQLite solution for developers who want the flexibility of NoSQL with the reliability of SQLite. This library serves as a bridge between NoSQL databases and SQLite, providing PyMongo compatibility for Python developers.

**Keywords**: NoSQL, NoSQLite, SQLite NoSQL, PyMongo alternative, SQLite document database, Python NoSQL, schemaless SQLite, MongoDB-like SQLite

[![NeoSQLite: SQLite with a MongoDB Disguise](https://img.youtube.com/vi/iZXoEjBaFdU/0.jpg)](https://www.youtube.com/watch?v=iZXoEjBaFdU)

## Features

- **`PyMongo`-like API**: A familiar interface for developers experienced with MongoDB.
- **Schemaless Documents**: Store flexible JSON-like documents.
- **Lazy Cursor**: `find()` returns a memory-efficient cursor for iterating over results.
- **Raw Batch Support**: `find_raw_batches()` returns raw JSON data in batches for efficient processing.
- **Advanced Indexing**: Supports single-key, compound-key, and nested-key indexes.
- **Text Search**: Full-text search capabilities using SQLite's FTS5 extension with the `$text` operator.
- **Modern API**: Aligned with modern `pymongo` practices (using methods like `insert_one`, `update_one`, `delete_many`, etc.)
- **70+ New MongoDB-compatible APIs**: Massive expansion including Bitwise operators, positional array updates (`$`, `$[]`, `$[identifier]`), and complex aggregation stages (`$bucket`, `$unionWith`, `$merge`).
- **Comprehensive Security Hardening**: Built-in SQL injection protection using centralized identifier and table name quoting across all engine layers.
- **MongoDB-compatible ObjectId**: Full 12-byte ObjectId implementation with automatic generation and hex interchangeability
- **Automatic JSON/JSONB Support**: Automatically detects and uses JSONB column type for better performance
- **Full GridFS Support**: Complete PyMongo-compatible GridFS with modern GridFSBucket API, legacy API, and schema migration
- **Modular Architecture (v1.8.0)**: Major refactoring transformed monolithic modules into well-organized packages for improved maintainability
- **Python 3.10+ Modernization**: Leveraging modern Python features like walrus operators and union type hints

See [CHANGELOG.md](CHANGELOG.md) for the latest features and improvements.

## v1.8.0 Major Refactoring Release

NeoSQLite v1.8.0 is a **major refactoring and quality release** that transforms the internal architecture for improved maintainability, readability, and long-term sustainability.

### Key Achievement

Successfully converted **3 massive monolithic modules** into well-organized modular packages, reducing individual file sizes from **2,000-4,700 lines** to manageable **50-500 line modules** while maintaining **100% backward compatibility**.

### Modular Package Architecture

**query_helper/ Package**: Transformed from a single 4,731-line file into 9 focused modules:
- `aggregation.py` - Aggregation pipeline logic
- `crud_operations.py` - Insert, update, delete operations
- `query_builder.py` - SQL query construction
- `update_operations.py` - Update operator implementations
- `positional_update.py` - Array positional operators
- `query_optimizer.py` - Query optimization
- Plus utility modules

**expr_evaluator/ Package**: Split from ~2,300 lines into 6 modules:
- `python_evaluators.py` - Python-based evaluation
- `sql_converters.py` - SQL expression conversion
- `constants.py`, `context.py`, `type_utils.py` - Supporting utilities

**query_engine/ Package**: Refactored from 2,038 lines into 5 modules using mixin architecture:
- `__init__.py` - Main QueryEngine class
- `crud_operations.py` - CRUD operations mixin
- `find_operations.py` - Find operations mixin
- `query_methods.py` - Count, distinct methods
- `base.py` - Protocol base class

### Code Deduplication

v1.8.0 eliminates duplicated code across the codebase:
- **Type utilities**: Centralized 14 type conversion functions, eliminating ~200 lines of duplication
- **Datetime detection**: Pre-compiled regex patterns, removing 95 lines of duplicated logic
- **Schema inspection**: Unified schema utilities across GridFS and collection modules
- **SQLite imports**: Single centralized import source for all modules
- **GridFS serialization**: Consolidated metadata handling utilities

### Python 3.10+ Modernization

- **Walrus operators**: Assignment expressions for more concise code
- **Union type hints**: Modern `int | None` syntax instead of `Optional[int]`
- **Better IDE support**: Enhanced type checking and linting capabilities

### Requirements

**Minimum Python Version**: 3.10+

The modernization to Python 3.10+ syntax means v1.8.0 requires Python 3.10 or later.

### Benefits

**For Users**: No breaking changes - all existing code continues to work without modification.

**For Contributors**: Easier onboarding, clearer module responsibilities, reduced merge conflicts, and better tooling support.

For more details, see [documents/releases/v1.8.0.md](documents/releases/v1.8.0.md).

## PyMongo Compatibility Tests

NeoSQLite maintains comprehensive PyMongo compatibility tests to ensure MongoDB-compatible behavior. Our automated test suite covers all major API categories:

### Test Results (v1.8.0+)

#### Unit Tests

| Metric | Result |
|--------|--------|
| **Total Tests** | 2,120 |
| **Passed** | 2,123 |
| **Failed** | 0 |
| **XFailed** | 5 (expected failures) |
| **XPassed** | 2 (unexpected successes) |
| **Code Coverage** | 82%+ |

#### API Comparison Tests

| Metric | v1.7.0 | v1.8.0 | v1.8.0+ |
|--------|--------|--------|---------|
| **Total Tests** | 304 | 304 | **329** |
| **Passed** | 300 | 300 | **319** |
| **Skipped** | 4 | 4 | **10** |
| **Failed** | 0 | 0 | 0 |
| **Compatibility** | 100% | 100% | **100%** |

**Skipped Tests Note**: The 10 skipped tests are due to architectural differences or environment limitations, not missing implementations:
1. `options()` - NeoSQLite returns detailed SQLite schema info (`{'columns': [...], 'indexes': [...]}`) while MongoDB returns `{}`. Backend-specific difference.
2. `$log2` - **NeoSQLite extension** using SQLite's native `log2()` function. Raises `UserWarning` about MongoDB incompatibility.
3. `watch()` (Collection & Database) - **Fully implemented in NeoSQLite** via SQLite triggers but cannot be compared because MongoDB requires a replica set for change streams.
4. `transaction_commit` / `transaction_abort` - **Fully implemented in NeoSQLite** via `ClientSession` but skipped in comparison because MongoDB requires a replica set for multi-document transactions.
5. `db_path` (Collection & Database) - **NeoSQLite extension** providing the underlying SQLite database file path. No MongoDB equivalent.
6. `initialize_ordered_bulk_op()` / `initialize_unordered_bulk_op()` - **Deprecated in NeoSQLite** to match PyMongo 4.x behavior.
7. `where()` - **NeoSQLite implementation** using Python function filter. MongoDB uses JavaScript `$where` which requires a JS engine.

All comparable MongoDB APIs are tested with 100% compatibility.

**Note on Removed Operators**: Two non-MongoDB operators (`$toBinData`, `$toRegex`) were removed in v1.6.1 to maintain 100% MongoDB API compatibility. These were experimental NeoSQLite extensions that never existed in MongoDB. Unlike `$log2` (which is kept as a convenient mathematical shorthand with explicit warnings), these type conversion operators could cause subtle data type issues and had no clear MongoDB equivalent. Use the standard `Binary()` constructor for binary data and Python's `re.compile()` or `$regexMatch` for regex patterns instead.

### Running the Tests
To run the PyMongo compatibility tests, install PyMongo first and ensure that either Podman or Docker is installed on your system.

```bash
./scripts/run-api-comparison.sh
```

For more details, see the [`examples/api_comparison/`](examples/api_comparison/) package and [`examples/api_comparison/README.md`](examples/api_comparison/README.md).

## Performance Benchmarks

NeoSQLite includes comprehensive benchmarks demonstrating the performance benefits of its SQL optimizations:

- **Three-Tier Aggregation Pipeline Processing**: Expanded SQL optimization coverage to over 85% of common aggregation pipelines
- **Enhanced SQL Optimization Benchmark**: Covers additional optimizations like pipeline reordering and text search with array processing
- **Text Search + json_each() Benchmark**: Demonstrates specialized optimizations for text search on array fields

See [`documents/AGGREGATION_PIPELINE_OPTIMIZATION.md`](documents/AGGREGATION_PIPELINE_OPTIMIZATION.md) for complete architecture details, operator support matrix, and performance benchmarks (10-100x speedup).

See the [`examples/`](examples/) directory for detailed benchmark implementations and results.

## Drop-in Replacement for PyMongo and NoSQL Solutions

For many common use cases, `NeoSQLite` can serve as a drop-in replacement for `PyMongo`. The API is designed to be compatible, meaning you can switch from MongoDB to a SQLite backend with minimal code changes. The primary difference is in the initial connection setup.

Once you have a `collection` object, the method calls for all implemented APIs are identical.

**PyMongo:**

```python
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client.mydatabase
collection = db.mycollection
```

**NeoSQLite (NoSQLite solution):**

```python
import neosqlite
# The Connection object is analogous to the database
client = neosqlite.Connection('mydatabase.db')
collection = client.mycollection
```

After the setup, your application logic for interacting with the collection remains the same:

```python
# This code works for both pymongo and neosqlite
collection.insert_one({"name": "test_user", "value": 123})
document = collection.find_one({"name": "test_user"})
print(document)
```

## Installation

```bash
pip install neosqlite
```

For enhanced JSON/JSONB support on systems where the built-in SQLite doesn't support these features, you can install with the `jsonb` extra:

```bash
pip install neosqlite[jsonb]
```

For memory-constrained processing of large result sets, you can install with the `memory-constrained` extra which includes the `quez` library:

```bash
pip install neosqlite[memory-constrained]
```

This will install `quez` which provides compressed in-memory queues for handling large aggregation results with reduced memory footprint.

You can also install multiple extras:

```bash
pip install neosqlite[jsonb,memory-constrained]
```

**Note**: `NeoSQLite` will work with any SQLite installation. The `jsonb` extra is only needed if:
1. Your system's built-in SQLite doesn't support JSON functions, **and**
2. You want to take advantage of JSONB column type for better performance with JSON operations

If your system's SQLite already supports JSONB column type, `NeoSQLite` will automatically use them without needing the extra dependency.

## Quickstart

Here is a quick example of how to use `NeoSQLite`:

```python
import neosqlite

# Connect to an in-memory database
with neosqlite.Connection(':memory:') as conn:
    # Get a collection
    users = conn.users

    # Insert a single document
    users.insert_one({'name': 'Alice', 'age': 30})

    # Insert multiple documents
    users.insert_many([
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 35}
    ])

    # Find a single document
    alice = users.find_one({'name': 'Alice'})
    print(f"Found user: {alice}")

    # Find multiple documents and iterate using the cursor
    print("\nAll users:")
    for user in users.find():
        print(user)

    # Update a document
    users.update_one({'name': 'Alice'}, {'$set': {'age': 31}})
    print(f"\nUpdated Alice's age: {users.find_one({'name': 'Alice'})}")

    # Delete documents
    result = users.delete_many({'age': {'$gt': 30}})
    print(f"\nDeleted {result.deleted_count} users older than 30.")

    # Count remaining documents
    print(f"There are now {users.count_documents({})} users.")

    # Process documents in raw batches for efficient handling of large datasets
    print("\nProcessing documents in batches:")
    cursor = users.find_raw_batches(batch_size=2)
    for i, batch in enumerate(cursor, 1):
        # Each batch is raw bytes containing JSON documents separated by newlines
        batch_str = batch.decode('utf-8')
        doc_strings = [s for s in batch_str.split('\n') if s]
        print(f"  Batch {i}: {len(doc_strings)} documents")
```

## JSON/JSONB Support

`NeoSQLite` automatically detects JSON support in your SQLite installation:

- **With JSON/JSONB support**: Uses JSONB column type for better performance with JSON operations
- **Without JSON support**: Falls back to TEXT column type with JSON serialization

The library will work correctly in all environments - the `jsonb` extra is completely optional and only needed for enhanced performance on systems where the built-in SQLite doesn't support JSONB column type.

## Binary Data Support

`NeoSQLite` now includes full support for binary data outside of GridFS through the `Binary` class, which provides a PyMongo-compatible interface for storing and retrieving binary data directly in documents:

```python
from neosqlite import Connection, Binary

# Create connection
with Connection(":memory:") as conn:
    collection = conn.my_collection

    # Store binary data in a document
    binary_data = Binary(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")
    collection.insert_one({
        "name": "binary_example",
        "data": binary_data,
        "metadata": {"description": "Binary data example"}
    })

    # Retrieve and use the binary data
    doc = collection.find_one({"name": "binary_example"})
    retrieved_data = doc["data"]  # Returns Binary instance
    raw_bytes = bytes(retrieved_data)  # Convert to bytes if needed

    # Query with binary data
    docs = list(collection.find({"data": binary_data}))
```

The `Binary` class supports different subtypes for specialized binary data:
- `Binary.BINARY_SUBTYPE` (0) - Default for general binary data
- `Binary.UUID_SUBTYPE` (4) - For UUID data with `Binary.from_uuid()` and `as_uuid()` methods
- `Binary.FUNCTION_SUBTYPE` (1) - For function data
- And other standard BSON binary subtypes

For large file storage, continue to use the GridFS support which is optimized for that use case.

## MongoDB-compatible ObjectId Support

`NeoSQLite` now includes full MongoDB-compatible ObjectId support with automatic generation and hex interchangeability:

```python
from neosqlite import Connection

# Create connection
with Connection(":memory:") as conn:
    collection = conn.my_collection

    # Insert document without _id - ObjectId automatically generated
    result = collection.insert_one({"name": "auto_id_doc", "value": 123})
    doc = collection.find_one({"_id": result.inserted_id})  # Uses integer ID returned from insert
    print(f"Document with auto-generated ObjectId: {doc}")

    # Document now has an ObjectId in the _id field
    print(f"Auto-generated ObjectId: {doc['_id']}")
    print(f"Type of _id: {type(doc['_id'])}")

    # Insert document with manual _id
    from neosqlite.objectid import ObjectId
    manual_oid = ObjectId()
    collection.insert_one({"_id": manual_oid, "name": "manual_id_doc", "value": 456})

    # Find using ObjectId
    found_doc = collection.find_one({"_id": manual_oid})
    print(f"Found document with manual ObjectId: {found_doc}")

    # Query using hex string (interchangeable with PyMongo)
    hex_result = collection.find_one({"_id": str(manual_oid)})
    print(f"Found document using hex string: {hex_result}")

    # Automatic ID type correction makes querying more robust
    # These all work automatically without requiring exact type matching:
    found1 = collection.find_one({"id": manual_oid})  # Corrected to query _id field
    found2 = collection.find_one({"id": str(manual_oid)})  # Corrected to query _id field
    found3 = collection.find_one({"_id": "123"})  # Corrected to integer 123
```

The ObjectId implementation automatically corrects common ID type mismatches:
- Queries using `id` field with ObjectId/hex string are automatically redirected to `_id` field
- Queries using `_id` field with integer strings are automatically converted to integers
- Works across all CRUD operations (find, update, delete, etc.) for enhanced robustness

The ObjectId implementation:
- Follows MongoDB's 12-byte specification (timestamp + random + PID + counter)
- Automatically generates ObjectIds when no `_id` is provided during insertion
- Uses dedicated `_id` column with unique indexing for performance
- Provides full hex string interchangeability with PyMongo ObjectIds
- Maintains complete backward compatibility: existing documents keep integer ID as `_id` until updated
- New documents get MongoDB-compatible ObjectId in `_id` field (integer ID still available in `id` field)
- Uses JSONB type for optimized storage when available
- Supports querying with both ObjectIds and integer IDs in the `_id` field

### Enhanced GridFSBucket API

NeoSQLite provides a complete PyMongo-compatible GridFSBucket interface:

```python
import io
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket

# Create connection and GridFS bucket
with Connection(":memory:") as conn:
    bucket = GridFSBucket(conn.db)

    # Upload files
    text_file_id = bucket.upload_from_stream(
        "document.txt",
        b"Hello, GridFS!"
    )

    image_file_id = bucket.upload_from_stream(
        "photo.jpg",
        b"fake_jpeg_data"
    )

    # Download files
    file = bucket.open_download_stream(text_file_id)
    print(f"Filename: {file.filename}")  # "document.txt"
    print(f"Data: {file.read().decode('utf-8')}")  # "Hello, GridFS!"

    # Find files
    files = list(bucket.find({"filename": "document.txt"}))

    # Delete files
    bucket.delete(text_file_id)
```

For more comprehensive examples including streaming operations and advanced querying, see the examples directory and [GridFS Documentation](documents/GRIDFS.md).

### Legacy GridFS API

For users familiar with the legacy PyMongo GridFS API, NeoSQLite also provides the simpler `GridFS` class:

```python
import io
from neosqlite import Connection
from neosqlite.gridfs import GridFS

# Create connection and legacy GridFS instance
with Connection(":memory:") as conn:
    fs = GridFS(conn.db)

    # Put a file
    file_data = b"Hello, legacy GridFS!"
    file_id = fs.put(file_data, filename="example.txt")

    # Get the file
    grid_out = fs.get(file_id)
    print(grid_out.read().decode('utf-8'))
```

### Collection Access with Auto-Delegation

NeoSQLite supports PyMongo-style collection access with automatic GridFS delegation:

```python
# All operations delegate to GridFSBucket methods
files = conn.fs.files.find({"filename": "document.txt"})
conn.fs.files.delete_one({"_id": file_id})
conn.fs.files.update_one({"_id": file_id}, {"$set": {"metadata": {"archived": True}}})
```

## Indexes

Indexes can significantly speed up query performance. `NeoSQLite` supports single-key, compound-key, and nested-key indexes.

```python
# Create a single-key index
users.create_index('age')

# Create a compound index
users.create_index([('name', neosqlite.ASCENDING), ('age', neosqlite.DESCENDING)])

# Create an index on a nested key
users.insert_one({'name': 'David', 'profile': {'followers': 100}})
users.create_index('profile.followers')

# Create multiple indexes at once
users.create_indexes([
    'age',
    [('name', neosqlite.ASCENDING), ('age', neosqlite.DESCENDING)],
    'profile.followers'
])

# Create FTS search indexes for text search
users.create_search_index('bio')
users.create_search_indexes(['title', 'content', 'description'])
```

Indexes are automatically used by `find()` operations where possible. You can also provide a `hint` to force the use of a specific index.

## Query Operators

`NeoSQLite` supports various query operators for filtering documents:

- `$eq` - Matches values that are equal to a specified value
- `$gt` - Matches values that are greater than a specified value
- `$gte` - Matches values that are greater than or equal to a specified value
- `$lt` - Matches values that are less than a specified value
- `$lte` - Matches values that are less than or equal to a specified value
- `$ne` - Matches all values that are not equal to a specified value
- `$all` - Matches any of the values specified in an array
- `$nin` - Matches none of the values specified in an array
- `$exists` - Matches documents that have the specified field
- `$bitsAllSet` / `$bitsAllClear` - Matches documents where specified bits are all set or clear
- `$bitsAnySet` / `$bitsAnyClear` - Matches documents where any of the specified bits are set or clear
- `$mod` - Performs a modulo operation on the value of a field and selects documents with a specified result
- `$size` - Matches the number of elements in an array
- `$regex` - Selects documents where values match a specified regular expression
- `$elemMatch` - Selects documents if array element matches specified conditions. **Enhanced**: Supports both simple value matching (`{"tags": {"$elemMatch": "c"}}`) and complex object matching (`{"tags": {"$elemMatch": {"name": "value"}}}`)
- `$`, `$[]`, `$[<identifier>]` - Positional operators for array updates with `array_filters` support
- `$contains` - **(Deprecated)** Performs case-insensitive substring search. **Will be removed in a future version**. Use `$text` with FTS5 indexing instead

## Text Search with $text Operator

NeoSQLite supports efficient full-text search using the `$text` operator, which leverages SQLite's FTS5 extension:

```python
# Create FTS index on content field
articles.create_index("content", fts=True)

# Perform text search
results = articles.find({"$text": {"$search": "python programming"}})
```

### Dedicated Search Index APIs

NeoSQLite also provides dedicated search index APIs for more explicit control:

```python
# Create a single search index
articles.create_search_index("content")

# Create multiple search indexes at once
articles.create_search_indexes(["title", "content", "description"])

# List all search indexes
indexes = articles.list_search_indexes()

# Drop a search index
articles.drop_search_index("content")

# Update a search index (drops and recreates)
articles.update_search_index("content")
```

### Custom FTS5 Tokenizers

NeoSQLite supports custom FTS5 tokenizers for improved language-specific text processing:

```python
# Load custom tokenizer when creating connection
conn = neosqlite.Connection(":memory:", tokenizers=[("icu", "/path/to/libfts5_icu.so")])

# Create FTS index with custom tokenizer
articles.create_index("content", fts=True, tokenizer="icu")

# For language-specific tokenizers like Thai
conn = neosqlite.Connection(":memory:", tokenizers=[("icu_th", "/path/to/libfts5_icu_th.so")])
articles.create_index("content", fts=True, tokenizer="icu_th")
```

Custom tokenizers can significantly improve text search quality for languages that don't use spaces between words (like Chinese, Japanese, Thai) or have complex tokenization rules.

For more information about building and using custom FTS5 tokenizers, see the [FTS5 ICU Tokenizer project](https://github.com/cwt/fts5-icu-tokenizer) ([SourceHut mirror](https://sr.ht/~cwt/fts5-icu-tokenizer/)).

For more details on text search capabilities, see the [Text Search Documentation](documents/TEXT_SEARCH.md).

## Memory-Constrained Processing

For applications that process large aggregation result sets, NeoSQLite provides memory-constrained processing through integration with the `quez` library. This optional feature compresses intermediate results in-memory, significantly reducing memory footprint for large datasets.

To enable memory-constrained processing:

```python
# Install with memory-constrained extra
# pip install neosqlite[memory-constrained]

# Enable quez processing on aggregation cursors
cursor = collection.aggregate(pipeline)
cursor.use_quez(True)

# Process results incrementally without loading all into memory
for doc in cursor:
    process_document(doc)  # Each document is decompressed and returned one at a time
```

The `quez` library provides:
- Compressed in-memory buffering using pluggable compression algorithms (zlib, bz2, lzma, zstd, lzo)
- Thread-safe queue implementations for both synchronous and asynchronous applications
- Real-time observability with compression ratio statistics
- Configurable batch sizes for memory management

This approach is particularly beneficial for:
- Large aggregation pipelines with many results
- Applications with limited memory resources
- Streaming processing of database results
- Microservices that need to forward results to other services

**Current Limitations**:
- Threshold control is memory-based, not document count-based
- Uses default quez compression algorithm (Zlib)

**Future Enhancement Opportunities**:
- Document count threshold control
- Compression algorithm selection
- More granular memory management controls
- Exposed quez queue statistics during processing

## Sorting

You can sort the results of a `find()` query by chaining the `sort()` method.

```python
# Sort users by age in descending order
for user in users.find().sort('age', neosqlite.DESCENDING):
    print(user)
```

## Contribution and License

This project was originally developed as [shaunduncan/nosqlite](https://github.com/shaunduncan/nosqlite) and was later forked as [plutec/nosqlite](https://github.com/plutec/nosqlite) before becoming NeoSQLite. It is now maintained by Chaiwat Suttipongsakul and is licensed under the MIT license.

Contributions are highly encouraged. If you find a bug, have an enhancement in mind, or want to suggest a new feature, please feel free to open an issue or submit a pull request.
