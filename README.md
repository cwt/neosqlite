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
- **Modern API**: Aligned with modern `pymongo` practices (using methods like `insert_one`, `update_one`, `delete_many`, etc.).
- **Automatic JSON/JSONB Support**: Automatically detects and uses JSONB column type when available for better performance.
- **GridFS Support**: Store and retrieve large files with a PyMongo-compatible GridFS implementation.

## Performance Benchmarks

NeoSQLite includes comprehensive benchmarks demonstrating the performance benefits of its SQL optimizations:

- **Three-Tier Aggregation Pipeline Processing**: Expanded SQL optimization coverage to over 85% of common aggregation pipelines
- **Enhanced SQL Optimization Benchmark**: Covers additional optimizations like pipeline reordering and text search with array processing
- **Text Search + json_each() Benchmark**: Demonstrates specialized optimizations for text search on array fields

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

### Modern GridFSBucket API

The implementation provides a PyMongo-compatible GridFSBucket interface:

```python
import io
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket

# Create connection and GridFS bucket
with Connection(":memory:") as conn:
    bucket = GridFSBucket(conn.db)

    # Upload a file
    file_data = b"Hello, GridFS!"
    file_id = bucket.upload_from_stream("example.txt", file_data)

    # Download the file
    output = io.BytesIO()
    bucket.download_to_stream(file_id, output)
    print(output.getvalue().decode('utf-8'))
```

### Legacy GridFS API

For users familiar with the legacy PyMongo GridFS API, `NeoSQLite` also provides the simpler `GridFS` class:

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

For more comprehensive examples, see the examples directory.

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
- `$in` - Matches any of the values specified in an array
- `$nin` - Matches none of the values specified in an array
- `$exists` - Matches documents that have the specified field
- `$mod` - Performs a modulo operation on the value of a field and selects documents with a specified result
- `$size` - Matches the number of elements in an array
- `$regex` - Selects documents where values match a specified regular expression
- `$elemMatch` - Selects documents if element in the array field matches all the specified conditions
- `$contains` - **(NeoSQLite-specific and deprecated)** Performs a case-insensitive substring search on string values

Example usage of the `$contains` operator:
> **DEPRECATED**: The `$contains` operator is deprecated and will be removed in a future version. Please use the `$text` operator with FTS5 indexing for better performance.

```python
# Find users whose name contains "ali" (case-insensitive)
users.find({"name": {"$contains": "ali"}})

# Find users whose bio contains "python" (case-insensitive)
users.find({"bio": {"$contains": "python"}})
```

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

For more details on text search capabilities, see the [Text Search Documentation](documents/TEXT_SEARCH.md), [Text Search with Logical Operators](documents/TEXT_SEARCH_Logical_Operators.md), and [PyMongo Compatibility Information](documents/TEXT_SEARCH_PyMongo_Compatibility.md).

**Performance Notes:**
- The `$contains` operator performs substring searches using SQL `LIKE` with wildcards (`%value%`) at the database level
- This type of search does not efficiently use standard B-tree indexes and may result in full table scans
- The `$text` operator with FTS indexes provides much better performance for text search operations
- However, for simple substring matching, `$contains` is faster than `$regex` at the Python level because it uses optimized string operations instead of regular expression compilation and execution
- The operator is intended as a lightweight convenience feature for basic substring matching, not as a replacement for proper full-text search solutions
- For high-performance text search requirements, consider using SQLite's FTS (Full-Text Search) extensions or other specialized search solutions
- The `$contains` operator is a NeoSQLite-specific extension that is not part of the standard MongoDB query operators
- **Deprecation Notice**: The `$contains` operator is deprecated and will be removed in a future version. Please use the `$text` operator with FTS5 indexing for better performance.

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
