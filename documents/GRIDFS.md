# NeoSQLite GridFS Documentation

This comprehensive guide covers NeoSQLite's GridFS implementation, including API compatibility with PyMongo, key differences, migration considerations, and all available features.

## Table of Contents

- [API Compatibility Matrix](#api-compatibility-matrix)
- [Key Differences from PyMongo](#key-differences-from-pymongo)
- [GridFS Features](#gridfs-features)
- [Migration Guide](#migration-guide)
- [Performance Considerations](#performance-considerations)
- [Recommendations](#recommendations)
- [Conclusion](#conclusion)

## API Compatibility Matrix

| Feature | PyMongo GridFS | NeoSQLite GridFS | Status |
|---------|----------------|------------------|--------|
| **GridFSBucket Creation** | `GridFSBucket(database)` | `GridFSBucket(db)` | ✅ Compatible |
| **upload_from_stream()** | ✅ Returns ObjectId | ✅ Returns ObjectId + content_type/aliases support | ✅ Compatible + Enhanced |
| **open_download_stream()** | ✅ Accepts ObjectId/int | ✅ Accepts ObjectId/int/str | ✅ Compatible |
| **delete()** | ✅ `delete(file_id)` | ✅ `delete(file_id)` | ✅ Compatible |
| **find()** | ✅ Returns GridFSFile cursor | ✅ Returns GridOut cursor | ✅ Compatible |
| **find_one()** | ✅ Available | ✅ Available (direct method + collection delegation) | ✅ Compatible |
| **rename()** | ✅ `rename(file_id, new_name)` | ✅ `rename(file_id, new_name)` | ✅ Compatible |
| **drop()** | ✅ `drop()` | ✅ `drop()` | ✅ Compatible |
| **get_last_version()** | ✅ Available | ✅ Available | ✅ Compatible |
| **get_version()** | ✅ Available | ✅ Available | ✅ Compatible |
| **list()** | ✅ Available | ✅ Available | ✅ Compatible |
| **Collection Access** | `db.fs.files.*` | `conn.fs.files.*` (auto-delegates) | ✅ Compatible |

## Key Differences from PyMongo

### 1. Collection Access Pattern

**PyMongo:**
```python
from pymongo import MongoClient
client = MongoClient()
db = client.mydb

# Direct collection access works fully
db.fs.files.find({})           # ✅ Works
db.fs.files.update_one(...)    # ✅ Works
db.fs.files.delete_one(...)    # ✅ Works
```

**NeoSQLite:**
```python
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket

conn = Connection(":memory:")
bucket = GridFSBucket(conn.db)

# Direct collection access - ALL operations work with auto-delegation
conn.fs.files.find({})           # ✅ Works! (delegates to bucket.find())
conn.fs.files.find_one(...)      # ✅ Works! (delegates to bucket.find_one())
conn.fs.files.update_one(...)    # ✅ Works for metadata updates
conn.fs.files.delete_one(...)    # ✅ Works! (delegates to bucket.delete())
conn.fs.files.delete_many(...)   # ✅ Works! (delegates to bucket.delete())

# Recommended: Use GridFSBucket API for write operations
bucket.upload_from_stream(...)   # ✅ Recommended for uploads
bucket.delete(file_id)           # ✅ Recommended for deletes
```

### 2. Database Backend

| Aspect | PyMongo | NeoSQLite |
|--------|---------|-----------|
| **Database** | MongoDB | SQLite |
| **Storage Format** | BSON | JSON/JSONB + BLOB |
| **File Chunks** | Stored in `fs.chunks` collection | Stored in `fs_chunks` table |
| **File Metadata** | Stored in `fs.files` collection | Stored in `fs_files` table |
| **Table Naming** | Dot notation: `fs.files` | Underscore: `fs_files` |

### 3. Enhanced Metadata Support

NeoSQLite provides additional metadata fields not available in standard PyMongo GridFS:

```python
# Content Type Support
bucket.upload_from_stream("image.png", data, content_type="image/png")
file = bucket.find_one({"filename": "image.png"})
print(file.content_type)  # "image/png"

# Aliases Support
bucket.upload_from_stream("doc.pdf", data, aliases=["document", "report"])
files = bucket.find({"aliases": "document"})  # Find by alias
print(file.aliases)  # ["document", "report"]
```

### 4. File ID Handling

**PyMongo:**
```python
# File ID is always an ObjectId
file_id = bucket.upload_from_stream("file.txt", data)
# file_id is ObjectId('...')
```

**NeoSQLite:**
```python
# File ID is ObjectId, but internal storage uses dual-ID system
file_id = bucket.upload_from_stream("file.txt", data)
# file_id is ObjectId('...') (returned to user)
# Internal storage: (id INTEGER PRIMARY KEY, _id JSONB, ...)
```

**Note:** NeoSQLite uses an internal integer `id` as SQLite primary key for performance, with ObjectId stored in `_id` column for MongoDB compatibility.

### 5. Metadata Storage

**PyMongo:**
```python
# Metadata stored as BSON document
bucket.upload_from_stream("file.txt", data, metadata={"key": "value"})
# Stored as BSON, retrieved as dict
```

**NeoSQLite:**
```python
# Metadata stored as JSON/JSONB
bucket.upload_from_stream("file.txt", data, metadata={"key": "value"})
# New databases: JSONB (CBOR binary format)
# Existing databases: TEXT (gradual migration to JSONB)
```

## GridFS Features

### Core Methods

All standard PyMongo GridFS methods are available:

```python
from neosqlite.gridfs import GridFSBucket

bucket = GridFSBucket(db)

# Upload files
file_id = bucket.upload_from_stream("file.txt", data, metadata={"author": "john"})

# Download files
with bucket.open_download_stream(file_id) as stream:
    data = stream.read()

# Find files
files = bucket.find({"filename": "file.txt"})
single_file = bucket.find_one({"filename": "file.txt"})

# Version management
latest = bucket.get_last_version("file.txt")
specific_version = bucket.get_version("file.txt", revision=1)

# List all filenames
filenames = bucket.list()

# Delete files
bucket.delete(file_id)
bucket.drop()  # Remove entire bucket
```

### Enhanced Features

#### Content Type Support
```python
# Upload with content type
bucket.upload_from_stream("image.png", data, content_type="image/png")

# Access content type
file = bucket.find_one({"filename": "image.png"})
print(file.content_type)  # "image/png"

# Find by content type
images = bucket.find({"content_type": "image/png"})
```

#### Aliases Support
```python
# Upload with aliases
bucket.upload_from_stream("report.pdf", data, aliases=["monthly", "finance"])

# Find by alias
reports = bucket.find({"aliases": "monthly"})
print(file.aliases)  # ["monthly", "finance"]
```

#### Streaming Operations
```python
# Streaming upload
with bucket.open_upload_stream("large_file.dat", content_type="application/octet-stream") as stream:
    for chunk in large_data_chunks:
        stream.write(chunk)

# Streaming download
with bucket.open_download_stream(file_id) as stream:
    while chunk := stream.read(8192):
        process_chunk(chunk)
```

### Collection Access

NeoSQLite supports PyMongo-style collection access with automatic delegation:

```python
# All these work and delegate to GridFSBucket methods
files = conn.fs.files.find({"filename": "test.txt"})
file = conn.fs.files.find_one({"filename": "test.txt"})
conn.fs.files.delete_one({"_id": file_id})
conn.fs.files.delete_many({"filename": "old_files"})
```

## Migration Guide

### Overview

Starting from NeoSQLite version 1.3.0, GridFS collections use underscore-based table names (e.g., `fs_files`, `fs_chunks`) instead of dot-based names (e.g., `fs.files`, `fs.chunks`) for better SQLite compatibility.

### Automatic Migration

NeoSQLite automatically detects and migrates existing GridFS tables when you first access them:

- **Detection**: Checks for tables with dot-based names (e.g., `fs.files`)
- **Migration**: Renames tables to underscore-based names (e.g., `fs_files`)
- **Fallback**: If migration fails, continues using old table names for backward compatibility

Migration happens transparently during GridFS initialization and requires no user action.

### Manual Migration (If Needed)

If you need to migrate manually or encounter issues:

```sql
-- For default 'fs' bucket
ALTER TABLE fs.files RENAME TO fs_files;
ALTER TABLE fs.chunks RENAME TO fs_chunks;

-- For custom bucket names (replace 'mybucket' with your bucket name)
ALTER TABLE mybucket.files RENAME TO mybucket_files;
ALTER TABLE mybucket.chunks RENAME TO mybucket_chunks;
```

### What Changed

- **Before**: `fs.files`, `fs.chunks` (caused SQLite parsing issues)
- **After**: `fs_files`, `fs_chunks` (clean, compatible)
- **Benefit**: Enables PyMongo-like `db.fs.files` syntax

### Metadata Column Changes

#### New Databases (1.3.0+)
- Metadata column created as `JSONB` type (when SQLite supports it)
- Stored in efficient CBOR binary format
- Better performance for JSON operations

#### Existing Databases (Upgraded)
- Metadata column remains `TEXT` (no schema alteration)
- **Gradual migration**: Individual rows migrate to JSONB when metadata is updated
- Uses `jsonb_set()` for updates, which stores as JSONB
- `json()` wrapper ensures both TEXT and JSONB read correctly

### Backward Compatibility

- Existing databases are automatically migrated
- No data loss during migration
- Old table access still works if migration fails
- New databases use underscore naming from the start

### Troubleshooting

If you encounter issues:

1. Check that your SQLite version supports `ALTER TABLE RENAME`
2. Ensure no other processes are accessing the database during migration
3. Verify table names in `sqlite_master`: `SELECT name FROM sqlite_master WHERE type='table'`

Migration is one-time and safe - it only runs when old tables are detected.

### Migrating from PyMongo

#### Basic Migration

```python
# PyMongo
from pymongo import MongoClient
from gridfs import GridFS

client = MongoClient()
db = client.mydb
fs = GridFS(db)

file_id = fs.put(data, filename="test.txt", metadata={"author": "john"})
data = fs.get(file_id).read()
fs.delete(file_id)

# NeoSQLite (equivalent)
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket

conn = Connection("mydb.sqlite")
bucket = GridFSBucket(conn.db)

file_id = bucket.upload_from_stream("test.txt", data, metadata={"author": "john"})
with bucket.open_download_stream(file_id) as grid_out:
    data = grid_out.read()
bucket.delete(file_id)
```

#### Query Migration

```python
# PyMongo
files = fs.find({"metadata.author": "john"}, {"filename": 1, "length": 1})

# NeoSQLite (equivalent)
files = bucket.find({"metadata.author": "john"})
for file in files:
    print(file.filename, file.length)

# Or use PyMongo-style access
files = conn.fs.files.find({"metadata.author": "john"})
for file in files:
    print(file.filename, file.length)
```

#### Metadata Update Migration

```python
# PyMongo
db.fs.files.update_one(
    {"_id": file_id},
    {"$set": {"metadata.author": "jane"}}
)

# NeoSQLite (equivalent)
conn.fs.files.update_one(
    {"_id": file_id},
    {"$set": {"metadata": {"author": "jane"}}}
)
# Note: Replace entire metadata dict, not nested field
```

## Performance Considerations

### PyMongo
- **Strengths:** Distributed storage, horizontal scaling, built-in replication
- **Best for:** Large-scale applications, high write throughput, distributed systems

### NeoSQLite
- **Strengths:** Embedded database, no server required, ACID compliance, low memory footprint
- **Best for:** Embedded applications, single-user scenarios, edge computing, testing

## Recommendations

### Use PyMongo GridFS When:
- You need horizontal scaling
- You require distributed storage
- Your application needs high concurrent write throughput
- You're already using MongoDB

### Use NeoSQLite GridFS When:
- You need an embedded solution
- You want zero-configuration deployment
- Your application is single-user or low-concurrency
- You're already using SQLite
- You need ACID compliance without a server

### Best Practices

For new applications, we recommend:
- Use `bucket.upload_from_stream()` for uploads (with content_type and aliases support)
- Use `bucket.delete()` for deletions
- Use either `bucket.find()` or `conn.fs.files.find()` for queries (both work!)
- Use `conn.fs.files.update_one()` for metadata updates
- Leverage the enhanced content_type and aliases features for better file organization

## Conclusion

NeoSQLite's GridFS implementation provides **full API compatibility** with PyMongo GridFS, plus enhanced features like content type and aliases support. The automatic migration system ensures seamless upgrades, and the dual-ID system maintains performance while preserving MongoDB compatibility.

The implementation supports both traditional GridFSBucket API usage and PyMongo-style collection access, making it easy to migrate existing applications or build new ones with familiar patterns.

---

**Implementation Status**: All previously missing features have been implemented and are available in the current version. NeoSQLite GridFS now provides 100% API compatibility for common use cases with additional enhancements for modern applications.</content>
<parameter name="filePath">documents/GRIDFS.md