# NeoSQLite GridFS Implementation

This directory contains the GridFS implementation for NeoSQLite, providing a PyMongo-compatible interface for storing and retrieving large files in SQLite databases.

## Overview

GridFS is MongoDB's specification for storing large files. This implementation provides the same functionality using SQLite as the backend storage, maintaining API compatibility with PyMongo's GridFS.

## Key Components

### GridFSBucket (Recommended)

The modern GridFS interface with the following key methods:

- `upload_from_stream(filename, source, metadata)` - Upload file data
- `upload_from_stream_with_id(file_id, filename, source, metadata)` - Upload with custom ID
- `open_upload_stream(filename, metadata)` - Get writable stream
- `open_upload_stream_with_id(file_id, filename, metadata)` - Get writable stream with custom ID
- `download_to_stream(file_id, destination)` - Download file to stream
- `download_to_stream_by_name(filename, destination, revision)` - Download by name
- `open_download_stream(file_id)` - Get readable stream
- `open_download_stream_by_name(filename, revision)` - Get readable stream by name
- `find(filter)` - Query files with cursor
- `delete(file_id)` - Delete by ID

### Supporting Classes

- `GridIn` - Writable file object for streaming uploads
- `GridOut` - Readable file object for streaming downloads
- `GridOutCursor` - Cursor for iterating over files

### Exception Classes

- `NoFile` - Raised when file not found
- `FileExists` - Raised when trying to overwrite existing file
- `CorruptGridFile` - Raised when file corruption detected

## Usage Examples

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
    
    # Stream upload
    with bucket.open_upload_stream("streamed.txt") as grid_in:
        grid_in.write(b"First part. ")
        grid_in.write(b"Second part.")
```

For more comprehensive examples, see the examples directory.

## Implementation Details

Files are stored in two tables:
1. `fs.files` - File metadata (filename, length, chunk size, upload date, MD5 hash, etc.)
2. `fs.chunks` - File data chunks (binary data split into manageable pieces)

The default chunk size is 255KB, which can be customized during bucket creation.

## Limitations

This implementation focuses on core GridFS functionality and may not implement all advanced features of PyMongo's GridFS. However, it provides a solid foundation for storing and retrieving large files in SQLite databases with a familiar API.