# Implementation of watch() Feature in neosqlite

## Overview
This document describes the implementation of the `watch()` method in neosqlite, which provides change stream functionality similar to PyMongo's change streams.

## Implementation Details

### ChangeStream Class
The `ChangeStream` class implements the change stream functionality using SQLite's built-in features:

1. **Trigger-based Change Capture**: 
   - Uses SQLite triggers (INSERT, UPDATE, DELETE) to capture changes to collections
   - Stores change events in a dedicated `_neosqlite_changestream` table

2. **Iterator Interface**:
   - Implements `__iter__` and `__next__` methods for iteration
   - Provides timeout mechanism using `max_await_time_ms` parameter
   - Supports batch processing with `batch_size` parameter

3. **Change Event Structure**:
   - Follows a structure similar to MongoDB change events
   - Includes operation type, document key, namespace, and cluster time
   - Optionally includes full document with `full_document="updateLookup"`

4. **Resource Management**:
   - Automatic cleanup of triggers when the change stream is closed
   - Context manager support for proper resource handling

### Collection.watch() Method
The `watch()` method in the `Collection` class creates and returns a `ChangeStream` instance with the following parameters:

- `pipeline`: Aggregation pipeline stages (not implemented in this version)
- `full_document`: Controls inclusion of full document in change events
- `resume_after`: Logical starting point for change stream (not implemented)
- `max_await_time_ms`: Maximum wait time for new changes
- `batch_size`: Number of changes to return per batch
- `collation`: Collation settings (not implemented)
- `start_at_operation_time`: Starting point based on operation time (not implemented)
- `session`: Client session (not implemented)
- `start_after`: Alternative starting point (not implemented)

## Usage Examples

```python
import neosqlite

# Create a connection
with neosqlite.Connection(':memory:') as conn:
    collection = conn.users
    
    # Watch for changes with full document
    with collection.watch(full_document="updateLookup") as change_stream:
        # Insert a document
        collection.insert_one({'name': 'Alice', 'age': 30})
        
        # Get the change notification
        change = next(change_stream)
        print(change)
        # Output: {
        #     '_id': {'id': 1}, 
        #     'operationType': 'insert', 
        #     'clusterTime': '2025-08-11 09:36:25', 
        #     'ns': {'db': 'default', 'coll': 'users'}, 
        #     'documentKey': {'_id': 1}, 
        #     'fullDocument': {'name': 'Alice', 'age': 30, '_id': 1}
        # }
```

## Limitations

1. **Resume Token Support**: The current implementation doesn't support resuming change streams from a specific point
2. **Aggregation Pipeline**: The `pipeline` parameter is accepted but not processed
3. **Advanced Options**: Several advanced options like `collation`, `session`, etc. are not implemented
4. **Database Name**: The database name is hardcoded as "default" since the Connection class doesn't have a name property

## Future Improvements

1. Implement resume token support for fault-tolerant change streams
2. Add support for aggregation pipeline processing
3. Implement advanced options like collation and session support
4. Add database name support to the Connection class
5. Improve performance with more sophisticated change detection mechanisms

## Testing

The implementation has been tested with:
- Insert, update, and delete operations
- Timeout mechanism
- Full document lookup
- Resource cleanup

All tests pass successfully, demonstrating that the basic functionality works correctly.