# Change Streams with watch() in NeoSQLite

NeoSQLite provides change stream functionality, similar to PyMongo's, through the `collection.watch()` method. This allows you to listen for data changes (inserts, updates, and deletes) in a collection.

## How It Works

The `watch()` feature is implemented using SQLite triggers. When you start a change stream, triggers are created on the collection's underlying table. These triggers capture any changes and record them in a dedicated `_neosqlite_changestream` table. The `ChangeStream` object then polls this table for new events.

## Usage

The `watch()` method returns a `ChangeStream` object, which is an iterator. The recommended way to use it is with a `with` statement to ensure resources are properly cleaned up.

```python
with collection.watch() as change_stream:
    # Perform some operations on the collection
    collection.insert_one({'name': 'Alice'})
    collection.update_one({'name': 'Alice'}, {'$set': {'age': 30}})
    collection.delete_one({'name': 'Alice'})

    # Iterate over the changes
    for change in change_stream:
        print(change)
```

### Change Event Structure
The change events are designed to be similar to MongoDB's:
```json
{
    "_id": {"id": 1}, 
    "operationType": "insert", 
    "clusterTime": "2025-08-11 09:36:25", 
    "ns": {"db": "default", "coll": "users"}, 
    "documentKey": {"_id": 1}, 
    "fullDocument": {"name": "Alice", "age": 30, "_id": 1}
}
```

### Options
- **`full_document="updateLookup"`**: By default, `update` events only include the changes. Set this option to include the full document after the update.
- **`max_await_time_ms`**: The maximum time to wait for new changes before the iterator yields `None`.

## Testing

The `watch()` feature has been thoroughly tested with 37 test cases covering:
- Basic insert, update, and delete operations.
- Full document lookups.
- Timeout and batching mechanisms.
- Context manager usage and resource cleanup.
- Error handling and edge cases.

## Limitations
- **No Resume Tokens**: The current implementation does not support resuming a change stream from a specific point in time.
- **No Pipeline Support**: The `pipeline` parameter is accepted for API compatibility but is not yet implemented.
- **Limited Advanced Options**: Features like `collation` and `session` are not implemented.
