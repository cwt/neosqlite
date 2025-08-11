# neosqlite Examples

This directory contains example scripts demonstrating various features of neosqlite.

## watch_example.py

Demonstrates the `watch()` method and change stream functionality:

```bash
python watch_example.py
```

This example shows how to:
- Create a change stream using `collection.watch()`
- Perform CRUD operations (insert, update, delete)
- Capture and process change notifications
- Use the `full_document="updateLookup"` option to include full documents in change events

## Other Examples

Additional examples can be found in the root directory:
- `api_comparison.py` - Compares neosqlite API with PyMongo
- Various test scripts in the `tests/` directory