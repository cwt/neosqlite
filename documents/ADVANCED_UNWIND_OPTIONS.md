# Advanced $unwind Options in NeoSQLite

## Overview

NeoSQLite now supports advanced options for the `$unwind` aggregation stage that provide greater control over how arrays are decomposed into separate documents. These options, inspired by MongoDB's advanced `$unwind` functionality, allow you to:

1. Include the array index of each element in the unwound documents
2. Preserve documents with null, empty, or missing array fields

## Supported Options

### includeArrayIndex

The `includeArrayIndex` option adds a field to each unwound document that contains the array index of the element that was unwound.

**Syntax:**
```javascript
{
  "$unwind": {
    "path": "$arrayField",
    "includeArrayIndex": "indexField"
  }
}
```

**Example:**
```python
# Document:
{
  "_id": 1,
  "name": "Alice",
  "scores": [85, 92, 78]
}

# Pipeline:
[
  {
    "$unwind": {
      "path": "$scores",
      "includeArrayIndex": "scoreIndex"
    }
  }
]

# Results:
[
  {"_id": 1, "name": "Alice", "scores": 85, "scoreIndex": 0},
  {"_id": 1, "name": "Alice", "scores": 92, "scoreIndex": 1},
  {"_id": 1, "name": "Alice", "scores": 78, "scoreIndex": 2}
]
```

### preserveNullAndEmptyArrays

The `preserveNullAndEmptyArrays` option controls whether documents should be preserved when the array field is null, empty, or missing.

**Syntax:**
```javascript
{
  "$unwind": {
    "path": "$arrayField",
    "preserveNullAndEmptyArrays": true
  }
}
```

**Behavior:**
- When `true`: Documents with null values, empty arrays `[]`, or missing fields are preserved with the field set to `null`
- When `false` (default): Documents with null values, empty arrays, or missing fields are omitted from the result

**Example:**
```python
# Documents:
[
  {"_id": 1, "name": "Alice", "hobbies": ["reading", "swimming"]},
  {"_id": 2, "name": "Bob", "hobbies": []},  # Empty array
  {"_id": 3, "name": "Charlie", "hobbies": null},  # Null value
  {"_id": 4, "name": "David"}  # Missing field
]

# Pipeline:
[
  {
    "$unwind": {
      "path": "$hobbies",
      "preserveNullAndEmptyArrays": true
    }
  }
]

# Results:
[
  {"_id": 1, "name": "Alice", "hobbies": "reading"},
  {"_id": 1, "name": "Alice", "hobbies": "swimming"},
  {"_id": 2, "name": "Bob", "hobbies": null},  # Preserved empty array
  {"_id": 3, "name": "Charlie", "hobbies": null}  # Preserved null value
  # David's document is omitted (missing field not preserved)
]
```

## Combining Options

Both advanced options can be used together:

```python
# Pipeline:
[
  {
    "$unwind": {
      "path": "$scores",
      "includeArrayIndex": "scoreIndex",
      "preserveNullAndEmptyArrays": true
    }
  }
]
```

## Backward Compatibility

The traditional string-based syntax for `$unwind` continues to work as before:

```python
# Traditional syntax (still supported):
[
  {"$unwind": "$scores"}
]
```

## Performance Considerations

The advanced `$unwind` options are implemented in Python rather than SQL, which means they don't benefit from the performance optimizations available in the SQL-based `$unwind` implementation. For large datasets, consider whether the advanced options are necessary or if the default behavior will suffice.

## Use Cases

### includeArrayIndex Use Cases

1. **Preserving Positional Information**: When the order of elements in an array is significant and you need to maintain that information in the unwound documents.

2. **Pagination of Array Elements**: When you need to implement pagination on array elements, having the index makes it easier to determine which elements to skip and limit.

3. **Debugging and Auditing**: When tracking which position in an array produced a particular result, the index can be valuable for debugging.

### preserveNullAndEmptyArrays Use Cases

1. **Complete Dataset Preservation**: When you need to ensure that all documents in a collection are represented in the result, even if they have empty or null arrays.

2. **Reporting Requirements**: When generating reports that need to show all entities, with a clear distinction between entities that have data and those that don't.

3. **Data Validation**: When validating data quality, preserving documents with empty arrays can help identify entities that might be missing information.

## Limitations

1. **Performance**: Advanced options are implemented in Python and don't benefit from SQL-level optimizations.

2. **Nested Field Support**: While nested fields work with advanced options, complex nested scenarios may have edge cases that require testing.

3. **Missing Field Preservation**: In the current implementation, documents with completely missing fields are not preserved even when `preserveNullAndEmptyArrays` is `true`. Only explicit `null` values and empty arrays `[]` are preserved.

## Examples

See the examples directory for practical demonstrations of these features:
- `examples/unwind_advanced_options_basic.py` - Basic usage of both options
- `examples/unwind_advanced_options_combined.py` - Using both options together
- `examples/unwind_advanced_options_nested.py` - Working with nested arrays