# Integrating json_each() with $unwind in NeoSQLite

## Overview

This document outlines how to integrate SQLite's `json_each()` function with NeoSQLite's `$unwind` aggregation operation to improve performance by leveraging SQL-based processing instead of Python-based iteration.

## Current Implementation

Currently, the `$unwind` operation in NeoSQLite is implemented entirely in Python in the `aggregate` method of the `Collection` class:

```python
elif stage_name == "$unwind":
    field = stage["$unwind"]
    unwound_docs = []
    field_name = field.lstrip("$")
    for doc in docs:
        array_to_unwind = self._get_val(doc, field_name)
        if isinstance(array_to_unwind, list):
            for item in array_to_unwind:
                new_doc = doc.copy()
                new_doc[field_name] = item
                unwound_docs.append(new_doc)
        else:
            unwound_docs.append(doc)
    docs = unwound_docs
```

This approach works but has performance limitations for large datasets since it requires loading all documents into Python memory and iterating through them.

## Proposed Implementation with json_each()

SQLite's `json_each()` function can decompose JSON arrays into rows at the database level, which can significantly improve performance. Here's how we can integrate it:

### 1. SQL Query Construction

For a simple unwind operation like `{"$unwind": "$hobbies"}`, we can construct a SQL query:

```sql
SELECT collection.id, 
       json_set(collection.data, '$."hobbies"', je.value) as data
FROM collection, 
     json_each(json_extract(collection.data, '$.hobbies')) as je
```

For nested fields like `{"$unwind": "$profile.skills"}`, we use:

```sql
SELECT collection.id, 
       json_set(collection.data, '$."profile.skills"', je.value) as data
FROM collection, 
     json_each(json_extract(collection.data, '$.profile.skills')) as je
```

### 2. Integration with _build_aggregation_query

We need to modify the `_build_aggregation_query` method to detect `$unwind` stages and generate appropriate SQL:

```python
elif stage_name == "$unwind":
    # Can only handle $unwind as the first stage or when combined with $match
    if i > 1 or (i == 1 and "$match" not in pipeline[0]):
        return None  # Fallback to Python implementation
    
    field = stage["$unwind"]
    if not isinstance(field, str) or not field.startswith("$"):
        return None  # Fallback to Python implementation
    
    field_name = field[1:]  # Remove leading $
    
    # For simple $unwind, we need to completely change the query structure
    # Instead of SELECT id, data FROM collection, we do:
    select_clause = "SELECT id, json_set(data, '$.\"" + field_name + "\"', je.value) as data"
    from_clause = f"FROM {self.name}, json_each(json_extract(data, '$.{field_name}')) as je"
    
    # If there's a previous $match stage, incorporate its WHERE clause
    if i == 1 and "$match" in pipeline[0]:
        match_query = pipeline[0]["$match"]
        where_result = self._build_simple_where_clause(match_query)
        if where_result:
            where_clause, params = where_result
            # Need to modify WHERE clause to work with the new FROM structure
            # This requires more complex logic to adapt the WHERE clause
    
    # Return the new query structure
    cmd = f"{select_clause} {from_clause} {where_clause}"
    return cmd, params, None
```

### 3. Handling Complex Cases

Some cases will still need to fall back to the Python implementation:

1. **Multiple $unwind stages**: Complex interactions between multiple unwind operations
2. **$unwind after other aggregation stages**: When documents have already been processed
3. **Non-array fields**: Error handling for fields that aren't arrays
4. **Complex field paths**: Very deeply nested or unusual field references

### 4. Performance Benefits

The SQL-based approach offers several performance advantages:

1. **Reduced Memory Usage**: Processing happens at the database level, not in Python
2. **Faster Execution**: SQLite's C implementation is faster than Python loops
3. **Reduced Data Transfer**: Less data movement between SQLite and Python
4. **Better Index Utilization**: Potential to leverage existing indexes

## Implementation Steps

1. **Modify `_build_aggregation_query`**: Add detection and handling for `$unwind` stages
2. **Create Helper Methods**: Implement utilities for generating `json_each()` queries
3. **Handle Edge Cases**: Properly manage fallback to Python implementation
4. **Update Tests**: Add comprehensive tests for the new functionality
5. **Performance Testing**: Benchmark the improvements against the Python implementation
6. **Documentation**: Update documentation to reflect the new capabilities

## Example Usage

After implementation, the following aggregation pipeline:

```python
pipeline = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"}
]
result = collection.aggregate(pipeline)
```

Would be processed using a single SQL query instead of Python iteration, resulting in better performance for large datasets.

## Considerations

1. **Backward Compatibility**: The Python implementation should remain as a fallback
2. **Error Handling**: Proper handling of malformed JSON or non-array fields
3. **Nested Field Preservation**: Ensuring original nested structures are maintained
4. **Integration with Other Stages**: How `$unwind` interacts with `$match`, `$sort`, etc.
```