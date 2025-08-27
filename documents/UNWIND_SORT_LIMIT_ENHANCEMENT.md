# NeoSQLite $unwind + $sort + $limit Enhancement

## Overview
This enhancement extends NeoSQLite's MongoDB-style aggregation pipeline optimization to support pushing `$sort` and `$limit` operations down to the SQLite level when combined with `$unwind` operations. This leverages SQLite's native `json_each()` function for improved performance.

## Implementation Details

### Supported Patterns
The optimization works with the following pipeline patterns:
1. `$unwind` + `$sort` + `$limit`
2. `$match` + `$unwind` + `$sort` + `$limit`
3. `$unwind` + `$sort` + `$skip` + `$limit`
4. Multiple consecutive `$unwind` stages followed by `$sort` and `$limit`

### SQL Generation

#### Basic $unwind + $sort + $limit
For a pipeline like:
```javascript
[
  {"$unwind": "$tags"},
  {"$sort": {"tags": 1}},
  {"$limit": 5}
]
```

The system generates SQL similar to:
```sql
SELECT collection.id, 
       json_set(collection.data, '$."tags"', je.value) as data
FROM collection, 
     json_each(json_extract(collection.data, '$.tags')) as je
ORDER BY je.value ASC
LIMIT 5
```

#### Sorting by Original Document Fields
When sorting by fields in the original document (not the unwound field):
```javascript
[
  {"$unwind": "$tags"},
  {"$sort": {"score": -1}},
  {"$limit": 5}
]
```

Generates:
```sql
SELECT collection.id, 
       json_set(collection.data, '$."tags"', je.value) as data
FROM collection, 
     json_each(json_extract(collection.data, '$.tags')) as je
ORDER BY json_extract(collection.data, '$.score') DESC
LIMIT 5
```

#### With $match Filter
When a `$match` stage precedes the `$unwind`:
```javascript
[
  {"$match": {"status": "active"}},
  {"$unwind": "$tags"},
  {"$sort": {"tags": 1}},
  {"$limit": 5}
]
```

Generates:
```sql
SELECT collection.id, 
       json_set(collection.data, '$."tags"', je.value) as data
FROM collection, 
     json_each(json_extract(collection.data, '$.tags')) as je
WHERE json_extract(data, '$.status') = ?
ORDER BY je.value ASC
LIMIT 5
```

#### With $skip Operation
When using `$skip` in combination with `$limit`:
```javascript
[
  {"$unwind": "$tags"},
  {"$sort": {"tags": 1}},
  {"$skip": 10},
  {"$limit": 5}
]
```

Generates:
```sql
SELECT collection.id, 
       json_set(collection.data, '$."tags"', je.value) as data
FROM collection, 
     json_each(json_extract(collection.data, '$.tags')) as je
ORDER BY je.value ASC
OFFSET 10
LIMIT 5
```

#### Multiple $unwind Stages
For multiple consecutive `$unwind` operations:
```javascript
[
  {"$unwind": "$tags"},
  {"$unwind": "$categories"},
  {"$sort": {"tags": 1}},
  {"$limit": 5}
]
```

Generates:
```sql
SELECT collection.id, 
       json_set(
         json_set(collection.data, '$."tags"', je1.value), 
         '$."categories"', je2.value
       ) as data
FROM collection,
     json_each(json_extract(collection.data, '$.tags')) as je1,
     json_each(json_extract(collection.data, '$.categories')) as je2
ORDER BY je1.value ASC
LIMIT 5
```

### Features
- Native SQLite sorting using `ORDER BY` clause
- Efficient limiting using `LIMIT` and `OFFSET` clauses
- Support for both ascending (1) and descending (-1) sort directions
- Sorting by unwound fields (e.g., the array elements themselves)
- Sorting by original document fields (e.g., other fields in the document)
- Integration with existing `$match` optimizations
- Support for `$skip` operations
- Works with multiple consecutive `$unwind` stages, including nested unwinds. See [NESTED_ARRAY_UNWIND.md](NESTED_ARRAY_UNWIND.md) for details.

## Performance Benefits
- Operations are performed at the database level, reducing data transfer to Python
- Native SQLite sorting is typically faster than Python-based sorting
- Limiting results at the database level reduces memory usage
- Combines well with existing `$unwind` optimizations
- Eliminates intermediate Python data structures

## Usage Examples

### Basic Sort and Limit
```python
pipeline = [
    {"$unwind": "$tags"},
    {"$sort": {"tags": 1}},
    {"$limit": 10}
]
result = collection.aggregate(pipeline)
```

### Sort by Original Field
```python
pipeline = [
    {"$unwind": "$tags"},
    {"$sort": {"score": -1}},  # Sort by score descending
    {"$limit": 5}
]
result = collection.aggregate(pipeline)
```

### With Match and Skip
```python
pipeline = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"},
    {"$sort": {"tags": 1}},
    {"$skip": 10},
    {"$limit": 5}
]
result = collection.aggregate(pipeline)
```

### Multiple Unwind Stages
```python
pipeline = [
    {"$unwind": "$tags"},
    {"$unwind": "$categories"},
    {"$sort": {"tags": 1}},
    {"$limit": 10}
]
result = collection.aggregate(pipeline)
```

## Limitations
- Only works when `$unwind` is the first or second stage (after `$match`)
- Complex sort expressions are not supported (fallback to Python)
- Only basic field sorting is supported (no computed expressions)

## Testing
The implementation includes comprehensive tests covering:
- Basic sort and limit operations
- Sorting by unwound fields vs. original document fields
- Integration with `$match` operations
- Descending sort operations
- Skip and limit combinations
- Multiple consecutive `$unwind` stages
- Performance benchmarks

## How It Works Internally

### Pattern Detection
The optimization works by detecting specific patterns in the aggregation pipeline:
1. `$unwind` as the first stage, followed by `$sort` and/or `$limit`/`$skip`
2. `$match` as the first stage, followed by `$unwind`, then `$sort` and/or `$limit`/`$skip`

### Query Construction
When a supported pattern is detected, the system:

1. **Modifies the SELECT clause**: Uses `json_set()` to replace the array field with individual values
2. **Adds json_each() to FROM clause**: Decomposes the array into rows
3. **Adds WHERE clause**: If there's a preceding `$match` stage
4. **Adds ORDER BY clause**: Based on the `$sort` specification
5. **Adds LIMIT/OFFSET clauses**: Based on `$limit` and `$skip` stages

### Sort Field Handling
The system intelligently handles sorting by determining whether to sort by:
- The unwound field values (`je.value`)
- Original document fields (`json_extract(collection.data, '$.field')`)

### Fallback Mechanism
Complex cases that don't match the supported patterns fall back to the existing Python-based implementation, ensuring backward compatibility.