# Additional Group Operations Implementation

## Overview

This document describes the implementation of two new MongoDB-style group accumulators in NeoSQLite:
- `$push`: Collects all values into an array, including duplicates
- `$addToSet`: Collects only unique values into an array

## Implementation Details

### SQL-Based Optimization

Both accumulators are optimized at the SQL level using SQLite's `json_group_array()` function:

1. **$push**: Uses `json_group_array(json_extract(data, '$.field'))` to collect all values
2. **$addToSet**: Uses `json_group_array(DISTINCT json_extract(data, '$.field'))` to collect unique values

This optimization ensures maximum performance by processing data directly at the database level.

### Example SQL Queries

For a collection with documents like:
```json
[
  {"category": "Electronics", "name": "Laptop", "price": 1200},
  {"category": "Electronics", "name": "Smartphone", "price": 800},
  {"category": "Books", "name": "Python Guide", "price": 30}
]
```

Using `$push`:
```javascript
{"$group": {"_id": "$category", "productNames": {"$push": "$name"}}}
```

Generates SQL:
```sql
SELECT json_extract(data, '$.category') AS _id,
       json_group_array(json_extract(data, '$.name')) AS "productNames"
FROM collection
GROUP BY json_extract(data, '$.category')
```

Using `$addToSet`:
```javascript
{"$group": {"_id": "$category", "uniquePrices": {"$addToSet": "$price"}}}
```

Generates SQL:
```sql
SELECT json_extract(data, '$.category') AS _id,
       json_group_array(DISTINCT json_extract(data, '$.price')) AS "uniquePrices"
FROM collection
GROUP BY json_extract(data, '$.category')
```

## Features

### $push Accumulator
- Preserves all values including duplicates
- Maintains order of values as they appear in the dataset
- Works with any data type (strings, numbers, etc.)

### $addToSet Accumulator
- Automatically removes duplicate values
- Produces arrays with unique values only
- Works with any data type (strings, numbers, etc.)

## Benefits

1. **Performance**: Both accumulators execute at the database level for maximum efficiency
2. **Memory Efficiency**: No intermediate Python data structures needed
3. **Compatibility**: Full compatibility with existing aggregation pipeline features
4. **Flexibility**: Can be combined with other accumulator functions in the same `$group` stage

## Usage Examples

```python
# Using $push to collect all product names by category
pipeline = [
    {"$group": {"_id": "$category", "productNames": {"$push": "$name"}}}
]

# Using $addToSet to collect unique prices by category
pipeline = [
    {"$group": {"_id": "$category", "uniquePrices": {"$addToSet": "$price"}}}
]

# Combining both with other accumulators
pipeline = [
    {"$group": {
        "_id": "$category",
        "productNames": {"$push": "$name"},
        "uniquePrices": {"$addToSet": "$price"},
        "avgPrice": {"$avg": "$price"},
        "count": {"$sum": 1}
    }}
]
```

## Testing

Comprehensive tests ensure:
- Correct behavior of both accumulators
- Proper handling of duplicates (preserved in $push, removed in $addToSet)
- Compatibility with other accumulator functions
- Performance optimization through SQL execution
- Edge case handling (null values, missing fields, etc.)