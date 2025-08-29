# Advanced Index-Aware Optimization

## Overview

The Advanced Index-Aware Optimization feature enhances NeoSQLite's query execution by leveraging existing indexes to improve performance. This feature is part of the #9 enhancement in the JSON_EACH() Enhancement Roadmap.

## How It Works

The optimization works by:

1. **Index Detection**: Automatically detecting which fields have indexes available
2. **Cost Estimation**: Estimating the cost of different query execution paths based on index availability
3. **Query Planning**: Using this information to select the most efficient execution path

## Implementation Details

### Cost Estimation

The system estimates query costs using the `_estimate_query_cost()` method:

- Fields with indexes reduce query cost (multiplier of 0.3)
- The `_id` field is always indexed and has the lowest cost (multiplier of 0.1)
- Fields without indexes have no cost reduction (multiplier of 1.0)

### Index Detection

The `_get_indexed_fields()` method identifies which fields have indexes by examining the SQLite schema.

## Usage

The optimization works automatically when you:

1. Create indexes on frequently queried fields:
   ```python
   collection.create_index("category")
   collection.create_index("status")
   ```

2. Use those fields in your aggregation pipelines:
   ```python
   pipeline = [
       {"$match": {"category": "Category5"}},
       {"$unwind": "$tags"},
       {"$sort": {"tags": 1}},
       {"$limit": 10}
   ]
   result = collection.aggregate(pipeline)
   ```

## Performance Benefits

- Queries using indexed fields execute faster
- Complex pipelines with multiple stages benefit from index usage
- Nested array operations can leverage indexes when available

## Limitations

- Currently, the cost estimation is used for internal optimization decisions but doesn't change the actual execution path
- Future enhancements could use this information to reorder pipeline stages for better performance
- The feature currently focuses on identifying and estimating rather than actively reordering queries

## Testing

The feature includes comprehensive tests that verify:

- Correct identification of indexed fields
- Accurate cost estimation
- Proper integration with existing aggregation pipeline stages
- No regression in existing functionality

## Future Enhancements

Planned improvements include:

- **Pipeline Reordering**: Automatically reordering pipeline stages to put indexed filters first
- **Advanced Cost Models**: More sophisticated cost estimation that considers data distribution
- **Join Optimization**: Better optimization for $lookup operations using indexes