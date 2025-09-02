# NeoSQLite Aggregation Pipeline Enhancements

## Overview

This document describes the enhancements made to NeoSQLite's aggregation pipeline processing, including temporary table aggregation, hybrid processing approaches, and additional group operations. These enhancements address the limitations of the current all-or-nothing approach to pipeline optimization.

## Current Architecture Limitations

NeoSQLite originally employed a binary approach to aggregation pipeline optimization:

1. **SQL Optimization Path**: Attempt to process the entire pipeline with a single optimized SQL query
2. **Python Fallback Path**: If SQL optimization is not possible, fall back to Python-based processing for the entire pipeline

This approach had several limitations:
- Complex pipeline combinations cannot be expressed in a single SQL query
- Position constraints for optimized stages (e.g., `$lookup` must be last)
- Intermediate results consume Python memory instead of database storage
- Limited optimization opportunities for multi-stage pipelines

## Enhanced Three-Tier Approach

The enhanced approach introduces a more sophisticated processing pipeline:

```mermaid
graph TD
    A["Input Pipeline"] --> B{"Can optimize with single query?"};
    B -- Yes --> C["Single SQL Query (Fastest)"];
    B -- No --> D{"Can process with temporary tables?"};
    D -- Yes --> E["Temporary Table Aggregation (Intermediate)"];
    D -- No --> F["Python Fallback (Slowest but most flexible)"];
    F --> G["Python Processing (Most Flexible)"];
```

## Temporary Table Aggregation Implementation

We've implemented a temporary table approach that:

1. **Processes pipeline stages incrementally** using temporary tables
2. **Stores intermediate results in temporary tables** rather than Python memory
3. **Executes compatible groups of stages** as SQL operations
4. **Automatically cleans up temporary tables** using transaction management

### Key Components

#### 1. Context Manager
- Automatic resource management with guaranteed cleanup
- Transaction-based atomicity using SQLite SAVEPOINTs
- Deterministic temporary table creation with unique names based on pipeline structure

#### 2. Pipeline Processor
- Support for `$match`, `$unwind`, `$sort`, `$skip`, `$limit`, `$lookup`, and `$addFields` stages
- Handling of consecutive `$unwind` stages
- Proper parameter handling and SQL injection prevention

#### 3. Integration Function
- Tries multiple approaches in order of preference:
  1. Existing SQL optimization
  2. Temporary table processing
  3. Python fallback

### Supported Stages

The current implementation supports:
- ✅ `$match` - Filtering documents with various operators (`$eq`, `$gt`, `$lt`, `$in`, etc.)
- ✅ `$unwind` - Array decomposition, including multiple consecutive stages
- ✅ `$sort` - Sorting documents by fields
- ✅ `$skip` - Skipping documents
- ✅ `$limit` - Limiting results
- ✅ `$lookup` - Joining collections (in any position)
- ✅ `$addFields` - Adding new fields to documents

The `$group` stage is not supported by the temporary table implementation but is handled by the Python fallback.

## Hybrid Pipeline Operations

Instead of rejecting entire pipelines with unsupported stages, we can implement granular fallback where:
- Each supported stage is processed using SQL temporary tables
- Each unsupported stage falls back to Python processing only for that specific operation
- Results from Python processing are used to create a new temporary table to continue SQL processing

### Supported Operations for SQL Processing

Currently supported in temporary table approach:
- `$match` (with all operators except `$text`)
- `$unwind` (including multiple consecutive unwinds)
- `$sort`, `$skip`, `$limit`
- `$lookup`
- `$addFields`

### Operations Requiring Python Fallback

Operations that would currently fall back to Python:
- `$project`
- Complex `$group` operations
- Advanced `$addFields` expressions with computations
- `$out`/`$merge` stages
- `$text` search operations
- Any stage with complex expressions not yet implemented in SQL

### Example Pipeline Processing

Consider this pipeline:
```javascript
[
  {"$match": {"status": "active"}},           // SQL - Filter from 10000→1000 docs
  {"$unwind": "$tags"},                       // SQL - Expand to 5000 docs
  {"$project": {"name": 1, "tags": 1}},       // Python fallback - Process 5000 docs
  {"$text": {"$search": "wireless"}},         // Python fallback - Filter to 500 docs
  {"$sort": {"name": 1}},                     // SQL - Sort 500 docs
  {"$limit": 10}                              // SQL - Take first 10
]
```

Current approach processes all 6 stages in Python.
Hybrid approach processes:
- Stages 1-2 in SQL (fast filtering)
- Stages 3-4 in Python (only for docs that passed SQL filtering)
- Stages 5-6 in SQL (fast sorting of reduced dataset)

## Additional Group Operations

Two new MongoDB-style group accumulators have been implemented in NeoSQLite:

### $push Accumulator
- Collects all values into an array, including duplicates
- Preserves all values including duplicates
- Maintains order of values as they appear in the dataset
- Works with any data type (strings, numbers, etc.)

### $addToSet Accumulator
- Collects only unique values into an array
- Automatically removes duplicate values
- Produces arrays with unique values only
- Works with any data type (strings, numbers, etc.)

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

## Integration Strategy

The enhancement is integrated into NeoSQLite's `QueryEngine.aggregate_with_constraints` method via the `integrate_with_neosqlite` function:

```python
def integrate_with_neosqlite(
    query_engine, pipeline: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    # First, try the existing SQL optimization approach
    try:
        query_result = query_engine.helpers._build_aggregation_query(pipeline)
        if query_result is not None:
            # Process with existing optimization
            return process_sql_results(query_result)
    except Exception:
        pass

    # Try the temporary table approach for supported pipelines
    if can_process_with_temporary_tables(pipeline):
        try:
            processor = TemporaryTableAggregationProcessor(
                query_engine.collection
            )
            return processor.process_pipeline(pipeline)
        except Exception:
            pass

    # Fall back to the existing Python implementation
    return process_with_python(pipeline)
```

## Benefits Achieved

### Performance Improvements
- **Reduced Memory Usage**: Intermediate results stored in database, not Python memory
- **Better Resource Management**: Automatic cleanup with guaranteed resource release
- **Scalability**: Ability to process larger datasets that might not fit in Python memory

### Flexibility
- **Granular Optimization**: Process individual stages or groups of stages
- **Position Independence**: Remove position constraints for optimized stages
- **Wider Coverage**: More pipeline combinations can benefit from SQL optimization

### Robustness
- **Atomic Operations**: Use SQLite transactions/SAVEPOINTs for atomicity
- **Error Handling**: Graceful fallback between approaches
- **Resource Cleanup**: Guaranteed cleanup of temporary resources

## Refactoring Python Operations

To support the hybrid approach, we need to refactor the Python implementation of aggregation operations into modular, reusable functions that can be called from both QueryEngine and TemporaryTableAggregationProcessor:

```python
# Modular Python operation functions
def python_match_stage(documents, match_spec):
    """Process $match stage in Python"""
    # Implementation from QueryEngine's Python fallback
    pass

def python_project_stage(documents, projection_spec):
    """Process $project stage in Python"""
    # Implementation from QueryEngine's Python fallback
    pass

def python_group_stage(documents, group_spec):
    """Process $group stage in Python"""
    # Implementation from QueryEngine's Python fallback
    pass

def python_text_search_stage(documents, text_spec):
    """Process $text search stage in Python"""
    # Implementation from QueryEngine's _apply_query method
    pass

# ... similar functions for other stages
```

This refactoring would:
- Make Python operations callable as standalone functions
- Enable granular fallback from SQL to Python for specific stages
- Eliminate code duplication between QueryEngine and temporary table processor
- Provide a clean interface for hybrid pipeline processing
- Allow better testing of individual Python operations

## Memory Efficiency with Quez

To optimize memory usage during data transfer between SQL and Python processing, we can leverage Quez with zlib compression:

1. **Compressed Data Transfer**: Use Quez compressed queues to transfer large result sets between SQL and Python stages
2. **Memory-Constrained Processing**: For large datasets, process data in compressed chunks rather than loading everything into memory
3. **Zlib Compression**: Apply zlib compression to reduce memory footprint during transfers
4. **Streaming Processing**: Enable streaming of results from SQL to Python to avoid memory bottlenecks

Since NeoSQLite already has Quez usage in QueryEngine, we should refactor this into a general utility function that can be called from both QueryEngine and the Temporary Table Aggregation processor:

```python
# Shared utility function
def transfer_sql_to_python_with_compression(sql_cursor, batch_size=1000):
    """Transfer data from SQL cursor to Python using Quez compression"""
    # Implementation using existing Quez patterns from QueryEngine
    pass

def transfer_python_to_sql_with_compression(data, temp_table_name):
    """Transfer data from Python to SQL temporary table using Quez compression"""
    # Implementation using existing Quez patterns from QueryEngine
    pass
```

This refactoring approach would:
- Eliminate code duplication between QueryEngine and TemporaryTableAggregationProcessor
- Ensure consistent memory-efficient processing across both components
- Make maintenance easier (changes to compression logic only need to be made in one place)
- Provide a standardized interface for data transfer between SQL and Python processing

## Usage Examples

### Using $push to collect all product names by category
```python
pipeline = [
    {"$group": {"_id": "$category", "productNames": {"$push": "$name"}}}
]
```

### Using $addToSet to collect unique prices by category
```python
pipeline = [
    {"$group": {"_id": "$category", "uniquePrices": {"$addToSet": "$price"}}}
]
```

### Combining both with other accumulators
```python
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

### Complex pipeline with multiple stages
```python
pipeline = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"},
    {"$unwind": "$categories"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]
```

## Future Enhancements

1. **Additional Stage Support**: Extend to support `$project` and other stages
2. **Query Planning**: Intelligently decide which approach to use based on pipeline complexity
3. **Streaming Results**: Stream results from temporary tables to reduce memory usage
4. **Parallel Processing**: Process independent pipeline branches in parallel
5. **Add `$project` support to SQL processing**
6. **Implement FTS5 `$text` search in SQL**
7. **Add complex expression support in `$addFields`**
8. **Implement more `$group` operations in SQL**
9. **Add support for `$out`/`$merge` stages**

Each enhancement increases the amount of pipeline processing that can stay in SQL, improving overall performance.

## Performance Considerations

1. **Context Switching Overhead**: Each Python fallback introduces some overhead
2. **Data Transfer**: Moving data between SQL and Python has costs
3. **Memory Usage**: Need to manage memory efficiently during transfers
4. **Transaction Management**: Ensure atomicity across SQL/Python boundaries
5. **Compression/Decompression Overhead**: CPU cost of compression may offset memory savings for small datasets

The key is that this overhead is typically much less than processing the entire pipeline in Python, especially for pipelines with selective filtering stages early on.

## Conclusion

The aggregation pipeline enhancements provide a significant improvement over the current binary approach by:

- Expanding the range of pipelines that can be processed efficiently with SQL
- Providing better resource management through automatic cleanup
- Maintaining full backward compatibility
- Offering a path toward processing even more complex pipelines efficiently

This enhancement represents a practical step toward the long-term goal of handling 95% of common aggregation pipelines at the SQL level while maintaining the flexibility of Python fallback for complex cases.