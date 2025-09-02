# Hybrid Pipeline Operations for NeoSQLite

## Overview

This document describes a proposed enhancement to NeoSQLite's aggregation pipeline processing that implements a hybrid approach combining SQL-based temporary table operations with granular Python fallback for unsupported operations.

## Current Architecture

NeoSQLite currently uses a three-tier approach:
1. **Tier 1**: SQL optimization for simple pipelines
2. **Tier 2**: Temporary table approach for complex pipelines (if all stages supported)
3. **Tier 3**: Python fallback for any pipeline with unsupported stages

The limitation is that if any stage in a pipeline is unsupported, the entire pipeline falls back to Python processing.

## Proposed Hybrid Approach

Instead of rejecting entire pipelines with unsupported stages, we can implement granular fallback where:
- Each supported stage is processed using SQL temporary tables
- Each unsupported stage falls back to Python processing only for that specific operation
- Results from Python processing are used to create a new temporary table to continue SQL processing

## Benefits

1. **Maximum Performance**: Use SQL for as much of the pipeline as possible
2. **Granular Fallback**: Only fallback to Python for specific unsupported operations
3. **Progressive Processing**: Continue with SQL after Python processing
4. **Better Resource Utilization**: Leverage database optimization for supported stages
5. **Backward Compatibility**: Existing functionality remains unchanged

## Implementation Strategy

### Pipeline Processing Flow

```python
# In process_pipeline method:
i = 0
while i < len(pipeline):
    stage = pipeline[i]
    stage_name = next(iter(stage.keys()))
    
    if can_process_stage_with_sql(stage_name):
        # Process with temporary tables
        current_table = process_stage_with_sql(stage, current_table)
        i += 1
    else:
        # Fallback to Python for this stage only
        intermediate_results = get_results_from_current_table(current_table)
        processed_results = process_stage_with_python(stage, intermediate_results)
        # Create new temp table with Python results
        current_table = create_temp_table_from_python_results(processed_results)
        i += 1
```

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

## Example Pipeline Processing

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

## Implementation Steps

1. **Modify Pipeline Processor**: Update the temporary table aggregation processor to handle granular fallback
2. **Result Transfer Mechanism**: Implement efficient transfer of data from SQL temp tables to Python and back
3. **Context Management**: Ensure proper savepoint and temporary table cleanup
4. **Performance Monitoring**: Add metrics to track when/why fallback occurs
5. **Gradual Enhancement**: Continue adding more operations to SQL support over time

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

## Future Enhancements

This approach allows incremental improvements:
- Add `$project` support to SQL processing
- Implement FTS5 `$text` search in SQL
- Add complex expression support in `$addFields`
- Implement more `$group` operations in SQL
- Add support for `$out`/`$merge` stages

Each enhancement increases the amount of pipeline processing that can stay in SQL, improving overall performance.

## Performance Considerations

1. **Context Switching Overhead**: Each Python fallback introduces some overhead
2. **Data Transfer**: Moving data between SQL and Python has costs
3. **Memory Usage**: Need to manage memory efficiently during transfers
4. **Transaction Management**: Ensure atomicity across SQL/Python boundaries
5. **Compression/Decompression Overhead**: CPU cost of compression may offset memory savings for small datasets

The key is that this overhead is typically much less than processing the entire pipeline in Python, especially for pipelines with selective filtering stages early on.