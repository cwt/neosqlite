# Temporary Table Aggregation Pipeline Enhancement

## Overview

This document proposes an enhancement to NeoSQLite's aggregation pipeline processing by using temporary tables to break down complex pipelines into manageable SQL subqueries, rather than having an all-or-nothing approach where the entire pipeline either executes in SQL or falls back to Python.

## Current Limitations

The existing implementation has a binary approach to pipeline optimization:
1. Either the entire pipeline can be optimized with a single SQL query
2. Or the entire pipeline falls back to Python processing

This creates limitations when:
- Complex combinations of stages cannot be expressed in a single SQL query
- Certain stage positions are required for optimization (e.g., `$lookup` must be last)
- Intermediate results would benefit from temporary storage for better performance

## Proposed Enhancement

Implement a temporary table approach that:
1. Processes pipeline stages incrementally using temporary tables
2. Stores intermediate results in temporary tables rather than Python memory
3. Executes each stage (or groups of compatible stages) as SQL operations
4. Automatically cleans up temporary tables using transaction/resource management

## Benefits

1. **Granular Optimization**: Optimize individual stages or groups of stages rather than the entire pipeline
2. **Flexibility**: Remove position constraints for optimized stages
3. **Memory Efficiency**: Process large intermediate results at the database level
4. **Better Performance**: More pipeline combinations can benefit from SQL optimization
5. **Resource Management**: Automatic cleanup with guaranteed resource release

## Implementation Approach

### 1. Temporary Table Context Manager

Create a context manager that:
- Creates temporary tables with unique names
- Tracks created tables for cleanup
- Provides automatic cleanup on exit or error
- Uses SQLite transactions/SAVEPOINTs for atomicity

### 2. Incremental Pipeline Processing

Process pipeline stages in groups:
- Identify compatible consecutive stages that can be processed together
- Create temporary tables for intermediate results
- Chain SQL operations through temporary tables
- Return final results from the last temporary table

### 3. Resource Management

Use SQLite's transaction features:
- Create a SAVEPOINT for the entire pipeline operation
- Create TEMP tables that are automatically cleaned up
- Rollback on errors, release on success
- Explicitly drop temporary tables for immediate resource cleanup

## Example Usage

```python
# Complex pipeline that couldn't be optimized before
pipeline = [
    {"$match": {"status": "active"}},
    {"$unwind": "$tags"},
    {"$lookup": {"from": "orders", "localField": "userId", "foreignField": "userId", "as": "userOrders"}},
    {"$unwind": "$userOrders"},
    {"$group": {"_id": "$tags", "total": {"$sum": "$userOrders.amount"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10}
]

# With temporary table approach, this could be processed as:
# 1. Create temp table with matched documents
# 2. Create temp table with unwound tags
# 3. Create temp table with lookup results
# 4. Create temp table with unwound orders
# 5. Create temp table with grouped results
# 6. Return sorted/limited results
```

## Technical Implementation

### Context Manager Design

```python
@contextmanager
def aggregation_pipeline_context(db_connection):
    """Context manager for temporary aggregation tables with automatic cleanup."""
    temp_tables = []
    savepoint_name = f"agg_pipeline_{uuid.uuid4().hex}"
    
    # Create savepoint for atomicity
    db_connection.execute(f"SAVEPOINT {savepoint_name}")
    
    def create_temp_table(name_suffix, query, params=None):
        """Create a temporary table for pipeline processing."""
        table_name = f"temp_{name_suffix}_{uuid.uuid4().hex}"
        if params:
            db_connection.execute(
                f"CREATE TEMP TABLE {table_name} AS {query}", params
            )
        else:
            db_connection.execute(
                f"CREATE TEMP TABLE {table_name} AS {query}"
            )
        temp_tables.append(table_name)
        return table_name
    
    try:
        yield create_temp_table
    except Exception:
        # Rollback on error
        db_connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        raise
    finally:
        # Cleanup
        db_connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Explicitly drop temp tables
        for table_name in temp_tables:
            try:
                db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass
```

### Pipeline Processing

```python
def _build_incremental_sql_pipeline(self, pipeline):
    """Process pipeline using temporary tables for intermediate results."""
    with self.aggregation_pipeline_context() as create_temp:
        # Start with base data
        current_table = create_temp(
            "base", 
            f"SELECT id, data FROM {self.collection.name}"
        )
        
        # Process each stage or compatible groups of stages
        i = 0
        while i < len(pipeline):
            stage = pipeline[i]
            stage_name = next(iter(stage.keys()))
            
            # Process compatible stage groups
            if stage_name == "$match":
                # Create filtered temp table
                where_clause, params = self._build_simple_where_clause(stage["$match"])
                new_table = create_temp(
                    "filtered",
                    f"SELECT * FROM {current_table} {where_clause}",
                    params
                )
                current_table = new_table
                i += 1
                
            elif stage_name == "$unwind":
                # Process one or more consecutive $unwind stages
                unwind_stages = []
                j = i
                while j < len(pipeline) and "$unwind" in pipeline[j]:
                    unwind_stages.append(pipeline[j]["$unwind"])
                    j += 1
                
                # Create temp table with unwound results
                new_table = self._create_unwind_temp_table(
                    create_temp, current_table, unwind_stages
                )
                current_table = new_table
                i = j  # Skip processed stages
                
            # ... handle other stages similarly
            
        # Return results from final temporary table
        return list(self.collection.db.execute(f"SELECT * FROM {current_table}"))
```

## Performance Considerations

1. **Temporary Table Overhead**: Creating and dropping tables has overhead, but this is offset by:
   - Reduced Python memory usage
   - Database-level processing of intermediate results
   - Better query optimization for complex operations

2. **When to Use**: The system should:
   - Use single-query optimization when possible (lowest overhead)
   - Fall back to temporary tables for complex pipelines
   - Use Python fallback only for unsupported operations

3. **Resource Management**: 
   - TEMP tables are automatically cleaned up when connection closes
   - Explicit cleanup ensures immediate resource release
   - SAVEPOINTs ensure atomicity

## Testing Approach

1. **Correctness**: Ensure results match existing Python implementation
2. **Performance**: Benchmark against both SQL single-query and Python fallback
3. **Memory Usage**: Monitor memory consumption during processing
4. **Error Handling**: Verify proper cleanup on errors
5. **Edge Cases**: Test with empty results, large datasets, etc.

## Future Enhancements

1. **Hybrid Processing**: Combine SQL temporary tables with targeted Python processing
2. **Query Planning**: Intelligently decide which approach to use based on pipeline complexity
3. **Parallel Processing**: Process independent pipeline branches in parallel
4. **Streaming Results**: Stream results from temporary tables to reduce memory usage

This enhancement would significantly expand the range of pipelines that can be processed efficiently with SQL optimization while maintaining the reliability of the Python fallback for complex cases.