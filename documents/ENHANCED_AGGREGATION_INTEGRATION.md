# Integration Proposal for Temporary Table Aggregation in NeoSQLite

This file shows how the temporary table approach could be integrated into the existing `QueryEngine` to handle complex pipelines.

```python
from typing import Any, Dict, List
from .temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
    can_process_with_temporary_tables
)


class EnhancedQueryEngine:
    """
    Enhanced QueryEngine that uses temporary tables for complex pipelines.

    This is a conceptual integration showing how temporary table aggregation
    could be incorporated into the existing NeoSQLite codebase.
    """

    def __init__(self, collection):
        self.collection = collection
        self.helpers = collection.query_helper  # Existing helper methods

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enhanced aggregate method that tries multiple approaches:

        1. First, try the existing SQL optimization approach
        2. If that fails, try the temporary table approach for supported pipelines
        3. Finally, fall back to the Python implementation for unsupported cases
        """
        # Try the existing SQL optimization approach first
        try:
            # This would call the existing _build_aggregation_query method
            query_result = self.helpers._build_aggregation_query(pipeline)
            if query_result is not None:
                cmd, params, output_fields = query_result
                db_cursor = self.collection.db.execute(cmd, params)
                if output_fields:
                    # Handle results from a GROUP BY query
                    from neosqlite.collection.json_helpers import neosqlite_json_loads

                    results = []
                    for row in db_cursor.fetchall():
                        processed_row = []
                        for i, value in enumerate(row):
                            # If this field contains a JSON array string, parse it
                            # This handles $push and $addToSet results
                            if (
                                output_fields[i] != "_id"
                                and isinstance(value, str)
                                and value.startswith("[")
                                and value.endswith("]")
                            ):
                                try:
                                    processed_row.append(neosqlite_json_loads(value))
                                except:
                                    processed_row.append(value)
                            else:
                                processed_row.append(value)
                        results.append(dict(zip(output_fields, processed_row)))
                    return results
                else:
                    # Handle results from a regular find query
                    return [
                        self.collection._load(row[0], row[1])
                        for row in db_cursor.fetchall()
                    ]
        except Exception:
            # If SQL optimization fails, continue to next approach
            pass

        # Try the temporary table approach for supported pipelines
        if can_process_with_temporary_tables(pipeline):
            try:
                processor = TemporaryTableAggregationProcessor(self.collection)
                return processor.process_pipeline(pipeline)
            except Exception:
                # If temporary table approach fails, continue to fallback
                pass

        # Fall back to the existing Python implementation
        # This is the existing code from the aggregate_with_constraints method
        docs: List[Dict[str, Any]] = list(self.collection.find())
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            # ... (existing Python processing logic)
            # For brevity, we're not including the full implementation here
            # but in a real implementation, this would be the complete fallback logic

        return docs
```

## Example Usage in the Collection Class

```python
class Collection:
    # ... existing code ...

    def aggregate(self, pipeline):
        '''
        Process an aggregation pipeline.

        This method now uses a hybrid approach:
        1. Try SQL optimization
        2. Try temporary table processing
        3. Fall back to Python processing
        '''
        return self.query_engine.aggregate(pipeline)
```

## Benefits of this approach

1.  **Backward Compatibility**: Existing code continues to work without changes
2.  **Performance Improvement**: More pipelines can be processed with SQL optimization
3.  **Resource Management**: Temporary tables provide better resource management
4.  **Gradual Enhancement**: Can be implemented incrementally without breaking changes
5.  **Flexible Fallback**: Multiple fallback options ensure robustness

## Implementation considerations

1.  **Performance Testing**: Need to benchmark to ensure temporary table approach is actually faster than Python fallback for the pipelines it handles
2.  **Error Handling**: Robust error handling to ensure smooth fallback between approaches
3.  **Resource Management**: Ensure temporary tables are always cleaned up properly
4.  **Testing**: Comprehensive testing to ensure all approaches produce identical results
5.  **Documentation**: Clear documentation of when each approach is used
