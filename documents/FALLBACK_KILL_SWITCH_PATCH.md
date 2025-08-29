"""
Patch demonstrating how to add a 'kill switch' for forcing Python fallback

This patch shows the modifications needed to add a global flag that forces
all aggregation queries to use the Python fallback implementation instead
of the SQL optimization.
"""

# --- Modifications needed in neosqlite/collection/query_helper.py ---

# Add at the top of the file:
"""
# Global flag to force fallback - would be added at module level
_FORCE_FALLBACK = False

def set_force_fallback(force=True):
    \"\"\"Set global flag to force all aggregation queries to use Python fallback\"\"\"
    global _FORCE_FALLBACK
    _FORCE_FALLBACK = force
"""

# --- Modifications needed in _build_aggregation_query method ---

"""
def _build_aggregation_query(
    self,
    pipeline: List[Dict[str, Any]],
) -> tuple[str, List[Any], List[str] | None] | None:
    \"\"\"
    Builds a SQL query for the given MongoDB-like aggregation pipeline.
    \"\"\"
    # NEW: Check if we should force fallback
    global _FORCE_FALLBACK
    if _FORCE_FALLBACK:
        return None  # Force fallback to Python implementation
    
    # ... rest of existing implementation ...
"""

# --- Example usage in tests ---
"""
def test_benchmark_fallback_vs_optimized():
    \"\"\"Example of how to benchmark with kill switch\"\"\"
    with neosqlite.Connection(\":memory:\") as conn:
        collection = conn[\"test_collection\"]
        
        # Insert test data
        test_data = [
            {\"category\": f\"Cat{i % 10}\", \"tags\": [f\"tag{j}\" for j in range(5)]}
            for i in range(1000)
        ]
        collection.insert_many(test_data)
        
        pipeline = [
            {\"$unwind\": \"$tags\"},
            {\"$group\": {\"_id\": \"$category\", \"count\": {\"$sum\": 1}}}
        ]
        
        # Test optimized path
        set_force_fallback(False)
        start_time = time.time()
        result_optimized = collection.aggregate(pipeline)
        optimized_time = time.time() - start_time
        
        # Test fallback path
        set_force_fallback(True)
        start_time = time.time()
        result_fallback = collection.aggregate(pipeline)
        fallback_time = time.time() - start_time
        
        # Reset
        set_force_fallback(False)
        
        # Results should be identical
        assert result_optimized == result_fallback
        
        print(f\"Optimized time: {optimized_time:.4f}s\")
        print(f\"Fallback time: {fallback_time:.4f}s\")
        print(f\"Performance ratio: {fallback_time/optimized_time:.2f}x\")
"""