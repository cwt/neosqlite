# Tier Optimization Plan

## Overview

This document outlines the plan for enhancing NeoSQLite's aggregation pipeline performance by moving operations from Tier-3 (Python fallback) to Tier-1 (Single SQL Query) and Tier-2 (Temporary Table SQL).

## Architecture Review

### Current Three-Tier System

```text
┌─────────────────────────────────────────────────────────────┐
│              Aggregation Pipeline Processing                 │
├─────────────────────────────────────────────────────────────┤
│  aggregate()                                                │
│    └─> QueryEngine.aggregate_with_constraints()             │
│         └─> Try Tier 1: SQL Optimization (CTEs)             │
│              ├─ Success → Return results                    │
│              └─ Fail → Try Tier 2: Temp Tables              │
│                   ├─ Success → Return results               │
│                   └─ Fail → Tier 3: Python Fallback         │
└─────────────────────────────────────────────────────────────┘
```

| Tier | Mechanism | Performance | Current Coverage |
|------|-----------|-------------|------------------|
| **Tier-1** | CTEs, single SQL query | 10-100x faster | ~85% |
| **Tier-2** | Temporary tables, sequential SQL | 5-10x faster | ~10% |
| **Tier-3** | Python evaluation | Baseline | ~5% (fallback) |

---

## Kill Switch Requirement

### Global Fallback Flag

All Tier-1 and Tier-2 implementations **MUST** respect the global kill switch that forces Python fallback.

**Location**: `neosqlite/collection/query_helper/utils.py`

```python
# Global flag for forcing Python fallback
_FORCE_FALLBACK = False

def get_force_fallback() -> bool:
    """Get current fallback force status."""
    return _FORCE_FALLBACK

def set_force_fallback(force: bool = True) -> None:
    """Force all aggregation queries to use Python fallback."""
    global _FORCE_FALLBACK
    _FORCE_FALLBACK = force
```

### Implementation Requirements

1. **Tier-1 Check** (`sql_tier_aggregator.py`):

   ```python
   def can_optimize_pipeline(self, pipeline: List[Dict[str, Any]]) -> bool:
       # Check kill switch FIRST
       if get_force_fallback():
           return False

       # ... rest of optimization checks
   ```

2. **Tier-2 Check** (`temporary_table_aggregation.py`):

   ```python
   def execute_2nd_tier_aggregation(collection, pipeline: List[Dict[str, Any]]):
       # Check kill switch FIRST
       if get_force_fallback():
           raise NotImplementedError("Force fallback - use Tier 3")

       # ... rest of Tier-2 processing
   ```

3. **Expr Evaluation** (`query_helper/__init__.py`):

   ```python
   def _build_expr_where_clause(self, query: Dict[str, Any]):
       # Check kill switch FIRST
       if get_force_fallback():
           return None  # Force Python fallback

       # ... rest of expr evaluation
   ```

### Kill Switch Usage

```python
from neosqlite.collection.query_helper.utils import set_force_fallback, get_force_fallback

# Force Python fallback for debugging/benchmarking
set_force_fallback(True)

# Run aggregation - will use Tier-3 Python
result = collection.aggregate(pipeline)

# Disable fallback
set_force_fallback(False)

# Run aggregation - will use Tier-1/Tier-2 optimization
result = collection.aggregate(pipeline)
```

---

## Unit Test Requirements

### Test Pattern: Tier Comparison

All Tier-1 and Tier-2 implementations **MUST** have unit tests that verify correctness by comparing against Tier-3 Python results.

### Test Template

```python
import pytest
from neosqlite.collection.query_helper.utils import set_force_fallback, get_force_fallback


class TestTierOptimization:
    """Test class for tier optimization correctness."""
    
    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)
    
    def test_addToSet_tier1_vs_tier3(self, collection):
        """Verify Tier-1 $addToSet produces identical results to Tier-3 Python."""
        pipeline = [
            {"$group": {
                "_id": "$category",
                "unique_tags": {"$addToSet": "$tag"}
            }}
        ]
        
        # Get Tier-1/Tier-2 optimized results
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))
        
        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))
        
        # Results MUST be identical
        assert self._normalize_result(tier1_result) == self._normalize_result(tier3_result)
    
    def test_first_last_tier1_vs_tier3(self, collection):
        """Verify Tier-1 $first/$last produces identical results to Tier-3 Python."""
        pipeline = [
            {"$sort": {"date": 1}},
            {"$group": {
                "_id": "$category",
                "first_date": {"$first": "$date"},
                "last_date": {"$last": "$date"}
            }}
        ]
        
        # Compare Tier-1 vs Tier-3
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))
        
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))
        
        assert self._normalize_result(tier1_result) == self._normalize_result(tier3_result)
    
    def test_stdDevPop_tier1_vs_tier3(self, collection):
        """Verify Tier-1 $stdDevPop produces identical results to Tier-3 Python."""
        pipeline = [
            {"$group": {
                "_id": "$category",
                "stddev": {"$stdDevPop": "$value"}
            }}
        ]
        
        set_force_fallback(False)
        tier1_result = list(collection.aggregate(pipeline))
        
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))
        
        # Compare with tolerance for floating point
        assert self._compare_numeric_results(tier1_result, tier3_result, tolerance=1e-10)
    
    def _normalize_result(self, result):
        """Normalize aggregation results for comparison."""
        # Sort, convert to comparable format, etc.
        return sorted(
            [
                {"_id": doc["_id"], **{k: sorted(v) if isinstance(v, list) else v 
                                       for k, v in doc.items() if k != "_id"}}
                for doc in result
            ],
            key=lambda x: str(x["_id"])
        )
    
    def _compare_numeric_results(self, result1, result2, tolerance=1e-10):
        """Compare numeric results with tolerance for floating point."""
        if len(result1) != len(result2):
            return False
        
        for doc1, doc2 in zip(result1, result2):
            for key in doc1:
                if isinstance(doc1[key], float) and isinstance(doc2.get(key), float):
                    if abs(doc1[key] - doc2[key]) > tolerance:
                        return False
                elif doc1[key] != doc2.get(key):
                    return False
        return True
```

### Test File Structure

New test files created in `tests/test_tier1/` (following the pattern of `tests/test_tier2/`):

```text
tests/test_tier1/
├── __init__.py
├── test_addtoset.py
├── test_stddev.py
└── ... (future Tier-1 tests)
```

### Test Coverage Requirements

| Enhancement | Required Tests |
|-------------|----------------|
| `$addToSet` | Basic, with nulls, with arrays, grouped |
| `$first`/`$last` | With sorting, without sorting, null handling |
| `$stdDevPop`/`$stdDevSamp` | Empty set, single value, multiple values |
| `$unwind` | Basic, preserveNullAndEmptyArrays, includeArrayIndex |
| `$facet` | Single pipeline, multiple pipelines, nested facets |
| `$split` | Basic split, null handling, empty string |
| Set operators | All 7 operators with various inputs |

---

## Implementation Priority

### P0: Quick Wins (Low Effort, High Impact)

#### 1. `$addToSet` Native SQL Support

**Current Status**: Implemented in Tier-2 using `json_group_array(DISTINCT ...)`

**Target**: Add Tier-1 support in `sql_tier_aggregator.py`

**Files to Modify**:
- `neosqlite/collection/sql_tier_aggregator.py` - `_map_accumulator_to_sql()`

**Implementation**:

```python
def _map_accumulator_to_sql(self, op: str) -> str | None:
    mapping = {
        "$sum": "SUM",
        "$avg": "AVG",
        "$min": "MIN",
        "$max": "MAX",
        "$count": "COUNT",
        "$first": None,  # Requires ordering
        "$last": None,   # Requires ordering
        "$push": "json_group_array",
        "$addToSet": "json_group_array",  # Use DISTINCT in caller
    }
    return mapping.get(op)
```

**SQL Pattern**:

```sql
SELECT
  group_key,
  json_group_array(DISTINCT json_extract(data, '$.field')) AS unique_values
FROM collection
GROUP BY group_key
```

**Estimated Effort**: 1-2 hours

---

#### 2. `$stdDevPop` / `$stdDevSamp` SQL Support

**Current Status**: Tier-3 only (Python calculation)

**Target**: Add Tier-1 support using SQL math functions

**Files to Modify**:
- `neosqlite/collection/sql_tier_aggregator.py` - `_build_group_sql()`

**Implementation**:

```python
def _build_group_sql(self, spec, prev_stage, context):
    # ... existing code ...

    for field, accumulator in spec.items():
        if field == "_id":
            continue

        for op, expr in accumulator.items():
            match op:
                case "$stdDevPop":
                    expr_sql, expr_params = self._convert_operand_to_sql(expr)
                    sql = f"SQRT(AVG({expr_sql} * {expr_sql}) - AVG({expr_sql}) * AVG({expr_sql}))"
                    select_parts.append(f"{sql} AS {field}")
                
                case "$stdDevSamp":
                    expr_sql, expr_params = self._convert_operand_to_sql(expr)
                    # Sample std dev formula
                    sql = f"""
                    SQRT(
                      (COUNT({expr_sql}) * SUM({expr_sql} * {expr_sql}) - SUM({expr_sql}) * SUM({expr_sql}))
                      / (COUNT({expr_sql}) * (COUNT({expr_sql}) - 1))
                    )
                    """
                    select_parts.append(f"{sql} AS {field}")
```

**Estimated Effort**: 2-4 hours

---

### P1: High Impact (Medium-High Effort)

#### 3. `$first` / `$last` with Window Functions

**Current Status**: Implemented in Tier-2 using subqueries

**Target**: Add Tier-1 support using SQL window functions

**Files to Modify**:
- `neosqlite/collection/sql_tier_aggregator.py`

**SQL Pattern**:

```sql
-- For $first with ordering
SELECT
  group_key,
  FIRST_VALUE(json_extract(data, '$.field'))
    OVER (PARTITION BY group_key ORDER BY sort_key) AS first_value
FROM collection
```

**Estimated Effort**: 4-8 hours

---

#### 4. `$unwind` Full Tier-1 Support

**Current Status**: Marked as "complex" - incomplete implementation

**Target**: Full Tier-1 support with all options

**Files to Modify**:
- `neosqlite/collection/sql_tier_aggregator.py` - `_build_unwind_sql()`

**Features**:
- Basic unwind
- `preserveNullAndEmptyArrays`
- `includeArrayIndex`

**Estimated Effort**: 16-24 hours

---

### P2: Medium Priority

#### 5. `$group` with Expression Keys (Tier-2)

**Current Status**: ✅ COMPLETED

**Target**: Support expressions in `$group` `_id` field

**Files Modified**:
- `neosqlite/collection/temporary_table_aggregation.py` - `_process_group_stage()`

**Implementation**:
- Added `ExprEvaluator` instance to `TemporaryTableAggregationProcessor`
- Uses `build_select_expression()` to translate expression keys to SQL
- Kill switch check at method entry
- Falls back to Python for parameterized expressions

**Test Coverage**:
- 10 new tests in `tests/test_tier2/test_group_expr_keys.py`
- All tests compare Tier-2 vs Tier-3 results for correctness

**Estimated Effort**: 4-8 hours ✅ COMPLETED

---

#### 6. `$split` String Operator

**Current Status**: ✅ COMPLETED

**Target**: Tier-1 using recursive CTE

**Files Modified**:
- `neosqlite/collection/expr_evaluator/sql_converters.py` - `_convert_string_operator()`

**Implementation**:
- Recursive CTE with `instr()` and `substr()` for string splitting
- Pattern: `WITH RECURSIVE split(remaining, element, idx) AS (...)`
- Handles edge cases: empty strings, leading/trailing delimiters, multiple consecutive delimiters
- Safety limit of 1000 iterations to prevent infinite recursion
- Returns `json_group_array()` of split elements

**Test Coverage**:
- 15 new tests in `tests/test_tier1/test_split.py`
- All tests compare Tier-1 vs Tier-3 results for correctness

**Estimated Effort**: 8-16 hours ✅ COMPLETED

---

### P3: Lower Priority

#### 7. Set Operators

**Operators**: `$setEquals`, `$setIntersection`, `$setUnion`, `$setDifference`, `$setIsSubset`, `$anyElementTrue`, `$allElementsTrue`

**Current Status**: All Tier-3 only

**Target**: Tier-1 using `json_each`

**Files to Modify**:
- `neosqlite/collection/expr_evaluator/sql_converters.py`

**Estimated Effort**: 16-24 hours (7 operators)

---

#### 8. `$facet` Stage

**Current Status**: Marked as "complex"

**Target**: Tier-1 using multiple CTEs

**Files to Modify**:
- `neosqlite/collection/sql_tier_aggregator.py`

**Estimated Effort**: 16-24 hours

---

## Implementation Checklist

### Phase 1: P0 Quick Wins ✅ COMPLETED

- [x] `$addToSet` Tier-1 support
  - [x] Implement in `sql_tier_aggregator.py`
  - [x] Add unit tests with kill switch comparison
  - [x] Verify results match Tier-3 Python
  - [x] All tests pass: `test_addToSet_basic`, `test_addToSet_with_expression`, `test_addToSet_with_literal`, `test_addToSet_empty_collection`
  
- [x] `$stdDevPop` / `$stdDevSamp` Tier-1 support
  - [x] Implement in `sql_tier_aggregator.py`
  - [x] Add unit tests with kill switch comparison
  - [x] Verify results match Tier-3 Python
  - [x] All tests pass: `test_stdDevPop_basic`, `test_stdDevSamp_basic`, `test_stdDevPop_with_expression`, `test_stdDevPop_known_values`, `test_stdDevPop_multiple_groups`

**Test Results**: 1923 passed, 5 xfailed, 2 xpassed - No regressions!

### Phase 2: P1 High Impact (COMPLEX - Requires Design)

- [ ] `$first` / `$last` Tier-1 support
  - **Status**: COMPLEX - Tier-2 has limitation, Tier-1 not implemented
  - **Current State**:
    - Tier-1: Correctly falls back to Python (returns `None` from `_map_accumulator_to_sql`)
    - Tier-2: **LIMITATION** - Falls back to Python when preceded by `$sort` stage
      - Without `$sort`: Uses Tier-2 SQL with insertion order (non-deterministic, matches MongoDB behavior)
      - With `$sort`: Raises `NotImplementedError` → forces Tier-3 Python fallback (correct results)
    - Tier-3 (Python): Works correctly for all cases
  - **MongoDB Behavior Note**:
    - MongoDB docs: "$first/$last are only meaningful when documents are in a defined order"
    - MongoDB does NOT enforce `$sort` - allows non-deterministic results without it
    - This is a design weakness; a stricter API would require `$sort` for these operators
    - NeoSQLite matches MongoDB's permissive behavior for compatibility
  - **Challenge**:
    - Correlated subqueries in Tier-2 don't preserve sort order across groups
    - Window functions (ROW_NUMBER, FIRST_VALUE, LAST_VALUE) require CTE restructuring
    - Sort order from preceding `$sort` stage must be preserved and used in subqueries
  - **Possible Solutions**:
    1. Two-stage CTE in Tier-1: First CTE adds ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...), second filters WHERE rn = 1
    2. Fix Tier-2: Pass sort order through pipeline context, use in correlated subqueries with proper ordering
    3. Keep current behavior: Fall back to Python for $first/$last with $sort (correct but slower)
  - **Recommendation**: Current Python fallback is correct; optimize only if performance becomes an issue
  - **Test Coverage**: Added to `examples/api_comparison/aggregation_stages.py`:
    - `$group $first (with name sort)` - Tier-2 SQL, deterministic
    - `$group $last (with name sort)` - Tier-2 SQL, deterministic
    - `$group $first (with salary sort)` - Tier-3 Python fallback, deterministic
    - `$group $last (with salary sort)` - Tier-3 Python fallback, deterministic

- [x] `$unwind` Full Tier-2 Support ✅ COMPLETED
  - **Status**: COMPLETED - Tier-2 enhanced with full options support
  - **Implementation**: `temporary_table_aggregation.py` - `_process_unwind_stages()`
  - **Features**:
    - Basic array unwinding using `json_each()`
    - `preserveNullAndEmptyArrays`: Includes documents with missing/null/empty arrays
    - `includeArrayIndex`: Adds array index as new field (e.g., `$idx`)
    - **MongoDB-compatible**: Empty arrays set to `null` (not preserved as `[]`)
  - **SQL Pattern**:

    ```sql
    -- Basic unwind
    SELECT id, _id, json_set(data, '$.field', je.value) as data
    FROM collection, json_each(json_extract(data, '$.field')) as je
    WHERE json_type(json_extract(data, '$.field')) = 'array'

    -- With preserveNullAndEmptyArrays (UNION ALL approach)
    SELECT ... FROM collection, json_each(...) WHERE json_type(...) = 'array'
    UNION ALL
    SELECT id, _id,
      CASE
        WHEN json_type(...) = 'array' AND json(...) = '[]'
        THEN json_set(data, '$.field', NULL)
        ELSE data
      END
    FROM collection
    WHERE json_type(...) IS NULL OR json_type(...) != 'array' OR json(...) = '[]'

    -- With includeArrayIndex
    SELECT id, _id, json_set(json_set(data, '$.field', je.value), '$.idx', CAST(je.key AS INTEGER)) as data
    FROM collection, json_each(...) as je
    ```

  - **Test Coverage**:
    - Unit tests: 6 new tests in `tests/test_tier2/test_unwind.py`
      - `test_unwind_basic_tier2_vs_tier3` - Basic unwind
      - `test_unwind_preserve_null_and_empty_arrays` - preserveNullAndEmptyArrays
      - `test_unwind_include_array_index` - includeArrayIndex
      - `test_unwind_combined_options` - Both options combined
      - `test_unwind_empty_collection` - Empty collection
      - `test_unwind_no_arrays` - No arrays to unwind
    - Updated tests: 5 existing tests updated to match MongoDB behavior
      - `tests/test_query_engine_suite.py` (3 tests)
      - `tests/test_aggregation_pipeline.py` (1 test)
      - `tests/test_nested_fields_unwind.py` (1 test)
    - API comparison: Added to `examples/api_comparison/aggregation_additional.py`
      - `$unwind (advanced)` - Tests both options with MongoDB comparison
    - All tests passing ✅
  - **Key Fixes**:
    - Fixed empty array detection for JSONB (use `json()` wrapper for comparison)
    - Fixed `_get_results_from_table` to handle JSONB column types in temp tables
    - Fixed `_get_results_from_table` to preserve `_id` column when present
    - Fixed `$lookup` to preserve `_id` column
    - Fixed type annotations for mypy compliance
  - **Note**: Tier-1 still falls back to Python (not needed since Tier-2 works)

- [x] `$facet` Streaming Implementation ✅ COMPLETED
  - **Status**: COMPLETED - Streaming approach with batch processing
  - **Implementation**:
    - `query_helper/aggregation.py` - `_run_subpipeline()` with batch streaming
    - `query_engine/__init__.py` - `$facet` handler with temp table management
  - **Features**:
    - Each sub-pipeline processes input docs in batches (default 101)
    - Results streamed to temp tables to bound memory usage
    - Mixed Tier-1/Tier-2/Tier-3 sub-pipelines supported
    - Temp tables automatically cleaned up after results loaded
    - Works with any sub-pipeline complexity
  - **Architecture**:

    ```text
    For each sub-pipeline:
      1. Create result temp table
      2. For each batch of 101 docs:
         a. Insert batch into temp collection
         b. Run sub-pipeline (Tier-1/2/3 optimization)
         c. Insert results to result temp table
         d. Clear temp collection for next batch
      3. Return temp table name

    Combine: Load all results from temp tables → Single document
    ```

  - **Test Coverage**:
    - 7 new tests in `tests/test_tier2/test_facet.py`
      - `test_facet_tier1_vs_tier3` - Mixed Tier-1 sub-pipelines
      - `test_facet_with_match_only` - Simple match
      - `test_facet_with_project` - Projection
      - `test_facet_with_skip_limit` - Pagination
      - `test_facet_with_count` - Count operations
      - `test_facet_empty_collection` - Edge case
      - `test_facet_multiple_subpipelines` - Multiple facets
    - All existing $facet tests pass ✅
  - **Memory Impact**:
    - Before: All input docs loaded at once per sub-pipeline
    - After: Bounded memory with batch_size (default 101)
    - For 10K docs × 5 sub-pipelines: 50K → ~500 docs in memory at once

---

### Phase 2.5: Memory Optimization ✅ COMPLETED

- [x] MongoDB-compatible `batchSize` default (101)
  - Changed default from 1000 to 101 (matches MongoDB)
  - Updated: `aggregate()`, `aggregate_with_constraints()`, `_aggregate_with_quez()`
  - Updated: `AggregationCursor`, `execute_2nd_tier_aggregation()`, `process_pipeline()`
  - All tests updated and passing ✅

- [x] Memory-efficient result fetching with `fetchmany()`
  - Replaced `fetchall()` with `fetchmany(batch_size)` in:
    - Tier-1 SQL aggregation (`query_engine/__init__.py`)
    - Legacy CTE aggregation (`query_engine/__init__.py`)
    - Tier-2 temp table results (`temporary_table_aggregation.py`)
  - Memory bounded to batch_size (default 101) regardless of result set size
  - No regressions - all 701 tests pass ✅

### Phase 3: P2 Medium Priority

- [x] `$group` with expression keys (Tier-2) ✅ COMPLETED
  - [x] Implement in `temporary_table_aggregation.py` - `_process_group_stage()`
  - [x] Add kill switch check at entry point
  - [x] Add unit tests with kill switch comparison (`tests/test_tier2/test_group_expr_keys.py`)
  - [x] Verify results match Tier-3 Python
  - [x] All tests pass: 10 new tests added
  - **Implementation Details**:
    - Uses `ExprEvaluator.build_select_expression()` for expression key translation
    - Supports `$concat` and other expression operators in `_id` field
    - Falls back to Python for parameterized expressions (CREATE TABLE AS SELECT limitation)
    - Respects kill switch (`get_force_fallback()`) at method entry
  - **Test Coverage**:
    - `test_group_by_concat_expression` - Expression key with $concat
    - `test_group_by_simple_field` - Baseline field reference
    - `test_group_by_literal_value` - Group all together (_id: null)
    - `test_group_by_id_field` - Group by _id column
    - `test_group_with_addtoSet_expression_key` - Expression key + $addToSet
    - `test_group_with_push_expression_key` - Expression key + $push
    - `test_kill_switch_forces_tier3` - Kill switch enforcement
    - `test_group_expression_with_null_values` - Null/missing field handling
    - `test_group_by_arithmetic_expression` - Arithmetic expressions (with skip for params)
    - `test_group_empty_collection` - Empty collection edge case
  - **Test Results**: All 1966 tests pass, no regressions

- [x] `$split` Tier-1 support ✅ COMPLETED
  - [x] Implement recursive CTE in `expr_evaluator/sql_converters.py`
  - [x] Add unit tests with kill switch comparison (`tests/test_tier1/test_split.py`)
  - [x] Verify results match Tier-3 Python
  - [x] All tests pass: 15 new tests added
  - **Implementation Details**:
    - Uses recursive CTE with `instr()` and `substr()` for string splitting
    - Pattern: `WITH RECURSIVE split(remaining, element, idx) AS (...)`
    - Handles edge cases: empty strings, leading/trailing delimiters, multiple consecutive delimiters
    - Safety limit of 1000 iterations to prevent infinite recursion
    - Returns `json_group_array()` of split elements
  - **Test Coverage**:
    - `test_split_basic` - Basic space delimiter
    - `test_split_with_comma_delimiter` - Comma delimiter
    - `test_split_with_hyphen_delimiter` - Hyphen delimiter
    - `test_split_no_delimiter_found` - Delimiter not in string
    - `test_split_empty_string` - Empty input string
    - `test_split_multiple_consecutive_delimiters` - Multiple delimiters
    - `test_split_with_literal_string` - Literal string input
    - `test_split_with_literal_delimiter` - Literal delimiter
    - `test_split_kill_switch_forces_tier3` - Kill switch enforcement
    - `test_split_in_group_context` - $split in $group
    - `test_split_leading_delimiter` - Leading delimiter edge case
    - `test_split_trailing_delimiter` - Trailing delimiter edge case
    - `test_split_only_delimiters` - String with only delimiters
    - `test_split_single_element` - No delimiter found
    - `test_split_empty_collection` - Empty collection
  - **Test Results**: All 1981 tests pass, no regressions

### Phase 4: P3 Lower Priority

- [ ] Set operators Tier-1 support
  - [ ] Implement all 7 set operators
  - [ ] Add unit tests with kill switch comparison

- [x] `$facet` Tier-1 support ✅ COMPLETED (Streaming Approach)
  - **Note**: Implemented using streaming batch approach instead of pure SQL CTEs
  - **Benefits**: Works with ALL sub-pipeline types (Tier-1/2/3), bounded memory
  - **Implementation**: See "Phase 2: P1 High Impact" - `$facet` Streaming Implementation

---

## Performance Targets

After implementing all phases:

| Metric | Current | Target |
|--------|---------|--------|
| Tier-1 Coverage | ~85% | ~92% |
| Tier-2 Coverage | ~10% | ~6% |
| Tier-3 Fallback | ~5% | ~2% |
| **Avg. Pipeline Speed** | Baseline | **2-5x faster** |

---

## Testing Strategy

### 1. Kill Switch Verification

Every new Tier-1/Tier-2 feature must include tests that:

1. Run pipeline with optimizations enabled (`set_force_fallback(False)`)
2. Run same pipeline with Python fallback (`set_force_fallback(True)`)
3. Assert results are identical

### 2. Edge Cases

All tests must cover:

- Null/None values
- Empty arrays
- Missing fields
- Type mismatches
- Large datasets (performance regression)

### 3. Integration Tests

After each phase, run full test suite:

```bash
# Run all aggregation tests
pytest tests/collection/test_aggregation/ -v

# Run PyMongo compatibility tests
pytest tests/pymongo_compat/ -v

# Run with coverage
pytest --cov=neosqlite --cov-report=html
```

---

## Code Review Checklist

Before merging any tier optimization:

- [ ] Kill switch check implemented at entry point
- [ ] Unit tests compare Tier-1/Tier-2 vs Tier-3 results
- [ ] All edge cases covered (nulls, empty, missing)
- [ ] Performance benchmarked (no regression)
- [ ] Code follows existing patterns in codebase
- [ ] Docstrings updated with SQL patterns
- [ ] Type hints complete
- [ ] No circular imports introduced

---

## References

### Key Files

| File | Purpose |
|------|---------|
| `neosqlite/collection/sql_tier_aggregator.py` | Tier-1 CTE optimization |
| `neosqlite/collection/temporary_table_aggregation.py` | Tier-2 temp table processing |
| `neosqlite/collection/expr_evaluator/sql_converters.py` | SQL conversion logic |
| `neosqlite/collection/expr_evaluator/python_evaluators.py` | Tier-3 Python fallback |
| `neosqlite/collection/query_helper/utils.py` | Kill switch implementation |

### Related Documentation

- `documents/releases/v1.8.0.md` - Architecture overview
- `documents/TODO/IMPLEMENTATION_ROADMAP.md` - General roadmap
- `documents/TODO/AGGREGATION_EXPRESSION_SUPPORT.md` - Expression support status
