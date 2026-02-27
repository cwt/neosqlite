# Aggregation Expression Support Implementation Plan

## Overview

This document describes what would be needed to extend `$expr` operator support to **aggregation pipeline expressions**, enabling operators like `$sin`, `$setIntersection`, `$toObjectId`, etc. to work in aggregation stages like `$addFields`, `$project`, `$group`, and `$facet`.

## Current State

### What Works Now

The `$expr` operators implemented in `expr_evaluator.py` work **only in query filters**:

```python
# ✅ Works - $expr in query filter
collection.find({
    "$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}
})

# ✅ Works - $expr in aggregation $match
collection.aggregate([
    {"$match": {"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}}
])
```

### What Doesn't Work

Operators **cannot** be used directly in aggregation expressions:

```python
# ❌ Doesn't work - aggregation expression
collection.aggregate([
    {"$addFields": {"sin_val": {"$sin": "$angle"}}}
])

# ❌ Doesn't work - projection
collection.find({}, {"sin_val": {"$sin": "$angle"}})

# ❌ Doesn't work - $group accumulator
collection.aggregate([
    {"$group": {"_id": "$category", "total": {"$sum": {"$multiply": ["$price", "$qty"]}}}}
])
```

## Architecture Analysis

### Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Query Processing                          │
├─────────────────────────────────────────────────────────────┤
│  find() / find_one()                                        │
│    └─> QueryEngine._build_query()                          │
│         └─> Handles $expr via ExprEvaluator                │
│              └─> expr_evaluator.py (3-tier architecture)   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                Aggregation Pipeline Processing               │
├─────────────────────────────────────────────────────────────┤
│  aggregate()                                                │
│    └─> AggregationCursor                                    │
│         └─> Processes stages: $match, $project, $group...  │
│              └─> Each stage has its own expression handler │
│                   └─> NO unified expression evaluator      │
└─────────────────────────────────────────────────────────────┘
```

### Key Files Involved

| File | Current Purpose | Changes Needed | Priority |
|------|-----------------|----------------|----------|
| `neosqlite/collection/expr_evaluator.py` | $expr expression evaluator | Add AggregationContext, evaluate_for_aggregation(), expression type detection | High |
| `neosqlite/collection/query_helper.py` | Aggregation pipeline processing | Update stage handlers ($addFields, $project, $group, $match) | High |
| `neosqlite/collection/temporary_table_aggregation.py` | Temp table management | Multi-stage pipeline optimization | Medium |
| `neosqlite/collection/aggregation_cursor.py` | Aggregation cursor | Minor updates for expression support | Low |
| `neosqlite/collection/sql_translator_unified.py` | SQL translation | Extend for aggregation expressions | Medium |

## Implementation Requirements

### 0. Design Principles

**Backward Compatibility**: All existing `$expr` queries and aggregation pipelines continue to work unchanged.

**Dual Syntax Support for $match**:
```python
# Both syntaxes work:
{"$match": {"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}}}  # Existing (with $expr)
{"$match": {"$gt": [{"$sin": "$angle"}, 0.5]}}            # New (direct expression)
```

**Gradual Adoption**: Users can adopt new expression features incrementally without breaking existing code.

### 1. Unified Expression Evaluator

**Goal**: Create a shared expression evaluation framework that works for both `$expr` queries and aggregation expressions.

**Current State**:
- `ExprEvaluator` class handles `$expr` expressions
- Aggregation stages use ad-hoc SQL generation

**Required Changes**:

```python
# New: AggregationContext class for variable scoping
class AggregationContext:
    """Manages variable scoping for aggregation expressions."""

    def __init__(self):
        self.variables = {
            "$$ROOT": None,      # Original document
            "$$CURRENT": None,   # Current document (may be modified)
            "$$REMOVE": None,    # Sentinel for field removal
        }
        self.stage_index = 0
        self.current_field = None  # For computed field context
        self.pipeline_id = None  # For temp table correlation

    def bind_document(self, doc):
        """Bind document to context."""
        self.variables["$$ROOT"] = doc
        self.variables["$$CURRENT"] = doc

    def update_current(self, doc):
        """Update current document after stage processing."""
        self.variables["$$CURRENT"] = doc

    def get_variable(self, name):
        """Get variable value."""
        return self.variables.get(name)

# Modified: ExprEvaluator class with aggregation support
class ExprEvaluator:
    """Unified expression evaluator for both $expr and aggregation."""

    def __init__(self, data_column="data", db_connection=None):
        self.data_column = data_column
        self._jsonb_supported = False
        # ... existing init code ...
        
    def evaluate(
        self, expr: Dict[str, Any], tier: int = 1, force_python: bool = False
    ) -> Tuple[Optional[str], List[Any]]:
        """Evaluate $expr expression (existing method)."""
        # ... existing implementation ...

    def evaluate_for_aggregation(
        self, 
        expr: Any, 
        context: Optional[AggregationContext] = None,
        as_alias: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """
        Evaluate expression for aggregation pipeline.
        
        Args:
            expr: Expression to evaluate (can be dict, str, or literal)
            context: Aggregation context for variable scoping
            as_alias: Optional alias for SELECT clause (e.g., "AS field_name")
            
        Returns:
            Tuple of (SQL expression, parameters)
            
        Examples:
            >>> evaluator.evaluate_for_aggregation({"$sin": "$angle"})
            ("sin(json_extract(data, '$.angle'))", [])
            
            >>> evaluator.evaluate_for_aggregation({"$sin": "$angle"}, as_alias="sin_val")
            ("sin(json_extract(data, '$.angle')) AS sin_val", [])
        """
        if context is None:
            context = AggregationContext()
            
        sql, params = self._convert_operand_to_sql_agg(expr, context)
        
        if as_alias:
            sql = f"{sql} AS {as_alias}"
            
        return sql, params
```

### 2. Stage-Specific Expression Handlers

Each aggregation stage needs to support expressions:

#### $addFields / $project

```python
# Current implementation (simplified)
def _handle_add_fields(self, stage):
    for field, value in stage.items():
        if value.startswith("$"):
            # Just field reference
            sql = f"json_extract(data, '$.{value[1:]}')"
        else:
            # Literal value
            sql = f"json(?)", [value]

# Required enhancement
def _handle_add_fields(self, stage):
    evaluator = ExpressionEvaluator(context="aggregation")
    for field, expr in stage.items():
        if isinstance(expr, dict) and self._is_expression(expr):
            # Evaluate full expression
            sql, params = evaluator.evaluate_for_aggregation(expr)
        elif isinstance(expr, str) and expr.startswith("$"):
            # Field reference
            sql = self._build_field_reference(expr)
        else:
            # Literal
            sql, params = self._build_literal(expr)
```

#### $group

```python
# Current implementation
def _handle_group(self, stage):
    group_fields = []
    for field, accumulator in stage.items():
        if field == "_id":
            # Group key
            sql = self._build_group_key(accumulator)
        else:
            # Accumulator: $sum, $avg, etc.
            sql = self._build_accumulator(accumulator)

# Required enhancement
def _handle_group(self, stage):
    evaluator = ExpressionEvaluator(context="aggregation")
    for field, accumulator in stage.items():
        if field == "_id":
            # Support expressions in group key
            if isinstance(accumulator, dict):
                sql, params = evaluator.evaluate_for_aggregation(accumulator)
            else:
                sql = self._build_group_key(accumulator)
        else:
            # Support expressions in accumulators
            # e.g., {"$sum": {"$multiply": ["$price", "$qty"]}}
            sql, params = self._build_accumulator_with_expr(accumulator, evaluator)
```

#### $match

```python
# Current: Already supports $expr
{"$match": {"$expr": {"$gt": ["$qty", 10]}}}

# Enhancement: Support expressions directly
{"$match": {"$gt": [{"$sin": "$angle"}, 0.5]}}  # Without $expr wrapper
```

#### $facet

```python
# Current: Sequential sub-pipeline execution
# Enhancement: Each sub-pipeline stage supports full expressions
```

### 2. Expression Type Detection

Need to distinguish between:
- **Field references**: `"$field"` or `"$nested.field"`
- **Literals**: `42`, `"string"`, `true`, `null`
- **Expressions**: `{"$operator": [...]}`
- **Aggregation variables**: `"$$ROOT"`, `"$$CURRENT"`, `"$$REMOVE"`

```python
# Reserved field names that are NOT operators
RESERVED_FIELDS = {
    "$field", "$index",  # Used in $let
    # Add other reserved names as needed
}

def _is_expression(value: Any) -> bool:
    """
    Check if value is an aggregation expression.
    
    An expression is a dict with exactly one key starting with '$'
    that is not a reserved field name.
    """
    if not isinstance(value, dict):
        return False
    if len(value) != 1:
        return False  # Could be a literal dict
    key = next(iter(value.keys()))
    return key.startswith("$") and key not in RESERVED_FIELDS

def _is_field_reference(value: Any) -> bool:
    """
    Check if value is a field reference.
    
    Field references start with '$' but are not expressions
    (i.e., they're simple strings like "$field" or "$nested.field").
    """
    return isinstance(value, str) and value.startswith("$") and not value.startswith("$$")

def _is_aggregation_variable(value: Any) -> bool:
    """
    Check for aggregation variables.
    
    Aggregation variables start with '$$' (e.g., $$ROOT, $$CURRENT).
    """
    return isinstance(value, str) and value.startswith("$$")

def _is_literal(value: Any) -> bool:
    """
    Check if value is a literal (not an expression or field reference).
    
    Literals include: numbers, strings, booleans, None, arrays, and plain dicts.
    """
    if isinstance(value, str):
        # Strings starting with $ are field refs or variables, not literals
        return not value.startswith("$")
    # All other types are literals
    return True

# Usage in stage handlers
def _handle_field_or_expression(self, value, evaluator, context):
    """Handle a value that could be a field ref, literal, or expression."""
    if _is_expression(value):
        return evaluator.evaluate_for_aggregation(value, context)
    elif _is_aggregation_variable(value):
        return self._handle_aggregation_variable(value, context)
    elif _is_field_reference(value):
        return self._build_field_reference(value), []
    else:
        return self._build_literal(value), []
```

### 3. Context and Variable Scoping

Aggregation expressions have different variable contexts:

```python
class AggregationContext:
    """Manages variable scoping for aggregation expressions."""
    
    def __init__(self):
        self.variables = {
            "$$ROOT": None,      # Original document
            "$$CURRENT": None,   # Current document (may be modified)
            "$$REMOVE": None,    # Sentinel for field removal
        }
        self.stage_index = 0
    
    def bind_document(self, doc):
        """Bind document to context."""
        self.variables["$$ROOT"] = doc
        self.variables["$$CURRENT"] = doc
    
    def update_current(self, doc):
        """Update current document after stage processing."""
        self.variables["$$CURRENT"] = doc
    
    def get_variable(self, name):
        """Get variable value."""
        return self.variables.get(name)
```

### 4. SQL Generation for Aggregation

Aggregation expressions need different SQL generation than query filters:

```python
# Query filter (WHERE clause)
{"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}}
# Generates: WHERE sin(json_extract(data, '$.angle')) > 0.5

# Aggregation expression (SELECT clause)
{"$addFields": {"sin_val": {"$sin": "$angle"}}}
# Generates: SELECT ..., sin(json_extract(data, '$.angle')) AS sin_val

# Aggregation with GROUP BY
{"$group": {"_id": "$category", "total": {"$sum": {"$multiply": ["$price", "$qty"]}}}}
# Generates: 
# SELECT json_extract(data, '$.category') AS _id,
#        sum(json_extract(data, '$.price') * json_extract(data, '$.qty')) AS total
# GROUP BY json_extract(data, '$.category')
```

### 5. Temporary Table Support for Complex Aggregations

For complex multi-stage aggregations, use temporary tables:

```python
# Pipeline with expressions
[
    {"$addFields": {"sin_angle": {"$sin": "$angle"}}},  # SQL tier
    {"$match": {"sin_angle": {"$gt": 0.5}}},            # SQL tier
    {"$addFields": {"rounded": {"$round": "$sin_angle"}}},  # SQL tier
]

# Implementation using temp tables
def process_aggregation_pipeline(pipeline):
    temp_table = create_temp_table()
    
    for i, stage in enumerate(pipeline):
        if can_process_in_sql(stage):
            # Generate SQL for stage
            sql = build_sql_for_stage(stage, temp_table)
            execute_sql(sql)
        else:
            # Fall back to Python
            docs = load_from_temp_table(temp_table)
            docs = process_stage_in_python(stage, docs)
            store_in_temp_table(temp_table, docs)
    
    return load_from_temp_table(temp_table)
```

## Implementation Phases (Updated)

### Phase 1: Foundation (Weeks 1-3) ✅ COMPLETED

**Task 1.1: Create Unified Expression Evaluator** (3 days) ✅
- Add `AggregationContext` class for variable scoping
- Add `evaluate_for_aggregation()` method to `ExprEvaluator`
- Support SELECT clause generation (vs. WHERE clause)
- File: `neosqlite/collection/expr_evaluator.py`

**Task 1.2: Expression Type Detection** (2 days) ✅
- Implement `_is_expression()` helper
- Implement `_is_field_reference()` helper
- Implement `_is_aggregation_variable()` helper
- Implement `_is_literal()` helper
- File: `neosqlite/collection/expr_evaluator.py`

**Task 1.3: Update `_convert_operand_to_sql_agg`** (2 days) ✅
- Support all expression types in aggregation context
- Handle `$$` variables
- File: `neosqlite/collection/expr_evaluator.py`

**Tests**: 32 tests in `tests/test_expr/test_aggregation_expressions.py`, all passing.

### Phase 2: Core Stages (Weeks 4-7) ✅ COMPLETED

**Task 2.1: Update `$addFields` Stage** (3 days) ✅
- Support full expressions in field values
- File: `neosqlite/collection/query_engine.py`
- Implementation: Python fallback with `ExprEvaluator._evaluate_expr_python()`

**Task 2.2: Update `$project` Stage** (3 days) ✅
- Support computed fields with expressions
- File: `neosqlite/collection/query_helper.py`
- Implementation: Enhanced `_apply_projection()` method

**Task 2.3: Update `$group` Stage** (5 days) ✅
- Support expressions in group keys
- Support expressions in accumulators (e.g., `{"$sum": {"$multiply": ["$price", "$qty"]}}`)
- File: `neosqlite/collection/query_helper.py`
- Implementation: Enhanced `_process_group_stage()` method

**Task 2.4: Update `$match` Stage** (2 days) ✅
- Support `$expr` in Python fallback for aggregation
- File: `neosqlite/collection/query_helper.py`
- Implementation: Added `$expr` case in `_apply_query()` method

**Tests**: 
- 9 tests in `tests/test_expr/test_addfields_expressions.py`
- 3 tests in `tests/test_expr/test_project_expressions.py`
- 4 tests in `tests/test_expr/test_group_expressions.py`
- All passing.

### Phase 3: Advanced Features (Weeks 8-10) ✅ COMPLETED

**Task 3.1: Variable Scoping Implementation** (3 days) ✅
- Support `$$ROOT`: Original document
- Support `$$CURRENT`: Current document (may be modified through pipeline)
- Support `$$REMOVE`: Sentinel for field removal in `$project`
- File: `neosqlite/collection/expr_evaluator.py`
- Implementation: `REMOVE_SENTINEL` class, document context wrapper

**Task 3.2: Update `$facet` Stage** (4 days) ✅
- Each sub-pipeline uses the unified evaluator
- File: `neosqlite/collection/query_helper.py`
- Implementation: `_run_subpipeline()` helper method

**Task 3.3: Temporary Table Optimization** (Deferred)
- Multi-stage pipeline optimization with expression caching
- Deferred to future phase (requires SQL tier optimization)

**Tests**:
- 6 tests in `tests/test_expr/test_aggregation_variables.py`
- 6 tests in `tests/test_expr/test_integration_advanced.py`
- All passing.

### Phase 4: Testing & Documentation (Weeks 11-12) ✅ COMPLETED

**Task 4.1: Unit Tests** ✅
- 32 tests for Phase 1 components
- 16 tests for Phase 2 stages
- 6 tests for Phase 3 features
- Location: `tests/test_expr/`

**Task 4.2: Integration Tests** ✅
- 6 advanced integration tests for complex pipelines
- Location: `tests/test_expr/test_integration_advanced.py`

**Task 4.3: Documentation** ✅
- User guide: `documents/AGGREGATION_EXPRESSION_GUIDE.md`
- Implementation plan: `documents/TODO/AGGREGATION_EXPRESSION_SUPPORT.md` (this file)

**Test Results**:
- **Total Tests**: 617 tests (all passing)
  - 511 expr tests
  - 106 aggregation pipeline tests

## Code Examples

### Example 1: $addFields with Expressions

```python
# After implementation
collection.aggregate([
    {"$addFields": {
        "sin_angle": {"$sin": "$angle"},
        "category": {
            "$cond": {
                "if": {"$gt": [{"$sin": "$angle"}, 0.5]},
                "then": "high",
                "else": "low"
            }
        },
        "total": {"$multiply": ["$price", "$qty", {"$add": [1, {"$tax": 0.1}]}]}
    }}
])
```

### Example 2: $group with Expressions

```python
# After implementation
collection.aggregate([
    {"$group": {
        "_id": {"$toLower": "$category"},
        "revenue": {"$sum": {"$multiply": ["$price", "$qty"]}},
        "avg_discount": {"$avg": {"$multiply": ["$discount", 100]}},
        "items": {"$push": {"$concat": ["$name", " (", "$sku", ")"]}}
    }}
])
```

### Example 3: $project with Computed Fields

```python
# After implementation
collection.aggregate([
    {"$project": {
        "name": 1,
        "price_with_tax": {"$add": ["$price", {"$multiply": ["$price", "$tax_rate"]}]},
        "category_upper": {"$toUpper": "$category"},
        "is_expensive": {"$gt": ["$price", 100]}
    }}
])
```

### Example 4: Complex Pipeline

```python
# After implementation
collection.aggregate([
    {"$match": {"status": "active"}},
    {"$addFields": {
        "sin_latitude": {"$sin": {"$degreesToRadians": "$latitude"}},
        "cos_latitude": {"$cos": {"$degreesToRadians": "$latitude"}}
    }},
    {"$group": {
        "_id": "$region",
        "avg_sin": {"$avg": "$sin_latitude"},
        "total_value": {"$sum": {"$multiply": ["$quantity", "$price"]}}
    }},
    {"$addFields": {
        "formatted_total": {"$concat": ["$", {"$toString": "$total_value"}]}
    }},
    {"$sort": {"total_value": -1}},
    {"$limit": 10}
])
```

## Testing Strategy

### Unit Tests

```python
class TestAggregationExpressions:
    def test_add_fields_with_trig(self):
        result = list(collection.aggregate([
            {"$addFields": {"sin_val": {"$sin": "$angle"}}}
        ]))
        assert "sin_val" in result[0]
    
    def test_group_with_expression(self):
        result = list(collection.aggregate([
            {"$group": {
                "_id": "$category",
                "total": {"$sum": {"$multiply": ["$price", "$qty"]}}
            }}
        ]))
        assert len(result) > 0
        assert "total" in result[0]
    
    def test_project_with_expression(self):
        result = list(collection.aggregate([
            {"$project": {
                "computed": {"$add": ["$field1", "$field2"]}
            }}
        ]))
        assert "computed" in result[0]
```

### Integration Tests

```python
class TestAggregationPipelineIntegration:
    def test_multi_stage_pipeline(self):
        result = list(collection.aggregate([
            {"$match": {"status": "active"}},
            {"$addFields": {"sin_val": {"$sin": "$angle"}}},
            {"$match": {"sin_val": {"$gt": 0.5}}},
            {"$group": {"_id": "$category", "avg_sin": {"$avg": "$sin_val"}}},
            {"$sort": {"avg_sin": -1}}
        ]))
        assert len(result) > 0
```

### Performance Benchmarks

```python
def benchmark_aggregation_expressions():
    # Compare SQL vs Python performance
    import time
    
    # SQL tier
    start = time.time()
    result_sql = list(collection.aggregate([...]))  # SQL-optimized
    sql_time = time.time() - start
    
    # Python tier (force fallback)
    set_force_fallback(True)
    start = time.time()
    result_python = list(collection.aggregate([...]))
    python_time = time.time() - start
    set_force_fallback(False)
    
    print(f"SQL: {sql_time:.3f}s, Python: {python_time:.3f}s")
    print(f"Speedup: {python_time / sql_time:.2f}x")
```

## Known Limitations

### Current Limitations

1. **$expr-only support**: Operators only work in `$expr` queries
2. **No aggregation expressions**: Cannot use in `$addFields`, `$project`, etc.
3. **Limited variable support**: `$$ROOT`, `$$CURRENT` not fully supported
4. **No let variables**: `$let` operator not available in aggregation

### Future Limitations (Even After Implementation)

1. **Performance**: Complex expressions may be slower than native MongoDB
2. **SQLite function availability**: Some operators require SQLite 3.35.0+
3. **Type coercion**: SQLite's dynamic typing may differ from MongoDB
4. **Unicode handling**: Code point operations may not be fully accurate

## Migration Path

### For Existing Code

Existing `$expr` queries continue to work:

```python
# This continues to work
collection.find({"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}})

# New syntax also available
collection.aggregate([
    {"$addFields": {"sin_angle": {"$sin": "$angle"}}},
    {"$match": {"sin_angle": {"$gt": 0.5}}}
])
```

### Backward Compatibility

- All existing aggregation pipelines continue to work
- No breaking changes to API
- Gradual adoption of new expression features

## Success Metrics

### Functional Metrics

- [ ] All 106 operators work in `$addFields`
- [ ] All 106 operators work in `$project`
- [ ] All 106 operators work in `$group` accumulators
- [ ] All 106 operators work in `$match` (without `$expr`)
- [ ] All 106 operators work in `$facet` sub-pipelines

### Performance Metrics

- [ ] SQL tier processes 80%+ of common aggregation expressions
- [ ] Average 2x speedup for SQL-optimized pipelines
- [ ] Python fallback maintains correctness

### Quality Metrics

- [ ] 95%+ test coverage for aggregation expressions
- [ ] All existing tests pass
- [ ] Documentation complete with examples

## References

- [MongoDB Aggregation Pipeline](https://docs.mongodb.com/manual/aggregation/)
- [MongoDB Aggregation Expressions](https://docs.mongodb.com/manual/meta/aggregation-quick-reference/)
- [SQLite Math Functions](https://sqlite.org/lang_mathfunc.html)
- [NeoSQLite Three-Tier Architecture](../AGGREGATION_PIPELINE_ENHANCEMENTS.md)
- [NeoSQLite $expr Implementation](../EXPR_IMPLEMENTATION.md)

## Conclusion

Implementing aggregation expression support requires:

1. **Unified expression evaluator** - Reuse `ExprEvaluator` for both contexts
2. **Stage-specific handlers** - Update each aggregation stage
3. **Variable scoping** - Support `$$ROOT`, `$$CURRENT`, etc.
4. **SQL generation** - Different SQL for SELECT vs WHERE clauses
5. **Temporary tables** - Optimize multi-stage pipelines

Estimated effort: **8-12 weeks** for full implementation with testing and documentation.

This enhancement would make NeoSQLite's aggregation pipeline fully compatible with MongoDB's expression system, enabling complex data transformations directly in the database layer.
