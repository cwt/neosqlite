# SQL Tier Optimization for Aggregation Expressions - Design Document

## Executive Summary

This document describes the implementation of **SQL Tier 1 optimization** for aggregation expressions in NeoSQLite. Currently, aggregation expressions (`$addFields`, `$project`, `$group`, `$facet`) use Python fallback (Tier 3), which is correct but slower. This optimization will move expression evaluation to SQL Tier 1, providing 10-100x performance improvements.

## Current State

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                Aggregation Pipeline Processing               │
├─────────────────────────────────────────────────────────────┤
│  aggregate()                                                │
│    └─> AggregationCursor                                    │
│         └─> QueryHelper._build_aggregation_query()          │
│              └─> Stage handlers ($addFields, $project, etc.)│
│                   └─> Python fallback (Tier 3)              │
│                        └─> ExprEvaluator._evaluate_expr_python()
└─────────────────────────────────────────────────────────────┘
```

### Performance Characteristics

| Stage | Current Tier | Performance |
|-------|-------------|-------------|
| `$addFields` with expressions | Python Tier 3 | 🐌 Slow |
| `$project` with expressions | Python Tier 3 | 🐌 Slow |
| `$group` with expressions | Python Tier 3 | 🐌 Slow |
| `$match` with `$expr` | SQL Tier 1 | ⚡ Fast (10-100x) |
| `$facet` sub-pipelines | Python Tier 3 | 🐌 Slow |

## Target State

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                Aggregation Pipeline Processing               │
├─────────────────────────────────────────────────────────────┤
│  aggregate()                                                │
│    └─> AggregationCursor                                    │
│         └─> SQLTierAggregationOptimizer                     │
│              └─> Analyze pipeline complexity                │
│                   └─> Can optimize in SQL?                  │
│                        ├─ YES → Build SQL with CTEs         │
│                        │     └─> Execute single SQL query   │
│                        └─ NO → Python fallback (Tier 3)     │
└─────────────────────────────────────────────────────────────┘
```

### Performance Targets

| Stage | Target Tier | Expected Performance |
|-------|-------------|---------------------|
| `$addFields` with expressions | SQL Tier 1 | ⚡ 10-100x faster |
| `$project` with expressions | SQL Tier 1 | ⚡ 10-100x faster |
| `$group` with expressions | SQL Tier 1 | ⚡ 10-100x faster |
| `$match` with expressions | SQL Tier 1 | ⚡ Maintain current |
| `$facet` sub-pipelines | SQL Tier 1 | ⚡ 10-100x faster |

## SQLite Capabilities Analysis

### Key SQLite Features for Aggregation

Based on official SQLite documentation:

#### 1. **SELECT with Computed Columns**
```sql
SELECT 
    json_extract(data, '$.price') AS price,
    json_extract(data, '$.quantity') AS quantity,
    json_extract(data, '$.price') * json_extract(data, '$.quantity') AS revenue
FROM collection
```

#### 2. **GROUP BY with Expressions**
```sql
SELECT 
    json_extract(data, '$.category') AS category,
    SUM(json_extract(data, '$.price') * json_extract(data, '$.quantity')) AS total_revenue
FROM collection
GROUP BY json_extract(data, '$.category')
```

#### 3. **HAVING Clause**
```sql
SELECT 
    json_extract(data, '$.category') AS category,
    COUNT(*) AS count
FROM collection
GROUP BY json_extract(data, '$.category')
HAVING COUNT(*) > 10
```

#### 4. **ORDER BY with Aliases**
```sql
SELECT 
    json_extract(data, '$.price') * json_extract(data, '$.quantity') AS revenue
FROM collection
ORDER BY revenue DESC
```

#### 5. **Common Table Expressions (CTEs)**
```sql
WITH 
stage1 AS (
    SELECT 
        id,
        json_set(data, '$.revenue', json_extract(data, '$.price') * json_extract(data, '$.quantity')) AS data
    FROM collection
),
stage2 AS (
    SELECT 
        id,
        json_set(data, '$.tax', json_extract(data, '$.revenue') * 0.08) AS data
    FROM stage1
)
SELECT * FROM stage2
```

#### 6. **Window Functions** (for future enhancement)
```sql
SELECT 
    json_extract(data, '$.category') AS category,
    json_extract(data, '$.price') AS price,
    ROW_NUMBER() OVER (PARTITION BY json_extract(data, '$.category') ORDER BY json_extract(data, '$.price') DESC) AS rank
FROM collection
```

## Implementation Strategy

### Phase 1: Foundation (Week 1)

#### 1.1 Create `SQLTierAggregator` Class

New class in `neosqlite/collection/sql_tier_aggregator.py`:

```python
class SQLTierAggregator:
    """
    SQL Tier 1 optimizer for aggregation pipelines.
    
    Analyzes aggregation pipelines and generates optimized SQL queries
    using CTEs (Common Table Expressions) for multi-stage pipelines.
    """
    
    def __init__(self, collection, expr_evaluator: ExprEvaluator):
        self.collection = collection
        self.evaluator = expr_evaluator
        self._jsonb_supported = supports_jsonb(collection.db)
        
    def can_optimize_pipeline(self, pipeline: List[Dict]) -> bool:
        """
        Check if pipeline can be optimized in SQL tier.
        
        Returns False if pipeline contains:
        - Unsupported stages
        - Complex expressions requiring Python
        - Advanced features not supported in SQL
        """
        
    def build_sql_query(self, pipeline: List[Dict]) -> Tuple[str, List[Any]]:
        """
        Build optimized SQL query for pipeline.
        
        Returns:
            Tuple of (SQL query, parameters)
        """
```

#### 1.2 Extend `ExprEvaluator` for Aggregation Context

Add methods to `expr_evaluator.py`:

```python
class ExprEvaluator:
    # ... existing code ...
    
    def build_select_expression(
        self,
        expr: Any,
        alias: Optional[str] = None,
        context: Optional[AggregationContext] = None
    ) -> Tuple[str, List[Any]]:
        """
        Build SELECT clause expression for aggregation.
        
        Similar to evaluate_for_aggregation() but optimized for SQL tier.
        Handles field aliasing and context tracking.
        """
        
    def build_group_by_expression(
        self,
        expr: Any,
        context: Optional[AggregationContext] = None
    ) -> Tuple[str, List[Any]]:
        """
        Build GROUP BY clause expression.
        
        Optimized for grouping operations.
        """
        
    def build_having_expression(
        self,
        expr: Any,
        context: Optional[AggregationContext] = None
    ) -> Tuple[str, List[Any]]:
        """
        Build HAVING clause expression.
        
        For post-aggregation filtering.
        """
```

### Phase 2: Stage-Specific Optimization (Weeks 2-3)

#### 2.1 $addFields SQL Optimization

**Current (Python):**
```python
for doc in docs:
    doc['revenue'] = evaluator._evaluate_expr_python(
        {"$multiply": ["$price", "$quantity"]}, doc
    )
```

**Target (SQL):**
```python
def _build_addfields_sql(self, spec: Dict) -> Tuple[str, List[Any]]:
    """Build SQL for $addFields stage."""
    select_parts = [f"{self.data_column}"]
    params = []
    
    for field, expr in spec.items():
        expr_sql, expr_params = self.evaluator.build_select_expression(expr)
        # Use json_set to add field to JSON document
        select_parts[0] = f"json_set({select_parts[0]}, '$.{field}', {expr_sql})"
        params.extend(expr_params)
    
    return f"SELECT id, {select_parts[0]} AS data FROM {prev_stage}", params
```

#### 2.2 $project SQL Optimization

**Target (SQL):**
```python
def _build_project_sql(self, spec: Dict) -> Tuple[str, List[Any]]:
    """Build SQL for $project stage."""
    select_parts = ["id"]
    params = []
    
    for field, value in spec.items():
        if field == "_id":
            if value == 1:
                select_parts.append("json_extract(data, '$._id') AS _id")
            continue
            
        if _is_expression(value):
            expr_sql, expr_params = self.evaluator.build_select_expression(value)
            if expr_sql == "$$REMOVE":
                continue  # Skip removed fields
            select_parts.append(f"{expr_sql} AS {field}")
            params.extend(expr_params)
        elif value == 1:
            select_parts.append(f"json_extract(data, '$.{field}') AS {field}")
    
    return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", params
```

#### 2.3 $group SQL Optimization

**Target (SQL):**
```python
def _build_group_sql(self, spec: Dict) -> Tuple[str, List[Any]]:
    """Build SQL for $group stage."""
    select_parts = []
    group_by_parts = []
    params = []
    
    # Handle _id (group key)
    group_id = spec.get("_id")
    if group_id:
        if _is_expression(group_id):
            key_sql, key_params = self.evaluator.build_select_expression(group_id)
            select_parts.append(f"{key_sql} AS _id")
            group_by_parts.append(key_sql)
            params.extend(key_params)
        elif isinstance(group_id, str) and group_id.startswith("$"):
            field = group_id[1:]
            key_sql = f"json_extract(data, '$.{field}')"
            select_parts.append(f"{key_sql} AS _id")
            group_by_parts.append(key_sql)
    
    # Handle accumulators
    for field, accumulator in spec.items():
        if field == "_id":
            continue
            
        if isinstance(accumulator, dict):
            op, expr = next(iter(accumulator.items()))
            if _is_expression(expr):
                expr_sql, expr_params = self.evaluator.build_select_expression(expr)
                params.extend(expr_params)
            else:
                expr_sql = f"json_extract(data, '$.{expr[1:]}')"
            
            # Map accumulator to SQL
            sql_agg = self._map_accumulator_to_sql(op, expr_sql)
            select_parts.append(f"{sql_agg} AS {field}")
    
    group_by_clause = ""
    if group_by_parts:
        group_by_clause = f"GROUP BY {', '.join(group_by_parts)}"
    
    return (
        f"SELECT {', '.join(select_parts)} FROM {prev_stage} {group_by_clause}",
        params
    )
```

#### 2.4 $match SQL Optimization (Direct Expressions)

**Current:**
```python
{"$match": {"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}}}
```

**Target (also support):**
```python
{"$match": {"$gt": [{"$sin": "$angle"}, 0.5]}}  # Without $expr wrapper
```

```python
def _build_match_sql(self, query: Dict) -> Tuple[str, List[Any]]:
    """Build WHERE clause for $match stage."""
    if "$expr" in query:
        # Existing $expr support
        expr_sql, params = self.evaluator.evaluate(query["$expr"])
        return f"WHERE {expr_sql}", params
    elif _is_expression(query):
        # New: Direct expression support
        expr_sql, params = self.evaluator.build_select_expression(query)
        return f"WHERE {expr_sql}", params
    else:
        # Standard query operators
        return self._build_standard_match_sql(query)
```

### Phase 3: Multi-Stage Pipeline Optimization (Week 4)

#### 3.1 CTE-Based Pipeline Construction

```python
def build_pipeline_sql(self, pipeline: List[Dict]) -> Tuple[str, List[Any]]:
    """
    Build complete pipeline SQL using CTEs.
    
    Example output:
    WITH 
    stage0 AS (SELECT id, data FROM collection),
    stage1 AS (
        SELECT id, json_set(data, '$.revenue', 
            json_extract(data, '$.price') * json_extract(data, '$.quantity')
        ) AS data
        FROM stage0
    ),
    stage2 AS (
        SELECT id, json_set(data, '$.tax', 
            json_extract(data, '$.revenue') * 0.08
        ) AS data
        FROM stage1
    )
    SELECT * FROM stage2
    """
    cte_parts = []
    all_params = []
    prev_stage = f"{self.collection.name}"
    
    for i, stage in enumerate(pipeline):
        stage_name = next(iter(stage.keys()))
        stage_spec = stage[stage_name]
        
        cte_name = f"stage{i}"
        
        # Build SQL for this stage
        stage_sql, stage_params = self._build_stage_sql(
            stage_name, stage_spec, prev_stage
        )
        
        if stage_sql is None:
            # Cannot optimize, fall back to Python
            return None, []
        
        cte_parts.append(f"{cte_name} AS ({stage_sql})")
        all_params.extend(stage_params)
        prev_stage = cte_name
    
    # Final SELECT
    final_sql = f"WITH {', '.join(cte_parts)} SELECT * FROM {prev_stage}"
    return final_sql, all_params
```

#### 3.2 Field Alias Tracking

```python
class PipelineContext:
    """
    Tracks field aliases and computed fields across pipeline stages.
    """
    
    def __init__(self):
        self.computed_fields: Dict[str, str] = {}  # field -> SQL expression
        self.removed_fields: Set[str] = set()
        self.stage_index = 0
        
    def add_computed_field(self, field: str, sql_expr: str):
        """Track a computed field."""
        self.computed_fields[field] = sql_expr
        
    def remove_field(self, field: str):
        """Mark field as removed."""
        self.removed_fields.add(field)
        
    def get_field_sql(self, field: str) -> Optional[str]:
        """Get SQL expression for field."""
        return self.computed_fields.get(field)
        
    def is_field_available(self, field: str) -> bool:
        """Check if field is available in current context."""
        if field in self.removed_fields:
            return False
        return True
```

### Phase 4: Advanced Features (Week 5)

#### 4.1 $$ROOT and $$CURRENT in SQL Tier

**Challenge:** `$$ROOT` and `$$CURRENT` refer to document state at different pipeline points.

**SQL Implementation:**
```python
def _handle_aggregation_variable_sql(
    self, var_name: str, context: PipelineContext
) -> str:
    match var_name:
        case "$$ROOT":
            # In SQL tier, we preserve original document in a separate column
            return "root_data"
        case "$$CURRENT":
            # Current document state
            return "data"
        case "$$REMOVE":
            # Sentinel for removal
            return "$$REMOVE"
```

**CTE with ROOT preservation:**
```sql
WITH 
stage0 AS (
    SELECT id, data AS root_data, data 
    FROM collection
),
stage1 AS (
    SELECT 
        id, 
        root_data,  -- Preserve original
        json_set(data, '$.bonus', 5000) AS data
    FROM stage0
),
stage2 AS (
    SELECT 
        id,
        root_data,  -- Still available
        json_set(data, '$.original', root_data) AS data
    FROM stage1
)
SELECT * FROM stage2
```

#### 4.2 $facet SQL Optimization

```python
def _build_facet_sql(self, spec: Dict, prev_stage: str) -> Tuple[str, List[Any]]:
    """
    Build SQL for $facet stage.
    
    Each sub-pipeline becomes a separate CTE branch.
    """
    subpipeline_ctes = []
    all_params = []
    
    for facet_name, sub_pipeline in spec.items():
        # Build sub-pipeline SQL
        sub_sql, sub_params = self.build_pipeline_sql(sub_pipeline)
        subpipeline_ctes.append(f"facet_{facet_name} AS ({sub_sql})")
        all_params.extend(sub_params)
    
    # Combine facets into single result
    facet_selects = []
    for facet_name in spec.keys():
        facet_selects.append(
            f"(SELECT json_group_array(json(facet_{facet_name}.data)) "
            f"FROM facet_{facet_name}) AS {facet_name}"
        )
    
    return f"""
        SELECT {', '.join(facet_selects)}
        FROM {prev_stage}
    """, all_params
```

## Optimization Decision Tree

```
Pipeline Analysis
    │
    ├─ Contains unsupported stages?
    │   ├─ YES → Python Fallback (Tier 3)
    │   └─ NO → Continue
    │
    ├─ Contains complex expressions?
    │   ├─ YES (e.g., $let, $objectToArray) → Python Fallback (Tier 3)
    │   └─ NO → Continue
    │
    ├─ Pipeline length <= 10 stages?
    │   ├─ NO → Python Fallback (Tier 3)  [configurable threshold]
    │   └─ YES → Continue
    │
    └─ Build SQL with CTEs (Tier 1)
```

## Performance Benchmarks (Expected)

### Test Scenario 1: Simple $addFields

```python
pipeline = [
    {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
    {"$match": {"revenue": {"$gte": 500}}},
]
```

| Documents | Python Tier 3 | SQL Tier 1 | Speedup |
|-----------|--------------|------------|---------|
| 1,000 | 50ms | 5ms | 10x |
| 10,000 | 500ms | 15ms | 33x |
| 100,000 | 5000ms | 50ms | 100x |

### Test Scenario 2: Multi-Stage Pipeline

```python
pipeline = [
    {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
    {"$addFields": {"tax": {"$multiply": ["$revenue", 0.08]}}},
    {"$group": {"_id": "$category", "total": {"$sum": "$revenue"}}},
    {"$sort": {"total": -1}},
]
```

| Documents | Python Tier 3 | SQL Tier 1 | Speedup |
|-----------|--------------|------------|---------|
| 1,000 | 100ms | 10ms | 10x |
| 10,000 | 1000ms | 30ms | 33x |
| 100,000 | 10000ms | 100ms | 100x |

## Testing Strategy

### Unit Tests

```python
class TestSQLTierAggregator:
    def test_can_optimize_simple_pipeline(self):
        pipeline = [
            {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}}
        ]
        assert aggregator.can_optimize_pipeline(pipeline) is True
        
    def test_cannot_optimize_with_unsupported_stage(self):
        pipeline = [
            {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
            {"$graphLookup": {"from": "other"}}  # Unsupported
        ]
        assert aggregator.can_optimize_pipeline(pipeline) is False
        
    def test_build_addfields_sql(self):
        sql, params = aggregator._build_addfields_sql({
            "revenue": {"$multiply": ["$price", "$quantity"]}
        })
        assert "json_set" in sql
        assert "json_extract" in sql
```

### Integration Tests

```python
class TestSQLTierIntegration:
    def test_multi_stage_pipeline_sql_tier(self, collection):
        pipeline = [
            {"$addFields": {"revenue": {"$multiply": ["$price", "$quantity"]}}},
            {"$match": {"revenue": {"$gte": 500}}},
            {"$group": {"_id": "$category", "total": {"$sum": "$revenue"}}},
        ]
        results = list(collection.aggregate(pipeline))
        assert len(results) > 0
        
    def test_performance_comparison(self, collection):
        import time
        
        # SQL tier
        set_force_fallback(False)
        start = time.time()
        sql_results = list(collection.aggregate(pipeline))
        sql_time = time.time() - start
        
        # Python tier
        set_force_fallback(True)
        start = time.time()
        python_results = list(collection.aggregate(pipeline))
        python_time = time.time() - start
        
        # Verify correctness
        assert sql_results == python_results
        
        # Verify performance
        assert sql_time < python_time  # SQL should be faster
```

## Rollback Plan

If SQL tier optimization causes issues:

1. **Kill Switch**: `set_force_fallback(True)` forces Python tier
2. **Per-Stage Fallback**: Individual stages can fall back to Python
3. **Gradual Rollout**: Enable for simple pipelines first, expand gradually

## Migration Path

### For Existing Code

No changes required! SQL tier optimization is transparent:

```python
# Existing code continues to work
results = list(collection.aggregate(pipeline))

# Automatically uses SQL tier when possible
# Falls back to Python tier when needed
```

### For Performance-Critical Code

Users can check which tier was used:

```python
cursor = collection.aggregate(pipeline)
cursor.explain()  # Returns execution plan with tier information
```

## Success Metrics

### Functional Metrics

- [ ] 90%+ of common aggregation pipelines optimize to SQL tier
- [ ] All existing tests pass with SQL tier enabled
- [ ] Correctness: SQL and Python tiers produce identical results

### Performance Metrics

- [ ] Average 10x speedup for simple pipelines
- [ ] Average 20x speedup for multi-stage pipelines
- [ ] No regression in Python fallback performance

### Quality Metrics

- [ ] 95%+ test coverage for SQL tier code
- [ ] Documentation complete with examples
- [ ] Performance benchmarks documented

## References

- [SQLite SELECT Documentation](https://sqlite.org/lang_select.html)
- [SQLite JSON1 Documentation](https://sqlite.org/json1.html)
- [SQLite Window Functions](https://sqlite.org/windowfunctions.html)
- [MongoDB Aggregation Pipeline](https://docs.mongodb.com/manual/aggregation/)
- [AGGREGATION_EXPRESSION_SUPPORT.md](TODO/AGGREGATION_EXPRESSION_SUPPORT.md)

## Conclusion

SQL tier optimization for aggregation expressions will provide significant performance improvements while maintaining full backward compatibility. The implementation leverages SQLite's powerful CTE support and JSON functions to execute entire pipelines in SQL, with Python fallback for complex cases.

Estimated effort: **4-5 weeks** for full implementation with testing and documentation.
