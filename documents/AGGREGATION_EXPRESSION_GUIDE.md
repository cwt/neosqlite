# Aggregation Expression Support - User Guide

**Last Updated:** February 27, 2026  
**Status:** SQL Tier 1 & 2 Optimization ✅ COMPLETE

## Overview

NeoSQLite now supports **aggregation expressions** in all pipeline stages, enabling powerful data transformations using the same 106+ operators available in `$expr` queries.

### 🚀 NEW: SQL Tier Optimization

As of February 2026, NeoSQLite includes **SQL Tier 1 and Tier 2 optimization** for aggregation pipelines, providing:

- **10-100x performance improvement** for supported pipelines
- **Automatic optimization** - no code changes required
- **JSONB support** - automatic detection and optimization (SQLite >= 3.45.0)
- **100% backward compatibility** - all existing code continues to work

See [SQL_TIER_PROGRESS_REPORT.md](SQL_TIER_PROGRESS_REPORT.md) for details.

## What's New

### Before (Limited Support)

```python
# Only worked in $expr queries
collection.find({"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}})

# Didn't work in aggregation stages
collection.aggregate([
    {"$addFields": {"sin_val": {"$sin": "$angle"}}}  # ❌ Not supported
])
```

### After (Full Support)

```python
# Works in all aggregation stages!
collection.aggregate([
    {"$addFields": {"sin_val": {"$sin": "$angle"}}},  # ✅ Works!
    {"$project": {"computed": {"$multiply": ["$price", "$qty"]}}},  # ✅ Works!
    {"$group": {"_id": "$cat", "total": {"$sum": {"$multiply": ["$price", "$qty"]}}}}  # ✅ Works!
])
```

## Supported Operators

All 106+ operators from `$expr` are now available in aggregation pipelines:

### Arithmetic
`$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$pow`, `$sqrt`

### Trigonometric
`$sin`, `$cos`, `$tan`, `$asin`, `$acos`, `$atan`, `$atan2`, `$sinh`, `$cosh`, `$tanh`, `$asinh`, `$acosh`, `$atanh`

### Logarithmic
`$ln`, `$log`, `$log10`, `$log2` (NeoSQLite extension)

### String
`$concat`, `$toLower`, `$toUpper`, `$strLenBytes`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$indexOfBytes`, `$regexMatch`, `$replaceAll`

### Conditional
`$cond`, `$ifNull`, `$switch`

### Comparison
`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$cmp`

### Logical
`$and`, `$or`, `$not`, `$nor`

### Array
`$size`, `$in`, `$isArray`, `$slice`, `$indexOfArray`, `$sum`, `$avg`, `$min`, `$max` (as aggregators)

### Date
`$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dateAdd`, `$dateSubtract`, `$dateDiff`

### Type Conversion
`$toString`, `$toInt`, `$toDouble`, `$toBool`, `$type`, `$convert`

### Object
`$mergeObjects`, `$getField`, `$setField`

## Stage-by-Stage Guide

### $addFields

Add computed fields using expressions:

```python
collection.aggregate([
    {"$addFields": {
        "revenue": {"$multiply": ["$price", "$quantity"]},
        "tax": {"$multiply": ["$revenue", 0.08]},
        "total": {"$add": ["$revenue", "$tax"]}
    }}
])
```

### $project

Project computed fields:

```python
collection.aggregate([
    {"$project": {
        "name": 1,
        "double_price": {"$multiply": ["$price", 2]},
        "price_with_tax": {"$add": ["$price", {"$multiply": ["$price", "$tax_rate"]}]}
    }}
])
```

### $group

Use expressions in accumulators:

```python
collection.aggregate([
    {"$group": {
        "_id": "$category",
        "total_revenue": {"$sum": {"$multiply": ["$price", "$quantity"]}},
        "avg_discount": {"$avg": {"$multiply": ["$discount", 100]}},
        "items": {"$push": {"$concat": ["$name", " (", "$sku", ")"]}}
    }}
])
```

### $match

Filter using expressions (with `$expr`):

```python
collection.aggregate([
    {"$match": {"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}}}
])
```

### $facet

Use expressions in sub-pipelines:

```python
collection.aggregate([
    {"$facet": {
        "high_value": [
            {"$addFields": {"revenue": {"$multiply": ["$price", "$qty"]}}},
            {"$match": {"revenue": {"$gte": 500}}}
        ],
        "by_category": [
            {"$group": {
                "_id": "$category",
                "total": {"$sum": {"$multiply": ["$price", "$qty"]}}
            }}
        ]
    }}
])
```

## Variable Scoping

### $$ROOT

References the **original document** at pipeline start:

```python
collection.aggregate([
    {"$addFields": {"bonus": 5000}},
    {"$addFields": {
        "original": "$$ROOT"  # Original doc without bonus
    }}
])
```

### $$CURRENT

References the **evolving document** (current state):

```python
collection.aggregate([
    {"$addFields": {"bonus": 5000}},
    {"$addFields": {
        "current": "$$CURRENT"  # Doc with bonus included
    }}
])
```

### $$REMOVE

Removes fields in `$project`:

```python
collection.aggregate([
    {"$project": {
        "name": 1,
        "secret": "$$REMOVE"  # Remove this field
    }}
])
```

## Examples

### Example 1: Sales Pipeline

Calculate revenue with tax and discount:

```python
pipeline = [
    {"$addFields": {
        "gross": {"$multiply": ["$price", "$quantity"]},
        "discount_amount": {"$multiply": ["$price", "$quantity", "$discount"]}
    }},
    {"$addFields": {
        "net": {"$subtract": ["$gross", "$discount_amount"]}
    }},
    {"$group": {
        "_id": "$category",
        "total_net": {"$sum": "$net"},
        "transaction_count": {"$sum": 1}
    }},
    {"$addFields": {
        "avg_transaction": {"$divide": ["$total_net", "$transaction_count"]}
    }}
]

results = list(collection.aggregate(pipeline))
```

### Example 2: Conditional Categorization

Categorize documents based on computed values:

```python
pipeline = [
    {"$addFields": {
        "revenue": {"$multiply": ["$price", "$quantity"]}
    }},
    {"$addFields": {
        "tier": {
            "$cond": {
                "if": {"$gte": ["$revenue", 500]},
                "then": "high",
                "else": "standard"
            }
        }
    }},
    {"$group": {
        "_id": "$tier",
        "count": {"$sum": 1}
    }}
]
```

### Example 3: Trigonometric Calculations

Use trigonometric functions:

```python
import math

collection.insert_one({"angle": math.pi / 2})  # 90 degrees

pipeline = [
    {"$addFields": {
        "sin_angle": {"$sin": "$angle"},
        "cos_angle": {"$cos": "$angle"}
    }}
]

results = list(collection.aggregate(pipeline))
# sin(π/2) = 1.0, cos(π/2) = 0.0
```

### Example 4: Complex Multi-Stage Pipeline

```python
pipeline = [
    # Stage 1: Calculate revenue
    {"$addFields": {
        "revenue": {"$multiply": ["$price", "$quantity"]}
    }},
    # Stage 2: Filter high value
    {"$match": {"revenue": {"$gte": 500}}},
    # Stage 3: Group by category
    {"$group": {
        "_id": "$category",
        "total_revenue": {"$sum": "$revenue"},
        "count": {"$sum": 1}
    }},
    # Stage 4: Compute average
    {"$addFields": {
        "avg_revenue": {"$divide": ["$total_revenue", "$count"]}
    }},
    # Stage 5: Sort
    {"$sort": {"avg_revenue": -1}}
]
```

## Performance Considerations

### Python Fallback (Current Implementation)

Currently, aggregation expressions are evaluated in **Python fallback mode** (Tier 3). This ensures correctness but may be slower for large datasets.

**Recommendations:**
- Use indexes for `$match` stages when possible
- Place `$match` stages early in pipelines to reduce document count
- For large datasets, consider batching

### Future SQL Tier Optimization

Future versions will move expression evaluation to **SQL tier** (Tier 1) for better performance.

## Technical Details

### $expr vs Aggregation Expressions: What's the Difference?

While both `$expr` queries and aggregation expressions use the same operators, they operate in different contexts:

| Aspect | `$expr` Queries | Aggregation Expressions |
|--------|-----------------|------------------------|
| **Context** | Query/Filter (WHERE clause) | Aggregation (SELECT clause) |
| **SQL Generation** | `WHERE sin(...) > 0.5` | `SELECT sin(...) AS sin_val` |
| **Evaluation Tier** | SQL Tier 1 (fast) | Python Tier 3 (slower) |
| **Stages Supported** | `$match` only | `$addFields`, `$project`, `$group`, `$match`, `$facet` |
| **Variable Scoping** | None | `$$ROOT`, `$$CURRENT`, `$$REMOVE` |

### Why SQL Tier for Aggregation is Harder

**`$expr` in WHERE clause** (already implemented, fast):
```sql
SELECT id, data FROM collection
WHERE sin(json_extract(data, '$.angle')) > 0.5
```
→ Straightforward: just add conditions to WHERE clause

**Expressions in SELECT clause** (future work, currently Python):
```sql
SELECT 
    id,
    data,
    sin(json_extract(data, '$.angle')) AS sin_val,  -- ← Need to generate this
    json_extract(data, '$.price') * json_extract(data, '$.qty') AS revenue  -- ← And this
FROM collection
```
→ Complex challenges:
- Need to track field aliases across multiple stages
- Handle GROUP BY with computed expressions
- Manage document context (`$$ROOT`, `$$CURRENT`) through pipeline
- Handle ORDER BY on computed fields
- Support HAVING clauses for post-aggregation filtering

### Current Implementation State (Updated February 2026)

| Feature | Status | Performance |
|---------|--------|-------------|
| `$expr` in `find()` | ✅ SQL Tier 1 | ⚡ Fast (10-100x) |
| `$expr` in `$match` (aggregation) | ✅ SQL Tier 1 | ⚡ Fast (10-100x) |
| Expressions in `$addFields` | ✅ SQL Tier 1/2 | ⚡ Fast (10-100x) |
| Expressions in `$project` | ✅ SQL Tier 1/2 | ⚡ Fast (10-100x) |
| Expressions in `$group` | ✅ SQL Tier 1/2 | ⚡ Fast (10-100x) |
| Expressions in `$facet` | ✅ SQL Tier 1/2 | ⚡ Fast (10-100x) |
| `$replaceRoot` / `$replaceWith` | ✅ SQL Tier 2 | 🔶 Medium (5-20x) |
| `$first` / `$last` accumulators | ✅ SQL Tier 2 | 🔶 Medium (5-20x) |
| `$addToSet` accumulator | ✅ SQL Tier 2 | 🔶 Medium (5-20x) |
| `$replaceOne` operator | ✅ SQL Tier 2 | 🔶 Medium (5-20x) |

**Note:** Pipelines that cannot be optimized in SQL automatically fall back to Python Tier 3 with 100% correctness guarantee.

### When to Use Which

**Use `$expr` queries when:**
- You need maximum performance
- You're only filtering documents
- You don't need data transformation

```python
# Fast: SQL tier evaluation
collection.find({"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}})
```

**Use aggregation expressions when:**
- You need data transformation
- You need computed fields
- You need grouping with complex accumulators
- Performance is secondary to functionality

```python
# Correct but slower: Python tier evaluation
collection.aggregate([
    {"$addFields": {"sin_val": {"$sin": "$angle"}}},
    {"$group": {"_id": "$category", "avg_sin": {"$avg": "$sin_val"}}}
])
```

### Performance Tips

1. **Filter early**: Place `$match` stages at the beginning of pipelines
2. **Use indexes**: Ensure filtered fields are indexed
3. **Limit results**: Use `$limit` to reduce output size
4. **Avoid unnecessary stages**: Each stage adds processing overhead
5. **Consider batching**: For large datasets, process in batches

## Null Handling

Arithmetic operations with `null` return `null` (MongoDB-compatible behavior):

```python
collection.insert_one({"value": 100, "nullable": None})

pipeline = [
    {"$addFields": {
        "result": {"$add": ["$value", "$nullable"]}  # Returns None
    }}
]
```

## Limitations

### Current Limitations (Updated February 2026)

1. **SQL Tier Coverage**: Not all pipelines can be optimized in SQL (automatic fallback to Python)
2. **Nested $$ Variables**: `$$ROOT.field` syntax not supported (use `{"$getField": {"field": "field", "input": "$$ROOT"}}` instead)
3. **Let Variables**: `$let` operator variables not available in aggregation context
4. **$lookup**: Not yet optimized in SQL tier (planned for Phase 2)
5. **Window Functions**: Not yet exposed in aggregation pipeline (planned for Phase 3)

### Known Issues

- Complex nested `$cond` expressions may have edge cases
- `$replaceOne` object form requires Tier 2 (Python fallback uses array form)
- `$unwind` + `$group` SQL optimization path has limitations (falls back to Tier 2)

## Migration Guide

### From MongoDB

NeoSQLite's aggregation expression support is compatible with MongoDB. Most pipelines will work without modification.

### From Previous NeoSQLite Versions

Existing `$expr` queries continue to work:

```python
# This still works
collection.find({"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}})

# New syntax also available
collection.aggregate([
    {"$addFields": {"sin_angle": {"$sin": "$angle"}}}
])
```

## Testing

Run the test suite to verify functionality:

```bash
# Run all expression tests
pytest tests/test_expr/

# Run aggregation pipeline tests
pytest tests/test_aggregation_pipeline.py

# Run SQL tier optimization tests
pytest tests/test_tier2/
```

## Performance Benchmarks

### SQL Tier 1 vs Python Tier 3

| Documents | Tier 3 (Python) | Tier 1 (SQL) | Speedup |
|-----------|----------------|--------------|---------|
| 1,000 | 50ms | 5ms | **10x** |
| 10,000 | 500ms | 15ms | **33x** |
| 100,000 | 5000ms | 50ms | **100x** |

### SQL Tier 2 vs Python Tier 3

| Documents | Tier 3 (Python) | Tier 2 (Temp Tables) | Speedup |
|-----------|----------------|---------------------|---------|
| 1,000 | 50ms | 10ms | **5x** |
| 10,000 | 500ms | 50ms | **10x** |
| 100,000 | 5000ms | 250ms | **20x** |

**Note:** Actual performance varies based on pipeline complexity, data size, and hardware. See [SQL_TIER_PROGRESS_REPORT.md](SQL_TIER_PROGRESS_REPORT.md) for detailed benchmarks.

## Additional Resources

- [AGGREGATION_EXPRESSION_SUPPORT.md](../documents/TODO/AGGREGATION_EXPRESSION_SUPPORT.md) - Implementation plan
- [MongoDB Aggregation Pipeline](https://docs.mongodb.com/manual/aggregation/)
- [MongoDB Aggregation Operators](https://docs.mongodb.com/manual/meta/aggregation-quick-reference/)
