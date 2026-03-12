# Aggregation Expressions - Quick Start Guide

**Quick Reference:** For comprehensive documentation including performance benchmarks, operator support matrix, and technical details, see [AGGREGATION_PIPELINE_OPTIMIZATION.md](AGGREGATION_PIPELINE_OPTIMIZATION.md).

---

## Overview

NeoSQLite supports **119+ aggregation expression operators** in all pipeline stages, enabling powerful data transformations.

```python
from neosqlite import Connection

conn = Connection(":memory:")
collection = conn["products"]

# Calculate revenue with expressions
results = list(collection.aggregate([
    {"$addFields": {
        "revenue": {"$multiply": ["$price", "$quantity"]},
        "tax": {"$multiply": ["$revenue", 0.08]}
    }},
    {"$match": {"revenue": {"$gte": 500}}},
    {"$group": {"_id": "$category", "total": {"$sum": "$revenue"}}}
]))
```

**Performance:** Most pipelines automatically use SQL optimization for **10-100x speedup**. Pipelines that can't be optimized fall back to Python with 100% correctness.

---

## Supported Operators

All operators from `$expr` queries work in aggregation pipelines:

### Common Operators

| Category | Operators |
|----------|-----------|
| **Arithmetic** | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$pow`, `$sqrt` |
| **Comparison** | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$cmp` |
| **Logical** | `$and`, `$or`, `$not`, `$nor` |
| **Conditional** | `$cond`, `$ifNull`, `$switch` |
| **String** | `$concat`, `$toLower`, `$toUpper`, `$trim`, `$replaceAll`, `$split`, `$regexMatch`, `$regexFind`, `$regexFindAll` |
| **Math** | `$sin`, `$cos`, `$tan`, `$ln`, `$log`, `$log10` |
| **Date** | `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second` |
| **Type Conversion** | `$toString`, `$toInt`, `$toDouble`, `$toBool` |
| **Array** | `$size`, `$in`, `$isArray` |
| **Object** | `$mergeObjects`, `$getField`, `$setField` |

**Full list:** See [AGGREGATION_PIPELINE_OPTIMIZATION.md](AGGREGATION_PIPELINE_OPTIMIZATION.md#operator-support-matrix)

---

## Stage-by-Stage Examples

### $addFields - Add Computed Fields

```python
collection.aggregate([
    {"$addFields": {
        "revenue": {"$multiply": ["$price", "$quantity"]},
        "tax": {"$multiply": ["$revenue", 0.08]},
        "total": {"$add": ["$revenue", "$tax"]}
    }}
])
```

### $project - Transform Documents

```python
collection.aggregate([
    {"$project": {
        "name": 1,
        "double_price": {"$multiply": ["$price", 2]},
        "price_with_tax": {"$add": ["$price", {"$multiply": ["$price", "$tax_rate"]}]}
    }}
])
```

### $group - Aggregate with Expressions

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

### $match - Filter with Expressions

```python
# Use $expr wrapper for complex comparisons
collection.aggregate([
    {"$match": {"$expr": {"$gt": [{"$sin": "$angle"}, 0.5]}}}
])
```

### $facet - Multiple Sub-Pipelines

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

---

## Variable Scoping

### $$ROOT - Original Document

References the document at pipeline start:

```python
collection.aggregate([
    {"$addFields": {"bonus": 5000}},
    {"$addFields": {
        "original": "$$ROOT"  # Original doc without bonus
    }}
])
```

### $$CURRENT - Current Document

References the evolving document:

```python
collection.aggregate([
    {"$addFields": {"bonus": 5000}},
    {"$addFields": {
        "current": "$$CURRENT"  # Doc with bonus included
    }}
])
```

### $$REMOVE - Remove Fields

Use in `$project` to remove fields:

```python
collection.aggregate([
    {"$project": {
        "name": 1,
        "secret": "$$REMOVE"  # Remove this field
    }}
])
```

---

## Complete Examples

### Example 1: Sales Pipeline

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

---

## Tips

### 1. Filter Early

Place `$match` stages first to reduce document count:

```python
# ✅ Good - filter first
[
    {"$match": {"status": "active"}},
    {"$addFields": {"computed": {"$multiply": ["$a", "$b"]}}}
]

# ❌ Bad - compute then filter
[
    {"$addFields": {"computed": {"$multiply": ["$a", "$b"]}}},
    {"$match": {"status": "active"}}
]
```

### 2. Use Indexes

Ensure filtered fields are indexed:

```python
collection.create_index("status")
collection.aggregate([
    {"$match": {"status": "active"}},
    # ... rest of pipeline
])
```

### 3. Null Handling

Arithmetic with `null` returns `null`:

```python
collection.insert_one({"value": 100, "nullable": None})

result = list(collection.aggregate([
    {"$addFields": {
        "result": {"$add": ["$value", "$nullable"]}  # Returns None
    }}
]))
```

---

## Testing

```bash
# Run expression tests
pytest tests/test_expr/

# Run aggregation tests
pytest tests/test_aggregation_pipeline.py

# Run Tier 2 tests
pytest tests/test_tier2/
```

---

## Need More Details?

For comprehensive documentation including:
- Complete operator support matrix
- Tier architecture details
- Performance benchmarks
- Implementation details
- Known limitations

See **[AGGREGATION_PIPELINE_OPTIMIZATION.md](AGGREGATION_PIPELINE_OPTIMIZATION.md)**
