---
type: design_doc
title: "Facet Implementation in NeoSQLite"
description: "Implementation specification for running multiple independent aggregation sub-pipelines via $facet in NeoSQLite."
tags:
  - facet
  - aggregation
  - multi-pipeline
  - design
timestamp: 2026-11-25T00:00:00Z
version: "1.18.1"
lifecycle: active
---

# $facet Implementation in NeoSQLite

## Overview

The `$facet` aggregation stage in NeoSQLite enables running multiple independent aggregation pipelines on the same input dataset, combining their results into a single output document. This implementation follows a broker-pattern approach that maximizes reuse of existing aggregation logic, ensuring 100% compatibility with PyMongo while adapting to SQLite's sequential processing constraints.

## Technical Implementation

### Architecture

NeoSQLite's aggregation pipeline uses a three-tier processing model (with 4 implementations):
1. **Tier 1 (SQL CTE)**: Direct SQL queries with CTE optimization
2. **Tier 1.5 (SQL Non-CTE)**: Direct SQL queries without CTE
3. **Tier 2 (Temp Tables)**: Complex pipelines using SQLite temp tables
4. **Tier 3 (Python Fallback)**: Full Python implementation for unsupported operations

The term "3-tier" refers to the 3 processing categories: SQL (Tier 1 + 1.5), Temp Tables (Tier 2), Python (Tier 3).

The `$facet` implementation supports all three execution tiers: CTE-based SQL (`_build_facet_sql` in `neosqlite/collection/_stage_builders.py`), temporary-table sub-pipeline execution (`_process_facet_stage` in `neosqlite/collection/temporary_table_aggregation/operators_advanced.py`), and the Python fallback engine (`execute_python_aggregation` in `neosqlite/collection/query_engine/python_aggregation_engine.py`).

### Core Logic

When a `$facet` stage runs in Tier 3 Python fallback:

```python
case "$facet":
    facet_spec = stage["$facet"]
    facet_results: dict[str, Any] = {}
    for facet_name, sub_pipeline in facet_spec.items():
        facet_results[facet_name] = list(
            execute_python_aggregation(collection, docs, sub_pipeline)
        )
    docs = [facet_results]
```

1. **Facet Execution**: Each sub-pipeline executes independently across SQL, temporary tables, or Python fallback depending on sub-pipeline complexity
2. **Result Collection**: Outputs are stored in a dictionary keyed by facet name
3. **Pipeline Continuation**: The combined results become the input for subsequent pipeline stages

### Key Components

- **Tier 1 SQL CTE (`neosqlite/collection/_stage_builders.py`)**: Compiles compatible `$facet` pipelines into a shared source CTE and sub-pipeline CTEs
- **Tier 2 Temp Tables (`neosqlite/collection/temporary_table_aggregation/operators_advanced.py`)**: Materializes intermediate rows and executes sub-pipelines via `_process_facet_stage`
- **Tier 3 Fallback (`neosqlite/collection/query_engine/python_aggregation_engine.py`)**: Evaluates arbitrary `$facet` pipelines in Python when SQL tiers cannot handle a sub-pipeline or when fallback is forced
- **Sequential Processing**: Facets execute sequentially due to SQLite's single-writer/single-connection model

### Benefits

- **Code Reuse**: 90%+ of existing aggregation code remains unchanged
- **Performance**: Maintains SQL optimizations within individual facets
- **Compatibility**: Identical behavior to PyMongo's `$facet`
- **Maintainability**: Minimal new code, leveraging battle-tested components

## Code Examples

### Basic Usage

```python
pipeline = [
    {
        "$facet": {
            "categorized": [{"$match": {"category": "electronics"}}],
            "priced": [{"$match": {"price": {"$gt": 100}}}],
            "total": [{"$count": "count"}]
        }
    }
]

result = collection.aggregate(pipeline)
# Output: [{"categorized": [...], "priced": [...], "total": [{"count": N}]}]
```

### With Post-Facet Processing

```python
pipeline = [
    {
        "$facet": {
            "high_value": [{"$match": {"price": {"$gt": 1000}}}],
            "low_value": [{"$match": {"price": {"$lte": 1000}}}]
        }
    },
    {"$project": {"high_count": {"$size": "$high_value"}, "low_count": {"$size": "$low_value"}}}
]

result = collection.aggregate(pipeline)
# Output: [{"high_count": X, "low_count": Y}]
```

## Comparison to PyMongo

| Aspect | PyMongo | NeoSQLite |
|--------|---------|-----------|
| Execution | Parallel facets | Sequential facets |
| Output Format | Identical single document | Identical single document |
| Sub-pipeline Support | Full aggregation pipeline | Full aggregation pipeline |
| Performance | Optimized for distributed processing | Optimized for SQLite constraints |
| Compatibility | Native MongoDB feature | 100% compatible implementation |

## Layman's Explanation

Imagine you have a giant pile of mixed LEGO bricks (your data) and want to sort them in different ways simultaneously—like one pile for red bricks, one for blue bricks, and a count of everything. In PyMongo, it's like having multiple workers sorting in parallel. In NeoSQLite, the workers sort one group at a time but use the same tools, giving you the exact same organized result: a single "report" with sections for each sorted pile.
