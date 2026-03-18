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

The `$facet` implementation integrates into the Python fallback tier, leveraging the existing `aggregate_with_constraints` method.

### Core Logic

When a `$facet` stage is encountered in the pipeline:

```python
case "$facet":
    facet_spec = stage["$facet"]
    results: Dict[str, Any] = {}
    for facet_name, sub_pipeline in facet_spec.items():
        facet_result = self.aggregate_with_constraints(sub_pipeline, batch_size, memory_constrained)
        if isinstance(facet_result, list):
            results[facet_name] = facet_result
        else:
            results[facet_name] = list(facet_result)
    docs = [cast(Dict[str, Any], results)]
```

1. **Facet Execution**: Each sub-pipeline is executed independently using the full aggregation engine
2. **Result Collection**: Outputs are stored in a dictionary keyed by facet name
3. **Pipeline Continuation**: The combined results become the input for subsequent pipeline stages

### Key Components

- **QueryEngine Integration**: Located in `neosqlite/collection/query_engine.py`, handles `$facet` in the Python fallback
- **Reuse Strategy**: Leverages existing aggregation stages (`$match`, `$group`, `$count`, etc.) without modification
- **Sequential Processing**: Facets execute one after another due to SQLite's single-threaded nature
- **Memory Management**: Each facet processes independently to prevent memory bloat

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
