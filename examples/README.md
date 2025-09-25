# NeoSQLite SQL Optimization Benchmarks

This directory contains comprehensive benchmarks that demonstrate the performance benefits of NeoSQLite's SQL optimizations.

## Benchmark Files

### 1. `comprehensive_sql_optimization_benchmark.py`
The main benchmark that covers all core SQL optimizations:
- Basic $match operations with index usage
- Single and multiple $unwind operations
- Nested array $unwind operations
- $unwind + $group combinations with various accumulators ($sum, $count, $push, $addToSet)
- $unwind + $sort + $limit combinations
- $match + $unwind + $group combinations
- Simple $lookup operations
- Advanced $unwind options (includeArrayIndex, preserveNullAndEmptyArrays)
- Pipeline reordering optimization
- Memory-constrained processing with quez

### 2. `enhanced_sql_optimization_benchmark.py`
Additional benchmarks covering specialized optimizations:
- Pipeline reordering with complex pipelines
- Advanced $unwind with preserveNullAndEmptyArrays
- Text search integration with json_each() for array processing

### 3. `objectid_interchangeability_demo.py`
Demonstrates MongoDB-compatible ObjectId interchangeability:
- Hex string compatibility between NeoSQLite and PyMongo ObjectIds
- Cross-conversion between NeoSQLite and PyMongo implementations
- Timestamp compatibility verification
- Round-trip conversion preservation
- Integration patterns for MongoDB interoperability
- Memory-constrained processing comparisons

### 3. `text_search_json_each_benchmark.py`
Specialized benchmark for text search combined with array operations:
- Basic $unwind + $text search
- $unwind + $text search + grouping
- $unwind + $text search + sorting + limiting
- Multiple $unwind + $text search
- Complex pipelines with text search

## Key Performance Benefits Demonstrated

- **$lookup operations**: Up to 480x faster with SQL optimization
- **Pipeline reordering**: Up to 58x faster by optimizing execution order
- **Text search with arrays**: Up to 60x faster using FTS5 + json_each()
- **$unwind + $group operations**: 12-15x faster with SQL-level processing
- **Simple $match operations**: 4-6x faster with index usage

## Running the Benchmarks

```bash
# Run the comprehensive benchmark
python examples/comprehensive_sql_optimization_benchmark.py

# Run the enhanced benchmark
python examples/enhanced_sql_optimization_benchmark.py

# Run the text search benchmark
python examples/text_search_json_each_benchmark.py
```

Each benchmark compares optimized SQL execution paths with Python fallback implementations, verifying both performance improvements and result consistency.