# NeoSQLite vs PyMongo API Comparison Package

A modular, comprehensive API compatibility testing framework for comparing NeoSQLite and PyMongo implementations.

## Overview

This package tests NeoSQLite's MongoDB API compatibility by running the same operations against both NeoSQLite and a real MongoDB instance, then comparing the results.

## Package Structure

```
api_comparison/
├── __init__.py              # Package initialization and exports
├── reporter.py              # CompatibilityReporter class
├── runner.py                # Test orchestration and function registry
├── utils.py                 # Utility functions (MongoDB connection)
├── crud.py                  # CRUD operations tests
├── query_operators.py       # Query operator tests
├── expr_*.py                # $expr operator tests (multiple files)
├── update_*.py              # Update operator tests
├── aggregation_*.py         # Aggregation tests
├── array_operators.py       # Array operator tests
├── string_operators.py      # String operator tests
├── math_operators.py        # Math operator tests
├── date_operators.py        # Date operator tests
├── object_operators.py      # Object operator tests
├── collection_*.py          # Collection method tests
├── database_methods.py      # Database method tests
├── cursor_*.py              # Cursor operation tests
├── index_operations.py      # Index operation tests
├── bulk_*.py                # Bulk operation tests
├── binary_operations.py     # Binary data tests
├── gridfs_operations.py     # GridFS tests
├── text_search.py           # Text search tests
├── ...                      # And more specialized test modules
└── MISSING-APIS.md          # List of untested features
```

## Usage

### Run All Comparisons

```bash
cd examples
python api_comparison_main.py
```

### Run Specific Category

```python
from api_comparison import run_category

# Run CRUD tests
run_category("crud")

# Run aggregation tests
run_category("aggregation")

# Run $expr operator tests
run_category("expr")
```

### Use as a Module

```python
from api_comparison import CompatibilityReporter, reporter, run_all_comparisons

# Access the reporter directly
print(f"Total tests: {reporter.total_tests}")
print(f"Passed: {reporter.passed_tests}")
print(f"Compatibility: {reporter.get_compatibility_percentage():.1f}%")

# Print detailed report
reporter.print_report()
```

## Available Categories

| Category | Module | Description |
|----------|--------|-------------|
| `crud` | crud.py | Basic CRUD operations |
| `query` | query_operators.py | Query operators ($eq, $gt, etc.) |
| `expr` | expr_operators.py | Core $expr operators |
| `expr_additional` | expr_additional.py | Additional $expr operators |
| `expr_extended` | expr_extended.py | Extended $expr operators |
| `expr_complete` | expr_complete.py | Complete $expr coverage |
| `expr_success` | expr_success.py | $expr success stories |
| `update` | update_operators.py | Update operators ($set, $inc, etc.) |
| `update_additional` | update_additional.py | Additional update operators |
| `update_modifiers` | update_modifiers.py | Update modifiers |
| `aggregation_stages` | aggregation_stages.py | Core aggregation stages |
| `aggregation_additional` | aggregation_additional.py | Additional aggregation |
| `aggregation_cursor` | aggregation_cursor.py | Aggregation cursor methods |
| `array` | array_operators.py | Array operators |
| `string` | string_operators.py | String operators |
| `math` | math_operators.py | Math operators |
| `date` | date_operators.py | Date operators |
| `object` | object_operators.py | Object operators |
| `collection_methods` | collection_methods.py | Collection methods |
| `database` | database_methods.py | Database methods |
| `cursor` | cursor_operations.py | Cursor operations |
| `cursor_methods` | cursor_methods.py | Cursor methods |
| `index` | index_operations.py | Index operations |
| `bulk` | bulk_operations.py | Bulk operations |
| `bulk_executors` | bulk_executors.py | Bulk executors |
| `find_modify` | find_modify.py | Find and modify operations |
| `distinct` | distinct.py | Distinct operations |
| `binary` | binary_operations.py | Binary data support |
| `nested` | nested_queries.py | Nested field queries |
| `raw_batches` | raw_batches.py | Raw batch operations |
| `change_streams` | change_streams.py | Change streams |
| `text` | text_search.py | Text search |
| `gridfs` | gridfs_operations.py | GridFS operations |
| `objectid` | objectid_ops.py | ObjectId operations |
| `type` | type_operator.py | $type operator |
| `mod` | mod_operator.py | $mod operator |
| `search_index` | search_index.py | Search index operations |
| `reindex` | reindex.py | Reindex operations |
| `elemmatch` | elemmatch.py | $elemMatch operator |

## Adding New Tests

To add tests for a missing API:

1. **Find the appropriate module** or create a new one
2. **Add the test function** following this pattern:

```python
"""Module docstring describing what this file tests"""
import copy
from typing import Any, Optional

import neosqlite
from neosqlite import ASCENDING, DESCENDING

# Suppress UserWarnings for NeoSQLite extensions
import warnings
warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

from .reporter import reporter
from .utils import test_pymongo_connection


def compare_your_feature():
    """Compare your feature between NeoSQLite and PyMongo"""
    print("\n=== Your Feature Comparison ===")

    # Test NeoSQLite
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        # ... your NeoSQLite tests ...
        neo_result = "some result"

    # Test MongoDB
    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        # ... your MongoDB tests ...
        mongo_result = "some result"
        client.close()

    # Record result
    reporter.record_result(
        "Your Category",
        "your_feature",
        passed=neo_result == mongo_result,
        neo_result=neo_result,
        mongo_result=mongo_result,
    )
```

3. **Register the function** in `runner.py`:

```python
from .your_module import compare_your_feature

COMPARISON_FUNCTIONS = [
    # ... existing functions ...
    ("your_feature", compare_your_feature),
]
```

4. **Update MISSING-APIS.md** to mark the feature as tested

## Requirements

- NeoSQLite installed
- PyMongo installed
- MongoDB server running on localhost:27017 (optional, tests will skip MongoDB if unavailable)

## Output

The comparison script produces:
- Real-time output of each test category
- Final compatibility report with:
  - Total tests run
  - Passed/Failed/Skipped counts
  - Compatibility percentage
  - Detailed list of incompatible APIs
  - List of skipped tests with reasons

## Exit Codes

- `0`: All tests passed (or skipped)
- `1`: One or more tests failed
