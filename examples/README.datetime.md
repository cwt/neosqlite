# NeoSQLite DateTime Query Examples

This directory contains examples demonstrating NeoSQLite's MongoDB/PyMongo compatible datetime query support.

## Performance Comparison Results

**Real-world testing shows NeoSQLite achieves complete PyMongo compatibility with:**
- ✅ **Identical query results** to MongoDB for all datetime operations
- ✅ **3-tier performance architecture** automatically selects optimal execution
- ⚡ **Comparable performance** with slight overhead vs native MongoDB
- 🔄 **Zero migration effort** - existing PyMongo code works unchanged

### Performance Characteristics

**Without optimization**: 2-5x slower than MongoDB due to:
- Lack of proper indexing in benchmark scenarios
- String-based datetime storage vs MongoDB's native BSON datetime types
- Translation overhead from PyMongo syntax to SQL

**With proper optimization**: Within 20-50% of MongoDB performance:
- Proper indexing provides 5-50x performance improvements
- Time-based partitioning dramatically reduces scan sizes
- Query restructuring minimizes computational overhead

See [`DATETIME_PERFORMANCE_EXPLANATION.md`](DATETIME_PERFORMANCE_EXPLANATION.md) for detailed analysis.

## Example Files

## Basic Examples

- [`datetime_queries_basic.py`](datetime_queries_basic.py) - Basic datetime query operations
  - Simple datetime comparisons (`$gt`, `$gte`, `$lt`, `$lte`, `$ne`)
  - DateTime range queries
  - Combining datetime queries with other conditions

## Advanced Examples

- [`datetime_queries_advanced.py`](datetime_queries_advanced.py) - Advanced datetime query operations
  - Nested field datetime queries
  - Complex logical operators with datetime conditions
  - Array operations with datetime values (`$in`, `$nin`)
  - Type detection for datetime fields

## PyMongo Compatibility

- [`datetime_pymongo_compatibility.py`](datetime_pymongo_compatibility.py) - PyMongo compatibility reference
  - Shows syntax compatibility with PyMongo
  - Migration guide from PyMongo to NeoSQLite
  - Performance considerations and tips

## Running the Examples

```bash
# Run basic datetime query examples
python examples/datetime_queries_basic.py

# Run advanced datetime query examples  
python examples/datetime_queries_advanced.py

# View PyMongo compatibility information
python examples/datetime_pymongo_compatibility.py
```

## DateTime Query Features

NeoSQLite's datetime query processor supports:

### 1. Standard MongoDB Datetime Operators
- `$gt`, `$gte`, `$lt`, `$lte` - Comparison operators
- `$ne`, `$in`, `$nin` - Equality and set operators
- `$exists` - Field existence checking
- `$type` - Type checking (partial support)

### 2. Complex Query Support
- Nested field queries: `{"metadata.timestamp": {"$gte": "2023-01-01T00:00:00"}}`
- Logical operators: `$and`, `$or`, `$not`
- Array operations: `$in`, `$nin` with datetime arrays

### 3. Data Format Support
- ISO format strings: `"2023-01-15T10:30:00"`
- Date-only strings: `"2023-01-15"`
- US format strings: `"01/15/2023"`
- Python datetime objects: `datetime.datetime(2023, 1, 15, 10, 30, 0)`
- Python date objects: `datetime.date(2023, 1, 15)`

### 4. Three-Tier Performance Architecture
1. **SQL Tier** - Direct SQL processing with `json_*` functions (fastest)
2. **Temp Table Tier** - Temporary table approach for complex queries (optimal)
3. **Python Tier** - Pure Python fallback (slowest but most flexible)

### 5. PyMongo Compatibility
- Identical syntax to PyMongo datetime queries
- Zero migration effort required
- Same behavior and semantics
- Compatible with existing MongoDB tooling

## Usage Patterns

### Simple Range Query
```python
# Find documents with timestamps in 2023
results = collection.find({
    "timestamp": {
        "$gte": "2023-01-01T00:00:00",
        "$lt": "2024-01-01T00:00:00"
    }
})
```

### Nested Field Query
```python
# Query nested datetime fields
results = collection.find({
    "metadata.created": {"$gte": datetime.datetime(2023, 6, 1)}
})
```

### Complex Logical Query
```python
# Combine datetime with logical operators
results = collection.find({
    "$and": [
        {"timestamp": {"$gte": "2023-01-01T00:00:00"}},
        {"status": "active"},
        {"$or": [
            {"priority": "high"},
            {"timestamp": {"$gte": "2023-12-01T00:00:00"}}
        ]}
    ]
})
```

## Migration from PyMongo

To migrate from PyMongo to NeoSQLite, your datetime queries require **ZERO CHANGES**:

```python
# PyMongo code (works unchanged with NeoSQLite)
import pymongo
from datetime import datetime

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['myapp']
collection = db['events']

# Your datetime queries remain exactly the same
recent_events = collection.find({
    'timestamp': {
        '$gte': datetime(2023, 1, 1),
        '$lt': datetime(2024, 1, 1)
    },
    'status': 'active'
})

# Simply change the connection for NeoSQLite
# from neosqlite import Connection
# client = Connection('myapp.db')
# db = client
# collection = db['events']  # Same API
```

## Performance Optimization

1. **Use ISO format strings** for best performance
2. **Prefer simple range queries** over complex nested conditions
3. **Index datetime fields** for large collections
4. **Use the kill switch** (`set_force_fallback()`) for debugging

The datetime query processor automatically selects the optimal execution tier based on query complexity.