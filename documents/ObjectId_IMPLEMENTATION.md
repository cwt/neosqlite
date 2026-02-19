# ObjectId Implementation in NeoSQLite

## Overview

NeoSQLite implements a fully MongoDB-compatible ObjectId class that provides seamless integration with PyMongo's ObjectId format. This implementation ensures that `_id` fields work identically to MongoDB, supporting automatic generation, manual assignment, and efficient querying.

## Technical Implementation

### ObjectId Class Structure

Located in `neosqlite/objectid.py`, the ObjectId class generates 12-byte values following MongoDB's specification:

- **4 bytes**: Unix timestamp (`int(time.time())`)
- **5 bytes**: Random value generated once per process (`os.urandom(5)`)
- **3 bytes**: Thread-safe incrementing counter

### Key Features

- **Thread Safety**: Uses proper locking for counter incrementation
- **Validation**: `is_valid()` method for hex string validation
- **Time Extraction**: `generation_time()` returns creation timestamp
- **Serialization**: JSON-compatible via `encode_for_storage()` and `decode_from_storage()`
- **String Representations**: `__str__()`, `__repr__()`, and `hex` property
- **Operators**: Full equality and comparison support with `__eq__`, `__ne__`, `__hash__`

### Storage Schema

Collections use a three-column schema: `(id INTEGER PRIMARY KEY AUTOINCREMENT, _id JSON UNIQUE, data JSON)`

- **Automatic Migration**: Existing collections get `_id` column added via `ALTER TABLE`
- **Indexing**: Unique index `idx_{collection_name}_id` on `_id` column
- **Performance**: Uses JSONB when available for better query performance

### CRUD Operations Integration

#### Insertion
- Documents without `_id`: Auto-generated ObjectId
- Documents with `_id`: Preserved as-is
- ObjectId hex stored in `_id` column, instance in document

#### Retrieval
- Loads both integer `id` and ObjectId `_id` columns
- Automatic ObjectId reconstruction via `_load_with_stored_id()`
- Fallback to integer ID for legacy documents

#### Updates/Deletions
- Support both integer and ObjectId queries
- `_get_integer_id_for_oid()` maps ObjectIds to internal integer IDs
- SQL translation handles `_id` queries efficiently

## Comparison with MongoDB ObjectId

### Structural Compatibility

| Aspect | MongoDB ObjectId | NeoSQLite ObjectId |
|--------|------------------|-------------------|
| **Byte Structure** | 12 bytes | 12 bytes (identical) |
| **Timestamp** | 4 bytes Unix time | 4 bytes Unix time |
| **Machine/Process ID** | 5 bytes (machine + pid) | 5 bytes random |
| **Counter** | 3 bytes | 3 bytes (thread-safe) |
| **Hex Format** | 24-character string | 24-character string (identical) |
| **JSON Storage** | BSON binary | JSON string |

### Functional Compatibility

- **Generation**: Both auto-generate when `_id` not provided
- **Manual Assignment**: Both accept user-provided ObjectIds
- **Querying**: Both support `_id` field queries with identical syntax
- **Indexing**: Both create indexes on `_id` for performance
- **Serialization**: Both work with JSON/BSON interchange

### Key Differences

- **Machine/Process Identifier**: MongoDB uses 3-byte machine hash + 2-byte process ID; NeoSQLite uses 5 bytes of random data for privacy
- **Storage**: MongoDB uses BSON ObjectId type; NeoSQLite stores as JSON strings in dedicated column
- **Counter**: Both thread-safe, but NeoSQLite uses Python-level locking and random initialization

### Performance Characteristics

- **Lookup Speed**: NeoSQLite's dedicated `_id` column with unique index provides fast lookups
- **Index Usage**: Verified through `EXPLAIN` query plans
- **Memory Usage**: Efficient Python object with lazy hex generation
- **Backward Compatibility**: Zero performance impact on existing collections

## Testing and Validation

### Unit Tests (`tests/test_objectid.py`)
- Creation from various sources (new, hex, bytes)
- Equality, comparison, and hashing
- String representations and JSON serialization
- Timestamp extraction and validation
- Thread safety and uniqueness verification

### Integration Tests
- Storage/retrieval with collections
- Backward compatibility with legacy data
- Index performance verification
- Cross-compatibility with PyMongo ObjectIds

## API Compatibility

The implementation provides 100% compatibility with PyMongo's ObjectId expectations:
- Automatic generation for insertions
- Manual assignment support
- Efficient querying and indexing
- JSON serialization compatibility
- Hex string interchangeability

## Implementation Benefits

1. **Full MongoDB Compatibility**: Drop-in replacement for PyMongo ObjectId usage
2. **Performance Optimized**: Dedicated column and indexing for fast lookups
3. **Backward Compatible**: Existing collections migrate automatically
4. **Thread Safe**: Proper synchronization for concurrent ObjectId generation
5. **Memory Efficient**: Lazy hex generation and efficient storage

This implementation ensures that NeoSQLite applications can work seamlessly with existing MongoDB/PyMongo codebases, providing identical ObjectId behavior while leveraging SQLite's efficiency.