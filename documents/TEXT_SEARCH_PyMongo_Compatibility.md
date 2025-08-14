# PyMongo $text Operator Compatibility

NeoSQLite's `$text` operator is designed to be compatible with PyMongo's `$text` operator, but there are some differences and limitations due to the underlying SQLite FTS5 implementation.

## Compatible Features

The following PyMongo `$text` features are supported in NeoSQLite:

- Basic syntax: `{"$text": {"$search": "search terms"}}`
- Case-insensitive search (default behavior)
- Multiple word search
- Combining with logical operators (`$and`, `$or`, `$not`, `$nor`)
- Nested field support

## Incompatible or Missing Features

### 1. Advanced $text Parameters

PyMongo supports additional parameters in the `$text` operator that are not implemented in NeoSQLite:

```javascript
// PyMongo features NOT supported in NeoSQLite:
{
  "$text": {
    "$search": "search terms",
    "$language": "en",           // NOT SUPPORTED
    "$caseSensitive": false,     // NOT SUPPORTED (always case-insensitive)
    "$diacriticSensitive": false // NOT SUPPORTED
  }
}
```

### 2. Text Scoring

PyMongo provides text scoring capabilities using `$meta`:

```javascript
// PyMongo feature NOT supported in NeoSQLite:
db.collection.find(
  {"$text": {"$search": "coffee shop"}},
  {"score": {"$meta": "textScore"}}
).sort({"score": {"$meta": "textScore"}})
```

NeoSQLite does not provide text scoring or ranking features.

### 3. Advanced Search Syntax

PyMongo supports advanced search syntax that is not implemented:

- **Phrase Search**: `"exact phrase"` - Not supported
- **Term Exclusion**: `-term` to exclude terms - Not supported
- **OR Operations**: `term1 OR term2` within search - Not supported

### 4. Index Creation Differences

The index creation syntax differs between PyMongo and NeoSQLite:

```javascript
// PyMongo:
db.collection.createIndex({"content": "text"})

// NeoSQLite:
collection.create_index("content", fts=True)
```

## Implementation Differences

### Backend Technology
- **PyMongo**: Uses MongoDB's native text search capabilities
- **NeoSQLite**: Uses SQLite's FTS5 extension

### Performance Characteristics
- Different performance profiles due to different underlying engines
- SQLite FTS5 has different optimization strategies than MongoDB

### Feature Set
- PyMongo exposes more advanced text search features
- NeoSQLite focuses on core functionality with SQLite FTS5 capabilities

## NeoSQLite-Specific Enhancements

NeoSQLite provides some features that go beyond PyMongo's standard `$text` operator:

### Logical Operator Support
NeoSQLite supports combining `$text` with any logical operators, which provides more flexibility than PyMongo's standard implementation:

```python
# NeoSQLite enhancement - works with any logical operators:
{
  "$and": [
    {"$text": {"$search": "python"}},
    {"category": "programming"}
  ]
}

{
  "$or": [
    {"$text": {"$search": "python"}},
    {"$and": [
      {"$text": {"$search": "javascript"}},
      {"version": {"$gte": 2020}}
    ]}
  ]
}
```

This hybrid approach uses FTS5 for text searches when indexes exist and combines results using Python set operations for logical operators.

## Migration Considerations

When migrating from PyMongo to NeoSQLite:

1. **Remove unsupported parameters**: Strip `$language`, `$caseSensitive`, `$diacriticSensitive` from `$text` queries
2. **Remove text scoring**: Remove `$meta` text score projections and sorts
3. **Simplify search terms**: Use simple space-separated terms instead of advanced syntax
4. **Update index creation**: Change from `{"field": "text"}` to `("field", fts=True)`

## Summary

NeoSQLite provides a solid subset of PyMongo's `$text` functionality that covers most common use cases. The implementation is particularly strong in its support for combining text search with logical operators, which is an enhancement over PyMongo's standard capabilities. For applications that rely on advanced PyMongo text search features, some code modifications will be necessary.