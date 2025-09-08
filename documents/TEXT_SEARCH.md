# Text Search in NeoSQLite

NeoSQLite supports efficient text search using the `$text` operator, which leverages SQLite's FTS5 (Full-Text Search) extension.

## Creating FTS Indexes

To enable text search, you must first create an FTS index on the desired fields using `fts=True`.

```python
collection.create_index("content", fts=True)
collection.create_index("metadata.description", fts=True)
```

Unlike MongoDB, NeoSQLite creates separate FTS tables for each field. When searching, it automatically queries all relevant FTS tables and combines the results.

### Search Index APIs

NeoSQLite also provides dedicated search index APIs for more explicit control:

```python
# Create a single search index
collection.create_search_index("content")

# Create multiple search indexes at once
collection.create_search_indexes(["title", "content", "description"])

# List all search indexes
indexes = collection.list_search_indexes()

# Drop a search index
collection.drop_search_index("content")

# Update a search index (drops and recreates)
collection.update_search_index("content")
```

### Custom FTS5 Tokenizers

You can use custom FTS5 tokenizers for language-specific text processing.

```python
conn = neosqlite.Connection(
    ":memory:",
    tokenizers=[("icu", "/path/to/libfts5_icu.so")]
)
collection.create_index("content", fts=True, tokenizer="icu")
```

## Using the $text Operator

Once indexes are created, you can use the `$text` operator for searching.

```python
# Search for a single word
results = list(collection.find({"$text": {"$search": "programming"}}))

# Search is case-insensitive
results = list(collection.find({"$text": {"$search": "PYTHON"}}))
```

If no FTS indexes exist, the search will fall back to a less efficient, case-insensitive substring search across all string fields in the documents.

## Integration with Logical Operators

The `$text` operator can be combined with logical operators like `$and`, `$or`, and `$not` for more complex queries. These queries are processed using a hybrid approach: `$text` conditions leverage FTS5, and the results are combined in Python using set operations.

```python
results = list(collection.find({
    "$and": [
        {"$text": {"$search": "python"}},
        {"category": "programming"}
    ]
}))
```

## Integration with $unwind (json_each)

For text searching within arrays, NeoSQLite provides a powerful optimization that combines `$unwind` and `$text` search at the SQL level.

```python
pipeline = [
  {"$unwind": "$comments"},
  {"$match": {"$text": {"$search": "performance"}}}
]
```

This pipeline is converted into a single, efficient SQL query using `json_each()`, which can be 10-100x faster than processing in Python.

## PyMongo Compatibility

NeoSQLite's `$text` operator is designed for compatibility with PyMongo, but there are some differences:

### Supported Features
- Basic syntax: `{"$text": {"$search": "..."}}`
- Case-insensitive search
- Searching across multiple indexed fields
- Dedicated search index APIs

### Incompatible or Missing Features
- **Advanced Parameters**: `$language`, `$caseSensitive`, `$diacriticSensitive` are not supported.
- **Text Scoring**: The `$meta: "textScore"` feature is not available.
- **Advanced Search Syntax**: Phrase searches (`"exact phrase"`) and term exclusion (`-term`) are not supported.

## Limitations
- Advanced FTS5 features like ranking and snippets are not exposed.
- Requires SQLite 3.9.0 or later for FTS5 support.