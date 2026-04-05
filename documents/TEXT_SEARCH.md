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

## Integration with Aggregation Pipelines ($unwind and Temp Tables)

NeoSQLite provides a unique extension allowing the `$text` operator to be used *anywhere* in an aggregation pipeline, even after stages like `$unwind` that create temporary tables.

```python
pipeline = [
  {"$unwind": "$comments"},
  {"$match": {"$text": {"$search": "performance"}}}
]
```

### How it works (v1.14.0+)

1. **Recursive Extraction**: NeoSQLite uses `json_tree` (or `jsonb_tree`) to recursively extract ALL string values from the JSON data in the current stage's temporary table.
2. **On-the-fly FTS Indexing**: It creates a temporary FTS5 virtual table and populates it with the extracted text content.
3. **SQL-Tier Search**: The search is performed directly in SQLite using `MATCH`, making it significantly faster than Python processing.

### Semantic Differences from MongoDB

- **Extension**: In standard MongoDB, `$text` can **only** be used as the first stage of a pipeline. NeoSQLite's ability to use it later is an extension.
- **Search Target**: In MongoDB, `$text` always uses the collection-level text index. In NeoSQLite, when used after an `$unwind`, it searches the *unwound* elements (the objects inside the array).
- **Warning**: NeoSQLite will issue a `UserWarning` when `$text` is used after `$unwind` to alert users to these semantic differences.

For standard MongoDB behavior, place the `$text` stage at the very beginning of your pipeline.

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
