# Text Search with $text Operator

NeoSQLite now supports text search using the `$text` operator, which leverages SQLite's FTS5 (Full-Text Search) extension for efficient text searching.

## Creating FTS Indexes

To enable text search functionality, you need to create FTS indexes on the fields you want to search. This is done by passing `fts=True` to the `create_index()` method:

```python
import neosqlite

# Create a connection and collection
conn = neosqlite.Connection(":memory:")
collection = conn["articles"]

# Insert sample documents
collection.insert_many([
    {"title": "Python Programming Basics", "content": "Python is a high-level programming language..."},
    {"title": "JavaScript Fundamentals", "content": "JavaScript is a versatile programming language..."}
])

# Create FTS index on content field
collection.create_index("content", fts=True)

# Create FTS index on nested fields
collection.create_index("metadata.description", fts=True)
```

## Using the $text Operator

Once you have created FTS indexes, you can use the `$text` operator to perform text searches:

```python
# Search for documents containing the word "programming"
results = list(collection.find({"$text": {"$search": "programming"}}))

# Search for documents containing multiple words
results = list(collection.find({"$text": {"$search": "python programming"}}))

# Search is case-insensitive
results = list(collection.find({"$text": {"$search": "PYTHON"}}))
```

## Using $text with Logical Operators

The `$text` operator can be combined with logical operators (`$and`, `$or`, `$not`, `$nor`) for complex queries. See [Text Search with Logical Operators](text_search_logical_operators.md) for detailed information.

```python
# $text with $and
results = list(collection.find({
    "$and": [
        {"$text": {"$search": "python"}},
        {"category": "programming"}
    ]
}))

# $text with $or
results = list(collection.find({
    "$or": [
        {"$text": {"$search": "python"}},
        {"$text": {"$search": "javascript"}}
    ]
}))
```

## How It Works

1. **FTS Index Creation**: When you create an FTS index, NeoSQLite:
   - Creates an FTS5 virtual table to store the indexed text
   - Sets up triggers to keep the FTS index synchronized with the main table
   - Populates the FTS index with existing data

2. **Query Processing**: When you use the `$text` operator:
   - NeoSQLite first checks if FTS indexes exist for the collection
   - If FTS indexes exist, it uses FTS5's efficient MATCH query
   - If no FTS indexes exist, it falls back to a Python-based substring search

3. **Automatic Synchronization**: FTS indexes are automatically kept in sync with the main table through triggers:
   - INSERT operations add new text to the FTS index
   - UPDATE operations update the text in the FTS index
   - DELETE operations remove text from the FTS index

## Limitations

1. **Single Field Search**: Currently, the implementation searches only the first FTS-indexed field it finds
2. **No Advanced FTS5 Features**: Advanced FTS5 features like ranking and snippets are not yet exposed
3. **FTS5 Availability**: Requires SQLite with FTS5 support (available in SQLite 3.9.0 and later)

## Python Fallback

If no FTS indexes exist for a collection, the `$text` operator will fall back to a Python-based substring search that:
- Searches all string fields in documents
- Performs case-insensitive matching
- Supports multiple word searches (all words must be present)

This ensures that text search functionality is always available, even without FTS indexes, though it will be less efficient for large collections.