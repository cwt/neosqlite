# Text Search with Logical Operators

NeoSQLite supports combining the `$text` operator with logical operators (`$and`, `$or`, `$not`, `$nor`) for complex text search queries.

## Using $text with Logical Operators

The `$text` operator can be combined with logical operators to create sophisticated search queries:

```python
# $text with $and
results = collection.find({
    "$and": [
        {"$text": {"$search": "python"}},
        {"category": "programming"}
    ]
})

# $text with $or
results = collection.find({
    "$or": [
        {"$text": {"$search": "python"}},
        {"$text": {"$search": "javascript"}}
    ]
})

# $text with $not
results = collection.find({
    "$not": {"$text": {"$search": "web"}}
})

# $text with $nor
results = collection.find({
    "$nor": [
        {"$text": {"$search": "python"}},
        {"$text": {"$search": "javascript"}}
    ]
})

# Complex nested logical operators
results = collection.find({
    "$and": [
        {
            "$or": [
                {"$text": {"$search": "web"}},
                {"category": "ai"}
            ]
        },
        {
            "$or": [
                {"tags": {"$in": ["web"]}},
                {"tags": {"$in": ["ai"]}}
            ]
        }
    ]
})
```

## How It Works

When using `$text` with logical operators, NeoSQLite employs a hybrid processing approach:

1. **Query Detection**: Queries containing logical operators are detected and processed using Python fallback
2. **Condition Evaluation**: Each condition is evaluated individually:
   - `$text` conditions use FTS5 when indexes exist
   - Other conditions use standard Python evaluation
3. **Result Combination**: Results are combined using set operations in Python:
   - `$and` → Set intersection
   - `$or` → Set union
   - `$not` → Set complement
   - `$nor` → Complement of set union

## Performance Considerations

1. **FTS Efficiency**: `$text` conditions still benefit from FTS5 when indexes exist
2. **Logical Operations**: Set operations in Python are fast for reasonable result set sizes
3. **Index Usage**: Non-text conditions can still use regular indexes when appropriate
4. **Memory Usage**: Intermediate result sets are kept in memory during processing

## Example Usage

```python
import neosqlite

# Create connection and collection
conn = neosqlite.Connection(":memory:")
articles = conn.articles

# Insert sample documents
articles.insert_many([
    {"title": "Python Guide", "content": "Python programming tutorial", "category": "programming"},
    {"title": "JavaScript Basics", "content": "JavaScript web development", "category": "web"},
    {"title": "Machine Learning", "content": "AI and machine learning", "category": "ai"}
])

# Create FTS index for efficient text search
articles.create_index("content", fts=True)

# Complex query combining text search with logical operators
results = list(articles.find({
    "$or": [
        {"$text": {"$search": "python"}},
        {"$and": [
            {"$text": {"$search": "javascript"}},
            {"category": "web"}
        ]}
    ]
}))
```

## Benefits

1. **Flexibility**: Supports any combination of logical operators with text search
2. **Performance**: Still leverages FTS5 for text searches when possible
3. **Compatibility**: Maintains full compatibility with PyMongo's API
4. **Simplicity**: No complex SQL generation needed
5. **Reliability**: Consistent behavior across all operator combinations

This hybrid approach provides both the efficiency of FTS5 for text searches and the flexibility of Python for complex logical operations, making it possible to create sophisticated search queries while maintaining good performance.