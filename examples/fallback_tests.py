import neosqlite

db = neosqlite.Connection(":memory:")
collection = db.test_collection

# Insert test documents
collection.insert_one({"title": "Hello World", "tags": ["python", "database"]})
collection.insert_one({"title": "Goodbye World", "tags": ["javascript", "web"]})

print("Testing queries that require Python fallback:")

# Test $or with $elemMatch (which is not supported in SQL)
try:
    result = list(
        collection.find(
            {
                "$or": [
                    {"tags": {"$elemMatch": {"$eq": "python"}}},
                    {"title": {"$contains": "goodbye"}},
                ]
            }
        )
    )
    print(f"$or with $elemMatch (Python fallback): {len(result)} documents")
except Exception as e:
    print(f"Error: {e}")

# Test $or with $size (which is not supported in SQL)
try:
    result = list(
        collection.find(
            {"$or": [{"tags": {"$size": 2}}, {"title": {"$contains": "hello"}}]}
        )
    )
    print(f"$or with $size (Python fallback): {len(result)} documents")
except Exception as e:
    print(f"Error: {e}")

print("\nFallback tests completed!")
