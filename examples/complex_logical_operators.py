import neosqlite

db = neosqlite.Connection(":memory:")
collection = db.test_collection

# Insert test documents
collection.insert_one(
    {"title": "Hello World", "subtitle": "This is a test", "category": "A"}
)
collection.insert_one(
    {
        "title": "Goodbye World",
        "subtitle": "This is another test",
        "category": "B",
    }
)
collection.insert_one(
    {"title": "Foo Bar", "subtitle": "Something else", "category": "A"}
)

print("Testing complex logical operators with SQL processing:")

# Test $or with multiple conditions
result = list(
    collection.find(
        {
            "$or": [
                {"title": {"$contains": "hello"}},
                {"subtitle": {"$contains": "another"}},
                {"category": "A"},
            ]
        }
    )
)
print(f"$or with multiple conditions: {len(result)} documents")

# Test $and with multiple conditions
result = list(
    collection.find(
        {"$and": [{"category": "A"}, {"title": {"$contains": "hello"}}]}
    )
)
print(f"$and with multiple conditions: {len(result)} documents")

# Test $nor operator
result = list(
    collection.find(
        {"$nor": [{"category": "A"}, {"title": {"$contains": "hello"}}]}
    )
)
print(f"$nor operator: {len(result)} documents")

# Test $not operator
result = list(collection.find({"$not": {"category": "A"}}))
print(f"$not operator: {len(result)} documents")

# Test nested logical operators
result = list(
    collection.find(
        {
            "$or": [
                {
                    "$and": [
                        {"category": "A"},
                        {"title": {"$contains": "hello"}},
                    ]
                },
                {"subtitle": {"$contains": "another"}},
            ]
        }
    )
)
print(f"Nested logical operators: {len(result)} documents")

print("\nAll tests completed successfully!")
