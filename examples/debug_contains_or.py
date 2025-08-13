import neosqlite

db = neosqlite.Connection(":memory:")
db.test_collection.insert_one(
    {"title": "Hello World", "subtitle": "This is a test"}
)
db.test_collection.insert_one(
    {"title": "Goodbye World", "subtitle": "This is another test"}
)

print("Direct test of _contains function:")
from neosqlite.query_operators import _contains

doc1 = {"title": "Hello World", "subtitle": "This is a test"}
doc2 = {"title": "Goodbye World", "subtitle": "This is another test"}

print("_contains('title', 'hello', doc1):", _contains("title", "hello", doc1))
print(
    "_contains('subtitle', 'another', doc2):",
    _contains("subtitle", "another", doc2),
)

print("\nTesting manual _apply_query:")
# Let's manually test the _apply_query method
collection = db.test_collection

# Test individual queries
query1 = {"title": {"$contains": "hello"}}
query2 = {"subtitle": {"$contains": "another"}}
query3 = {
    "$or": [
        {"title": {"$contains": "hello"}},
        {"subtitle": {"$contains": "another"}},
    ]
}

print("Query 1 result:", collection._apply_query(query1, doc1))
print("Query 2 result:", collection._apply_query(query2, doc2))
print("Query 3 result:", collection._apply_query(query3, doc1))
print("Query 3 result (doc2):", collection._apply_query(query3, doc2))

# Let's also test with find to see what's happening
print("\nTesting with find method:")
try:
    result = list(collection.find(query3))
    print("Find with $or result:", result)
except Exception as e:
    print("Error with find:", e)
    import traceback

    traceback.print_exc()
