import neosqlite

db = neosqlite.Connection(":memory:")
db.test_collection.insert_one(
    {"title": "Hello World", "subtitle": "This is a test"}
)

collection = db.test_collection

# Test queries
query1 = {"title": {"$contains": "hello"}}  # Should work with SQL
query2 = {
    "$or": [{"title": {"$contains": "hello"}}]
}  # Should fallback to Python

print("Testing _build_simple_where_clause:")
result1 = collection._build_simple_where_clause(query1)
print("Simple query result:", result1)

result2 = collection._build_simple_where_clause(query2)
print("Complex query result:", result2)

# Let's also test a mixed query
query3 = {"$and": [{"title": {"$contains": "hello"}}]}
result3 = collection._build_simple_where_clause(query3)
print("And query result:", result3)
