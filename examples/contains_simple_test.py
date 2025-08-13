import neosqlite

db = neosqlite.Connection(":memory:")
db.test_collection.insert_one(
    {"title": "Hello World", "subtitle": "This is a test"}
)
try:
    result = list(db.test_collection.find({"title": {"$contains": "hello"}}))
    print("Success:", result)
except Exception as e:
    print("Error:", e)
