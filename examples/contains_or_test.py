import neosqlite

db = neosqlite.Connection(":memory:")
db.test_collection.insert_one(
    {"title": "Hello World", "subtitle": "This is a test"}
)
db.test_collection.insert_one(
    {"title": "Goodbye World", "subtitle": "This is another test"}
)

print("Testing $contains with $or operator:")
try:
    # This should work - testing $or with $contains
    result = list(
        db.test_collection.find(
            {
                "$or": [
                    {"title": {"$contains": "hello"}},
                    {"subtitle": {"$contains": "another"}},
                ]
            }
        )
    )
    print("Success:", result)
except Exception as e:
    print("Error:", e)

print("\nTesting $contains individually (should work):")
try:
    result1 = list(db.test_collection.find({"title": {"$contains": "hello"}}))
    print("Title contains 'hello':", result1)

    result2 = list(
        db.test_collection.find({"subtitle": {"$contains": "another"}})
    )
    print("Subtitle contains 'another':", result2)
except Exception as e:
    print("Error:", e)
