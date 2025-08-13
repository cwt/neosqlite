import neosqlite
import pytest


def test_or_with_contains_operator():
    """Test that $or operator works correctly with $contains operator."""
    db = neosqlite.Connection(":memory:")
    collection = db.test_collection

    # Insert test documents
    collection.insert_one(
        {"title": "Hello World", "subtitle": "This is a test"}
    )
    collection.insert_one(
        {"title": "Goodbye World", "subtitle": "This is another test"}
    )
    collection.insert_one({"title": "Foo Bar", "subtitle": "Something else"})

    # Test $or with $contains
    result = list(
        collection.find(
            {
                "$or": [
                    {"title": {"$contains": "hello"}},
                    {"subtitle": {"$contains": "another"}},
                ]
            }
        )
    )

    # Should match first two documents
    assert len(result) == 2

    # Check that we got the right documents
    titles = {doc["title"] for doc in result}
    assert titles == {"Hello World", "Goodbye World"}

    # Test with a query that matches all documents
    result = list(
        collection.find(
            {
                "$or": [
                    {"title": {"$contains": "hello"}},
                    {"title": {"$contains": "goodbye"}},
                    {"subtitle": {"$contains": "something"}},
                ]
            }
        )
    )

    # Should match all documents
    assert len(result) == 3


def test_nested_logical_operators():
    """Test nested logical operators."""
    db = neosqlite.Connection(":memory:")
    collection = db.test_collection

    # Insert test documents
    collection.insert_one(
        {"title": "Hello World", "category": "A", "priority": 1}
    )
    collection.insert_one(
        {"title": "Goodbye World", "category": "B", "priority": 2}
    )
    collection.insert_one({"title": "Foo Bar", "category": "A", "priority": 3})

    # Test nested $and within $or
    result = list(
        collection.find(
            {
                "$or": [
                    {"$and": [{"category": "A"}, {"priority": {"$gte": 2}}]},
                    {"title": {"$contains": "goodbye"}},
                ]
            }
        )
    )

    # Should match the second and third documents
    assert len(result) == 2

    titles = {doc["title"] for doc in result}
    assert titles == {"Goodbye World", "Foo Bar"}


def test_not_operator():
    """Test $not operator."""
    db = neosqlite.Connection(":memory:")
    collection = db.test_collection

    # Insert test documents
    collection.insert_one({"title": "Hello World", "category": "A"})
    collection.insert_one({"title": "Goodbye World", "category": "B"})

    # Test $not operator
    result = list(collection.find({"$not": {"category": "A"}}))

    # Should match only the second document
    assert len(result) == 1
    assert result[0]["title"] == "Goodbye World"


def test_nor_operator():
    """Test $nor operator."""
    db = neosqlite.Connection(":memory:")
    collection = db.test_collection

    # Insert test documents
    collection.insert_one({"title": "Hello World", "category": "A"})
    collection.insert_one({"title": "Goodbye World", "category": "B"})
    collection.insert_one({"title": "Foo Bar", "category": "C"})

    # Test $nor operator
    result = list(
        collection.find({"$nor": [{"category": "A"}, {"category": "B"}]})
    )

    # Should match only the third document
    assert len(result) == 1
    assert result[0]["title"] == "Foo Bar"


if __name__ == "__main__":
    test_or_with_contains_operator()
    test_nested_logical_operators()
    test_not_operator()
    test_nor_operator()
    print("All tests passed!")
