# coding: utf-8
"""
Test cases for advanced $unwind options (includeArrayIndex and preserveNullAndEmptyArrays)
These tests verify the implementation of advanced $unwind options as described in the roadmap.
"""
import neosqlite
import pytest


def test_unwind_with_include_array_index():
    """Test $unwind with includeArrayIndex option"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with arrays
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "hobbies": ["reading", "swimming", "coding"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "hobbies": ["gaming", "cooking"],
                },
            ]
        )

        # Test unwind with includeArrayIndex
        pipeline = [
            {"$unwind": {"path": "$hobbies", "includeArrayIndex": "hobbyIndex"}}
        ]
        result = collection.aggregate(pipeline)

        # Should have 5 documents (3+2 hobbies)
        assert len(result) == 5

        # Check that each document has the hobbyIndex field
        alice_docs = [doc for doc in result if doc["name"] == "Alice"]
        bob_docs = [doc for doc in result if doc["name"] == "Bob"]

        assert len(alice_docs) == 3
        assert len(bob_docs) == 2

        # Check indices for Alice
        alice_indices = [doc["hobbyIndex"] for doc in alice_docs]
        assert alice_indices == [0, 1, 2]

        # Check indices for Bob
        bob_indices = [doc["hobbyIndex"] for doc in bob_docs]
        assert bob_indices == [0, 1]

        # Check that hobbies are correct
        alice_hobbies = [doc["hobbies"] for doc in alice_docs]
        assert alice_hobbies == ["reading", "swimming", "coding"]

        bob_hobbies = [doc["hobbies"] for doc in bob_docs]
        assert bob_hobbies == ["gaming", "cooking"]


def test_unwind_with_preserve_null_and_empty_arrays():
    """Test $unwind with preserveNullAndEmptyArrays option"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with various array types
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "hobbies": ["reading", "swimming"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "hobbies": [],  # Empty array
                },
                {
                    "_id": 3,
                    "name": "Charlie",
                    "hobbies": None,  # Null value
                },
                {
                    "_id": 4,
                    "name": "David",
                    # No hobbies field
                },
            ]
        )

        # Test unwind with preserveNullAndEmptyArrays=True
        pipeline = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 4 documents (2 from Alice + 1 each for Bob, Charlie, David)
        assert len(result) == 4

        # Check documents with actual array values
        alice_docs = [doc for doc in result if doc["name"] == "Alice"]
        assert len(alice_docs) == 2
        alice_hobbies = [doc["hobbies"] for doc in alice_docs]
        assert set(alice_hobbies) == {"reading", "swimming"}

        # Check documents with empty/null/missing arrays are preserved
        other_docs = [
            doc for doc in result if doc["name"] in ["Bob", "Charlie", "David"]
        ]
        assert (
            len(other_docs) == 2
        )  # Bob and Charlie should be preserved, David should be missing

        # Bob should have hobbies as None (since array was empty)
        bob_doc = [doc for doc in other_docs if doc["name"] == "Bob"][0]
        assert "hobbies" not in bob_doc or bob_doc["hobbies"] is None

        # Charlie should have hobbies as None (since it was null)
        charlie_doc = [doc for doc in other_docs if doc["name"] == "Charlie"][0]
        assert "hobbies" not in charlie_doc or charlie_doc["hobbies"] is None


def test_unwind_with_both_advanced_options():
    """Test $unwind with both includeArrayIndex and preserveNullAndEmptyArrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "scores": [85, 92, 78],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "scores": [],  # Empty array
                },
            ]
        )

        # Test unwind with both options
        pipeline = [
            {
                "$unwind": {
                    "path": "$scores",
                    "includeArrayIndex": "scoreIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 4 documents (3 from Alice + 1 from Bob with empty array preserved)
        assert len(result) == 4

        # Check Alice's documents
        alice_docs = [doc for doc in result if doc["name"] == "Alice"]
        assert len(alice_docs) == 3

        # Check indices and scores
        for doc in alice_docs:
            assert "scoreIndex" in doc
            assert doc["scores"] in [85, 92, 78]

        alice_indices = [doc["scoreIndex"] for doc in alice_docs]
        assert sorted(alice_indices) == [0, 1, 2]

        # Check Bob's document (empty array preserved)
        bob_docs = [doc for doc in result if doc["name"] == "Bob"]
        assert len(bob_docs) == 1
        bob_doc = bob_docs[0]
        assert "scoreIndex" in bob_doc
        assert bob_doc["scoreIndex"] is None
        assert "scores" not in bob_doc or bob_doc["scores"] is None


def test_unwind_with_include_array_index_nested():
    """Test $unwind with includeArrayIndex on nested arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with nested arrays
        collection.insert_one(
            {
                "_id": 1,
                "name": "Alice",
                "orders": [
                    {
                        "orderId": "A001",
                        "items": [
                            {"product": "Book", "quantity": 2},
                            {"product": "Pen", "quantity": 5},
                        ],
                    },
                    {
                        "orderId": "A002",
                        "items": [{"product": "Notebook", "quantity": 3}],
                    },
                ],
            }
        )

        # Test nested unwind with includeArrayIndex
        pipeline = [
            {"$unwind": {"path": "$orders", "includeArrayIndex": "orderIndex"}},
            {
                "$unwind": {
                    "path": "$orders.items",
                    "includeArrayIndex": "itemIndex",
                }
            },
        ]
        result = collection.aggregate(pipeline)

        # Should have 3 documents (2 items in first order + 1 item in second order)
        assert len(result) == 3

        # Check that both index fields are present
        for doc in result:
            assert "orderIndex" in doc
            assert "itemIndex" in doc

        # Check specific values
        first_order_items = [doc for doc in result if doc["orderIndex"] == 0]
        assert len(first_order_items) == 2
        first_order_indices = [doc["itemIndex"] for doc in first_order_items]
        assert sorted(first_order_indices) == [0, 1]

        second_order_items = [doc for doc in result if doc["orderIndex"] == 1]
        assert len(second_order_items) == 1
        assert second_order_items[0]["itemIndex"] == 0


def test_unwind_with_preserve_on_nested_arrays():
    """Test $unwind with preserveNullAndEmptyArrays on nested arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with nested arrays including empty ones
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "orders": [
                        {
                            "orderId": "A001",
                            "items": [{"product": "Book", "quantity": 2}],
                        },
                        {"orderId": "A002", "items": []},  # Empty items array
                    ],
                },
                {"_id": 2, "name": "Bob", "orders": []},  # Empty orders array
            ]
        )

        # Test nested unwind with preserveNullAndEmptyArrays
        pipeline = [
            {
                "$unwind": {
                    "path": "$orders",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$orders.items",
                    "preserveNullAndEmptyArrays": True,
                }
            },
        ]
        result = collection.aggregate(pipeline)

        # Should have 2 documents:
        # 1. Alice's order with items (1 item)
        # 2. Alice's order without items (empty array preserved)
        # Bob's empty orders array should not produce any documents
        assert len(result) == 2

        # Check that Alice's documents are present
        alice_docs = [doc for doc in result if doc["name"] == "Alice"]
        assert len(alice_docs) == 2


def test_unwind_string_path_still_works():
    """Test that the original string path syntax still works"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_one(
            {
                "_id": 1,
                "name": "Alice",
                "hobbies": ["reading", "swimming", "coding"],
            }
        )

        # Test original string syntax
        pipeline = [{"$unwind": "$hobbies"}]
        result = collection.aggregate(pipeline)

        # Should have 3 documents
        assert len(result) == 3

        # Check that hobbies are correct
        hobbies = [doc["hobbies"] for doc in result]
        assert set(hobbies) == {"reading", "swimming", "coding"}


if __name__ == "__main__":
    pytest.main([__file__])
