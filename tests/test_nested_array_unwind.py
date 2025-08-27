# coding: utf-8
"""
Test cases for nested array unwinding with json_each()
These tests verify the implementation of nested array unwinding as described in the roadmap.
"""
import neosqlite
import pytest


def test_nested_array_unwind_basic():
    """Test basic nested array unwinding functionality"""
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

        # Test nested unwind: first unwind orders, then unwind items within each order
        pipeline = [{"$unwind": "$orders"}, {"$unwind": "$orders.items"}]
        result = collection.aggregate(pipeline)

        # Should have 3 documents (2 items in first order + 1 item in second order)
        assert len(result) == 3

        # Check that both levels of unwinding worked
        # After unwinding, doc["orders"] is the unwound order object
        # and doc["orders.items"] is the unwound item object
        order_ids = [doc["orders"]["orderId"] for doc in result]
        products = [doc["orders.items"]["product"] for doc in result]
        quantities = [doc["orders.items"]["quantity"] for doc in result]

        # Each order should appear as many times as it has items
        assert order_ids.count("A001") == 2
        assert order_ids.count("A002") == 1

        # Check specific products
        assert "Book" in products
        assert "Pen" in products
        assert "Notebook" in products

        # Check quantities
        assert 2 in quantities
        assert 5 in quantities
        assert 3 in quantities


def test_nested_array_unwind_with_match():
    """Test nested array unwinding with a match stage before unwinding"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "status": "active",
                    "orders": [
                        {
                            "orderId": "A001",
                            "items": [
                                {"product": "Book", "quantity": 2},
                                {"product": "Pen", "quantity": 5},
                            ],
                        }
                    ],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "status": "inactive",
                    "orders": [
                        {
                            "orderId": "B001",
                            "items": [{"product": "Desk", "quantity": 1}],
                        }
                    ],
                },
            ]
        )

        # Test with match before nested unwind
        pipeline = [
            {"$match": {"status": "active"}},
            {"$unwind": "$orders"},
            {"$unwind": "$orders.items"},
        ]
        result = collection.aggregate(pipeline)

        # Should have 2 documents (2 items in Alice's order, Bob's order filtered out)
        assert len(result) == 2

        # All documents should be for Alice
        names = [doc["name"] for doc in result]
        assert all(name == "Alice" for name in names)

        # Check products
        products = [doc["orders.items"]["product"] for doc in result]
        assert "Book" in products
        assert "Pen" in products
        assert "Desk" not in products


def test_nested_array_unwind_three_levels():
    """Test three levels of nested array unwinding"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with three levels of nesting
        collection.insert_one(
            {
                "_id": 1,
                "name": "Charlie",
                "years": [
                    {
                        "year": 2022,
                        "months": [
                            {
                                "month": "January",
                                "days": [
                                    {"date": 1, "event": "New Year"},
                                    {"date": 15, "event": "Meeting"},
                                ],
                            },
                            {
                                "month": "February",
                                "days": [
                                    {"date": 14, "event": "Valentine's Day"}
                                ],
                            },
                        ],
                    }
                ],
            }
        )

        # Test three levels of unwinding
        pipeline = [
            {"$unwind": "$years"},
            {"$unwind": "$years.months"},
            {"$unwind": "$years.months.days"},
        ]

        result = collection.aggregate(pipeline)
        assert len(result) == 3

        events = [doc["years.months.days"]["event"] for doc in result]
        assert "New Year" in events
        assert "Meeting" in events
        assert "Valentine's Day" in events

        months = [doc["years.months"]["month"] for doc in result]
        assert months.count("January") == 2
        assert months.count("February") == 1


def test_nested_unwind_with_sort_limit():
    """Test nested array unwinding with sort and limit stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "orders": [
                        {
                            "orderId": "A001",
                            "items": [
                                {"product": "Book", "quantity": 2, "price": 10},
                                {"product": "Pen", "quantity": 5, "price": 1},
                            ],
                        }
                    ],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "orders": [
                        {
                            "orderId": "B001",
                            "items": [
                                {
                                    "product": "Desk",
                                    "quantity": 1,
                                    "price": 100,
                                },
                                {
                                    "product": "Chair",
                                    "quantity": 2,
                                    "price": 50,
                                },
                            ],
                        }
                    ],
                },
            ]
        )

        # Test nested unwind with sort and limit
        pipeline = [
            {"$unwind": "$orders"},
            {"$unwind": "$orders.items"},
            {"$sort": {"orders.items.price": -1}},  # Sort by price descending
            {"$limit": 3},  # Limit to top 3 most expensive items
        ]
        result = collection.aggregate(pipeline)

        assert len(result) == 3
        prices = [doc["orders.items"]["price"] for doc in result]
        assert prices == [100, 50, 10]


def test_nested_unwind_with_skip():
    """Test nested array unwinding with a skip stage"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_one(
            {
                "_id": 1,
                "name": "Alice",
                "orders": [
                    {
                        "orderId": "A001",
                        "items": [
                            {"product": "Book", "quantity": 2, "price": 10},
                            {"product": "Pen", "quantity": 5, "price": 1},
                            {"product": "Paper", "quantity": 10, "price": 5},
                        ],
                    },
                ],
            }
        )

        pipeline = [
            {"$unwind": "$orders"},
            {"$unwind": "$orders.items"},
            {"$sort": {"orders.items.price": -1}},
            {"$skip": 1},
        ]
        result = collection.aggregate(pipeline)

        assert len(result) == 2
        prices = [doc["orders.items"]["price"] for doc in result]
        assert prices == [5, 1]


def test_nested_unwind_with_missing_field():
    """Test nested unwind when some documents are missing the nested array"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "orders": [
                        {
                            "orderId": "A001",
                            "items": [
                                {"product": "Book", "quantity": 2},
                            ],
                        },
                    ],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                },
            ]
        )

        pipeline = [{"$unwind": "$orders"}, {"$unwind": "$orders.items"}]
        result = collection.aggregate(pipeline)

        assert len(result) == 1
        assert result[0]["name"] == "Alice"


def test_nested_unwind_with_empty_nested_array():
    """Test nested unwind with an empty nested array"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_one(
            {
                "_id": 1,
                "name": "Alice",
                "orders": [
                    {
                        "orderId": "A001",
                        "items": [],
                    },
                    {
                        "orderId": "A002",
                        "items": [{"product": "Notebook", "quantity": 3}],
                    },
                ],
            }
        )

        pipeline = [{"$unwind": "$orders"}, {"$unwind": "$orders.items"}]
        result = collection.aggregate(pipeline)

        assert len(result) == 1
        assert result[0]["orders"]["orderId"] == "A002"


def test_unwind_on_non_array_field():
    """Test that unwinding a non-array field does not produce an error"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_many(
            [
                {"_id": 1, "a": 1},
                {"_id": 2, "a": [1, 2, 3]},
            ]
        )

        pipeline = [{"$unwind": "$a"}]
        result = collection.aggregate(pipeline)

        # Should have 3 documents, as the non-array is ignored
        assert len(result) == 3


def test_invalid_unwind_expression():
    """Test that an invalid unwind expression falls back to Python implementation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_one({"_id": 1, "a": [1, 2]})
        pipeline = [{"$unwind": "a"}]  # Invalid expression
        result = collection.aggregate(pipeline)
        assert len(result) == 2


def test_match_with_no_results():
    """Test unwind with a match stage that returns no results"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_one({"_id": 1, "a": [1, 2]})
        pipeline = [{"$match": {"_id": 2}}, {"$unwind": "$a"}]
        result = collection.aggregate(pipeline)
        assert len(result) == 0


def test_unwind_with_skip_no_limit():
    """Test unwind with a skip stage but no limit"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_one({"_id": 1, "a": [1, 2, 3]})
        pipeline = [{"$unwind": "$a"}, {"$skip": 1}]
        result = collection.aggregate(pipeline)
        assert len(result) == 2


def test_unwind_with_complex_sort():
    """Test unwind with a complex sort expression"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_many(
            [
                {"_id": 1, "a": [{"b": 1}, {"b": 3}]},
                {"_id": 2, "a": [{"b": 2}, {"b": 4}]},
            ]
        )
        pipeline = [{"$unwind": "$a"}, {"$sort": {"a.b": -1}}]
        result = collection.aggregate(pipeline)
        assert [r["a"]["b"] for r in result] == [4, 3, 2, 1]


if __name__ == "__main__":
    pytest.main([__file__])
