# coding: utf-8
"""
Test cases for $lookup operations with json_each() optimization
"""
import neosqlite
import pytest


def test_lookup_basic():
    """Test basic $lookup functionality"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        customers = conn["customers"]
        orders = conn["orders"]

        # Insert test data
        customers.insert_many(
            [
                {"_id": 1, "name": "Alice", "customerId": "C001"},
                {"_id": 2, "name": "Bob", "customerId": "C002"},
            ]
        )

        orders.insert_many(
            [
                {
                    "_id": 1,
                    "customerId": "C001",
                    "product": "Book",
                    "amount": 10.99,
                },
                {
                    "_id": 2,
                    "customerId": "C001",
                    "product": "Pen",
                    "amount": 1.99,
                },
                {
                    "_id": 3,
                    "customerId": "C002",
                    "product": "Notebook",
                    "amount": 5.99,
                },
            ]
        )

        # Test $lookup operation
        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            }
        ]

        result = customers.aggregate(pipeline)

        # Should have 2 documents (one for each customer)
        assert len(result) == 2

        # Find Alice's document
        alice_doc = next(doc for doc in result if doc["name"] == "Alice")
        assert alice_doc["name"] == "Alice"
        assert len(alice_doc["customerOrders"]) == 2
        assert any(
            order["product"] == "Book" for order in alice_doc["customerOrders"]
        )
        assert any(
            order["product"] == "Pen" for order in alice_doc["customerOrders"]
        )

        # Find Bob's document
        bob_doc = next(doc for doc in result if doc["name"] == "Bob")
        assert bob_doc["name"] == "Bob"
        assert len(bob_doc["customerOrders"]) == 1
        assert bob_doc["customerOrders"][0]["product"] == "Notebook"


def test_lookup_with_match():
    """Test $lookup with preceding $match stage"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        customers = conn["customers"]
        orders = conn["orders"]

        # Insert test data
        customers.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "customerId": "C001",
                    "status": "active",
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "customerId": "C002",
                    "status": "inactive",
                },
                {
                    "_id": 3,
                    "name": "Charlie",
                    "customerId": "C003",
                    "status": "active",
                },
            ]
        )

        orders.insert_many(
            [
                {
                    "_id": 1,
                    "customerId": "C001",
                    "product": "Book",
                    "amount": 10.99,
                },
                {
                    "_id": 2,
                    "customerId": "C002",
                    "product": "Pen",
                    "amount": 1.99,
                },
                {
                    "_id": 3,
                    "customerId": "C003",
                    "product": "Notebook",
                    "amount": 5.99,
                },
            ]
        )

        # Test $match then $lookup
        pipeline = [
            {"$match": {"status": "active"}},
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            },
        ]

        result = customers.aggregate(pipeline)

        # Should have 2 documents (only active customers)
        assert len(result) == 2

        # Should only contain Alice and Charlie
        names = [doc["name"] for doc in result]
        assert "Alice" in names
        assert "Charlie" in names
        assert "Bob" not in names

        # Check that each has their orders
        for doc in result:
            assert len(doc["customerOrders"]) == 1


def test_lookup_empty_results():
    """Test $lookup when no matching documents are found"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        customers = conn["customers"]
        orders = conn["orders"]

        # Insert test data
        customers.insert_many(
            [
                {"_id": 1, "name": "Alice", "customerId": "C001"},
                {
                    "_id": 2,
                    "name": "Bob",
                    "customerId": "C002",
                },  # No matching orders
            ]
        )

        orders.insert_many(
            [
                {
                    "_id": 1,
                    "customerId": "C001",
                    "product": "Book",
                    "amount": 10.99,
                },
            ]
        )

        # Test $lookup operation
        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            }
        ]

        result = customers.aggregate(pipeline)

        # Should have 2 documents
        assert len(result) == 2

        # Alice should have orders
        alice_doc = next(doc for doc in result if doc["name"] == "Alice")
        assert len(alice_doc["customerOrders"]) == 1

        # Bob should have empty orders array
        bob_doc = next(doc for doc in result if doc["name"] == "Bob")
        assert len(bob_doc["customerOrders"]) == 0
        assert bob_doc["customerOrders"] == []


def test_lookup_with_unwind():
    """Test $lookup followed by $unwind"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        customers = conn["customers"]
        orders = conn["orders"]

        # Insert test data
        customers.insert_many(
            [
                {"_id": 1, "name": "Alice", "customerId": "C001"},
            ]
        )

        orders.insert_many(
            [
                {
                    "_id": 1,
                    "customerId": "C001",
                    "product": "Book",
                    "amount": 10.99,
                },
                {
                    "_id": 2,
                    "customerId": "C001",
                    "product": "Pen",
                    "amount": 1.99,
                },
            ]
        )

        # Test $lookup followed by $unwind
        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            },
            {"$unwind": "$customerOrders"},
        ]

        result = customers.aggregate(pipeline)

        # Should have 2 documents (one for each order)
        assert len(result) == 2

        # Both documents should be for Alice
        assert all(doc["name"] == "Alice" for doc in result)

        # Each document should have one order
        products = [doc["customerOrders"]["product"] for doc in result]
        assert "Book" in products
        assert "Pen" in products


if __name__ == "__main__":
    pytest.main([__file__])
