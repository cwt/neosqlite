"""
Integration tests for $expr operator with database queries.

Tests actual database queries using neosqlite.Connection.
"""

import neosqlite


class TestExprIntegration:
    """Integration tests for $expr with collection queries."""

    def test_comparison_integration(self):
        """Test comparison operators with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5, "b": 5},
                    {"a": 5, "b": 10},
                    {"a": 10, "b": 5},
                ]
            )

            # Test $eq
            results = list(collection.find({"$expr": {"$eq": ["$a", "$b"]}}))
            assert len(results) == 1
            assert results[0]["a"] == 5

            # Test $gt
            results = list(collection.find({"$expr": {"$gt": ["$a", "$b"]}}))
            assert len(results) == 1
            assert results[0]["a"] == 10

    def test_arithmetic_integration(self):
        """Test arithmetic operators with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"price": 10, "qty": 5, "total": 50},
                    {"price": 20, "qty": 3, "total": 60},
                    {"price": 15, "qty": 4, "total": 70},  # Wrong total
                ]
            )

            expr = {
                "$expr": {"$ne": [{"$multiply": ["$price", "$qty"]}, "$total"]}
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["total"] == 70

    def test_conditional_integration(self):
        """Test conditional operators with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"qty": 5, "price": 10},
                    {"qty": 15, "price": 10},
                ]
            )

            expr = {
                "$expr": {
                    "$lt": [
                        {
                            "$cond": {
                                "if": {"$gte": ["$qty", 10]},
                                "then": {"$multiply": ["$price", 0.5]},
                                "else": "$price",
                            }
                        },
                        6,
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["qty"] == 15

    def test_array_integration(self):
        """Test array operators with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"items": [1, 2, 3]},
                    {"items": [1, 2]},
                    {"items": [1]},
                ]
            )

            expr = {"$expr": {"$gt": [{"$size": ["$items"]}, 2]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert len(results[0]["items"]) == 3

    def test_string_integration(self):
        """Test string operators with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"firstName": "John", "lastName": "Doe"},
                    {"firstName": "Jane", "lastName": "Smith"},
                ]
            )

            expr = {
                "$expr": {
                    "$eq": [
                        {"$concat": ["$firstName", " ", "$lastName"]},
                        "John Doe",
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["firstName"] == "John"

    def test_date_integration(self):
        """Test date operators with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"date": "2024-01-15"},
                    {"date": "2023-12-01"},
                ]
            )

            expr = {"$expr": {"$eq": [{"$year": ["$date"]}, 2024]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["date"] == "2024-01-15"

    def test_nested_field_paths(self):
        """Test nested field paths."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"stats": {"wins": 10, "losses": 5}},
                    {"stats": {"wins": 5, "losses": 5}},
                    {"stats": {"wins": 3, "losses": 8}},
                ]
            )

            expr = {"$expr": {"$gt": ["$stats.wins", "$stats.losses"]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["stats"]["wins"] == 10

    def test_null_values(self):
        """Test null values handling."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5, "b": None},
                    {"a": None, "b": 5},
                    {"a": 5, "b": 5},
                ]
            )

            expr = {"$expr": {"$eq": ["$a", "$b"]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["a"] == 5

    def test_missing_fields(self):
        """Test missing fields handling."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5},
                    {"b": 5},
                    {"a": 5, "b": 5},
                ]
            )

            expr = {"$expr": {"$eq": ["$a", "$b"]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["a"] == 5

    def test_complex_nested_expression(self):
        """Test complex nested expression."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 5, "b": 10, "c": 15},
                    {"a": 10, "b": 5, "c": 15},
                    {"a": 5, "b": 5, "c": 5},
                ]
            )

            # Complex: (a + b) * 2 > c AND (a != b)
            expr = {
                "$expr": {
                    "$and": [
                        {
                            "$gt": [
                                {"$multiply": [{"$add": ["$a", "$b"]}, 2]},
                                "$c",
                            ]
                        },
                        {"$ne": ["$a", "$b"]},
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 2

    def test_expr_with_regular_query(self):
        """Test $expr combined with regular query operators."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"category": "A", "qty": 10, "reserved": 5},
                    {"category": "A", "qty": 3, "reserved": 5},
                    {"category": "B", "qty": 15, "reserved": 10},
                ]
            )

            # Find documents where category == "A" AND qty > reserved
            expr = {"category": "A", "$expr": {"$gt": ["$qty", "$reserved"]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["qty"] == 10
