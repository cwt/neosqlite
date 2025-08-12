# coding: utf-8
import neosqlite


def test_nested_field_queries_now_use_sql():
    """Test that nested field queries now use SQL implementation."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "profile": {"age": 25, "city": "New York"}},
                {"name": "Bob", "profile": {"age": 30, "city": "Boston"}},
                {"name": "Charlie", "profile": {"age": 25, "city": "New York"}},
            ]
        )

        # Test that nested field query works and now uses SQL
        results = list(collection.find({"profile.age": 25}))
        assert len(results) == 2
        names = {doc["name"] for doc in results}
        assert names == {"Alice", "Charlie"}

        # Test that _build_simple_where_clause now handles nested fields
        where_result = collection._build_simple_where_clause(
            {"profile.age": 25}
        )
        assert where_result is not None  # Now handled by SQL
        where_clause, params = where_result
        assert "json_extract(data, '$.profile.age') = ?" in where_clause
        assert params == [25]


def test_nested_field_with_operators_now_use_sql():
    """Test that nested field queries with operators now use SQL."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "profile": {"age": 25}},
                {"name": "Bob", "profile": {"age": 30}},
                {"name": "Charlie", "profile": {"age": 35}},
            ]
        )

        # Test that nested field query with operators works and now uses SQL
        results = list(collection.find({"profile.age": {"$gt": 25}}))
        assert len(results) == 2
        names = {doc["name"] for doc in results}
        assert names == {"Bob", "Charlie"}

        # Test that _build_simple_where_clause now handles nested fields with operators
        where_result = collection._build_simple_where_clause(
            {"profile.age": {"$gt": 25}}
        )
        assert where_result is not None  # Now handled by SQL
        where_clause, params = where_result
        assert "json_extract(data, '$.profile.age') > ?" in where_clause
        assert params == [25]


def test_multiple_nested_fields():
    """Test queries with multiple nested fields."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "profile": {"age": 25, "address": {"city": "New York"}},
                },
                {
                    "name": "Bob",
                    "profile": {"age": 30, "address": {"city": "Boston"}},
                },
                {
                    "name": "Charlie",
                    "profile": {"age": 25, "address": {"city": "New York"}},
                },
            ]
        )

        # Test query with nested field two levels deep
        results = list(collection.find({"profile.address.city": "New York"}))
        assert len(results) == 2
        names = {doc["name"] for doc in results}
        assert names == {"Alice", "Charlie"}

        # Test _build_simple_where_clause with deeply nested fields
        where_result = collection._build_simple_where_clause(
            {"profile.address.city": "New York"}
        )
        assert where_result is not None  # Now handled by SQL
        where_clause, params = where_result
        assert (
            "json_extract(data, '$.profile.address.city') = ?" in where_clause
        )
        assert params == ["New York"]
