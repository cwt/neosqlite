# coding: utf-8
import neosqlite
from neosqlite.query_operators import _contains


def test_contains_operator_sql_generation():
    """Test that $contains operator generates proper SQL."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Test $contains with string
        result = collection.query_engine.helpers._build_simple_where_clause(
            {"name": {"$contains": "alice"}}
        )
        assert result is not None
        sql, params = result
        assert "lower(json_extract(data, '$.name')) LIKE ?" in sql
        assert params == ["%alice%"]


def test_contains_operator_functionality():
    """Test that $contains operator works correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice Smith", "bio": "Loves Python and SQL"},
                {"name": "Bob Johnson", "bio": "Enjoys JavaScript and HTML"},
                {"name": "Charlie Brown", "bio": "Prefers Go and Rust"},
            ]
        )

        # Test case-insensitive search in name field
        results = list(collection.find({"name": {"$contains": "alice"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice Smith"

        # Test case-insensitive search in bio field
        results = list(collection.find({"bio": {"$contains": "PYTHON"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice Smith"

        # Test partial match
        results = list(collection.find({"bio": {"$contains": "java"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Bob Johnson"

        # Test no match
        results = list(collection.find({"name": {"$contains": "david"}}))
        assert len(results) == 0


def test_contains_operator_python_implementation():
    """Test that $contains operator works with Python implementation."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice Smith", "tags": ["python", "sql"]},
                {"name": "Bob Johnson", "tags": ["javascript", "html"]},
                {"name": "Charlie Brown", "tags": ["go", "rust"]},
            ]
        )

        # Test $contains with array field (should fall back to Python implementation)
        results = list(collection.find({"tags": {"$contains": "python"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice Smith"


def test_contains_operator_edge_cases():
    """Test edge cases for $contains operator."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data with edge cases
        collection.insert_many(
            [
                {"name": "Alice", "bio": None},
                {"name": "Bob", "bio": 123},
                {"name": "Charlie", "bio": {"nested": "value"}},
            ]
        )

        # Test with None value (should not match)
        results = list(collection.find({"bio": {"$contains": "alice"}}))
        assert len(results) == 0

        # Test with non-string field value (should not match)
        results = list(collection.find({"bio": {"$contains": "123"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Bob"

        # Test with non-string query value
        results = list(collection.find({"name": {"$contains": 123}}))
        assert len(results) == 0


def test_contains_operator_exception_handling():
    """Test exception handling in _contains function."""

    # Test with object that raises AttributeError when calling .get()
    class BadDocument:
        def get(self, key):
            raise AttributeError("Mock AttributeError")

    result = _contains("field", "value", BadDocument())
    assert result is False

    # Test with object that raises TypeError when converting to string
    class BadValue:
        def __str__(self):
            raise TypeError("Mock TypeError")

    # Create a document with a field that has a bad value
    doc = {"field": BadValue()}
    result = _contains("field", "value", doc)
    assert result is False

    # Test with None field value
    result = _contains("field", "value", {"field": None})
    assert result is False
