# coding: utf-8
import neosqlite


def test_exists_operator_sql_generation():
    """Test that $exists operator generates proper SQL."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Test $exists: True
        result = collection.query_engine.helpers._build_simple_where_clause(
            {"tags": {"$exists": True}}
        )
        assert result is not None
        sql, params = result
        assert "json_extract(data, '$.tags') IS NOT NULL" in sql
        assert params == []

        # Test $exists: False
        result = collection.query_engine.helpers._build_simple_where_clause(
            {"tags": {"$exists": False}}
        )
        assert result is not None
        sql, params = result
        assert "json_extract(data, '$.tags') IS NULL" in sql
        assert params == []


def test_mod_operator_sql_generation():
    """Test that $mod operator generates proper SQL."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Test $mod with [divisor, remainder]
        result = collection.query_engine.helpers._build_simple_where_clause(
            {"age": {"$mod": [2, 1]}}
        )
        assert result is not None
        sql, params = result
        assert "json_extract(data, '$.age') % ? = ?" in sql
        assert params == [2, 1]


def test_size_operator_sql_generation():
    """Test that $size operator generates proper SQL."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Test $size with integer
        result = collection.query_engine.helpers._build_simple_where_clause(
            {"tags": {"$size": 2}}
        )
        assert result is not None
        sql, params = result
        assert "json_array_length(json_extract(data, '$.tags')) = ?" in sql
        assert params == [2]


def test_exists_operator_functionality():
    """Test that $exists operator works correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "sql"]},
                {"name": "Bob", "age": 30},  # No tags field
            ]
        )

        # Test $exists: True
        results = list(collection.find({"tags": {"$exists": True}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

        # Test $exists: False
        results = list(collection.find({"tags": {"$exists": False}}))
        assert len(results) == 1
        assert results[0]["name"] == "Bob"


def test_mod_operator_functionality():
    """Test that $mod operator works correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 25},  # Odd
                {"name": "Bob", "age": 30},  # Even
                {"name": "Charlie", "age": 35},  # Odd
            ]
        )

        # Test odd ages
        results = list(collection.find({"age": {"$mod": [2, 1]}}))
        assert len(results) == 2
        names = {doc["name"] for doc in results}
        assert names == {"Alice", "Charlie"}

        # Test even ages
        results = list(collection.find({"age": {"$mod": [2, 0]}}))
        assert len(results) == 1
        assert results[0]["name"] == "Bob"


def test_size_operator_functionality():
    """Test that $size operator works correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "sql"]},  # 2 tags
                {
                    "name": "Bob",
                    "tags": ["javascript", "html", "css"],
                },  # 3 tags
                {"name": "Charlie", "tags": ["go"]},  # 1 tag
            ]
        )

        # Test documents with exactly 2 tags
        results = list(collection.find({"tags": {"$size": 2}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

        # Test documents with exactly 1 tag
        results = list(collection.find({"tags": {"$size": 1}}))
        assert len(results) == 1
        assert results[0]["name"] == "Charlie"
