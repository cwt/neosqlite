# coding: utf-8
import neosqlite


def test_options_empty_collection():
    """Test options() on an empty collection."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    options = collection.options()

    # Verify basic options
    assert "name" in options
    assert options["name"] == "test"

    # Verify columns info
    assert "columns" in options
    columns = options["columns"]
    assert len(columns) == 2  # id and data columns
    assert any(
        col["name"] == "id" and col["type"] == "INTEGER" for col in columns
    )
    # The data column type can be either TEXT (fallback) or JSONB (enhanced)
    assert any(
        col["name"] == "data" and col["type"] in ("TEXT", "JSONB")
        for col in columns
    )

    # Verify count
    assert "count" in options
    assert options["count"] == 0


def test_options_with_data():
    """Test options() on a collection with data."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some data
    collection.insert_many(
        [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    )

    # Create an index
    collection.create_index("name")

    options = collection.options()

    # Verify basic options
    assert "name" in options
    assert options["name"] == "test"

    # Verify count
    assert "count" in options
    assert options["count"] == 2

    # Verify columns info
    assert "columns" in options
    columns = options["columns"]
    assert len(columns) == 2  # id and data columns

    # Verify indexes info
    assert "indexes" in options
    indexes = options["indexes"]
    assert len(indexes) >= 1  # At least our created index
    assert any("name" in idx["name"] for idx in indexes)


def test_options_with_multiple_indexes():
    """Test options() with multiple indexes."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create multiple indexes
    collection.create_index("name")
    collection.create_index("age")
    collection.create_index(["name", "age"])

    options = collection.options()

    # Verify indexes info
    assert "indexes" in options
    indexes = options["indexes"]
    assert len(indexes) >= 3  # At least our created indexes

    # Check that all our indexes are present
    index_names = [idx["name"] for idx in indexes]
    assert any("name" in name for name in index_names)
    assert any("age" in name for name in index_names)
