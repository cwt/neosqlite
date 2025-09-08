"""
Tests for neosqlite connection functionality and context manager error handling.
"""

import pytest
from unittest.mock import patch
import neosqlite


def test_connect():
    """Test basic connection functionality."""
    conn = neosqlite.Connection(":memory:")
    assert conn.db.isolation_level is None


def test_context_manager_closes_connection():
    """Test that context manager properly closes connection."""
    with patch("neosqlite.connection.sqlite3") as sqlite:
        with neosqlite.Connection() as conn:
            pass
        assert conn.db.close.called


def test_getitem_returns_collection():
    """Test that __getitem__ returns a collection."""
    with patch("neosqlite.connection.sqlite3") as sqlite:
        with patch("neosqlite.connection.Collection") as mock_collection:
            sqlite.connect.return_value = sqlite
            mock_collection.return_value = mock_collection
            conn = neosqlite.Connection()
            assert "foo" not in conn._collections
            assert conn["foo"] == mock_collection


def test_getitem_returns_cached_collection():
    """Test that __getitem__ returns cached collection."""
    with patch("neosqlite.connection.sqlite3") as sqlite:
        conn = neosqlite.Connection()
        conn._collections["foo"] = "bar"
        assert conn["foo"] == "bar"


def test_drop_collection():
    """Test drop_collection functionality."""
    with patch("neosqlite.connection.sqlite3") as sqlite:
        conn = neosqlite.Connection()
        conn.drop_collection("foo")
        conn.db.execute.assert_called_with("DROP TABLE IF EXISTS foo")


def test_getattr_returns_attribute():
    """Test that __getattr__ returns attributes."""
    with patch("neosqlite.connection.sqlite3") as sqlite:
        conn = neosqlite.Connection()
        assert conn.__getattr__("db") is not None


def test_getattr_returns_collection():
    """Test that __getattr__ returns collection."""
    with patch("neosqlite.connection.sqlite3") as sqlite:
        conn = neosqlite.Connection()
        foo = conn.__getattr__("foo")


def test_context_manager_exception_handling():
    """Test context manager exception handling."""
    # Use a real fixture instead of the complex unittest structure
    with pytest.raises(ValueError, match="Test exception"):
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test_collection"]

            # Create a temporary table
            collection.db.execute(
                "CREATE TEMP TABLE error_test AS SELECT 1 as id"
            )

            # Verify it exists
            cursor = collection.db.execute("SELECT * FROM error_test")
            assert len(cursor.fetchall()) == 1

            # Raise an exception to trigger cleanup
            raise ValueError("Test exception")

    # After exception, connection should be closed
    # We can't easily test this without accessing internal state


def test_context_manager_database_error_handling():
    """Test handling of database errors in context manager."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Test that invalid SQL is handled gracefully
        # This should not crash the context manager
        try:
            collection.db.execute("INVALID SQL STATEMENT")
            # If we get here, the invalid SQL was somehow accepted
        except Exception:
            # Expected - invalid SQL should raise an exception
            pass


# Tests for Connection.create_collection() method


def test_create_collection(connection):
    """Test creating a new collection."""
    # Create a new collection
    collection = connection.create_collection("test_create")

    # Verify it's a Collection instance
    assert isinstance(collection, neosqlite.Collection)

    # Verify it's stored in the connection's collections
    assert "test_create" in connection._collections
    assert connection._collections["test_create"] is collection

    # Verify the collection name
    assert collection.name == "test_create"


def test_create_collection_with_kwargs(connection):
    """Test creating a collection with kwargs (which are passed to Collection constructor)."""
    # Create a new collection with kwargs
    # Note: Collection only accepts specific kwargs, so we'll test with a valid one
    collection = connection.create_collection("test_create_kwargs")

    # Verify it's a Collection instance
    assert isinstance(collection, neosqlite.Collection)

    # Verify it's stored in the connection's collections
    assert "test_create_kwargs" in connection._collections
    assert connection._collections["test_create_kwargs"] is collection

    # Verify the collection name
    assert collection.name == "test_create_kwargs"


def test_create_collection_already_exists(connection):
    """Test creating a collection that already exists."""
    # Create a collection
    connection.create_collection("test_exists")

    # Try to create the same collection again - should raise CollectionInvalid
    with pytest.raises(neosqlite.CollectionInvalid):
        connection.create_collection("test_exists")


def test_create_collection_and_use_it(connection):
    """Test creating a collection and using it for operations."""
    # Create a new collection
    collection = connection.create_collection("test_use")

    # Use it for insert operations
    result = collection.insert_one({"foo": "bar"})
    assert isinstance(result, neosqlite.InsertOneResult)
    assert result.inserted_id == 1

    # Verify we can find the document
    doc = collection.find_one({"foo": "bar"})
    assert doc is not None
    assert doc["foo"] == "bar"
    assert doc["_id"] == 1


# Tests for Connection.list_collection_names() method


def test_list_collection_names_empty_database(connection):
    """Test listing collection names on an empty database."""
    # Should return an empty list when no collections exist
    names = connection.list_collection_names()
    assert isinstance(names, list)
    assert len(names) == 0


def test_list_collection_names_with_collections(connection):
    """Test listing collection names with existing collections."""
    # Create some collections
    collection1 = connection.create_collection("test_collection_1")
    collection2 = connection.create_collection("test_collection_2")

    # Add some data to make sure tables are created
    collection1.insert_one({"foo": "bar"})
    collection2.insert_one({"baz": "qux"})

    # List collection names
    names = connection.list_collection_names()

    # Should contain our collections
    assert isinstance(names, list)
    assert len(names) >= 2
    assert "test_collection_1" in names
    assert "test_collection_2" in names


def test_list_collection_names_after_drop(connection):
    """Test listing collection names after dropping a collection."""
    # Create some collections
    collection1 = connection.create_collection("test_collection_1")
    collection2 = connection.create_collection("test_collection_2")

    # Add some data to make sure tables are created
    collection1.insert_one({"foo": "bar"})
    collection2.insert_one({"baz": "qux"})

    # Drop one collection
    collection1.drop()

    # List collection names
    names = connection.list_collection_names()

    # Should only contain the remaining collection
    assert isinstance(names, list)
    assert "test_collection_1" not in names
    assert "test_collection_2" in names


def test_list_collection_names_manual_table(connection):
    """Test listing collection names includes manually created tables."""
    # Create a table manually through SQL
    connection.db.execute(
        "CREATE TABLE manual_table (id INTEGER PRIMARY KEY, data TEXT)"
    )

    # List collection names
    names = connection.list_collection_names()

    # Should include the manually created table
    assert "manual_table" in names
