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
