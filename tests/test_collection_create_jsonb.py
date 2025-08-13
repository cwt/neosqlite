# coding: utf-8
"""
Direct tests for Collection.create() method JSONB detection logic.

These tests verify that the Collection.create() method correctly detects
JSONB support and creates tables with the appropriate column type.
"""
from unittest.mock import MagicMock, patch

import neosqlite

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


def test_collection_create_with_jsonb_support():
    """Test that Collection.create() uses JSONB when supported."""

    # Create a mock database connection
    mock_db = MagicMock()

    # Make the JSONB test succeed
    def execute_side_effect(query, *args, **kwargs):
        if "jsonb(" in query.lower():
            # Success - JSONB is supported
            return MagicMock()
        elif "CREATE TABLE" in query:
            # Capture the CREATE TABLE query
            mock_db.create_table_query = query
            return MagicMock()
        return MagicMock()

    mock_db.execute.side_effect = execute_side_effect

    # Create a Collection with our mock database
    collection = neosqlite.Collection(mock_db, "test_collection", create=False)

    # Call create method
    collection.create()

    # Verify that JSONB was used in the CREATE TABLE statement
    assert "data JSONB" in mock_db.create_table_query
    assert "data TEXT" not in mock_db.create_table_query


def test_collection_create_without_jsonb_support():
    """Test that Collection.create() falls back to TEXT when JSONB is not supported."""

    # Create a mock database connection
    mock_db = MagicMock()

    # Make the JSONB test fail with OperationalError
    def execute_side_effect(query, *args, **kwargs):
        if "jsonb(" in query.lower():
            # Fail - JSONB is not supported
            raise sqlite3.OperationalError("JSONB not supported")
        elif "CREATE TABLE" in query:
            # Capture the CREATE TABLE query
            mock_db.create_table_query = query
            return MagicMock()
        return MagicMock()

    mock_db.execute.side_effect = execute_side_effect

    # Create a Collection with our mock database
    collection = neosqlite.Collection(mock_db, "test_collection", create=False)

    # Call create method
    collection.create()

    # Verify that TEXT was used as fallback in the CREATE TABLE statement
    assert "data TEXT" in mock_db.create_table_query
    assert "data JSONB" not in mock_db.create_table_query


if __name__ == "__main__":
    test_collection_create_with_jsonb_support()
    test_collection_create_without_jsonb_support()
    print("All Collection.create() tests passed!")
