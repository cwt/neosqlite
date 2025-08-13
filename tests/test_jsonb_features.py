# coding: utf-8
"""
Tests for JSONB features in neosqlite.

These tests verify that the JSONB functionality works correctly
when pysqlite3-binary is available.
"""
import pytest

# Try to import pysqlite3 for consistent JSON/JSONB support
try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


import neosqlite


@pytest.fixture
def connection():
    """Fixture to set up and tear down a neosqlite connection."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


def test_jsonb_table_creation():
    """Test that tables are created with JSONB column when supported."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_jsonb_collection"]

        # Check the table schema
        cursor = collection.db.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (collection.name,),
        )
        row = cursor.fetchone()
        table_sql = row[0] if row else ""

        # Print the table schema for debugging
        print(f"Table schema: {table_sql}")

        # Check if JSONB is supported in this SQLite installation
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        # If JSONB is supported, the column should be JSONB
        # Otherwise, it should be TEXT
        if jsonb_supported:
            assert "data JSONB" in table_sql
            print("JSONB column correctly created")
        else:
            assert "data TEXT" in table_sql
            print("TEXT column correctly created as fallback")


def test_jsonb_operations():
    """Test JSONB operations with enhanced SQLite support."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_operations_test"]

        # Verify that JSONB is actually supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert a document with nested data
        doc = {
            "name": "Test User",
            "profile": {
                "age": 30,
                "settings": {"theme": "dark", "notifications": True},
            },
            "tags": ["python", "sqlite", "jsonb"],
        }

        result = collection.insert_one(doc)
        assert result.inserted_id == 1

        # Retrieve and verify the document
        found_doc = collection.find_one({"_id": 1})
        assert found_doc["name"] == "Test User"
        assert found_doc["profile"]["age"] == 30
        assert found_doc["profile"]["settings"]["theme"] == "dark"
        assert found_doc["tags"] == ["python", "sqlite", "jsonb"]

        # Test updates
        collection.update_one(
            {"name": "Test User"}, {"$set": {"profile.settings.theme": "light"}}
        )

        updated_doc = collection.find_one({"_id": 1})
        assert updated_doc["profile"]["settings"]["theme"] == "light"


def test_jsonb_query_operations():
    """Test querying JSONB data."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_query_test"]

        # Verify that JSONB is supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert test documents
        docs = [
            {
                "name": "Alice",
                "age": 25,
                "city": "New York",
                "skills": ["Python", "SQL"],
            },
            {
                "name": "Bob",
                "age": 30,
                "city": "San Francisco",
                "skills": ["JavaScript", "HTML"],
            },
            {
                "name": "Charlie",
                "age": 35,
                "city": "New York",
                "skills": ["Python", "JSONB"],
            },
        ]

        collection.insert_many(docs)

        # Test simple queries
        assert collection.count_documents({"city": "New York"}) == 2
        assert collection.count_documents({"age": {"$gt": 30}}) == 1

        # Test find_one
        user = collection.find_one({"name": "Alice"})
        assert user["age"] == 25
        assert "Python" in user["skills"]


def test_jsonb_indexing():
    """Test JSONB indexing capabilities."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_index_test"]

        # Verify that JSONB is supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert test documents
        docs = [
            {
                "name": "Alice",
                "profile": {"age": 25, "department": "Engineering"},
            },
            {"name": "Bob", "profile": {"age": 30, "department": "Marketing"}},
            {
                "name": "Charlie",
                "profile": {"age": 35, "department": "Engineering"},
            },
        ]

        collection.insert_many(docs)

        # Create an index on a nested field
        collection.create_index("profile.department")

        # Verify the index was created
        indexes = collection.list_indexes()
        assert any("profile_department" in idx for idx in indexes)

        # Test querying with the index
        engineers = list(collection.find({"profile.department": "Engineering"}))
        assert len(engineers) == 2


if __name__ == "__main__":
    # Run a simple test to check JSONB support
    with neosqlite.Connection(":memory:") as conn:
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            print("JSONB support is available")
        except sqlite3.OperationalError:
            print("JSONB support is not available")

        try:
            conn.db.execute('SELECT json(\'{"key": "value"}\')')
            print("JSON support is available")
        except sqlite3.OperationalError:
            print("JSON support is not available")
