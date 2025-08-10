import pytest
import tempfile
import os
from neosqlite import Connection


def test_database_property():
    """Test that the database property returns the correct database object."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create a connection
        with Connection(tmp_path) as conn:
            # Get a collection
            collection = conn["test_collection"]

            # Verify that the database property returns the connection
            assert collection.database is conn
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_database_property_multiple_collections():
    """Test that the database property works correctly with multiple collections."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create a connection
        with Connection(tmp_path) as conn:
            # Get multiple collections
            collection1 = conn["test_collection1"]
            collection2 = conn["test_collection2"]

            # Verify that both collections have the same database
            assert collection1.database is conn
            assert collection2.database is conn
            assert collection1.database is collection2.database
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_database_property_attribute_access():
    """Test that the database property works with attribute-style access."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create a connection
        with Connection(tmp_path) as conn:
            # Get a collection using attribute access
            collection = conn.test_collection

            # Verify that the database property returns the connection
            assert collection.database is conn
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
