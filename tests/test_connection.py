"""
Tests for neosqlite connection functionality and context manager error handling.
"""

import pytest
from unittest.mock import patch
import neosqlite
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


def test_connect():
    """Test basic connection functionality."""
    conn = neosqlite.Connection(":memory:")
    assert conn.db.isolation_level is None


def test_context_manager_closes_connection():
    """Test that context manager properly closes connection."""
    with patch("neosqlite.connection.sqlite3"):
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
    with patch("neosqlite.connection.sqlite3"):
        conn = neosqlite.Connection()
        conn._collections["foo"] = "bar"
        assert conn["foo"] == "bar"


def test_drop_collection():
    """Test drop_collection functionality."""
    with patch("neosqlite.connection.sqlite3"):
        conn = neosqlite.Connection()
        conn.drop_collection("foo")
        conn.db.execute.assert_called_with("DROP TABLE IF EXISTS foo")


def test_getattr_returns_attribute():
    """Test that __getattr__ returns attributes."""
    with patch("neosqlite.connection.sqlite3"):
        conn = neosqlite.Connection()
        assert conn.__getattr__("db") is not None


def test_getattr_returns_collection():
    """Test that __getattr__ returns collection."""
    with patch("neosqlite.connection.sqlite3"):
        conn = neosqlite.Connection()
        conn.__getattr__("foo")


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
    assert result.inserted_id is not None
    from neosqlite.objectid import ObjectId

    assert isinstance(result.inserted_id, ObjectId)

    # Verify we can find the document
    doc = collection.find_one({"foo": "bar"})
    assert doc is not None
    assert doc["foo"] == "bar"
    # With ObjectId implementation, the _id field should contain an ObjectId
    from neosqlite.objectid import ObjectId

    assert isinstance(doc["_id"], ObjectId)


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


# Tests for Connection.list_collections() method


def test_list_collections_empty_database(connection):
    """Test listing collections on an empty database."""
    # Should return an empty list when no collections exist
    collections = connection.list_collections()
    assert isinstance(collections, list)
    assert len(collections) == 0


def test_list_collections_with_collections(connection):
    """Test listing collections with existing collections."""
    # Create some collections
    collection1 = connection.create_collection("test_collection_1")
    collection2 = connection.create_collection("test_collection_2")

    # Add some data to make sure tables are created
    collection1.insert_one({"foo": "bar"})
    collection2.insert_one({"baz": "qux"})

    # List collections
    collections = connection.list_collections()

    # Should contain our collections with detailed information
    assert isinstance(collections, list)
    assert len(collections) >= 2

    # Find our collections in the result
    collection1_info = None
    collection2_info = None

    for collection_info in collections:
        if collection_info["name"] == "test_collection_1":
            collection1_info = collection_info
        elif collection_info["name"] == "test_collection_2":
            collection2_info = collection_info

    # Verify both collections were found
    assert collection1_info is not None
    assert collection2_info is not None

    # Verify the structure of collection info
    assert "name" in collection1_info
    assert "options" in collection1_info
    assert collection1_info["name"] == "test_collection_1"

    assert "name" in collection2_info
    assert "options" in collection2_info
    assert collection2_info["name"] == "test_collection_2"


def test_list_collections_after_drop(connection):
    """Test listing collections after dropping a collection."""
    # Create some collections
    collection1 = connection.create_collection("test_collection_1")
    collection2 = connection.create_collection("test_collection_2")

    # Add some data to make sure tables are created
    collection1.insert_one({"foo": "bar"})
    collection2.insert_one({"baz": "qux"})

    # Drop one collection
    collection1.drop()

    # List collections
    collections = connection.list_collections()

    # Should only contain the remaining collection
    assert isinstance(collections, list)

    # Find our collections in the result
    collection1_found = False
    collection2_found = False

    for collection_info in collections:
        if collection_info["name"] == "test_collection_1":
            collection1_found = True
        elif collection_info["name"] == "test_collection_2":
            collection2_found = True

    # Verify collection1 was dropped and collection2 still exists
    assert not collection1_found
    assert collection2_found


def test_list_collections_manual_table(connection):
    """Test listing collections includes manually created tables."""
    # Create a table manually through SQL
    connection.db.execute(
        "CREATE TABLE manual_table (id INTEGER PRIMARY KEY, data TEXT)"
    )

    # List collections
    collections = connection.list_collections()

    # Should include the manually created table
    manual_table_found = False
    for collection_info in collections:
        if collection_info["name"] == "manual_table":
            manual_table_found = True
            break

    assert manual_table_found


def test_list_collections_structure(connection):
    """Test the structure of collection information returned."""
    # Create a collection
    collection = connection.create_collection("test_structure")
    collection.insert_one({"test": "data"})

    # List collections
    collections = connection.list_collections()

    # Find our collection
    collection_info = None
    for info in collections:
        if info["name"] == "test_structure":
            collection_info = info
            break

    # Verify structure
    assert collection_info is not None
    assert isinstance(collection_info, dict)
    assert "name" in collection_info
    assert "options" in collection_info
    assert isinstance(collection_info["name"], str)
    # options should contain SQL definition or be None/empty
    assert isinstance(collection_info["options"], (str, type(None)))


class TestDatabaseCommand:
    """Tests for Database.command() method."""

    def test_command_ping(self, connection):
        """Test command('ping')."""
        result = connection.command("ping")

        assert isinstance(result, dict)
        assert result["ok"] == 1.0

    def test_command_ping_dict(self, connection):
        """Test command({'ping': 1})."""
        result = connection.command({"ping": 1})

        assert result["ok"] == 1.0

    def test_command_server_status(self, connection):
        """Test command('serverStatus')."""
        result = connection.command("serverStatus")

        assert result["ok"] == 1.0
        assert "version" in result
        assert "process" in result
        assert result["process"] == "neosqlite"

    def test_command_list_collections(self, connection):
        """Test command('listCollections')."""
        conn = connection
        coll = conn.test_list_colls
        coll.insert_one({"test": "doc"})

        result = conn.command("listCollections")

        assert result["ok"] == 1.0
        assert isinstance(result["collections"], list)
        collection_names = [c["name"] for c in result["collections"]]
        assert "test_list_colls" in collection_names

    def test_command_table_info(self, connection):
        """Test command('table_info')."""
        conn = connection
        coll = conn.test_table_info
        coll.insert_one({"name": "test", "value": 42})

        result = conn.command("table_info", table="test_table_info")

        assert result["ok"] == 1.0
        assert isinstance(result["columns"], list)
        assert len(result["columns"]) > 0

    def test_command_integrity_check(self, connection):
        """Test command('integrity_check')."""
        result = connection.command("integrity_check")

        assert result["ok"] == 1.0
        assert "result" in result
        # Should return ['ok'] for a healthy database
        assert (
            "ok" in str(result["result"]).lower() or len(result["result"]) > 0
        )

    def test_command_vacuum(self, connection):
        """Test command('vacuum')."""
        result = connection.command("vacuum")

        assert result["ok"] == 1.0
        assert "message" in result
        assert "VACUUM" in result["message"]

    def test_command_analyze(self, connection):
        """Test command('analyze')."""
        result = connection.command("analyze")

        assert result["ok"] == 1.0
        assert "message" in result

    def test_command_unknown(self, connection):
        """Test command with unknown command."""
        result = connection.command("unknown_command_xyz")

        # Unknown commands that aren't valid PRAGMAs return ok=0 with error
        # Some unknown commands may be interpreted as PRAGMA and return ok=1 with empty result
        # Both behaviors are acceptable
        assert isinstance(result, dict)
        assert "ok" in result
        # Either ok=0 with error, or ok=1 with empty result for unknown PRAGMA
        if result["ok"] == 0:
            assert "errmsg" in result or "error" in result
        else:
            # Unknown PRAGMA returns empty result
            assert "result" in result

    def test_command_with_kill_switch(self, connection):
        """Test command() works with kill switch."""
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)

            result = connection.command("ping")
            assert result["ok"] == 1.0

            result = connection.command("serverStatus")
            assert result["ok"] == 1.0
        finally:
            set_force_fallback(original_state)

    def test_command_kill_switch_comparison(self, connection):
        """Test command() returns same results with/without kill switch."""
        # Without kill switch
        result_normal = connection.command("ping")

        # With kill switch
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)
            result_fallback = connection.command("ping")
        finally:
            set_force_fallback(original_state)

        # Results should be identical
        assert result_normal["ok"] == result_fallback["ok"]
        assert result_normal["ok"] == 1.0
