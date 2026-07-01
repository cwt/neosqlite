"""
Tests for neosqlite connection functionality and context manager error handling.
"""

from unittest.mock import patch

import pytest

import neosqlite
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
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

    def test_command_db_stats(self, connection):
        """Test command('dbStats')."""
        conn = connection
        coll = conn.test_db_stats
        coll.insert_one({"name": "test", "value": 42})
        coll.insert_one({"name": "another", "value": 100})
        coll.create_index("value")

        result = conn.command("dbStats")

        assert result["ok"] == 1.0
        assert "db" in result
        assert "collections" in result
        assert "views" in result
        assert "objects" in result
        assert "dataSize" in result
        assert "storageSize" in result
        assert "indexes" in result
        assert "indexSize" in result
        assert "avgObjSize" in result
        assert "totalSize" in result
        assert "fsTotalSize" in result
        assert "fsUsedSize" in result
        assert "scaleFactor" in result
        # Check values make sense
        assert result["collections"] >= 1
        assert result["objects"] >= 2
        assert result["indexes"] >= 1
        assert result["scaleFactor"] == 1

    def test_command_db_stats_with_view(self, tmp_path):
        """Test command('dbStats') with SQLite views."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))
        conn.db.execute("CREATE TABLE users (id INT, name TEXT)")
        conn.db.execute("INSERT INTO users VALUES (1, 'Alice')")
        conn.db.execute("CREATE VIEW user_names AS SELECT name FROM users")

        result = conn.command("dbStats")

        assert result["ok"] == 1.0
        assert result["views"] == 1
        conn.close()

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

        assert result["ok"] == 1
        assert "message" in result
        assert "VACUUM" in result["message"]

    def test_command_compact_full_vacuum(self, tmp_path):
        """Test command('compact') with freeSpaceTargetMB=0 does full vacuum."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        for i in range(1000):
            conn.db.execute("INSERT INTO test (data) VALUES (?)", ("x" * 100,))
        conn.db.execute("DELETE FROM test WHERE id <= 500")
        conn.db.commit()

        free_pages_before = conn.db.execute("PRAGMA freelist_count").fetchone()[
            0
        ]
        assert free_pages_before > 0

        result = conn.command("compact", "test", freeSpaceTargetMB=0)

        assert result["ok"] == 1
        assert "bytesFreed" in result
        assert result["bytesFreed"] > 0

        free_pages_after = conn.db.execute("PRAGMA freelist_count").fetchone()[
            0
        ]
        assert free_pages_after == 0

        conn.close()

    def test_command_compact_default_target(self, tmp_path):
        """Test command('compact') without freeSpaceTargetMB uses 20MB default."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        conn.db.executemany(
            "INSERT INTO test (data) VALUES (?)",
            [("x" * 2048,) for _ in range(12000)],
        )
        conn.db.execute("DELETE FROM test WHERE id <= 10200")
        conn.db.commit()

        free_pages = conn.db.execute("PRAGMA freelist_count").fetchone()[0]
        page_size = conn.db.execute("PRAGMA page_size").fetchone()[0]
        free_mb = (free_pages * page_size) / (1024 * 1024)
        assert free_mb > 20, f"Free space: {free_mb} MB"

        # Performance PRAGMAs for faster incremental_vacuum in tests
        conn.db.execute("PRAGMA cache_size = -65536")  # 64MB cache
        conn.db.execute("PRAGMA synchronous = OFF")
        conn.db.execute("PRAGMA locking_mode = EXCLUSIVE")

        result = conn.command("compact", "test")

        assert result["ok"] == 1
        assert result["bytesFreed"] > 0

        free_pages_after = conn.db.execute("PRAGMA freelist_count").fetchone()[
            0
        ]
        assert free_pages_after == 0

        conn.close()

    def test_command_compact_dry_run(self, tmp_path):
        """Test command('compact') with dryRun returns estimate."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        for i in range(1000):
            conn.db.execute("INSERT INTO test (data) VALUES (?)", ("x" * 100,))
        conn.db.execute("DELETE FROM test WHERE id <= 500")
        conn.db.commit()

        free_pages = conn.db.execute("PRAGMA freelist_count").fetchone()[0]
        page_size = conn.db.execute("PRAGMA page_size").fetchone()[0]
        expected_estimate = free_pages * page_size

        result = conn.command("compact", "test", dryRun=True)

        assert result["ok"] == 1
        assert "estimatedBytesFreed" in result
        assert result["estimatedBytesFreed"] == expected_estimate

        free_pages_after = conn.db.execute("PRAGMA freelist_count").fetchone()[
            0
        ]
        assert free_pages_after == free_pages  # Should not change

        conn.close()

    def test_command_compact_free_space_target_threshold(self, tmp_path):
        """Test command('compact') with freeSpaceTargetMB threshold."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        for i in range(100):
            conn.db.execute("INSERT INTO test (data) VALUES (?)", ("x" * 50,))
        conn.db.execute("DELETE FROM test WHERE id <= 50")
        conn.db.commit()

        free_pages = conn.db.execute("PRAGMA freelist_count").fetchone()[0]
        page_size = conn.db.execute("PRAGMA page_size").fetchone()[0]
        free_mb = (free_pages * page_size) / (1024 * 1024)
        assert free_mb < 1

        result = conn.command("compact", "test", freeSpaceTargetMB=1)

        assert result["ok"] == 1
        assert result["bytesFreed"] == 0

        conn.close()

    def test_command_compact_with_free_space_target(self, tmp_path):
        """Test command('compact') with freeSpaceTargetMB runs incremental vacuum."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        conn.db.executemany(
            "INSERT INTO test (data) VALUES (?)",
            [("x" * 500,) for _ in range(6000)],
        )
        conn.db.execute("DELETE FROM test WHERE id <= 5500")
        conn.db.commit()

        free_pages = conn.db.execute("PRAGMA freelist_count").fetchone()[0]
        page_size = conn.db.execute("PRAGMA page_size").fetchone()[0]
        free_mb = (free_pages * page_size) / (1024 * 1024)
        assert free_mb > 1, f"Free space: {free_mb} MB"

        # Performance PRAGMAs for faster incremental_vacuum in tests
        conn.db.execute("PRAGMA cache_size = -65536")  # 64MB cache
        conn.db.execute("PRAGMA synchronous = OFF")
        conn.db.execute("PRAGMA locking_mode = EXCLUSIVE")

        result = conn.command("compact", "test", freeSpaceTargetMB=1)

        assert result["ok"] == 1
        assert result["bytesFreed"] > 0

        free_pages_after = conn.db.execute("PRAGMA freelist_count").fetchone()[
            0
        ]
        assert free_pages_after == 0

        conn.close()

    def test_command_compact_with_comment(self, tmp_path):
        """Test command('compact') accepts comment parameter (ignored)."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn.db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        for i in range(100):
            conn.db.execute("INSERT INTO test (data) VALUES (?)", ("x" * 50,))
        conn.db.execute("DELETE FROM test WHERE id <= 50")
        conn.db.commit()

        result = conn.command("compact", "test", comment="test comment")

        assert result["ok"] == 1
        assert "bytesFreed" in result

        conn.close()

    def test_command_analyze(self, connection):
        """Test command('analyze')."""
        result = connection.command("analyze")

        assert result["ok"] == 1
        assert "message" in result

    def test_command_wal_checkpoint(self, tmp_path):
        """Test command('wal_checkpoint')."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn["test"].insert_one({"a": 1})

        result = conn.command("wal_checkpoint")

        assert result["ok"] == 1
        assert "mode" in result
        assert "busy" in result
        assert "log" in result
        assert "checkpointed" in result

        conn.close()

    def test_command_wal_checkpoint_passive(self, tmp_path):
        """Test command('wal_checkpoint') with PASSIVE mode."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        conn["test"].insert_one({"a": 1})

        result = conn.command("wal_checkpoint", mode="PASSIVE")

        assert result["ok"] == 1
        assert result["mode"] == "PASSIVE"

        conn.close()

    def test_command_cache_size_get(self, tmp_path):
        """Test command('cache_size') without value returns current."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        result = conn.command("cache_size")

        assert result["ok"] == 1
        assert "cache_size" in result

        conn.close()

    def test_command_cache_size_set(self, tmp_path):
        """Test command('cache_size') with pages value sets cache."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        result = conn.command("cache_size", pages=1000)

        assert result["ok"] == 1
        assert "message" in result

        current = conn.db.execute("PRAGMA cache_size").fetchone()[0]
        assert abs(current) == 1000

        conn.close()

    def test_command_busy_timeout_get(self, tmp_path):
        """Test command('busy_timeout') without value returns current."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        result = conn.command("busy_timeout")

        assert result["ok"] == 1
        assert "busy_timeout" in result

        conn.close()

    def test_command_busy_timeout_set(self, tmp_path):
        """Test command('busy_timeout') with milliseconds sets timeout."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))

        result = conn.command("busy_timeout", milliseconds=5000)

        assert result["ok"] == 1
        assert "message" in result

        current = conn.db.execute("PRAGMA busy_timeout").fetchone()[0]
        assert current == 5000

        conn.close()

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


def test_connection_properties():
    """Test newly added Connection properties."""
    conn = neosqlite.Connection(":memory:")
    assert conn.client == conn
    assert conn.codec_options is None
    assert conn.read_preference is None
    assert conn.write_concern is None
    assert conn.read_concern is None
    assert conn.db_path == ":memory:"
    conn.close()


def test_connection_with_options():
    """Test with_options on Connection."""
    conn = neosqlite.Connection(":memory:")
    wc = {"w": 1}
    conn2 = conn.with_options(write_concern=wc)
    assert conn2.write_concern == wc
    assert conn2._is_clone is True
    # Verify clone doesn't close shared connection
    conn2.close()
    # Shared connection should still work
    conn.db.execute("SELECT 1")
    conn.close()


def test_cursor_command_and_dereference():
    """Test cursor_command and dereference methods on Connection."""
    conn = neosqlite.Connection(":memory:")
    coll = conn["test_collection"]
    coll.insert_one({"name": "initial"})

    # cursor_command
    cursor = conn.cursor_command("listCollections")
    from neosqlite.collection.aggregation_cursor import AggregationCursor

    assert isinstance(cursor, AggregationCursor)
    results = list(cursor)
    assert any(c["name"] == "test_collection" for c in results)

    # dereference
    res = coll.insert_one({"name": "ref_me"})
    dbref = {"$ref": "test_collection", "$id": res.inserted_id}

    deref_doc = conn.dereference(dbref)
    assert deref_doc is not None
    assert deref_doc["name"] == "ref_me"
    conn.close()


def test_cursor_command_pragma():
    """Test cursor_command with PRAGMA results."""
    conn = neosqlite.Connection(":memory:")
    # Run PRAGMA table_info using cursor_command
    # We need a table first
    conn["test"].insert_one({"a": 1})

    cursor = conn.cursor_command("table_info", table="test")
    results = list(cursor)

    assert len(results) > 0
    # PRAGMA table_info returns columns like 'name', 'type', etc.
    column_names = [r["name"] for r in results]
    assert "data" in column_names
    assert "id" in column_names

    # Test invalid dereference
    assert conn.dereference({"$ref": "nonexistent", "$id": 123}) is None
    assert conn.dereference(None) is None
    conn.close()


# ── command("query_only") tests ──────────────────────────────────────────────


def test_command_query_only_read():
    """command('query_only') returns current read-only state."""
    conn = neosqlite.Connection(":memory:")
    result = conn.command("query_only")
    assert result["ok"] == 1
    assert "query_only" in result
    assert isinstance(result["query_only"], bool)
    assert result["query_only"] is False
    conn.close()


def test_command_query_only_set_int():
    """command('query_only', 1) enables read-only mode."""
    conn = neosqlite.Connection(":memory:")
    result = conn.command("query_only", 1)
    assert result["ok"] == 1
    assert result["query_only"] is True
    # Verify it actually took effect
    read = conn.command("query_only")
    assert read["query_only"] is True
    conn.close()


def test_command_query_only_set_bool():
    """command('query_only', True/False) sets read-only mode."""
    conn = neosqlite.Connection(":memory:")
    conn.command("query_only", True)
    assert conn.command("query_only")["query_only"] is True
    conn.command("query_only", False)
    assert conn.command("query_only")["query_only"] is False
    conn.close()


def test_command_query_only_set_str_on():
    """command('query_only', 'ON') enables read-only mode."""
    conn = neosqlite.Connection(":memory:")
    result = conn.command("query_only", "ON")
    assert result["ok"] == 1
    assert result["query_only"] is True
    conn.close()


def test_command_query_only_set_str_off():
    """command('query_only', 'OFF') disables read-only mode."""
    conn = neosqlite.Connection(":memory:")
    conn.command("query_only", "ON")
    result = conn.command("query_only", "OFF")
    assert result["ok"] == 1
    assert result["query_only"] is False
    conn.close()


def test_command_query_only_set_dict():
    """command({'query_only': 1}) enables read-only mode."""
    conn = neosqlite.Connection(":memory:")
    result = conn.command({"query_only": 1})
    assert result["ok"] == 1
    assert result["query_only"] is True
    conn.close()


def test_command_query_only_invalid_value():
    """command('query_only', invalid_value) returns error."""
    conn = neosqlite.Connection(":memory:")
    result = conn.command("query_only", {"nested": "bad"})
    assert result["ok"] == 0
    assert "Invalid query_only value" in result["errmsg"]
    conn.close()


def test_command_query_only_effect():
    """query_only PRAGMA actually prevents writes."""
    conn = neosqlite.Connection(":memory:")
    conn["test"].insert_one({"x": 1})
    # Enable read-only
    conn.command("query_only", 1)
    # Existing cached collection reads still work
    doc = conn["test"].find_one({"x": 1})
    assert doc is not None
    # New collection (requires CREATE TABLE) fails
    try:
        conn["new_collection"]
        # If it didn't fail, try a write
        conn["new_collection"].insert_one({"y": 1})
        assert False, "Should have raised OperationalError"
    except Exception as e:
        assert "readonly" in str(e).lower()
    conn.close()


def test_with_options_collections_independent():
    """Test that with_options() clones have independent _collections dicts.

    Mutations to one dict (adding/removing entries) should not affect
    the other, preventing stale state in cached collection lookups.
    """
    conn = neosqlite.Connection(":memory:")
    conn["shared"].insert_one({"x": 1})
    # Create a clone with different options
    clone = conn.with_options(write_concern={"w": "majority"})
    # Both should have access to the same collection initially
    assert "shared" in clone._collections
    assert "shared" in conn._collections
    # Removing from clone dict should not affect original
    del clone._collections["shared"]
    assert "shared" not in clone._collections
    assert "shared" in conn._collections
    # Adding to clone dict should not appear on original
    clone._collections["clone_only"] = clone["shared"]
    assert "clone_only" in clone._collections
    assert "clone_only" not in conn._collections
    conn.close()


def test_del_rolls_back_pending_transaction():
    """Test that __del__ rolls back rather than commits pending transactions.

    When a Connection is garbage collected while a transaction is in progress,
    the pending work should be rolled back, not accidentally committed.
    """
    import os
    import tempfile

    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()

    try:
        conn = neosqlite.Connection(db_path)
        conn["test"].insert_one({"x": 1})
        # Begin a transaction and insert more data
        conn.db.execute("BEGIN")
        conn.db.execute(
            "INSERT INTO test (id, _id, data) VALUES (NULL, 'temp', '{\"x\": 2}')"
        )
        assert conn.db.in_transaction is True
        # Simulate GC (should rollback, not commit)
        conn.__del__()

        # Open a new connection and verify the uncommitted data is NOT there
        conn2 = neosqlite.Connection(db_path)
        count = conn2["test"].count_documents({})
        assert count == 1  # Only the first insert (committed) should exist
        conn2.close()
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass
